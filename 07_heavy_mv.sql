-- =====================================================================
-- HEAVY MATERIALIZED VIEWS on Olist  (staging -> mart)
-- Heavy = multi-table joins + aggregation + window functions + a 1M-row table.
-- These sit in a "mart" layer on top of staging (later: on top of dw).
--
-- Prerequisites: staging tables loaded (CALL staging.run_etl();)
--   plus raw.order_payments and raw.geolocation (standard Olist names assumed:
--   payment_value, payment_type, payment_installments,
--   geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
--
-- Layering: mv_order_summary is the BASE. mv_customer_rfm and mv_cohort_retention
-- read from it (MV on MV), so REFRESH ORDER MATTERS.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- Indexes on the underlying tables make every heavy MV build faster
CREATE INDEX IF NOT EXISTS ix_items_product ON staging.order_items (product_id);
CREATE INDEX IF NOT EXISTS ix_items_seller  ON staging.order_items (seller_id);
CREATE INDEX IF NOT EXISTS ix_orders_cust   ON staging.orders (customer_id);

DROP MATERIALIZED VIEW IF EXISTS mart.mv_order_summary CASCADE;   -- also drops dependents
DROP MATERIALIZED VIEW IF EXISTS mart.mv_monthly_category_revenue;
DROP MATERIALIZED VIEW IF EXISTS mart.mv_seller_scorecard;
DROP MATERIALIZED VIEW IF EXISTS mart.mv_delivery_distance;

-- ---------------------------------------------------------------------
-- 1. BASE: one row per order, joined from 5 sources
--    Teaching point: aggregate each child table BEFORE joining (CTEs).
--    Joining orders to items AND payments directly would multiply rows
--    (join fan-out) and double count amounts.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW mart.mv_order_summary AS
WITH items AS (
    SELECT order_id,
           COUNT(*)                   AS item_count,
           COUNT(DISTINCT seller_id)  AS seller_count,
           SUM(price)                 AS items_amount,
           SUM(freight_value)         AS freight_amount
    FROM staging.order_items
    GROUP BY order_id
),
pay AS (
    SELECT order_id,
           SUM(payment_value::NUMERIC)        AS paid_amount,
           COUNT(DISTINCT payment_type)       AS payment_methods,
           MAX(payment_installments::INT)     AS max_installments
    FROM raw.order_payments
    GROUP BY order_id
)
SELECT o.order_id,
       o.customer_id,
       c.customer_unique_id,
       c.state                                          AS customer_state,
       o.order_status,
       o.purchase_ts,
       date_trunc('month', o.purchase_ts)::DATE         AS purchase_month,
       o.delivery_days,
       o.is_late,
       COALESCE(i.item_count, 0)                        AS item_count,
       COALESCE(i.seller_count, 0)                      AS seller_count,
       COALESCE(i.items_amount, 0)                      AS items_amount,
       COALESCE(i.freight_amount, 0)                    AS freight_amount,
       COALESCE(i.items_amount, 0) + COALESCE(i.freight_amount, 0) AS order_total,
       p.paid_amount,
       p.payment_methods,
       p.max_installments,
       r.review_score
FROM staging.orders o
JOIN      staging.customers c ON c.customer_id = o.customer_id
LEFT JOIN items i             ON i.order_id     = o.order_id
LEFT JOIN pay   p             ON p.order_id     = o.order_id
LEFT JOIN staging.reviews r   ON r.order_id     = o.order_id;

CREATE UNIQUE INDEX ux_mv_order_summary ON mart.mv_order_summary (order_id);
CREATE INDEX ix_mv_order_summary_month ON mart.mv_order_summary (purchase_month);

-- ---------------------------------------------------------------------
-- 2. Monthly revenue per category with window functions
--    LAG = previous month, RANK = position in the month, running SUM = cumulative
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW mart.mv_monthly_category_revenue AS
WITH base AS (
    SELECT date_trunc('month', o.purchase_ts)::DATE      AS month,
           COALESCE(pr.category_en, 'unknown')           AS category,
           COUNT(DISTINCT o.order_id)                    AS orders,
           COUNT(*)                                      AS items_sold,
           SUM(i.price)                                  AS revenue,
           SUM(i.freight_value)                          AS freight,
           ROUND(AVG(r.review_score), 2)                 AS avg_review,   -- per item row
           ROUND(100.0 * AVG(CASE WHEN o.is_late IS NULL THEN NULL
                                  WHEN o.is_late THEN 1 ELSE 0 END), 1) AS late_pct
    FROM staging.order_items i
    JOIN      staging.orders   o  ON o.order_id   = i.order_id
    LEFT JOIN staging.products pr ON pr.product_id = i.product_id
    LEFT JOIN staging.reviews  r  ON r.order_id   = o.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1, 2
)
SELECT b.*,
       LAG(revenue) OVER w_cat                                        AS prev_month_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER w_cat)
             / NULLIF(LAG(revenue) OVER w_cat, 0), 1)                 AS mom_growth_pct,
       RANK() OVER (PARTITION BY month ORDER BY revenue DESC)         AS rank_in_month,
       SUM(revenue) OVER (PARTITION BY category ORDER BY month)       AS running_revenue
FROM base b
WINDOW w_cat AS (PARTITION BY category ORDER BY month);

CREATE UNIQUE INDEX ux_mv_monthly_cat ON mart.mv_monthly_category_revenue (month, category);

-- ---------------------------------------------------------------------
-- 3. Seller scorecard: per-seller KPIs + percentile rank + tier
--    (tier thresholds are illustrative, tune them in class)
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW mart.mv_seller_scorecard AS
WITH seller_orders AS (
    SELECT i.seller_id, o.order_id, o.is_late, o.delivery_days, o.order_status,
           r.review_score,
           SUM(i.price) AS order_revenue            -- this seller's part of the order
    FROM staging.order_items i
    JOIN      staging.orders  o ON o.order_id = i.order_id
    LEFT JOIN staging.reviews r ON r.order_id = o.order_id
    GROUP BY i.seller_id, o.order_id, o.is_late, o.delivery_days, o.order_status, r.review_score
),
agg AS (
    SELECT seller_id,
           COUNT(*)                                   AS orders,
           SUM(order_revenue)                         AS revenue,
           ROUND(AVG(review_score), 2)                AS avg_review,
           ROUND(AVG(delivery_days), 1)               AS avg_delivery_days,
           ROUND(AVG(CASE WHEN is_late IS NULL THEN NULL
                          WHEN is_late THEN 1.0 ELSE 0 END), 3)   AS late_rate,
           ROUND(AVG(CASE WHEN order_status = 'canceled' THEN 1.0 ELSE 0 END), 3) AS cancel_rate
    FROM seller_orders
    GROUP BY seller_id
)
SELECT a.seller_id, s.city, s.state,
       a.orders, a.revenue, a.avg_review, a.avg_delivery_days, a.late_rate, a.cancel_rate,
       ROUND(PERCENT_RANK() OVER (ORDER BY a.revenue)::NUMERIC, 3)  AS revenue_pct_rank,
       NTILE(10) OVER (ORDER BY a.revenue DESC)                     AS revenue_decile,
       CASE WHEN a.orders >= 30 AND a.avg_review >= 4.3 AND a.late_rate <= 0.05 THEN 'A'
            WHEN a.orders >= 10 AND a.avg_review >= 4.0                         THEN 'B'
            ELSE 'C' END                                            AS tier
FROM agg a
LEFT JOIN staging.sellers s ON s.seller_id = a.seller_id;

CREATE UNIQUE INDEX ux_mv_seller_scorecard ON mart.mv_seller_scorecard (seller_id);

-- ---------------------------------------------------------------------
-- 4. Customer RFM (Recency, Frequency, Monetary)  -- MV on MV
--    Reference date = last date IN THE DATA, not today (dataset is historical).
--    Frequency uses LEAST(...,5), because most Olist customers ordered once and
--    NTILE would split those ties arbitrarily.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW mart.mv_customer_rfm AS
WITH cust AS (
    SELECT customer_unique_id,
           MAX(purchase_ts)::DATE  AS last_purchase,
           COUNT(*)                AS frequency,
           SUM(items_amount)       AS monetary
    FROM mart.mv_order_summary
    WHERE order_status NOT IN ('canceled', 'unavailable')
    GROUP BY customer_unique_id
),
ref AS (SELECT MAX(last_purchase) AS ref_date FROM cust)
SELECT c.customer_unique_id,
       c.last_purchase,
       (ref.ref_date - c.last_purchase)                                   AS recency_days,
       c.frequency,
       c.monetary,
       NTILE(5) OVER (ORDER BY (ref.ref_date - c.last_purchase) DESC)     AS r_score,  -- 5 = most recent
       LEAST(c.frequency, 5)                                              AS f_score,
       NTILE(5) OVER (ORDER BY c.monetary)                                AS m_score,  -- 5 = biggest spenders
       CASE WHEN NTILE(5) OVER (ORDER BY (ref.ref_date - c.last_purchase) DESC) >= 4
                 AND NTILE(5) OVER (ORDER BY c.monetary) >= 4 THEN 'Champion'
            WHEN NTILE(5) OVER (ORDER BY (ref.ref_date - c.last_purchase) DESC) >= 4 THEN 'Recent'
            WHEN NTILE(5) OVER (ORDER BY c.monetary) >= 4                           THEN 'Big spender at risk'
            ELSE 'Other' END                                              AS segment
FROM cust c
CROSS JOIN ref;

CREATE UNIQUE INDEX ux_mv_customer_rfm ON mart.mv_customer_rfm (customer_unique_id);

-- ---------------------------------------------------------------------
-- 5. Cohort retention: of customers whose first order was in month X,
--    how many ordered again N months later?   -- MV on MV
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW mart.mv_cohort_retention AS
WITH first_buy AS (
    SELECT customer_unique_id,
           date_trunc('month', MIN(purchase_ts))::DATE AS cohort_month
    FROM mart.mv_order_summary
    GROUP BY customer_unique_id
),
activity AS (
    SELECT DISTINCT customer_unique_id, purchase_month
    FROM mart.mv_order_summary
),
cohort_size AS (
    SELECT cohort_month, COUNT(*) AS cohort_customers
    FROM first_buy
    GROUP BY cohort_month
),
joined AS (
    SELECT f.cohort_month,
           ((EXTRACT(YEAR  FROM a.purchase_month) - EXTRACT(YEAR  FROM f.cohort_month)) * 12
          + (EXTRACT(MONTH FROM a.purchase_month) - EXTRACT(MONTH FROM f.cohort_month)))::INT AS months_since,
           f.customer_unique_id
    FROM first_buy f
    JOIN activity a ON a.customer_unique_id = f.customer_unique_id
)
SELECT j.cohort_month,
       j.months_since,
       cs.cohort_customers,
       COUNT(*)                                                     AS active_customers,
       ROUND(100.0 * COUNT(*) / cs.cohort_customers, 2)             AS retention_pct
FROM joined j
JOIN cohort_size cs ON cs.cohort_month = j.cohort_month
GROUP BY j.cohort_month, j.months_since, cs.cohort_customers;

CREATE UNIQUE INDEX ux_mv_cohort ON mart.mv_cohort_retention (cohort_month, months_since);

-- ---------------------------------------------------------------------
-- 6. Seller-to-customer distance vs delivery time (the really heavy one)
--    Collapses ~1M geolocation rows to one point per zip prefix, then
--    computes great-circle distance (spherical law of cosines) per order item.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW mart.mv_delivery_distance AS
WITH geo AS (
    SELECT geolocation_zip_code_prefix::TEXT::INT   AS zip,
           AVG(geolocation_lat::NUMERIC)            AS lat,
           AVG(geolocation_lng::NUMERIC)            AS lng
    FROM raw.geolocation
    GROUP BY 1
),
pairs AS (
    SELECT i.order_id, i.order_item_id,
           o.delivery_days, o.is_late,
           s.state AS seller_state, c.state AS customer_state,
           cg.lat AS clat, cg.lng AS clng, sg.lat AS slat, sg.lng AS slng
    FROM staging.order_items i
    JOIN      staging.orders    o ON o.order_id    = i.order_id
    JOIN      staging.customers c ON c.customer_id = o.customer_id
    JOIN      staging.sellers   s ON s.seller_id   = i.seller_id
    LEFT JOIN geo cg ON cg.zip = c.zip_code_prefix::INT
    LEFT JOIN geo sg ON sg.zip = s.zip_code_prefix::INT
)
SELECT order_id, order_item_id, seller_state, customer_state, delivery_days, is_late,
       ROUND((6371 * acos(LEAST(1, GREATEST(-1,
              cos(radians(clat)) * cos(radians(slat)) * cos(radians(slng) - radians(clng))
            + sin(radians(clat)) * sin(radians(slat)))))), 1)   AS distance_km
FROM pairs;

CREATE UNIQUE INDEX ux_mv_delivery_distance ON mart.mv_delivery_distance (order_id, order_item_id);

-- ---------------------------------------------------------------------
-- 7. REFRESH ALL (order matters: base first, then the MVs that read it)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE mart.refresh_heavy_mvs()
LANGUAGE plpgsql AS $$
DECLARE t0 TIMESTAMP;
BEGIN
    t0 := clock_timestamp();  REFRESH MATERIALIZED VIEW mart.mv_order_summary;
    RAISE NOTICE 'mv_order_summary            % ', clock_timestamp() - t0;
    t0 := clock_timestamp();  REFRESH MATERIALIZED VIEW mart.mv_monthly_category_revenue;
    RAISE NOTICE 'mv_monthly_category_revenue % ', clock_timestamp() - t0;
    t0 := clock_timestamp();  REFRESH MATERIALIZED VIEW mart.mv_seller_scorecard;
    RAISE NOTICE 'mv_seller_scorecard         % ', clock_timestamp() - t0;
    t0 := clock_timestamp();  REFRESH MATERIALIZED VIEW mart.mv_customer_rfm;
    RAISE NOTICE 'mv_customer_rfm             % ', clock_timestamp() - t0;
    t0 := clock_timestamp();  REFRESH MATERIALIZED VIEW mart.mv_cohort_retention;
    RAISE NOTICE 'mv_cohort_retention         % ', clock_timestamp() - t0;
    t0 := clock_timestamp();  REFRESH MATERIALIZED VIEW mart.mv_delivery_distance;
    RAISE NOTICE 'mv_delivery_distance        % ', clock_timestamp() - t0;
END $$;

-- ---------------------------------------------------------------------
-- 8. DEMO IDEAS
-- ---------------------------------------------------------------------
-- A) Heavy query vs MV: run both and compare the execution time shown by DBeaver
--      -- slow: recompute from scratch
--      SELECT * FROM ( <paste the SELECT of mv_seller_scorecard> ) x ORDER BY revenue DESC LIMIT 10;
--      -- fast: read the stored result
--      SELECT * FROM mart.mv_seller_scorecard ORDER BY revenue DESC LIMIT 10;
--
-- B) Size of each MV
--      SELECT matviewname, pg_size_pretty(pg_total_relation_size(('mart.'||matviewname)::regclass))
--      FROM pg_matviews WHERE schemaname = 'mart';
--
-- C) Business questions that are now simple one-table queries
--      -- top 5 categories every month
--      SELECT month, category, revenue FROM mart.mv_monthly_category_revenue
--      WHERE rank_in_month <= 5 ORDER BY month, rank_in_month;
--      -- how many sellers per tier
--      SELECT tier, COUNT(*), ROUND(AVG(avg_review),2) FROM mart.mv_seller_scorecard GROUP BY tier;
--      -- customer segments
--      SELECT segment, COUNT(*), ROUND(AVG(monetary),2) FROM mart.mv_customer_rfm GROUP BY segment;
--      -- retention curve of one cohort
--      SELECT months_since, retention_pct FROM mart.mv_cohort_retention
--      WHERE cohort_month = '2017-01-01' ORDER BY months_since;
--      -- does distance explain late deliveries?
--      SELECT width_bucket(distance_km, 0, 3000, 6) AS km_bucket,
--             ROUND(AVG(delivery_days),1) AS avg_days,
--             ROUND(100.0*AVG(CASE WHEN is_late THEN 1 ELSE 0 END),1) AS late_pct
--      FROM mart.mv_delivery_distance WHERE distance_km IS NOT NULL GROUP BY 1 ORDER BY 1;
--
-- D) Refresh without blocking readers (needs the unique indexes created above)
--      REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_seller_scorecard;
--      -- layered MVs: refresh mv_order_summary first, then mv_customer_rfm / mv_cohort_retention
