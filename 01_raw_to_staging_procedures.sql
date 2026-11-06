-- =====================================================================
-- OLIST ETL: raw -> staging (pure Postgres)
-- Assumes raw tables: raw.customers, raw.sellers, raw.products,
--   raw.category_translation, raw.orders, raw.order_items, raw.order_reviews
-- Works whether raw columns are TEXT or already typed (all cast via ::TEXT first).
--
-- E = SELECT ... FROM raw.xxx         (read from the source)
-- T = casts, trim, COALESCE, dedupe, calculated columns
-- L = INSERT INTO staging.xxx         (write the result)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------
-- 1. TARGET TABLES (proper data types, run once)
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS staging.order_items, staging.reviews, staging.orders,
                     staging.products, staging.sellers, staging.customers CASCADE;

CREATE TABLE staging.customers (
    customer_id          TEXT PRIMARY KEY,
    customer_unique_id   TEXT,
    zip_code_prefix      TEXT,
    city                 TEXT,
    state                CHAR(2)
);

CREATE TABLE staging.sellers (
    seller_id            TEXT PRIMARY KEY,
    zip_code_prefix      TEXT,
    city                 TEXT,
    state                CHAR(2)
);

CREATE TABLE staging.products (
    product_id           TEXT PRIMARY KEY,
    category_pt          TEXT,
    category_en          TEXT,
    weight_g             INT,
    length_cm            INT,
    height_cm            INT,
    width_cm             INT
);

CREATE TABLE staging.orders (
    order_id                 TEXT PRIMARY KEY,
    customer_id              TEXT,
    order_status             TEXT,
    purchase_ts              TIMESTAMP,
    approved_ts              TIMESTAMP,
    delivered_carrier_ts     TIMESTAMP,
    delivered_customer_ts    TIMESTAMP,
    estimated_delivery_ts    TIMESTAMP,
    delivery_days            INT,        -- calculated
    is_late                  BOOLEAN     -- calculated
);

CREATE TABLE staging.order_items (
    order_id             TEXT,
    order_item_id        INT,
    product_id           TEXT,
    seller_id            TEXT,
    shipping_limit_ts    TIMESTAMP,
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2),
    total_amount         NUMERIC(10,2),  -- calculated
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE staging.reviews (
    order_id             TEXT PRIMARY KEY,   -- one review per order
    review_id            TEXT,
    review_score         INT,
    review_creation_ts   TIMESTAMP
);

-- ---------------------------------------------------------------------
-- 2. ONE PROCEDURE PER TABLE
-- ---------------------------------------------------------------------

-- ---- customers ------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.load_customers()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE staging.customers;                              -- rerunnable

    INSERT INTO staging.customers                            -- L
    SELECT DISTINCT ON (customer_id)                         -- T: dedupe
           customer_id,
           customer_unique_id,
           NULLIF(TRIM(customer_zip_code_prefix::TEXT), ''),
           INITCAP(TRIM(customer_city)),
           UPPER(TRIM(customer_state))
    FROM raw.customers                                       -- E
    WHERE customer_id IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'staging.customers: % rows loaded', v_rows;
END $$;

-- ---- sellers --------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.load_sellers()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE staging.sellers;

    INSERT INTO staging.sellers
    SELECT DISTINCT ON (seller_id)
           seller_id,
           NULLIF(TRIM(seller_zip_code_prefix::TEXT), ''),
           INITCAP(TRIM(seller_city)),
           UPPER(TRIM(seller_state))
    FROM raw.sellers
    WHERE seller_id IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'staging.sellers: % rows loaded', v_rows;
END $$;

-- ---- products (join translation table) ------------------------------
CREATE OR REPLACE PROCEDURE staging.load_products()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE staging.products;

    INSERT INTO staging.products
    SELECT DISTINCT ON (p.product_id)
           p.product_id,
           COALESCE(NULLIF(TRIM(p.product_category_name), ''), 'unknown'),
           COALESCE(t.product_category_name_english, 'unknown'),
           NULLIF(p.product_weight_g::TEXT, '')::NUMERIC::INT,
           NULLIF(p.product_length_cm::TEXT, '')::NUMERIC::INT,
           NULLIF(p.product_height_cm::TEXT, '')::NUMERIC::INT,
           NULLIF(p.product_width_cm::TEXT, '')::NUMERIC::INT
    FROM raw.products p
    LEFT JOIN raw.category_translation t
           ON TRIM(p.product_category_name) = t.product_category_name
    WHERE p.product_id IS NOT NULL;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'staging.products: % rows loaded', v_rows;
END $$;

-- ---- orders (calculated columns) ------------------------------------
CREATE OR REPLACE PROCEDURE staging.load_orders()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE staging.orders;

    INSERT INTO staging.orders
    SELECT order_id,
           customer_id,
           LOWER(TRIM(order_status)),
           purchase_ts,
           approved_ts,
           carrier_ts,
           customer_ts,
           estimated_ts,
           (customer_ts::DATE - purchase_ts::DATE)            AS delivery_days,
           CASE WHEN customer_ts IS NULL THEN NULL
                ELSE customer_ts > estimated_ts END           AS is_late
    FROM (
        SELECT DISTINCT ON (order_id)
               order_id,
               customer_id,
               order_status,
               NULLIF(order_purchase_timestamp::TEXT, '')::TIMESTAMP          AS purchase_ts,
               NULLIF(order_approved_at::TEXT, '')::TIMESTAMP                 AS approved_ts,
               NULLIF(order_delivered_carrier_date::TEXT, '')::TIMESTAMP      AS carrier_ts,
               NULLIF(order_delivered_customer_date::TEXT, '')::TIMESTAMP     AS customer_ts,
               NULLIF(order_estimated_delivery_date::TEXT, '')::TIMESTAMP     AS estimated_ts
        FROM raw.orders
        WHERE order_id IS NOT NULL
    ) o
    -- filter invalid rows: delivered before purchase
    WHERE customer_ts IS NULL OR customer_ts >= purchase_ts;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'staging.orders: % rows loaded', v_rows;
END $$;

-- ---- order_items ----------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.load_order_items()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE staging.order_items;

    INSERT INTO staging.order_items
    SELECT order_id,
           order_item_id::INT,
           product_id,
           seller_id,
           NULLIF(shipping_limit_date::TEXT, '')::TIMESTAMP,
           price::NUMERIC(10,2),
           freight_value::NUMERIC(10,2),
           price::NUMERIC(10,2) + freight_value::NUMERIC(10,2)   -- total_amount
    FROM raw.order_items
    WHERE order_id IS NOT NULL
      AND price::NUMERIC >= 0                                    -- filter invalid
      AND freight_value::NUMERIC >= 0;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'staging.order_items: % rows loaded', v_rows;
END $$;

-- ---- reviews (keep one review per order) ----------------------------
CREATE OR REPLACE PROCEDURE staging.load_reviews()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE staging.reviews;

    INSERT INTO staging.reviews
    SELECT order_id, review_id, review_score, review_creation_ts
    FROM (
        SELECT order_id,
               review_id,
               review_score::INT                                  AS review_score,
               NULLIF(review_creation_date::TEXT, '')::TIMESTAMP        AS review_creation_ts,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY NULLIF(review_answer_timestamp::TEXT, '')::TIMESTAMP DESC NULLS LAST
               ) AS rn                                            -- newest review wins
        FROM raw.order_reviews
        WHERE order_id IS NOT NULL
    ) r
    WHERE rn = 1;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE 'staging.reviews: % rows loaded', v_rows;
END $$;

-- ---------------------------------------------------------------------
-- 3. MASTER PROCEDURE: run the whole ETL with one command
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.run_etl()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '--- ETL raw -> staging started at % ---', clock_timestamp();

    CALL staging.load_customers();
    CALL staging.load_sellers();
    CALL staging.load_products();
    CALL staging.load_orders();
    CALL staging.load_order_items();
    CALL staging.load_reviews();

    RAISE NOTICE '--- ETL finished at % ---', clock_timestamp();
END $$;

-- ---------------------------------------------------------------------
-- 4. RUN IT
-- ---------------------------------------------------------------------
-- CALL staging.run_etl();

-- ---------------------------------------------------------------------
-- 5. VALIDATION: raw vs staging row counts
-- ---------------------------------------------------------------------
-- SELECT 'customers'   AS tbl, (SELECT COUNT(*) FROM raw.customers)    AS raw_rows, (SELECT COUNT(*) FROM staging.customers)   AS staging_rows
-- UNION ALL SELECT 'sellers',     (SELECT COUNT(*) FROM raw.sellers),       (SELECT COUNT(*) FROM staging.sellers)
-- UNION ALL SELECT 'products',    (SELECT COUNT(*) FROM raw.products),      (SELECT COUNT(*) FROM staging.products)
-- UNION ALL SELECT 'orders',      (SELECT COUNT(*) FROM raw.orders),        (SELECT COUNT(*) FROM staging.orders)
-- UNION ALL SELECT 'order_items', (SELECT COUNT(*) FROM raw.order_items),   (SELECT COUNT(*) FROM staging.order_items)
-- UNION ALL SELECT 'reviews',     (SELECT COUNT(*) FROM raw.order_reviews), (SELECT COUNT(*) FROM staging.reviews);
