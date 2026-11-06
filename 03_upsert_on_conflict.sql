-- =====================================================================
-- UPSERT : INSERT ... ON CONFLICT   (raw -> staging)
-- Idea: no TRUNCATE. For every source row:
--         key not in target  -> INSERT
--         key already there  -> UPDATE (only if something changed)
-- Prerequisite: staging tables from 01_raw_to_staging_procedures.sql exist
--               (ON CONFLICT needs a PRIMARY KEY or UNIQUE constraint)
-- =====================================================================

-- Audit column so students can see which rows were touched
ALTER TABLE staging.orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();

-- ---------------------------------------------------------------------
-- 1. orders: DO UPDATE  (status and dates change over time)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.upsert_orders()
LANGUAGE plpgsql AS $$
DECLARE
    v_inserted INT;
    v_updated  INT;
BEGIN
    WITH src AS (
        -- E + T: same cleaning rules as the full load.
        -- DISTINCT ON is REQUIRED: if the source has the same key twice,
        -- ON CONFLICT DO UPDATE fails ("cannot affect row a second time").
        SELECT order_id,
               customer_id,
               LOWER(TRIM(order_status::TEXT))                         AS order_status,
               purchase_ts, approved_ts, carrier_ts, customer_ts, estimated_ts,
               (customer_ts::DATE - purchase_ts::DATE)                 AS delivery_days,
               CASE WHEN customer_ts IS NULL THEN NULL
                    ELSE customer_ts > estimated_ts END                AS is_late
        FROM (
            SELECT DISTINCT ON (order_id)
                   order_id,
                   customer_id,
                   order_status,
                   NULLIF(order_purchase_timestamp::TEXT, '')::TIMESTAMP        AS purchase_ts,
                   NULLIF(order_approved_at::TEXT, '')::TIMESTAMP               AS approved_ts,
                   NULLIF(order_delivered_carrier_date::TEXT, '')::TIMESTAMP    AS carrier_ts,
                   NULLIF(order_delivered_customer_date::TEXT, '')::TIMESTAMP   AS customer_ts,
                   NULLIF(order_estimated_delivery_date::TEXT, '')::TIMESTAMP   AS estimated_ts
            FROM raw.orders
            WHERE order_id IS NOT NULL
        ) o
        WHERE customer_ts IS NULL OR customer_ts >= purchase_ts
    ),
    up AS (
        -- L: the upsert
        INSERT INTO staging.orders AS t
               (order_id, customer_id, order_status, purchase_ts, approved_ts,
                delivered_carrier_ts, delivered_customer_ts, estimated_delivery_ts,
                delivery_days, is_late, updated_at)
        SELECT order_id, customer_id, order_status, purchase_ts, approved_ts,
               carrier_ts, customer_ts, estimated_ts, delivery_days, is_late, now()
        FROM src
        ON CONFLICT (order_id) DO UPDATE
        SET customer_id           = EXCLUDED.customer_id,
            order_status          = EXCLUDED.order_status,
            approved_ts           = EXCLUDED.approved_ts,
            delivered_carrier_ts  = EXCLUDED.delivered_carrier_ts,
            delivered_customer_ts = EXCLUDED.delivered_customer_ts,
            estimated_delivery_ts = EXCLUDED.estimated_delivery_ts,
            delivery_days         = EXCLUDED.delivery_days,
            is_late               = EXCLUDED.is_late,
            updated_at            = now()
        -- only touch rows that really changed (avoids useless writes)
        WHERE (t.customer_id, t.order_status, t.approved_ts, t.delivered_carrier_ts,
               t.delivered_customer_ts, t.estimated_delivery_ts)
              IS DISTINCT FROM
              (EXCLUDED.customer_id, EXCLUDED.order_status, EXCLUDED.approved_ts,
               EXCLUDED.delivered_carrier_ts, EXCLUDED.delivered_customer_ts,
               EXCLUDED.estimated_delivery_ts)
        RETURNING (xmax = 0) AS was_inserted   -- trick: xmax = 0 means a fresh insert
    )
    SELECT COUNT(*) FILTER (WHERE was_inserted),
           COUNT(*) FILTER (WHERE NOT was_inserted)
    INTO v_inserted, v_updated
    FROM up;

    RAISE NOTICE 'orders upsert: % inserted, % updated', v_inserted, v_updated;
END $$;

-- ---------------------------------------------------------------------
-- 2. order_items: DO NOTHING  (items never change, only new ones arrive)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.upsert_order_items()
LANGUAGE plpgsql AS $$
DECLARE v_inserted INT;
BEGIN
    INSERT INTO staging.order_items
           (order_id, order_item_id, product_id, seller_id,
            shipping_limit_ts, price, freight_value, total_amount)
    SELECT order_id,
           order_item_id::INT,
           product_id,
           seller_id,
           NULLIF(shipping_limit_date::TEXT, '')::TIMESTAMP,
           price::NUMERIC(10,2),
           freight_value::NUMERIC(10,2),
           price::NUMERIC(10,2) + freight_value::NUMERIC(10,2)
    FROM raw.order_items
    WHERE order_id IS NOT NULL
      AND price::NUMERIC >= 0
      AND freight_value::NUMERIC >= 0
    ON CONFLICT (order_id, order_item_id) DO NOTHING;   -- existing rows are skipped

    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    RAISE NOTICE 'order_items upsert: % new rows inserted', v_inserted;
END $$;

-- ---------------------------------------------------------------------
-- 3. DEMO
-- ---------------------------------------------------------------------
-- Run 1: nothing new -> 0 inserted, 0 updated
--   CALL staging.upsert_orders();
--   CALL staging.upsert_order_items();
--
-- Run 2: simulate new + changed data
--   DELETE FROM staging.orders WHERE order_id IN (SELECT order_id FROM staging.orders LIMIT 3);
--   UPDATE raw.orders SET order_status = 'canceled'
--   WHERE order_id = (SELECT order_id FROM staging.orders LIMIT 1);
--   CALL staging.upsert_orders();         -- 3 inserted (deleted ones), 1 updated
--
-- Check what was touched
--   SELECT order_id, order_status, updated_at FROM staging.orders
--   ORDER BY updated_at DESC LIMIT 5;
--
-- Plain-SQL version of the idea (what ON CONFLICT does for you):
--   IF EXISTS (key) THEN UPDATE ... ELSE INSERT ...
