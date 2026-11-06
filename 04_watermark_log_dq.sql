-- =====================================================================
-- WATERMARK incremental load + ETL LOG + DATA QUALITY (raw -> staging)
--
-- Flow for staging.orders:
--   1. read last watermark from etl_control
--   2. EXTRACT only raw rows newer than the watermark   (incremental)
--   3. TRANSFORM + run quality rules on each row
--   4. bad rows  -> staging.rejected_rows (with a reason)
--      good rows -> upsert into staging.orders
--   5. move the watermark forward
--   6. write the result to staging.etl_log (SUCCESS or FAILED)
--
-- Prerequisites: staging tables from 01_raw_to_staging_procedures.sql,
--                staging.customers loaded (used by the orphan check).
-- =====================================================================

ALTER TABLE staging.orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();

-- ---------------------------------------------------------------------
-- 1. CONTROL, LOG, REJECT, DQ RESULT TABLES (create once)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.etl_control (
    table_name       TEXT PRIMARY KEY,
    last_loaded_ts   TIMESTAMP NOT NULL DEFAULT '1900-01-01'   -- the watermark
);

CREATE TABLE IF NOT EXISTS staging.etl_log (
    log_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          UUID,
    step_name       TEXT,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          TEXT,            -- RUNNING / SUCCESS / FAILED
    rows_read       INT,
    rows_loaded     INT,
    rows_rejected   INT,
    watermark_from  TIMESTAMP,
    watermark_to    TIMESTAMP,
    message         TEXT
);

CREATE TABLE IF NOT EXISTS staging.rejected_rows (
    reject_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        UUID,
    table_name    TEXT,
    business_key  TEXT,
    reason        TEXT,
    row_data      JSONB,             -- the full rejected row, for investigation
    rejected_at   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.dq_check_result (
    check_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_at        TIMESTAMP,
    check_name    TEXT,
    failed_rows   INT,
    status        TEXT               -- PASS / FAIL
);

-- ---------------------------------------------------------------------
-- 2. INCREMENTAL LOAD OF ORDERS (watermark + DQ rules + log)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.load_orders_incremental()
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id    UUID := gen_random_uuid();
    v_log_id    BIGINT;
    v_wm_from   TIMESTAMP;
    v_wm_to     TIMESTAMP;
    v_read      INT := 0;
    v_rejected  INT := 0;
    v_loaded    INT := 0;
BEGIN
    -- make sure a control row exists, then read the watermark
    INSERT INTO staging.etl_control (table_name) VALUES ('orders')
    ON CONFLICT (table_name) DO NOTHING;

    SELECT last_loaded_ts INTO v_wm_from
    FROM staging.etl_control WHERE table_name = 'orders';

    -- log start (outside the exception block, so it survives a failure)
    INSERT INTO staging.etl_log (run_id, step_name, started_at, status, watermark_from)
    VALUES (v_run_id, 'load_orders_incremental', clock_timestamp(), 'RUNNING', v_wm_from)
    RETURNING log_id INTO v_log_id;

    BEGIN
        -- E + T: only rows newer than the watermark, with a DQ reason column
        DROP TABLE IF EXISTS tmp_orders_delta;
        CREATE TEMP TABLE tmp_orders_delta AS
        SELECT d.*,
               CASE
                   WHEN d.order_id IS NULL
                        THEN 'NULL_KEY'
                   WHEN d.customer_ts < d.purchase_ts
                        THEN 'DELIVERED_BEFORE_PURCHASE'
                   WHEN d.order_status NOT IN ('created','approved','invoiced','processing',
                                               'shipped','delivered','unavailable','canceled')
                        THEN 'UNKNOWN_STATUS'
                   WHEN NOT EXISTS (SELECT 1 FROM staging.customers c
                                    WHERE c.customer_id = d.customer_id)
                        THEN 'ORPHAN_CUSTOMER'
               END AS dq_reason
        FROM (
            SELECT DISTINCT ON (order_id)
                   order_id, customer_id,
                   LOWER(TRIM(order_status::TEXT))                    AS order_status,
                   purchase_ts, approved_ts, carrier_ts, customer_ts, estimated_ts,
                   (customer_ts::DATE - purchase_ts::DATE)            AS delivery_days,
                   CASE WHEN customer_ts IS NULL THEN NULL
                        ELSE customer_ts > estimated_ts END           AS is_late
            FROM (
                SELECT order_id, customer_id, order_status,
                       NULLIF(order_purchase_timestamp::TEXT, '')::TIMESTAMP        AS purchase_ts,
                       NULLIF(order_approved_at::TEXT, '')::TIMESTAMP               AS approved_ts,
                       NULLIF(order_delivered_carrier_date::TEXT, '')::TIMESTAMP    AS carrier_ts,
                       NULLIF(order_delivered_customer_date::TEXT, '')::TIMESTAMP   AS customer_ts,
                       NULLIF(order_estimated_delivery_date::TEXT, '')::TIMESTAMP   AS estimated_ts
                FROM raw.orders
            ) r
            WHERE r.purchase_ts > v_wm_from                 -- the incremental filter
            ORDER BY order_id
        ) d;

        SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_reason IS NOT NULL)
        INTO v_read, v_rejected
        FROM tmp_orders_delta;

        -- bad rows go to the reject table, nothing is silently dropped
        INSERT INTO staging.rejected_rows (run_id, table_name, business_key, reason, row_data)
        SELECT v_run_id, 'orders', order_id, dq_reason, to_jsonb(t) - 'dq_reason'
        FROM tmp_orders_delta t
        WHERE dq_reason IS NOT NULL;

        -- L: good rows are upserted
        INSERT INTO staging.orders AS o
               (order_id, customer_id, order_status, purchase_ts, approved_ts,
                delivered_carrier_ts, delivered_customer_ts, estimated_delivery_ts,
                delivery_days, is_late, updated_at)
        SELECT order_id, customer_id, order_status, purchase_ts, approved_ts,
               carrier_ts, customer_ts, estimated_ts, delivery_days, is_late, now()
        FROM tmp_orders_delta
        WHERE dq_reason IS NULL
        ON CONFLICT (order_id) DO UPDATE
        SET customer_id           = EXCLUDED.customer_id,
            order_status          = EXCLUDED.order_status,
            approved_ts           = EXCLUDED.approved_ts,
            delivered_carrier_ts  = EXCLUDED.delivered_carrier_ts,
            delivered_customer_ts = EXCLUDED.delivered_customer_ts,
            estimated_delivery_ts = EXCLUDED.estimated_delivery_ts,
            delivery_days         = EXCLUDED.delivery_days,
            is_late               = EXCLUDED.is_late,
            updated_at            = now();
        GET DIAGNOSTICS v_loaded = ROW_COUNT;

        -- move the watermark forward (only if we read something)
        SELECT MAX(purchase_ts) INTO v_wm_to FROM tmp_orders_delta;
        IF v_wm_to IS NOT NULL THEN
            UPDATE staging.etl_control
            SET last_loaded_ts = v_wm_to
            WHERE table_name = 'orders';
        ELSE
            v_wm_to := v_wm_from;
        END IF;

        UPDATE staging.etl_log
        SET status = 'SUCCESS', finished_at = clock_timestamp(),
            rows_read = v_read, rows_loaded = v_loaded, rows_rejected = v_rejected,
            watermark_to = v_wm_to
        WHERE log_id = v_log_id;

        RAISE NOTICE 'orders incremental: read %, loaded %, rejected %, watermark % -> %',
                     v_read, v_loaded, v_rejected, v_wm_from, v_wm_to;

    EXCEPTION WHEN OTHERS THEN
        -- the block's changes are rolled back (watermark NOT moved), the log row stays
        UPDATE staging.etl_log
        SET status = 'FAILED', finished_at = clock_timestamp(), message = SQLERRM
        WHERE log_id = v_log_id;
        RAISE WARNING 'load_orders_incremental FAILED: %', SQLERRM;
    END;
END $$;

-- ---------------------------------------------------------------------
-- 3. POST-LOAD DATA QUALITY CHECKS (report PASS / FAIL, change nothing)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.run_dq_checks()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO staging.dq_check_result (run_at, check_name, failed_rows, status)
    SELECT now(), check_name, failed_rows,
           CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM (
        SELECT 'orders_without_customer' AS check_name, COUNT(*)::INT AS failed_rows
        FROM staging.orders o
        LEFT JOIN staging.customers c ON c.customer_id = o.customer_id
        WHERE c.customer_id IS NULL
        UNION ALL
        SELECT 'negative_delivery_days', COUNT(*)::INT
        FROM staging.orders WHERE delivery_days < 0
        UNION ALL
        SELECT 'negative_amounts', COUNT(*)::INT
        FROM staging.order_items WHERE price < 0 OR freight_value < 0
        UNION ALL
        SELECT 'items_without_order', COUNT(*)::INT
        FROM staging.order_items i
        LEFT JOIN staging.orders o ON o.order_id = i.order_id
        WHERE o.order_id IS NULL
        UNION ALL
        SELECT 'orders_without_items', COUNT(*)::INT
        FROM staging.orders o
        WHERE NOT EXISTS (SELECT 1 FROM staging.order_items i WHERE i.order_id = o.order_id)
    ) c;
END $$;

-- ---------------------------------------------------------------------
-- 4. DEMO
-- ---------------------------------------------------------------------
-- Prepare (full load once, so customers exist):
--   CALL staging.run_etl();
--
-- Run 1: watermark is 1900-01-01, so everything is loaded
--   CALL staging.load_orders_incremental();
--
-- Run 2: nothing new -> read 0, loaded 0
--   CALL staging.load_orders_incremental();
--
-- Run 3: add new raw rows (2019 is newer than all Olist data):
--   one good, one whose customer is missing from STAGING, one delivered before purchase
--   (raw.orders has a foreign key to raw.customers, so the fake customer must exist in raw;
--    do NOT reload staging.customers before the next CALL, or the orphan check will not fire)
--   INSERT INTO raw.customers (customer_id, customer_unique_id, customer_zip_code_prefix,
--                              customer_city, customer_state)
--   VALUES ('demo_customer', 'demo_unique', '1000', 'demo city', 'SP');
--   INSERT INTO raw.orders (order_id, customer_id, order_status, order_purchase_timestamp,
--                           order_delivered_customer_date, order_estimated_delivery_date)
--   VALUES ('demo_good',   (SELECT customer_id FROM staging.customers LIMIT 1),
--           'delivered', '2019-01-10 10:00:00', '2019-01-15 10:00:00', '2019-01-20 10:00:00'),
--          ('demo_orphan', 'demo_customer',
--           'shipped',   '2019-01-11 10:00:00', NULL, '2019-01-21 10:00:00'),
--          ('demo_badts',  (SELECT customer_id FROM staging.customers LIMIT 1),
--           'delivered', '2019-01-12 10:00:00', '2019-01-02 10:00:00', '2019-01-22 10:00:00');
--   CALL staging.load_orders_incremental();    -- read 3, loaded 1, rejected 2
--
-- Inspect
--   SELECT * FROM staging.etl_control;
--   SELECT * FROM staging.etl_log ORDER BY log_id DESC;
--   SELECT reason, COUNT(*) FROM staging.rejected_rows GROUP BY reason;
--   SELECT business_key, reason, row_data FROM staging.rejected_rows ORDER BY reject_id DESC;
--
-- Quality checks
--   CALL staging.run_dq_checks();
--   SELECT * FROM staging.dq_check_result ORDER BY check_id DESC LIMIT 5;
--
-- Clean up the demo rows
--   DELETE FROM raw.orders WHERE order_id LIKE 'demo\_%';
--   DELETE FROM raw.customers WHERE customer_id = 'demo_customer';
--   DELETE FROM staging.orders WHERE order_id LIKE 'demo\_%';
--   DELETE FROM staging.rejected_rows WHERE business_key LIKE 'demo\_%';
--
-- Reset the watermark to reprocess everything (e.g. after fixing source data)
--   UPDATE staging.etl_control SET last_loaded_ts = '1900-01-01' WHERE table_name = 'orders';
