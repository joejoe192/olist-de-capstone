-- =====================================================================
-- MERGE : insert + update + delete in ONE statement  (raw -> staging)
-- Needs PostgreSQL 15+.  WHEN NOT MATCHED BY SOURCE and RETURNING with
-- merge_action() need PostgreSQL 17+ (you are on 18, so all work).
--
-- Mental model: MERGE joins SOURCE to TARGET, then each row falls in one of:
--     MATCHED               key exists in both          -> UPDATE (or DELETE)
--     NOT MATCHED           key only in source (new)    -> INSERT
--     NOT MATCHED BY SOURCE key only in target (gone)   -> UPDATE flag / DELETE
--
-- Prerequisite: staging tables from 01_raw_to_staging_procedures.sql
-- =====================================================================

ALTER TABLE staging.orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();
ALTER TABLE staging.orders ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE;

-- ---------------------------------------------------------------------
-- 1. SOURCE VIEW: the cleaned version of raw.orders (E + T)
--    MERGE needs a clean, one-row-per-key source, so we wrap the logic in a view.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW staging.v_orders_src AS
SELECT order_id,
       customer_id,
       LOWER(TRIM(order_status::TEXT))                      AS order_status,
       purchase_ts,
       approved_ts,
       delivered_carrier_ts,
       delivered_customer_ts,
       estimated_delivery_ts,
       (delivered_customer_ts::DATE - purchase_ts::DATE)    AS delivery_days,
       CASE WHEN delivered_customer_ts IS NULL THEN NULL
            ELSE delivered_customer_ts > estimated_delivery_ts END AS is_late
FROM (
    SELECT DISTINCT ON (order_id)
           order_id, customer_id, order_status,
           NULLIF(order_purchase_timestamp::TEXT, '')::TIMESTAMP        AS purchase_ts,
           NULLIF(order_approved_at::TEXT, '')::TIMESTAMP               AS approved_ts,
           NULLIF(order_delivered_carrier_date::TEXT, '')::TIMESTAMP    AS delivered_carrier_ts,
           NULLIF(order_delivered_customer_date::TEXT, '')::TIMESTAMP   AS delivered_customer_ts,
           NULLIF(order_estimated_delivery_date::TEXT, '')::TIMESTAMP   AS estimated_delivery_ts
    FROM raw.orders
    WHERE order_id IS NOT NULL
    ORDER BY order_id
) o
WHERE delivered_customer_ts IS NULL OR delivered_customer_ts >= purchase_ts;

-- ---------------------------------------------------------------------
-- 2. MERGE PROCEDURE
--    NOTE: the "NOT MATCHED BY SOURCE" branch is only safe when the source is
--    the FULL data set. Do not combine it with a watermark delta, or every
--    order outside the delta would be flagged as deleted.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.merge_orders()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    MERGE INTO staging.orders AS t
    USING staging.v_orders_src AS s
       ON t.order_id = s.order_id

    -- key exists and something changed (or it was soft-deleted): UPDATE
    WHEN MATCHED AND (t.customer_id, t.order_status, t.approved_ts, t.delivered_carrier_ts,
                      t.delivered_customer_ts, t.estimated_delivery_ts, t.is_deleted)
                     IS DISTINCT FROM
                     (s.customer_id, s.order_status, s.approved_ts, s.delivered_carrier_ts,
                      s.delivered_customer_ts, s.estimated_delivery_ts, FALSE)
    THEN UPDATE SET
         customer_id           = s.customer_id,
         order_status          = s.order_status,
         approved_ts           = s.approved_ts,
         delivered_carrier_ts  = s.delivered_carrier_ts,
         delivered_customer_ts = s.delivered_customer_ts,
         estimated_delivery_ts = s.estimated_delivery_ts,
         delivery_days         = s.delivery_days,
         is_late               = s.is_late,
         is_deleted            = FALSE,
         updated_at            = now()

    -- new key in the source: INSERT
    WHEN NOT MATCHED THEN
         INSERT (order_id, customer_id, order_status, purchase_ts, approved_ts,
                 delivered_carrier_ts, delivered_customer_ts, estimated_delivery_ts,
                 delivery_days, is_late, is_deleted, updated_at)
         VALUES (s.order_id, s.customer_id, s.order_status, s.purchase_ts, s.approved_ts,
                 s.delivered_carrier_ts, s.delivered_customer_ts, s.estimated_delivery_ts,
                 s.delivery_days, s.is_late, FALSE, now())

    -- key no longer in the source: SOFT DELETE (keep the row, flag it)
    WHEN NOT MATCHED BY SOURCE AND NOT t.is_deleted THEN
         UPDATE SET is_deleted = TRUE, updated_at = now();

    GET DIAGNOSTICS v_rows = ROW_COUNT;     -- total rows inserted + updated + flagged
    RAISE NOTICE 'merge_orders: % rows affected', v_rows;
END $$;

-- ---------------------------------------------------------------------
-- 3. DEMO
-- ---------------------------------------------------------------------
-- Run 1: nothing changed -> 0 rows affected (if staging.orders was already loaded)
--   CALL staging.merge_orders();
--
-- Run 2: simulate all three cases in the source
--   UPDATE raw.orders SET order_status = 'canceled'                    -- MATCHED (update)
--   WHERE order_id = (SELECT order_id FROM staging.orders LIMIT 1);
--   DELETE FROM raw.orders                                             -- NOT MATCHED BY SOURCE
--   WHERE order_id = (SELECT order_id FROM staging.orders OFFSET 1 LIMIT 1);
--   DELETE FROM staging.orders                                         -- NOT MATCHED (insert)
--   WHERE order_id = (SELECT order_id FROM staging.orders OFFSET 2 LIMIT 1);
--   CALL staging.merge_orders();                                       -- 3 rows affected
--
--   SELECT order_id, order_status, is_deleted, updated_at
--   FROM staging.orders ORDER BY updated_at DESC LIMIT 5;
--
-- See WHICH action ran per row (PostgreSQL 17+): run the MERGE by hand with RETURNING
--   MERGE INTO staging.orders AS t
--   USING staging.v_orders_src AS s ON t.order_id = s.order_id
--   WHEN MATCHED AND t.order_status IS DISTINCT FROM s.order_status
--        THEN UPDATE SET order_status = s.order_status, updated_at = now()
--   WHEN NOT MATCHED THEN
--        INSERT (order_id, customer_id, order_status, purchase_ts, updated_at)
--        VALUES (s.order_id, s.customer_id, s.order_status, s.purchase_ts, now())
--   RETURNING merge_action() AS action, t.order_id, t.order_status;
--
-- Hard delete instead of soft delete (replace the last WHEN branch with):
--   WHEN NOT MATCHED BY SOURCE THEN DELETE
--
-- Undo the demo: put the raw order back, or reload raw from the CSV.
