-- =====================================================================
-- SCD TYPE 2 (incremental) : staging.sellers -> staging.sellers_scd2
-- Idea: never overwrite history. When a tracked attribute changes,
--       close the old row (valid_to, is_current=false) and insert a new row.
-- Business key : seller_id
-- Tracked attrs: zip_code_prefix, city, state
-- Prerequisite : staging.sellers already loaded (CALL staging.load_sellers();)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. HISTORY TABLE (create once, do NOT drop on every run)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.sellers_scd2 (
    seller_sk        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- surrogate key
    seller_id        TEXT      NOT NULL,                               -- business key
    zip_code_prefix  TEXT,
    city             TEXT,
    state            CHAR(2),
    row_hash         TEXT      NOT NULL,   -- fingerprint of tracked attributes
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP,            -- NULL = still open
    is_current       BOOLEAN   NOT NULL DEFAULT TRUE
);

-- only one current row per seller
CREATE UNIQUE INDEX IF NOT EXISTS ux_sellers_scd2_current
    ON staging.sellers_scd2 (seller_id) WHERE is_current;

-- ---------------------------------------------------------------------
-- 2. PROCEDURE: incremental SCD2 load
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE staging.load_sellers_scd2()
LANGUAGE plpgsql AS $$
DECLARE
    v_now     TIMESTAMP := now();
    v_closed  INT;
    v_new     INT;
BEGIN
    -- Step A: CLOSE current rows whose attributes changed
    UPDATE staging.sellers_scd2 t
    SET    valid_to   = v_now,
           is_current = FALSE
    FROM  (SELECT seller_id,
                  md5(COALESCE(zip_code_prefix,'') || '|' ||
                      COALESCE(city,'')            || '|' ||
                      COALESCE(state,''))          AS row_hash
           FROM staging.sellers) s
    WHERE  t.seller_id  = s.seller_id
      AND  t.is_current
      AND  t.row_hash  <> s.row_hash;
    GET DIAGNOSTICS v_closed = ROW_COUNT;

    -- Step B: INSERT new sellers + new versions of changed sellers
    --         (changed sellers now have no current row after Step A)
    INSERT INTO staging.sellers_scd2
           (seller_id, zip_code_prefix, city, state, row_hash, valid_from, valid_to, is_current)
    SELECT s.seller_id, s.zip_code_prefix, s.city, s.state,
           md5(COALESCE(s.zip_code_prefix,'') || '|' ||
               COALESCE(s.city,'')            || '|' ||
               COALESCE(s.state,'')),
           v_now, NULL, TRUE
    FROM   staging.sellers s
    LEFT JOIN staging.sellers_scd2 t
           ON t.seller_id = s.seller_id AND t.is_current
    WHERE  t.seller_id IS NULL;
    GET DIAGNOSTICS v_new = ROW_COUNT;

    RAISE NOTICE 'sellers_scd2: % rows closed, % rows inserted', v_closed, v_new;
END $$;

-- ---------------------------------------------------------------------
-- 3. DEMO
-- ---------------------------------------------------------------------
-- Run 1: initial load (everything is new)
--   CALL staging.load_sellers();
--   CALL staging.load_sellers_scd2();
--
-- Run 2: nothing changed -> 0 closed, 0 inserted (this is the "incremental" part)
--   CALL staging.load_sellers_scd2();
--
-- Run 3: simulate a change in the source, then reload
--   UPDATE raw.sellers SET seller_city = 'rio de janeiro', seller_state = 'RJ'
--   WHERE seller_id = (SELECT seller_id FROM raw.sellers LIMIT 1);
--   CALL staging.load_sellers();
--   CALL staging.load_sellers_scd2();     -- 1 closed, 1 inserted
--
-- See the history of that seller (2 rows: one old, one current)
--   SELECT * FROM staging.sellers_scd2
--   WHERE seller_id = (SELECT seller_id FROM raw.sellers LIMIT 1)
--   ORDER BY valid_from;
--
-- Current view only
--   SELECT * FROM staging.sellers_scd2 WHERE is_current;
--
-- Point-in-time query ("what did the seller look like on a date?")
--   SELECT * FROM staging.sellers_scd2
--   WHERE valid_from <= '2026-11-03' AND (valid_to IS NULL OR valid_to > '2026-11-03');
