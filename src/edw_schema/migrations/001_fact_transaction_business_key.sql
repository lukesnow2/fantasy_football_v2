-- Give edw.fact_transaction a business key so republication is idempotent.
--
-- The table had only a surrogate PK (transaction_key), so re-publishing the
-- warehouse re-inserted every transaction instead of updating it. Yahoo's
-- transaction key plus the player it moved is a true natural key: verified
-- 11,446 raw rows / 11,446 distinct (transaction_id, player_id).
--
-- Existing rows predate the column, so they are BACKFILLED from
-- public.transactions rather than deleted. The web application reads edw.*
-- live, so clearing the table would empty every transaction-derived view on
-- the site until the next successful publication.
--
-- The backfill joins on (league, player, type, date). That is unique for all
-- but ~2% of rows, where a player was moved more than once the same day with
-- the same transaction type. Those rows are interchangeable -- same player,
-- so the same player_key -- so a deterministic row_number pairing yields the
-- same SET of (source_transaction_id, player_key) pairs regardless of which
-- surrogate key receives which id, which is what upsert matching depends on.
-- Verified against a warehouse whose values were already populated by the
-- pipeline: 10,326 rows compared, pair sets identical.
--
-- Idempotent: safe to re-run.

BEGIN;

ALTER TABLE edw.fact_transaction
    ADD COLUMN IF NOT EXISTS source_transaction_id VARCHAR(100);

WITH fact_rows AS (
    SELECT ft.transaction_key,
           dl.league_id,
           dp.player_id,
           ft.transaction_type,
           ft.transaction_date::date AS on_date,
           row_number() OVER (
               PARTITION BY dl.league_id, dp.player_id,
                            ft.transaction_type, ft.transaction_date::date
               ORDER BY ft.transaction_key) AS seq
    FROM edw.fact_transaction ft
    JOIN edw.dim_league dl ON dl.league_key = ft.league_key
    JOIN edw.dim_player dp ON dp.player_key = ft.player_key
    WHERE ft.source_transaction_id IS NULL
),
raw_rows AS (
    SELECT t.transaction_id,
           t.league_id,
           CASE WHEN t.player_id LIKE '%.p.%'
                THEN split_part(t.player_id, '.p.', 2)
                ELSE t.player_id END AS player_id,
           t.type AS transaction_type,
           t.timestamp::date AS on_date,
           row_number() OVER (
               PARTITION BY t.league_id,
                            CASE WHEN t.player_id LIKE '%.p.%'
                                 THEN split_part(t.player_id, '.p.', 2)
                                 ELSE t.player_id END,
                            t.type, t.timestamp::date
               ORDER BY t.transaction_id) AS seq
    FROM public.transactions t
)
UPDATE edw.fact_transaction ft
SET source_transaction_id = raw_rows.transaction_id
FROM fact_rows
JOIN raw_rows USING (league_id, player_id, transaction_type, on_date, seq)
WHERE ft.transaction_key = fact_rows.transaction_key;

-- Anything the backfill could not identify has no source row to match. Do
-- NOT delete it: run this against a warehouse whose public.transactions is
-- empty or partial -- an edw-only restore, or a cutover target seeded before
-- the raw tables land -- and a blanket delete empties the table and every
-- transaction-derived view on the site. Stop instead, so the operator sees
-- the unmet precondition rather than losing the data.
DO $$
DECLARE
    unmatched bigint;
BEGIN
    SELECT count(*) INTO unmatched
    FROM edw.fact_transaction WHERE source_transaction_id IS NULL;

    IF unmatched > 0 THEN
        RAISE EXCEPTION
            'migration 001: % of % fact_transaction rows could not be matched '
            'to public.transactions. Load the raw transactions first, then '
            're-run. (Refusing to delete unmatched rows.)',
            unmatched, (SELECT count(*) FROM edw.fact_transaction);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fact_transaction_source_transaction_id_player_key_key'
          AND conrelid = 'edw.fact_transaction'::regclass
    ) THEN
        ALTER TABLE edw.fact_transaction
            ADD CONSTRAINT fact_transaction_source_transaction_id_player_key_key
            UNIQUE (source_transaction_id, player_key);
    END IF;
END $$;

COMMIT;
