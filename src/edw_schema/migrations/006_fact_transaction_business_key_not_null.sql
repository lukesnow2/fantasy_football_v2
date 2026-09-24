-- Make edw.fact_transaction.source_transaction_id NOT NULL.
--
-- It is half of the table's business key, and it was the only nullable
-- conflict-key column in the warehouse. That mattered beyond tidiness:
-- batched upserts dedupe with pandas, which treats NaN as equal to NaN,
-- while Postgres treats NULL as distinct in a unique index. A batch
-- carrying several NULL keys would therefore be collapsed to one row where
-- the database would have kept them all, silently losing transactions.
--
-- Migration 001 guarantees every existing row has a value (it refuses to
-- run while any row is unmatched), so this only enforces what is already
-- true and removes the divergence for good.
--
-- Idempotent: safe to re-run.

BEGIN;

DO $$
DECLARE
    nulls bigint;
BEGIN
    SELECT count(*) INTO nulls
    FROM edw.fact_transaction WHERE source_transaction_id IS NULL;

    IF nulls > 0 THEN
        RAISE EXCEPTION
            'migration 006: % fact_transaction row(s) still have a NULL '
            'source_transaction_id. Run migration 001 first (it backfills '
            'them from public.transactions).', nulls;
    END IF;
END $$;

ALTER TABLE edw.fact_transaction
    ALTER COLUMN source_transaction_id SET NOT NULL;

COMMIT;
