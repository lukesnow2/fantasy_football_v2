-- Give edw.fact_transaction a business key so republication is idempotent.
--
-- The table had only a surrogate PK (transaction_key), so re-publishing the
-- warehouse re-inserted every transaction instead of updating it. Yahoo's
-- transaction key plus the player it moved is a true natural key: verified
-- 11,446 raw rows / 11,446 distinct (transaction_id, player_id).
--
-- Existing rows predate the column and would not match ON CONFLICT (NULLs are
-- distinct in a unique index), so they are cleared and repopulated by the next
-- publication. fact_transaction is fully derived from public.transactions, so
-- this loses nothing.
--
-- Idempotent: safe to re-run.

BEGIN;

ALTER TABLE edw.fact_transaction
    ADD COLUMN IF NOT EXISTS source_transaction_id VARCHAR(100);

DELETE FROM edw.fact_transaction WHERE source_transaction_id IS NULL;

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
