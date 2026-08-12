-- Chat hardening: timezone-correct timestamps, the foreign keys the chat tables
-- never had, indexes the new queries need, and the removal of a table nothing
-- has ever written to.
--
-- Hand-written rather than generated: drizzle-kit 0.30 cannot express the USING
-- clause on a type change, and the repo already hand-appends expression indexes
-- for the same reason (see the tail of 0000_nosy_stark_industries.sql).

-- `timestamp` without a zone is parsed by postgres.js in the *server process's*
-- local zone. On Vercel that is UTC and happens to be right; in local dev it is
-- six or seven hours wrong, silently. Verified safe to convert: the database's
-- TimeZone is GMT, so every value already present was written by now() in a UTC
-- session and `AT TIME ZONE 'UTC'` reproduces it exactly.
ALTER TABLE "app"."chat_message"
	ALTER COLUMN "created_at" TYPE timestamptz USING "created_at" AT TIME ZONE 'UTC',
	ALTER COLUMN "updated_at" TYPE timestamptz USING "updated_at" AT TIME ZONE 'UTC',
	ALTER COLUMN "edited_at"  TYPE timestamptz USING "edited_at"  AT TIME ZONE 'UTC',
	ALTER COLUMN "deleted_at" TYPE timestamptz USING "deleted_at" AT TIME ZONE 'UTC';
--> statement-breakpoint
ALTER TABLE "app"."chat_reaction"
	ALTER COLUMN "created_at" TYPE timestamptz USING "created_at" AT TIME ZONE 'UTC';
--> statement-breakpoint
ALTER TABLE "app"."chat_read"
	ALTER COLUMN "last_read_at" TYPE timestamptz USING "last_read_at" AT TIME ZONE 'UTC',
	ALTER COLUMN "updated_at"   TYPE timestamptz USING "updated_at"   AT TIME ZONE 'UTC';
--> statement-breakpoint
ALTER TABLE "app"."chat_custom_emoji"
	ALTER COLUMN "created_at" TYPE timestamptz USING "created_at" AT TIME ZONE 'UTC',
	ALTER COLUMN "updated_at" TYPE timestamptz USING "updated_at" AT TIME ZONE 'UTC';
--> statement-breakpoint

-- parent_message_key was declared as a plain integer, so a reply could point at
-- a message key that never existed. Self-referencing, cascading on a hard delete
-- (deletes are soft, so this only fires on a genuine purge).
DELETE FROM "app"."chat_message" c
	WHERE c."parent_message_key" IS NOT NULL
	  AND NOT EXISTS (
		SELECT 1 FROM "app"."chat_message" p WHERE p."message_key" = c."parent_message_key"
	  );
--> statement-breakpoint
ALTER TABLE "app"."chat_message"
	ADD CONSTRAINT "chat_message_parent_message_key_fk"
	FOREIGN KEY ("parent_message_key") REFERENCES "app"."chat_message"("message_key") ON DELETE CASCADE;
--> statement-breakpoint

-- chat_reaction had no foreign keys at all, so reactions outlived their messages.
DELETE FROM "app"."chat_reaction" cr
	WHERE NOT EXISTS (
		SELECT 1 FROM "app"."chat_message" m WHERE m."message_key" = cr."message_key"
	)
	OR NOT EXISTS (
		SELECT 1 FROM "app"."league_member" lm WHERE lm."manager_key" = cr."author_key"
	);
--> statement-breakpoint
ALTER TABLE "app"."chat_reaction"
	ADD CONSTRAINT "chat_reaction_message_key_fk"
	FOREIGN KEY ("message_key") REFERENCES "app"."chat_message"("message_key") ON DELETE CASCADE;
--> statement-breakpoint
ALTER TABLE "app"."chat_reaction"
	ADD CONSTRAINT "chat_reaction_author_key_fk"
	FOREIGN KEY ("author_key") REFERENCES "app"."league_member"("manager_key");
--> statement-breakpoint

DELETE FROM "app"."chat_read" cr
	WHERE NOT EXISTS (
		SELECT 1 FROM "app"."league_member" lm WHERE lm."manager_key" = cr."manager_key"
	);
--> statement-breakpoint
ALTER TABLE "app"."chat_read"
	ADD CONSTRAINT "chat_read_manager_key_fk"
	FOREIGN KEY ("manager_key") REFERENCES "app"."league_member"("manager_key") ON DELETE CASCADE;
--> statement-breakpoint

-- The root-message list pages on message_key DESC, which the existing
-- (channel_id, created_at) index cannot serve.
CREATE INDEX IF NOT EXISTS "idx_chat_message_channel_key"
	ON "app"."chat_message" ("channel_id", "message_key" DESC);
--> statement-breakpoint
-- The delta poll's second arm: edits and tombstones arrive by updated_at.
CREATE INDEX IF NOT EXISTS "idx_chat_message_channel_updated"
	ON "app"."chat_message" ("channel_id", "updated_at");
--> statement-breakpoint

-- chat_thread has never been read or written by any code, and its message_count
-- / last_message_at columns are derivable from parent_message_key. A
-- denormalisation with no writer is a lie waiting to be believed.
DROP TABLE IF EXISTS "app"."chat_thread";
