-- The bet board: one table recording side bets between managers.
--
-- Hand-written rather than generated, for the same reason 0002 was. The meta
-- snapshot has been stale since 0001, so `drizzle-kit generate` opens an
-- interactive "is app.wager created, or renamed from app.chat_thread?" prompt
-- (chat_thread was dropped in 0002 without a snapshot to match). The DDL below
-- is exactly what drizzle would emit for the table declared in schema.ts,
-- including its constraint naming.

CREATE TABLE "app"."wager" (
	"wager_key" serial PRIMARY KEY NOT NULL,
	"wager_id" varchar(50) NOT NULL,
	"title" varchar(200) NOT NULL,
	"terms" text NOT NULL,
	"stake" varchar(200) NOT NULL,
	"season_year" integer,
	"proposed_by" integer NOT NULL,
	-- NULL means an open prop any member may take; a value means head-to-head
	-- and only that manager may accept.
	"counterparty_key" integer,
	"accepted_by" integer,
	"accepted_at" timestamptz,
	"status" varchar(20) DEFAULT 'open' NOT NULL,
	"resolution_requested_by" integer,
	"resolution_requested_at" timestamptz,
	"resolution_note" text,
	"outcome" varchar(20),
	"winner_key" integer,
	"ruling_note" text,
	"resolved_by" integer,
	"resolved_at" timestamptz,
	"created_at" timestamptz DEFAULT now() NOT NULL,
	"updated_at" timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT "wager_wager_id_unique" UNIQUE("wager_id")
);
--> statement-breakpoint
-- Every manager reference points at app.league_member, never edw.dim_manager.
-- dim_manager holds every manager across 20 seasons including manager_key 1,
-- '-- hidden --', so it would happily accept a bet nobody can be held to. The
-- allowlist makes an unattributable bet a constraint violation at insert time.
ALTER TABLE "app"."wager" ADD CONSTRAINT "wager_proposed_by_league_member_manager_key_fk" FOREIGN KEY ("proposed_by") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."wager" ADD CONSTRAINT "wager_counterparty_key_league_member_manager_key_fk" FOREIGN KEY ("counterparty_key") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."wager" ADD CONSTRAINT "wager_accepted_by_league_member_manager_key_fk" FOREIGN KEY ("accepted_by") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."wager" ADD CONSTRAINT "wager_resolution_requested_by_league_member_manager_key_fk" FOREIGN KEY ("resolution_requested_by") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."wager" ADD CONSTRAINT "wager_winner_key_league_member_manager_key_fk" FOREIGN KEY ("winner_key") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."wager" ADD CONSTRAINT "wager_resolved_by_league_member_manager_key_fk" FOREIGN KEY ("resolved_by") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
-- The page renders four status buckets on every load, and the ledger scans by
-- proposer. Both are index scans rather than a seq scan the moment the board
-- has a season's worth of bets on it.
CREATE INDEX "wager_status_idx" ON "app"."wager" USING btree ("status");
--> statement-breakpoint
CREATE INDEX "wager_proposed_by_idx" ON "app"."wager" USING btree ("proposed_by");
