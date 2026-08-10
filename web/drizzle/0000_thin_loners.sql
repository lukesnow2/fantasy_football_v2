CREATE SCHEMA "app";
--> statement-breakpoint
CREATE TABLE "app"."chat_custom_emoji" (
	"emoji_key" serial PRIMARY KEY NOT NULL,
	"emoji_id" varchar(100) NOT NULL,
	"name" varchar(50) NOT NULL,
	"image_url" varchar(500) NOT NULL,
	"created_by" integer NOT NULL,
	"category" varchar(50) DEFAULT 'custom',
	"is_active" boolean DEFAULT true,
	"usage_count" integer DEFAULT 0,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now(),
	CONSTRAINT "chat_custom_emoji_emoji_id_unique" UNIQUE("emoji_id"),
	CONSTRAINT "chat_custom_emoji_name_unique" UNIQUE("name")
);
--> statement-breakpoint
CREATE TABLE "app"."chat_message" (
	"message_key" serial PRIMARY KEY NOT NULL,
	"message_id" varchar(100) NOT NULL,
	"content" text NOT NULL,
	"author_key" integer NOT NULL,
	"channel_id" varchar(100) DEFAULT 'general' NOT NULL,
	"parent_message_key" integer,
	"message_type" varchar(20) DEFAULT 'message',
	"edited_at" timestamp,
	"deleted_at" timestamp,
	"attachments" text,
	"mentions" text,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now(),
	CONSTRAINT "chat_message_message_id_unique" UNIQUE("message_id")
);
--> statement-breakpoint
CREATE TABLE "app"."chat_reaction" (
	"reaction_key" serial PRIMARY KEY NOT NULL,
	"message_key" integer NOT NULL,
	"author_key" integer NOT NULL,
	"emoji" varchar(100) NOT NULL,
	"emoji_type" varchar(20) DEFAULT 'unicode',
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "app"."chat_read" (
	"read_key" serial PRIMARY KEY NOT NULL,
	"manager_key" integer NOT NULL,
	"channel_id" varchar(100) NOT NULL,
	"last_read_message_key" integer,
	"last_read_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "app"."chat_thread" (
	"thread_key" serial PRIMARY KEY NOT NULL,
	"thread_id" varchar(100) NOT NULL,
	"root_message_key" integer NOT NULL,
	"channel_id" varchar(100) NOT NULL,
	"title" varchar(255),
	"message_count" integer DEFAULT 0,
	"last_message_at" timestamp,
	"last_message_key" integer,
	"is_locked" boolean DEFAULT false,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now(),
	CONSTRAINT "chat_thread_thread_id_unique" UNIQUE("thread_id")
);
--> statement-breakpoint
CREATE TABLE "app"."constitution_clause" (
	"clause_key" serial PRIMARY KEY NOT NULL,
	"section_key" integer NOT NULL,
	"parent_key" integer,
	"clause_uid" text NOT NULL,
	"depth" smallint NOT NULL,
	"sort_order" integer NOT NULL,
	"label" varchar(16) NOT NULL,
	"body" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."constitution_section" (
	"section_key" serial PRIMARY KEY NOT NULL,
	"version_key" integer NOT NULL,
	"section_id" varchar(50) NOT NULL,
	"title" varchar(255) NOT NULL,
	"kind" varchar(20) NOT NULL,
	"icon" varchar(50),
	"sort_order" integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."constitution_version" (
	"version_key" serial PRIMARY KEY NOT NULL,
	"version_no" integer NOT NULL,
	"effective_at" timestamp with time zone DEFAULT now() NOT NULL,
	"amendment_key" integer,
	"note" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "constitution_version_version_no_unique" UNIQUE("version_no")
);
--> statement-breakpoint
CREATE TABLE "app"."league_member" (
	"id" text PRIMARY KEY NOT NULL,
	"email" varchar(255) NOT NULL,
	"manager_key" integer NOT NULL,
	"role" varchar(20) DEFAULT 'member' NOT NULL,
	"active" boolean DEFAULT true NOT NULL,
	"display_name" varchar(255),
	"invited_at" timestamp with time zone,
	"first_login_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."login_token" (
	"token_hash" text PRIMARY KEY NOT NULL,
	"email" varchar(255) NOT NULL,
	"purpose" varchar(20) DEFAULT 'login' NOT NULL,
	"redirect_to" text,
	"expires_at" timestamp with time zone NOT NULL,
	"consumed_at" timestamp with time zone,
	"request_ip" varchar(45),
	"user_agent" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."rule_amendment" (
	"amendment_key" serial PRIMARY KEY NOT NULL,
	"proposal_key" integer NOT NULL,
	"amendment_year" integer NOT NULL,
	"amendment_title" varchar(255) NOT NULL,
	"amendment_description" text NOT NULL,
	"effective_season" integer NOT NULL,
	"vote_results" text,
	"approved_by" integer,
	"approved_at" timestamp,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "app"."rule_proposal" (
	"proposal_key" serial PRIMARY KEY NOT NULL,
	"proposal_id" varchar(50) NOT NULL,
	"title" varchar(255) NOT NULL,
	"description" text NOT NULL,
	"proposal_type" varchar(50) NOT NULL,
	"category" varchar(40) DEFAULT 'general' NOT NULL,
	"affected_section" varchar(100),
	"target_clause_uid" text,
	"subject_manager_key" integer,
	"current_language" text,
	"proposed_language" text NOT NULL,
	"rationale" text NOT NULL,
	"effective_season" integer NOT NULL,
	"submitted_by" integer NOT NULL,
	"submitted_at" timestamp DEFAULT now(),
	"voting_start_date" timestamp,
	"voting_end_date" timestamp,
	"status" varchar(20) DEFAULT 'draft' NOT NULL,
	"required_votes" integer NOT NULL,
	"eligible_voters" integer NOT NULL,
	"yes_votes" integer DEFAULT 0 NOT NULL,
	"no_votes" integer DEFAULT 0 NOT NULL,
	"abstain_votes" integer DEFAULT 0 NOT NULL,
	"settled_at" timestamp with time zone,
	"applied_at" timestamp with time zone,
	"amendment_key" integer,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now(),
	CONSTRAINT "rule_proposal_proposal_id_unique" UNIQUE("proposal_id")
);
--> statement-breakpoint
CREATE TABLE "app"."rule_vote" (
	"vote_key" serial PRIMARY KEY NOT NULL,
	"proposal_key" integer NOT NULL,
	"manager_key" integer NOT NULL,
	"vote" varchar(10) NOT NULL,
	"comment" text,
	"voted_at" timestamp DEFAULT now(),
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "app"."session" (
	"id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now(),
	"last_used_at" timestamp with time zone DEFAULT now(),
	"user_agent" text,
	"ip_address" varchar(45),
	"device_type" varchar(50)
);
--> statement-breakpoint
CREATE TABLE "app"."user" (
	"id" text PRIMARY KEY NOT NULL,
	"username" text NOT NULL,
	"manager_key" integer,
	"email" varchar(255),
	"display_name" varchar(255),
	"account_status" varchar(20) DEFAULT 'active',
	"notification_preferences" text,
	"profile_settings" text,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now(),
	CONSTRAINT "user_username_unique" UNIQUE("username"),
	CONSTRAINT "user_email_unique" UNIQUE("email")
);
--> statement-breakpoint
ALTER TABLE "app"."chat_message" ADD CONSTRAINT "chat_message_author_key_league_member_manager_key_fk" FOREIGN KEY ("author_key") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."constitution_clause" ADD CONSTRAINT "constitution_clause_section_key_constitution_section_section_key_fk" FOREIGN KEY ("section_key") REFERENCES "app"."constitution_section"("section_key") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."constitution_section" ADD CONSTRAINT "constitution_section_version_key_constitution_version_version_key_fk" FOREIGN KEY ("version_key") REFERENCES "app"."constitution_version"("version_key") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."constitution_version" ADD CONSTRAINT "constitution_version_amendment_key_rule_amendment_amendment_key_fk" FOREIGN KEY ("amendment_key") REFERENCES "app"."rule_amendment"("amendment_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."league_member" ADD CONSTRAINT "league_member_manager_key_dim_manager_manager_key_fk" FOREIGN KEY ("manager_key") REFERENCES "edw"."dim_manager"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."rule_amendment" ADD CONSTRAINT "rule_amendment_proposal_key_rule_proposal_proposal_key_fk" FOREIGN KEY ("proposal_key") REFERENCES "app"."rule_proposal"("proposal_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."rule_amendment" ADD CONSTRAINT "rule_amendment_approved_by_league_member_manager_key_fk" FOREIGN KEY ("approved_by") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."rule_proposal" ADD CONSTRAINT "rule_proposal_subject_manager_key_league_member_manager_key_fk" FOREIGN KEY ("subject_manager_key") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."rule_proposal" ADD CONSTRAINT "rule_proposal_submitted_by_league_member_manager_key_fk" FOREIGN KEY ("submitted_by") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."rule_vote" ADD CONSTRAINT "rule_vote_proposal_key_rule_proposal_proposal_key_fk" FOREIGN KEY ("proposal_key") REFERENCES "app"."rule_proposal"("proposal_key") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."rule_vote" ADD CONSTRAINT "rule_vote_manager_key_league_member_manager_key_fk" FOREIGN KEY ("manager_key") REFERENCES "app"."league_member"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
ALTER TABLE "app"."user" ADD CONSTRAINT "user_manager_key_dim_manager_manager_key_fk" FOREIGN KEY ("manager_key") REFERENCES "edw"."dim_manager"("manager_key") ON DELETE no action ON UPDATE no action;
--> statement-breakpoint
CREATE INDEX "chat_message_channel_created" ON "app"."chat_message" USING btree ("channel_id","created_at");
--> statement-breakpoint
CREATE INDEX "chat_message_thread" ON "app"."chat_message" USING btree ("parent_message_key","created_at");
--> statement-breakpoint
CREATE UNIQUE INDEX "unique_chat_reaction" ON "app"."chat_reaction" USING btree ("message_key","author_key","emoji");
--> statement-breakpoint
CREATE UNIQUE INDEX "unique_chat_read" ON "app"."chat_read" USING btree ("manager_key","channel_id");
--> statement-breakpoint
CREATE INDEX "chat_thread_channel_updated" ON "app"."chat_thread" USING btree ("channel_id","updated_at");
--> statement-breakpoint
CREATE UNIQUE INDEX "constitution_clause_section_uid_idx" ON "app"."constitution_clause" USING btree ("section_key","clause_uid");
--> statement-breakpoint
CREATE INDEX "constitution_clause_section_idx" ON "app"."constitution_clause" USING btree ("section_key","sort_order");
--> statement-breakpoint
CREATE UNIQUE INDEX "constitution_section_version_id_idx" ON "app"."constitution_section" USING btree ("version_key","section_id");
--> statement-breakpoint
CREATE UNIQUE INDEX "league_member_manager_key_idx" ON "app"."league_member" USING btree ("manager_key");
--> statement-breakpoint
CREATE INDEX "login_token_email_idx" ON "app"."login_token" USING btree ("email");
--> statement-breakpoint
CREATE INDEX "login_token_expires_idx" ON "app"."login_token" USING btree ("expires_at");
--> statement-breakpoint
CREATE UNIQUE INDEX "unique_vote_per_manager" ON "app"."rule_vote" USING btree ("proposal_key","manager_key");
--> statement-breakpoint
CREATE UNIQUE INDEX "unique_user_manager_key" ON "app"."user" USING btree ("manager_key");
--> statement-breakpoint
-- Hand-appended: drizzle-kit 0.30 cannot express an index on an expression.
-- These are not cosmetic. Without them "Bob@x.com" and "bob@x.com" can both
-- exist, and findMemberByEmail's lower(email) lookup would have two rows to
-- choose between — an ambiguity that decides who gets to log in.
-- Keep these in sync if the migration is ever regenerated.
CREATE UNIQUE INDEX "league_member_email_lower_idx" ON "app"."league_member" (lower("email"));--> statement-breakpoint
CREATE UNIQUE INDEX "user_email_lower_idx" ON "app"."user" (lower("email"));
