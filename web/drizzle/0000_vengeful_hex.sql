CREATE SCHEMA "edw";
--> statement-breakpoint
CREATE TABLE "edw"."dim_league" (
	"league_key" integer PRIMARY KEY NOT NULL,
	"league_id" varchar(50) NOT NULL,
	"league_name" varchar(255) NOT NULL,
	"season_year" integer NOT NULL,
	"num_teams" integer NOT NULL,
	"league_type" varchar(50) NOT NULL,
	"scoring_type" varchar(50),
	"draft_type" varchar(50),
	"is_active" boolean DEFAULT true,
	"valid_from" timestamp DEFAULT now(),
	"valid_to" timestamp,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."dim_manager" (
	"manager_key" integer PRIMARY KEY NOT NULL,
	"manager_name" varchar(255) NOT NULL,
	"manager_id" varchar(100),
	"first_season_year" integer,
	"last_season_year" integer,
	"total_seasons" integer DEFAULT 0,
	"total_leagues" integer DEFAULT 0,
	"is_current" boolean DEFAULT true,
	"include_in_analysis" boolean DEFAULT true,
	"email" varchar(255),
	"display_name" varchar(255),
	"profile_image_url" varchar(500),
	"is_active" boolean DEFAULT true,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."dim_season" (
	"season_key" integer PRIMARY KEY NOT NULL,
	"season_year" integer NOT NULL,
	"season_start_date" date,
	"season_end_date" date,
	"playoff_start_week" integer,
	"championship_week" integer,
	"total_weeks" integer,
	"is_current_season" boolean DEFAULT false,
	"season_status" varchar(20) DEFAULT 'completed',
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."dim_team" (
	"team_key" integer PRIMARY KEY NOT NULL,
	"team_id" varchar(50) NOT NULL,
	"league_key" integer NOT NULL,
	"manager_key" integer,
	"team_name" varchar(255) NOT NULL,
	"manager_name" varchar(255),
	"manager_id" varchar(100),
	"team_logo_url" varchar(500),
	"is_active" boolean DEFAULT true,
	"valid_from" timestamp DEFAULT now(),
	"valid_to" timestamp,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."fact_matchup" (
	"matchup_key" integer PRIMARY KEY NOT NULL,
	"league_key" integer NOT NULL,
	"season_year" integer NOT NULL,
	"week_key" integer NOT NULL,
	"week_number" integer NOT NULL,
	"team1_key" integer NOT NULL,
	"team2_key" integer NOT NULL,
	"manager1_key" integer NOT NULL,
	"manager2_key" integer NOT NULL,
	"team1_points" numeric(8, 2),
	"team2_points" numeric(8, 2),
	"winner_team_key" integer,
	"winner_manager_key" integer,
	"matchup_type" varchar(20) DEFAULT 'regular',
	"is_playoffs" boolean DEFAULT false,
	"is_championship" boolean DEFAULT false,
	"is_semifinal" boolean DEFAULT false,
	"is_quarterfinal" boolean DEFAULT false,
	"is_last_place_game" boolean DEFAULT false,
	"margin_of_victory" numeric(8, 2),
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."fact_team_performance" (
	"performance_key" integer PRIMARY KEY NOT NULL,
	"team_key" integer NOT NULL,
	"manager_key" integer NOT NULL,
	"league_key" integer NOT NULL,
	"week_key" integer NOT NULL,
	"season_year" integer NOT NULL,
	"week_number" integer NOT NULL,
	"points_for" numeric(8, 2),
	"points_against" numeric(8, 2),
	"wins" integer DEFAULT 0,
	"losses" integer DEFAULT 0,
	"ties" integer DEFAULT 0,
	"weekly_rank" integer,
	"season_rank" integer,
	"playoff_seed" integer,
	"is_playoff_team" boolean DEFAULT false,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."fact_transaction" (
	"transaction_key" integer PRIMARY KEY NOT NULL,
	"league_key" integer NOT NULL,
	"player_key" integer NOT NULL,
	"from_team_key" integer,
	"to_team_key" integer,
	"from_manager_key" integer,
	"to_manager_key" integer,
	"season_year" integer NOT NULL,
	"transaction_week" integer,
	"transaction_date" date NOT NULL,
	"transaction_type" varchar(30) NOT NULL,
	"faab_bid" numeric(8, 2),
	"trade_group_id" varchar(100),
	"is_successful" boolean DEFAULT true,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."mart_manager_performance" (
	"manager_key" integer PRIMARY KEY NOT NULL,
	"manager_name" varchar(255) NOT NULL,
	"total_seasons" integer,
	"total_wins" integer,
	"total_losses" integer,
	"total_ties" integer,
	"win_percentage" numeric(5, 4),
	"avg_points_for" numeric(8, 2),
	"avg_points_against" numeric(8, 2),
	"total_championships" integer,
	"total_playoff_appearances" integer,
	"best_season" varchar(4),
	"worst_season" varchar(4),
	"first_season" varchar(4),
	"last_season" varchar(4),
	"longest_win_streak" integer,
	"longest_loss_streak" integer,
	"total_transactions" integer,
	"avg_transactions_per_season" numeric(5, 2),
	"updated_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."rule_amendment" (
	"amendment_key" integer PRIMARY KEY NOT NULL,
	"proposal_key" integer NOT NULL,
	"amendment_year" integer NOT NULL,
	"amendment_title" varchar(255) NOT NULL,
	"amendment_description" text NOT NULL,
	"effective_season" integer NOT NULL,
	"vote_results" text,
	"manager_key" integer,
	"approved_at" timestamp,
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "edw"."rule_proposal" (
	"proposal_key" integer PRIMARY KEY NOT NULL,
	"proposal_id" varchar(50) NOT NULL,
	"title" varchar(255) NOT NULL,
	"description" text NOT NULL,
	"proposal_type" varchar(50) NOT NULL,
	"affected_section" varchar(100),
	"current_language" text,
	"proposed_language" text NOT NULL,
	"rationale" text NOT NULL,
	"effective_season" integer NOT NULL,
	"manager_key" integer NOT NULL,
	"submitted_at" timestamp DEFAULT now(),
	"voting_start_date" timestamp,
	"voting_end_date" timestamp,
	"status" varchar(20) DEFAULT 'draft',
	"required_votes" integer DEFAULT 7,
	"yes_votes" integer DEFAULT 0,
	"no_votes" integer DEFAULT 0,
	"abstain_votes" integer DEFAULT 0,
	"created_at" timestamp DEFAULT now(),
	"updated_at" timestamp DEFAULT now(),
	CONSTRAINT "rule_proposal_proposal_id_unique" UNIQUE("proposal_id")
);
--> statement-breakpoint
CREATE TABLE "edw"."rule_vote" (
	"vote_key" integer PRIMARY KEY NOT NULL,
	"proposal_key" integer NOT NULL,
	"manager_key" integer NOT NULL,
	"vote" varchar(10) NOT NULL,
	"comment" text,
	"voted_at" timestamp DEFAULT now(),
	"created_at" timestamp DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "session" (
	"id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL
);
--> statement-breakpoint
CREATE TABLE "user" (
	"id" text PRIMARY KEY NOT NULL,
	"age" integer,
	"username" text NOT NULL,
	"password_hash" text NOT NULL,
	CONSTRAINT "user_username_unique" UNIQUE("username")
);
--> statement-breakpoint
CREATE TABLE "edw"."vw_current_season_dashboard" (
	"league_name" varchar(255),
	"season_year" integer,
	"team_name" varchar(255),
	"manager_name" varchar(255),
	"wins" integer,
	"losses" integer,
	"ties" integer,
	"points_for" numeric(8, 2),
	"points_against" numeric(8, 2),
	"point_differential" numeric(8, 2),
	"win_percentage" numeric(5, 4),
	"season_rank" integer,
	"playoff_probability" numeric(5, 4),
	"is_playoff_team" boolean,
	"playoff_seed" integer
);
--> statement-breakpoint
CREATE UNIQUE INDEX "unique_vote_per_manager" ON "edw"."rule_vote" USING btree ("proposal_key","manager_key");