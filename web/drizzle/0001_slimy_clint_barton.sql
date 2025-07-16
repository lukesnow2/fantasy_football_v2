CREATE SCHEMA "app";
--> statement-breakpoint
ALTER TABLE "edw"."rule_amendment" SET SCHEMA "app";
--> statement-breakpoint
ALTER TABLE "edw"."rule_proposal" SET SCHEMA "app";
--> statement-breakpoint
ALTER TABLE "edw"."rule_vote" SET SCHEMA "app";
--> statement-breakpoint
ALTER TABLE "public"."session" SET SCHEMA "app";
--> statement-breakpoint
ALTER TABLE "public"."user" SET SCHEMA "app";
