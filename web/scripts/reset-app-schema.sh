#!/usr/bin/env bash
#
# Rebuild the `app` schema from the drizzle migration baseline.
#
# Safe only because `app` is verified empty — the tables it holds (user, session,
# rule_proposal, rule_vote, chat_*) had 0 rows in both local dev and production
# Neon at the time this was written. Re-verify before running it anywhere new:
#
#   node scripts/inspect-prod-state.js
#
# NEVER touches `edw` or `public`. Those hold 20 years of league data built by
# the Python ETL in src/edw_schema/. The migration itself is filtered by
# scripts/filter-migration.js so it cannot contain pipeline-owned DDL.
#
# Usage: DATABASE_URL=postgres://... ./scripts/reset-app-schema.sh

set -euo pipefail

if [[ -z "${DATABASE_URL:-}" ]]; then
	echo "DATABASE_URL is not set." >&2
	exit 1
fi

echo "Target: $(echo "$DATABASE_URL" | sed 's|:[^:@]*@|:***@|')"

app_rows=$(psql "$DATABASE_URL" -tAc "
	select coalesce(sum(n), 0) from (
		select (xpath('/row/c/text()',
			query_to_xml(format('select count(*) c from %I.%I', table_schema, table_name),
			false, true, '')))[1]::text::int as n
		from information_schema.tables where table_schema = 'app'
	) t;" 2>/dev/null || echo 0)

if [[ "$app_rows" != "0" ]]; then
	echo "REFUSING: app schema holds $app_rows rows. This script only rebuilds an empty schema." >&2
	echo "Write an incremental migration instead." >&2
	exit 1
fi

edw_before=$(psql "$DATABASE_URL" -tAc \
	"select count(*) from information_schema.tables where table_schema = 'edw'")
echo "app rows: 0 — safe to rebuild. edw tables before: $edw_before"

# Drop only. The baseline migration opens with CREATE SCHEMA "app", so creating
# it here too makes the migration fail on its first statement.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -c 'DROP SCHEMA IF EXISTS app CASCADE;'

npx drizzle-kit migrate

edw_after=$(psql "$DATABASE_URL" -tAc \
	"select count(*) from information_schema.tables where table_schema = 'edw'")

if [[ "$edw_before" != "$edw_after" ]]; then
	echo "ALARM: edw table count changed from $edw_before to $edw_after." >&2
	exit 1
fi

echo "Done. edw tables after: $edw_after (unchanged)."
