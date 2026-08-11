# The League — Operations Runbook

How to rebuild and operate the data layer. The stack: a Python ETL (Yahoo → JSON →
`public.*` raw → `edw.*` warehouse) feeding a SvelteKit app (Drizzle, `app.*` schema)
on Vercel, backed by **Neon** (serverless Postgres). Local dev uses a local Postgres.

## Databases & schemas
- `public.*` — raw Yahoo tables (leagues, teams, rosters, matchups, transactions, draft_picks, statistics). Owned by the Python pipeline.
- `edw.*` — dimensional warehouse (dim_/fact_/mart_ + analytical views). Built by the ETL. **What the web app reads.**
- `meta_data.*` — data-dictionary metric definitions.
- `app.*` — auth/chat/rules (user, session, webauthn_*, chat_*, rule_*). Owned by Drizzle.

Every connection must resolve `app, edw, public` on its search_path. This is set at the
**database level** (`ALTER DATABASE <db> SET search_path TO app, edw, public`) — do this once
per database. (The app's per-connection search_path option is not reliable on Neon/PG18.)

## One-time setup
```bash
# Local Postgres (Homebrew) + dev DB
createdb the_league
psql "postgresql://<user>@localhost:5432/the_league" -c "ALTER DATABASE the_league SET search_path TO app, edw, public;"

# Python env (must match your CPU arch — Apple Silicon needs arm64 wheels)
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Web env: web/.env -> DATABASE_URL=postgres://<user>@localhost:5432/the_league
```

## Full warehouse rebuild from existing JSON
Run against any `DATABASE_URL` (local or Neon). The 44 MB historical file holds 2005–2024.
```bash
DB="postgresql://<user>@localhost:5432/the_league"
# 1. raw JSON -> public.*
.venv/bin/python src/deployment/heroku_deployer.py \
  --data-file data/current/yahoo_fantasy_nfl_private_20250610_124031.json --database-url "$DB"
# 2. build edw.* (schema + ETL + views). --force-rebuild = clean bulk reload.
.venv/bin/python scripts/deploy_complete_edw.py --database-url "$DB" --force-rebuild
# 3. app.* schema (auth/chat/rules) — from the committed migration, not push
cd web && DATABASE_URL="$DB" npm run db:migrate
DATABASE_URL="$DB" npm run seed:constitution   # constitution v1, idempotent
DATABASE_URL="$DB" npm run seed:members        # league allowlist, idempotent
# 4. meta_data.* (data dictionary)
psql "$DB" -f src/edw_schema/meta_data_schema.sql
psql "$DB" -f src/edw_schema/populate_metric_definitions.sql
```
Expected counts (verification): fact_matchup 1499, fact_transaction 9691, fact_draft 3192,
fact_player_statistics 40715, fact_team_performance 2998.

## Promote local → Neon
The per-row ETL is slow over the network; dump the built warehouse and restore instead.
```bash
NEON="postgresql://...neon.tech/neondb?sslmode=require"
psql "$NEON" -c "DROP SCHEMA IF EXISTS edw CASCADE; DROP SCHEMA IF EXISTS meta_data CASCADE;"
pg_dump "$LOCAL" --schema=edw --schema=meta_data --no-owner --no-privileges -f /tmp/edw.sql
psql "$NEON" -f /tmp/edw.sql
cd web && DATABASE_URL="$NEON" npx drizzle-kit push     # app.* on Neon (first time only)
psql "$NEON" -c "ALTER DATABASE neondb SET search_path TO app, edw, public;"
# DROP SCHEMA edw CASCADE also drops the app.user -> edw.dim_manager FK; re-add it:
psql "$NEON" -c "ALTER TABLE app.\"user\" ADD CONSTRAINT user_manager_key_dim_manager_manager_key_fk \
  FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key);"
```
Then load the raw `public.*` on Neon too (step 1 above with `--database-url "$NEON"`) so weekly
incremental updates have their landing tables.


## Authentication & the league roster
Sign-in is an emailed magic link. There is no password and no registration: a link
is only ever issued to an address already in `app.league_member`, which is the
allowlist and the only access control. Everything else (rate limiting, the
identical-response behaviour of the login form) is a second layer.

```bash
# Add or correct an address, then re-seed. Keyed on managerKey, so re-running
# updates in place. Blank emails are skipped — that manager just cannot sign in yet.
$EDITOR web/data/league-members.json
cd web && DATABASE_URL="$DB" npm run seed:members
```

The commissioner can send invites, correct addresses and deactivate managers at
`/admin/members`. Deactivating both blocks future sign-ins and kills any live
session.

**Resend**: the sending domain must be verified before any mail leaves. Sends
log rather than throw on failure — deliberately, so that a mail outage cannot
turn the login form into a roster-enumeration oracle — which also means an
unverified domain fails *silently*. Send yourself a link before inviting anyone.

Verification scripts (need a running dev server with `EMAIL_PROVIDER=console`):
```bash
cd web
ORIGIN=http://localhost:5175 DATABASE_URL="$DB" ./scripts/verify-login-flow.sh
ORIGIN=http://localhost:5175 DATABASE_URL="$DB" ./scripts/verify-voting-flow.sh
DATABASE_URL="$DB" node scripts/verify-attribution.js
```
Note `verify-voting-flow.sh` rewrites `app.*` test data — never point it at production.

## Constitution & amendments
The constitution is data, in `app.constitution_version/_section/_clause`. It is
versioned copy-on-write: a passing proposal clones the whole document into a new
version and edits the clone, so history is queryable and "Last Updated" is real.
Proposals target a clause by its stable `clause_uid`, which survives the clone.

Vote thresholds come from Article 8 and are chosen explicitly on the proposal
form (`src/lib/server/constitution/thresholds.ts`). The denominator is the count
of active `league_member` rows — never `edw.dim_manager`, or an ETL run could
silently change what counts as a super-majority. Abstaining and not voting both
count against passage.

Proposals settle on every vote and on every page load, so there is no cron to
depend on. `computeOutcome` in `constitution/outcome.ts` is pure and unit-tested;
it is the single source of truth for passage.

## Migrations
`db:push` is local-dev only. Production uses committed migrations:
```bash
cd web
npm run db:generate   # drizzle-kit generate, then scripts/filter-migration.js
npm run db:migrate
```
The filter step is not optional. `schemaFilter: ['app']` in `drizzle.config.ts`
only constrains `push` and `introspect` — `generate` renders every table in
`schema.ts`, including the `edw.*` read models, and will happily emit
`CREATE TABLE "edw"."dim_manager"`. The filter strips anything whose target is a
pipeline-owned schema and aborts if any survives.

## Weekly in-season updates (Phase 6 — GitHub Action)
`.github/workflows/weekly-data-extraction.yml` runs Sundays in-season: extract current week →
load → ETL. Requires repo secrets `DATABASE_URL` (Neon), `YAHOO_CLIENT_ID/SECRET`,
`YAHOO_REFRESH_TOKEN`. Re-auth Yahoo locally to regenerate `oauth2.json` when the refresh token expires.

## Gotchas
- **Don't run `drizzle-kit push` without `schemaFilter: ['app']`** — it defaults to managing only `public` and will DROP the pipeline's raw tables.
- **SSL**: app/drizzle disable SSL for `localhost`, require it for remote (Neon). Set automatically by host detection.
- **Manager attribution**: some teams have Yahoo-private (`--hidden--`) names and are mapped by team_id in `edw_etl_processor.get_manager_name_by_team_id`. Add new ones there.
- **League of record**: only one league per season is loaded into `edw.*` (the canonical league); list lives in the ETL / `scripts/fix_championship_flags.py`.

## Owner / config
- Manager canonical names + aliases: `edw_etl_processor.consolidate_manager_name` and `get_manager_name_by_team_id`.
- Vercel env: `DATABASE_URL`, `ORIGIN` (prod URL, no trailing slash), `NODE_ENV=production`,
  `EMAIL_PROVIDER=resend`, `RESEND_API_KEY`, `EMAIL_FROM`, `EMAIL_FROM_NAME`.
  (`AUTH_SECRET`/`SESSION_SECRET` are no longer read by anything — sessions are random
  tokens stored as their sha256, so there is nothing to sign.)
