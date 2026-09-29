#!/usr/bin/env zsh
# Promote the local warehouse to a Neon database in ONE transaction.
#
# Copies raw public.* (the seven Yahoo tables), public.pipeline_* (periods, run
# ledger, applied migrations), edw.* and meta_data.*; leaves app.* (users,
# chat, bets, constitution) and drizzle.* alone, and re-adds the two
# app -> edw.dim_manager foreign keys that dropping edw removes. Any error
# rolls everything back: the target is either fully promoted or untouched.
#
#   export TARGET_DATABASE_URL='postgresql://...neon.tech/neondb_rehearsal?sslmode=require'
#   scripts/promote_to_neon.sh
#
# Optional: LOCAL_DATABASE_URL (default the local dev DB), PG_BIN (a client
# at least as new as the target server - Neon runs PG18, pg_dump refuses to
# dump from a newer server and older psql can't read PG18 dumps).
# Rehearse on a copy first (RUNBOOK "Promote local → Neon").
set -euo pipefail
cd "${0:A:h}/.."
: "${TARGET_DATABASE_URL:?set TARGET_DATABASE_URL}"
LOCAL=${LOCAL_DATABASE_URL:-postgresql://localhost/the_league}
B=${PG_BIN:-/usr/local/opt/postgresql@18/bin}
LOCK_KEY=815200517   # src/pipeline/state.py ADVISORY_LOCK_KEY
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
red() { sed -E 's#://[^@ ]*@#://***@#g'; }
q() { $B/psql "$1" -XAtq -v ON_ERROR_STOP=1 -c "$2"; }

# Gates. The source must be quiescent and fully published, and the manager
# keys must match: app.* stores them (users, chat, bets, votes), so a key
# that changed meaning would silently re-attribute a member's history.
[[ $(q "$LOCAL" "select count(*) from pg_locks where locktype='advisory' and objid=$LOCK_KEY") == 0 ]] \
  || { echo "A pipeline run holds the lock on the source; wait for it."; exit 1; }
[[ $(q "$LOCAL" "select count(*) from public.pipeline_periods where not published") == 0 ]] \
  || { echo "The source has unpublished periods; run the pipeline there first."; exit 1; }
keys="select coalesce(string_agg(manager_key||'='||manager_name, ',' order by manager_key), '') from edw.dim_manager"
src_keys=$(q "$LOCAL" "$keys")
# A target with no warehouse yet (a fresh database) has nothing to compare.
if [[ $(q "$TARGET_DATABASE_URL" "select to_regclass('edw.dim_manager') is not null") == t ]]; then
  [[ $(q "$TARGET_DATABASE_URL" "$keys") == "$src_keys" ]] \
    || { echo "edw.dim_manager differs between source and target - STOP (app.* references these keys)."; exit 1; }
fi

$B/pg_dump "$LOCAL" --clean --if-exists --no-owner --no-privileges -f "$work/raw.sql" \
  -t public.leagues -t public.teams -t public.rosters -t public.matchups \
  -t public.transactions -t public.draft_picks -t public.statistics -t 'public.pipeline_*'
$B/pg_dump "$LOCAL" --schema=edw --schema=meta_data --no-owner --no-privileges -f "$work/edw.sql"

cat > "$work/begin.sql" <<SQL
-- Same key space as the pipeline's session lock: a weekly run on the target
-- makes this fail instead of racing it, and none can start until we commit.
DO \$\$ BEGIN
  IF NOT pg_try_advisory_xact_lock($LOCK_KEY) THEN
    RAISE EXCEPTION 'a pipeline run is active on the target; try again after it finishes';
  END IF;
END \$\$;
SQL
cat > "$work/mid.sql" <<'SQL'
DROP SCHEMA IF EXISTS edw CASCADE;        -- also drops both app -> edw.dim_manager FKs
DROP SCHEMA IF EXISTS meta_data CASCADE;
SQL
cat > "$work/end.sql" <<'SQL'
ALTER TABLE app."user" ADD CONSTRAINT user_manager_key_dim_manager_manager_key_fk
  FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key);
ALTER TABLE app.league_member ADD CONSTRAINT league_member_manager_key_dim_manager_manager_key_fk
  FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key);
SQL
cat "$work/begin.sql" "$work/raw.sql" "$work/mid.sql" "$work/edw.sql" "$work/end.sql" > "$work/all.sql"

echo "Promoting $(wc -c < "$work/all.sql" | tr -d ' ') bytes in one transaction..."
start=$(date +%s)
$B/psql "$TARGET_DATABASE_URL" -X -q -1 -v ON_ERROR_STOP=1 -o /dev/null -f "$work/all.sql" 2>&1 | red
echo "Committed in $(( $(date +%s) - start ))s. Next: incremental_load.py --dry-run against the target."
