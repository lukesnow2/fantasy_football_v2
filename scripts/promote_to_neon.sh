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
# The target URL is handed to psql through PG* environment variables, never
# argv, so its password doesn't sit in the process list.
# Optional: LOCAL_DATABASE_URL (default the local dev DB), PG_BIN (a client
# at least as new as the target server - Neon runs PG18: pg_dump refuses to
# dump from a newer server and older psql can't read PG18 dumps).
# Rehearse on a copy first (RUNBOOK "Promote local → Neon").
set -euo pipefail
cd "${0:A:h}/.."
: "${TARGET_DATABASE_URL:?set TARGET_DATABASE_URL}"
LOCAL=${LOCAL_DATABASE_URL:-postgresql://localhost/the_league}
PY=${PYTHON:-.venv/bin/python}
B=${PG_BIN:-$(brew --prefix postgresql@18 2>/dev/null)/bin}
[[ -x $B/psql && -x $B/pg_dump ]] \
  || { echo "No PostgreSQL 18 client at '$B': brew install postgresql@18, or set PG_BIN."; exit 1; }
LOCK_KEY=$($PY -c 'from src.pipeline.state import ADVISORY_LOCK_KEY as k; print(k)')
work=$(mktemp -d)
red() { sed -E 's#://[^@ ]*@#://***@#g'; }
q() { $B/psql "$LOCAL" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
# Target connection as PG* variables, set only inside a subshell: exported for
# the whole script they would also redirect the LOCAL connections.
target_env=$($PY - <<'PY'
import os, shlex
from urllib.parse import urlsplit, parse_qsl, unquote
u = urlsplit(os.environ['TARGET_DATABASE_URL'])
env = {'PGHOST': u.hostname, 'PGPORT': str(u.port or 5432), 'PGDATABASE': u.path.lstrip('/'),
       'PGUSER': unquote(u.username or ''), 'PGPASSWORD': unquote(u.password or '')}
names = {'sslmode': 'PGSSLMODE', 'channel_binding': 'PGCHANNELBINDING', 'options': 'PGOPTIONS'}
for k, v in parse_qsl(u.query):
    if k not in names:
        raise SystemExit(f"unsupported TARGET_DATABASE_URL parameter: {k}")
    env[names[k]] = v
print('; '.join(f'export {k}={shlex.quote(v)}' for k, v in env.items() if v))
PY
)
tq() { ( eval "$target_env"; $B/psql -XAtq -v ON_ERROR_STOP=1 "$@" ) }

# Hold the SOURCE's pipeline lock (the same session lock incremental_load
# takes) from the gates through both dumps: checking once and then dumping
# would let a run commit between the raw and edw dumps, and the target would
# get periods marked published for data its warehouse copy doesn't contain.
coproc $B/psql "$LOCAL" -XAtq
source_pid=$!
# Quit the lock session only while it is alive: writing to an exited
# coprocess raises SIGPIPE, which turned a committed promote into exit 141.
release_source() { if kill -0 $source_pid 2>/dev/null; then print -rp -- '\q'; wait $source_pid 2>/dev/null || true; fi }
trap 'release_source; rm -rf "$work"' EXIT
print -rp -- "select pg_try_advisory_lock($LOCK_KEY);"
read -t 30 -rp got || got=
[[ $got == t ]] || { echo "A pipeline run holds the lock on the source; wait for it."; exit 1; }

# Gates. Values are captured before comparing so a failed query stops the
# script (errexit) instead of reading as a failed check with the wrong reason.
server=$(tq -c "show server_version_num")
client=$($B/psql --version); client=${${client#*) }%%.*}   # "psql (PostgreSQL) 18.6 ..." -> 18
(( client >= server / 10000 )) \
  || { echo "psql $client is older than the target server ($server); set PG_BIN to a newer client."; exit 1; }
unpublished=$(q "select count(*) from public.pipeline_periods where not published")
[[ $unpublished == 0 ]] || { echo "The source has $unpublished unpublished period(s); run the pipeline there first."; exit 1; }
# The manager keys must match: app.* stores them (users, chat, bets, votes),
# so a key that changed meaning would silently re-attribute a member's history.
keys="select coalesce(string_agg(manager_key||'='||manager_name, ',' order by manager_key), '') from edw.dim_manager"
src_keys=$(q "$keys")
has_edw=$(tq -c "select to_regclass('edw.dim_manager') is not null")
if [[ $has_edw == t ]]; then   # a target with no warehouse yet has nothing to compare
  dst_keys=$(tq -c "$keys")
  [[ $dst_keys == "$src_keys" ]] \
    || { echo "edw.dim_manager differs between source and target - STOP (app.* references these keys)."; exit 1; }
fi

$B/pg_dump "$LOCAL" --clean --if-exists --no-owner --no-privileges -f "$work/raw.sql" \
  -t public.leagues -t public.teams -t public.rosters -t public.matchups \
  -t public.transactions -t public.draft_picks -t public.statistics -t 'public.pipeline_*'
$B/pg_dump "$LOCAL" --schema=edw --schema=meta_data --no-owner --no-privileges -f "$work/edw.sql"
release_source   # source dumped; its lock can go

{
  cat <<SQL
-- Same key space as the pipeline's session lock: a weekly run on the target
-- makes this fail instead of racing it, and none can start until we commit.
DO \$\$ BEGIN
  IF NOT pg_try_advisory_xact_lock($LOCK_KEY) THEN
    RAISE EXCEPTION 'a pipeline run is active on the target; try again after it finishes';
  END IF;
END \$\$;
SQL
  cat "$work/raw.sql"
  cat <<'SQL'
DROP SCHEMA IF EXISTS edw CASCADE;        -- also drops both app -> edw.dim_manager FKs
DROP SCHEMA IF EXISTS meta_data CASCADE;
SQL
  cat "$work/edw.sql"
  # A brand-new target has no app.* yet: db:migrate creates it, FKs included,
  # after this promote (drizzle 0000 references edw.dim_manager).
  cat <<'SQL'
DO $$ BEGIN
  IF to_regclass('app."user"') IS NOT NULL THEN
    ALTER TABLE app."user" ADD CONSTRAINT user_manager_key_dim_manager_manager_key_fk
      FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key);
  END IF;
  IF to_regclass('app.league_member') IS NOT NULL THEN
    ALTER TABLE app.league_member ADD CONSTRAINT league_member_manager_key_dim_manager_manager_key_fk
      FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key);
  END IF;
END $$;
SQL
} > "$work/all.sql"

echo "Promoting $(wc -c < "$work/all.sql" | tr -d ' ') bytes in one transaction..."
start=$(date +%s)
tq -1 -o /dev/null -f "$work/all.sql" 2>&1 | red
echo "Committed in $(( $(date +%s) - start ))s. Next: incremental_load.py --dry-run against the target."
