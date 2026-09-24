#!/usr/bin/env python3
"""The weekly incremental load - and, with flags, the backfill/repair tool.

The database is the state: this job asks the warehouse which periods are
verified complete, asks Yahoo which weeks are complete, and loads the
difference through scoped upserts. Backfill, weekly updates, and
corrections are the same mechanism with different scopes.

    Weekly (scheduled):   incremental_load.py
    Repair a season:      incremental_load.py --season 2007 --weeks 15 16
    Stats-only repair:    incremental_load.py --season 2019 --stats-only
    Show the gap:         incremental_load.py --dry-run

Safety properties (enforced by the database, not the scheduler):
  - pg_try_advisory_lock, fail-fast: a second concurrent run exits 0
    with "another run is active" (use --wait-for-lock to queue).
  - The full raw delta + period flags commit in one transaction.
  - EDW publication is snapshot-guarded: a failed refresh atomically
    restores the previous complete generation, and the run starts by
    re-publishing any raw-complete-but-unpublished periods (no Yahoo
    calls) before extracting anything new.
"""
import argparse
import glob
import gzip
import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.comprehensive_data_extractor import (  # noqa: E402
    YahooFantasyExtractor, completed_weeks, week_is_complete)
from src.pipeline import raw_loader, publish as pub  # noqa: E402
from src.pipeline.state import PipelineState, LockHeld  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('incremental_load')

RUNS_DIR = 'data/runs'
MIGRATIONS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'src', 'edw_schema', 'migrations')

# Raw tables -> EDW refresh triggers, matching EdwEtlProcessor's
# EDW_PROCESSING_STRATEGIES keys.
ALL_OPERATIONAL_TABLES = {'leagues', 'teams', 'rosters', 'matchups',
                          'transactions', 'draft_picks', 'statistics'}


def is_fantasy_season(now=None):
    """Aug 18 - Jan 18. (Harvested from the retired weekly_extractor.)"""
    now = now or datetime.now()
    year = now.year
    if now.month == 1:
        return now <= datetime(year, 1, 18)
    return now >= datetime(year, 8, 18)


def current_season_year(now=None):
    now = now or datetime.now()
    return now.year - 1 if now.month <= 7 else now.year


def local_hour_matches(hour, tz_name, now=None):
    """True when it is currently `hour` in `tz_name`.

    GitHub cron only speaks UTC and does not follow daylight saving, so a
    fixed UTC time drifts an hour against Mountain Time mid-season. The
    workflow schedules both candidate UTC hours and this gate lets exactly
    the right one through, keeping the real-world slot fixed year-round.
    """
    tz = ZoneInfo(tz_name)
    now = now.astimezone(tz) if now else datetime.now(tz)
    return now.hour == hour


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--database-url', default=os.getenv('DATABASE_URL'),
                   help='Postgres URL (default: DATABASE_URL env var)')
    p.add_argument('--season', type=int,
                   help='Season to load (default: the current season)')
    p.add_argument('--weeks', type=int, nargs='+',
                   help='Explicit weeks (default: the computed gap)')
    p.add_argument('--league-id',
                   help='Explicit league (default: resolved from Yahoo for '
                        'the current season, or public.leagues for --season)')
    p.add_argument('--stats-only', action='store_true',
                   help='Extract/load only player statistics for the scope')
    p.add_argument('--full', action='store_true',
                   help='Ignore state; treat every completed week as the gap')
    p.add_argument('--dry-run', action='store_true',
                   help='Print the computed gap and exit without loading')
    p.add_argument('--wait-for-lock', action='store_true',
                   help='Queue behind a running pipeline instead of exiting')
    p.add_argument('--force', action='store_true',
                   help='Run even outside the fantasy season window')
    p.add_argument('--reload-window', type=int, default=2,
                   help='Recent complete weeks to re-fetch for stat '
                        'corrections (default 2)')
    p.add_argument('--require-local-hour', type=int, metavar='HH',
                   help='Exit 0 unless it is this hour in --local-tz. Lets a '
                        'UTC-only scheduler hold a fixed local time across DST')
    p.add_argument('--local-tz', default='America/Denver',
                   help='Timezone for --require-local-hour '
                        '(default America/Denver)')
    return p.parse_args(argv)


def resolve_league(extractor, state, args):
    """Return (league_id, season, league_obj_or_None, is_new_league)."""
    season = args.season or current_season_year()

    if args.league_id:
        league_id = args.league_id
    elif args.season:
        from sqlalchemy import text
        with state.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT league_id FROM public.leagues WHERE season = :s"),
                {'s': str(season)}).fetchone()
        if not row:
            raise SystemExit(f"no league found in public.leagues for season {season}")
        league_id = row[0]
    else:
        ids = extractor.game.league_ids(year=season)
        if not ids:
            logger.info("No Yahoo league for season %s - nothing to do", season)
            return None, season, None, False
        if len(ids) > 1:
            logger.warning("Multiple leagues for %s: %s - using the first; "
                           "pass --league-id to choose", season, ids)
        league_id = str(ids[0])

    from sqlalchemy import text
    with state.engine.connect() as conn:
        known = conn.execute(text(
            "SELECT 1 FROM public.leagues WHERE league_id = :l"),
            {'l': league_id}).fetchone() is not None
    league = extractor.game.to_league(league_id)
    return league_id, season, league, not known


def yahoo_completed_weeks(league, already_complete=()) -> list:
    """Weeks Yahoo reports as finished.

    Weeks we have already recorded complete stay complete - play does not
    un-finish - so only the candidates beyond them are probed, and each is
    confirmed with Yahoo's own per-week scoreboard status. That keeps this
    to one or two API calls while removing the dependency on when Yahoo
    advances current_week: a Tuesday-morning run must not read Monday
    night's finished week as still in progress just because the season
    pointer has not moved yet.
    """
    settings = league.settings()
    current_week = int(settings.get('current_week', 1))
    end_week = int(settings.get('end_week', 17))

    settled = set(already_complete)
    # current_week itself can be finished (Yahoo may not have advanced yet),
    # so it is a candidate too.
    candidates = [w for w in range(1, min(current_week, end_week) + 1)
                  if w not in settled]

    confirmed = set(settled)
    for week in candidates:
        if week_is_complete(league, week):
            confirmed.add(week)

    if confirmed:
        return sorted(confirmed)

    # No scoreboard answered (preseason, or an unexpected shape): fall back
    # to the settings-level inference rather than reporting nothing.
    return completed_weeks(current_week, end_week,
                           settings.get('is_finished', ''))


def extract_scope(extractor, league_id, weeks, stats_only, is_new,
                  since, league_info_rows):
    """Fetch the scoped delta from Yahoo. Errors raise - never empty-on-fail.

    Only keys the run actually fetched are present. load_delta treats an
    absent key as "not fetched" and leaves that table untouched; a key
    present but empty means "fetched, genuinely nothing". Pre-seeding every
    key made --stats-only delete the period's matchups and rosters.
    """
    data = {}

    # Rosters first: each week's statistics are fetched for the players on
    # THAT week's rosters, not whoever is rostered today (see
    # extract_statistics_for_league). A --stats-only repair fetches them for
    # the same reason but does not load them - it must not touch rosters.
    rosters = [r.__dict__ for r in
               extractor.extract_rosters_for_league(league_id, weeks)]
    players_by_week = {}
    for r in rosters:
        players_by_week.setdefault(r['week'], set()).add(r['player_id'])

    data['statistics'] = [s.__dict__ for s in
                          extractor.extract_statistics_for_league(
                              league_id, weeks, players_by_week=players_by_week)]
    if stats_only:
        return data

    data['leagues'] = league_info_rows
    data['teams'] = [t.__dict__ for t in
                     extractor.extract_teams_for_league(league_id)]
    data['rosters'] = rosters
    data['matchups'] = extractor.extract_matchups_for_league(league_id, weeks)

    txns = extractor.extract_transactions_for_league(league_id)
    if since is not None:
        txns = [t for t in txns if t.timestamp >= since.replace(tzinfo=None)]
    data['transactions'] = [t.__dict__ for t in txns]

    if is_new:
        data['draft_picks'] = [d.__dict__ for d in
                               extractor.extract_draft_for_league(league_id)]
    return data


RAW_TABLES = ('leagues', 'teams', 'rosters', 'matchups', 'statistics',
              'transactions', 'draft_picks')


def require_warehouse(engine):
    """Fail with a readable message when this database is not a warehouse.

    Checked BEFORE migrations, which target edw and would otherwise abort
    with a bare 'schema "edw" does not exist'; and before ensure_constraints,
    which indexes public.leagues and surfaced as
    'relation "public.leagues" does not exist'. The cutover - pointing the
    pipeline at a fresh target - is exactly when the operator needs to be
    told what is missing rather than handed a driver traceback.
    """
    from sqlalchemy import text
    with engine.connect() as conn:
        has_edw = conn.execute(text(
            "SELECT 1 FROM information_schema.schemata "
            "WHERE schema_name = 'edw'")).scalar()
        missing = [t for t in RAW_TABLES
                   if not conn.execute(text("SELECT to_regclass(:t)"),
                                       {'t': f'public.{t}'}).scalar()]
    problems = []
    if not has_edw:
        problems.append("the edw schema is absent")
    if missing:
        problems.append("raw tables are absent: "
                        + ', '.join(f'public.{t}' for t in missing))
    if problems:
        raise RuntimeError(
            "This database is not a built warehouse (" + "; ".join(problems)
            + "). Load a baseline snapshot and build the EDW first (see "
              "RUNBOOK, 'Full warehouse rebuild'); the incremental pipeline "
              "extends an existing warehouse, it does not create one.")


def pending_migrations(engine):
    """Migration filenames this database has not recorded. Read-only."""
    from sqlalchemy import text
    with engine.connect() as conn:
        exists = conn.execute(text(
            "SELECT to_regclass('public.pipeline_migrations')")).scalar()
        done = set()
        if exists:
            done = {r[0] for r in conn.execute(text(
                "SELECT filename FROM public.pipeline_migrations"))}
    return [os.path.basename(p)
            for p in sorted(glob.glob(os.path.join(MIGRATIONS_DIR, '*.sql')))
            if os.path.basename(p) not in done]


def apply_pending_migrations(engine):
    """Apply edw migrations this database has not yet recorded.

    The incremental path depends on constraints these migrations add; a
    database without them fails mid-run with an opaque "no unique or
    exclusion constraint matching the ON CONFLICT specification". Each
    migration is idempotent, but applied-tracking keeps runs cheap and
    makes the state auditable.

    Call this with the pipeline advisory lock HELD. Schema migration is
    pipeline work: two runs starting together against an unmigrated
    database would otherwise execute the same backfill concurrently.

    Returns the number of migrations applied by this call.
    """
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.pipeline_migrations (
                filename    text PRIMARY KEY,
                applied_at  timestamptz NOT NULL DEFAULT now()
            )
        """))
    with engine.connect() as conn:
        done = {r[0] for r in conn.execute(text(
            "SELECT filename FROM public.pipeline_migrations"))}

    applied = 0
    for path in sorted(glob.glob(os.path.join(MIGRATIONS_DIR, '*.sql'))):
        name = os.path.basename(path)
        if name in done:
            continue
        logger.info("Applying migration %s", name)
        with open(path) as f:
            sql = f.read()
        # Each file manages its own BEGIN/COMMIT, so run it outside a
        # SQLAlchemy transaction and record it separately.
        #
        # Executed on a raw cursor with NO parameter argument, the only form
        # that passes arbitrary SQL through untouched: text() reads :token as
        # a bind parameter, and exec_driver_sql hands psycopg2 an empty
        # parameter set, which makes it interpret % - so a migration
        # containing LIKE '%.p.%' dies with "KeyError: 0".
        with engine.connect().execution_options(
                isolation_level='AUTOCOMMIT') as conn:
            cursor = conn.connection.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO public.pipeline_migrations (filename) "
                "VALUES (:f) ON CONFLICT (filename) DO NOTHING"),
                {'f': name})
        applied += 1
    return applied


def write_run_snapshot(data, league_id, season):
    """Gzipped, pre-sanitized audit-trail snapshot. Never pipeline state."""
    os.makedirs(RUNS_DIR, exist_ok=True)
    from scripts.sanitize_snapshot import sanitize
    sanitize(data)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = os.path.join(RUNS_DIR, f'run_{season}_{ts}.json.gz')
    tmp = path + '.tmp'
    with gzip.open(tmp, 'wt') as f:
        json.dump(data, f, default=str)
    os.replace(tmp, path)
    logger.info("Run snapshot: %s", path)
    return path


def make_edw_refresh(database_url, changed_tables):
    """The real EDW refresh callable: DB-mode (data_file=None) so the
    processor reads public.*, never a run-scoped JSON. The processor
    requires explicit connect() and load_data() before processing
    (load_data with no data_file takes the from-database path)."""
    def refresh():
        from src.edw_schema.edw_etl_processor import EdwEtlProcessor
        processor = EdwEtlProcessor(database_url=database_url, data_file=None)
        if not processor.connect():
            return False
        if not processor.load_data():
            return False
        # The fact transforms resolve dimension surrogate keys through an
        # in-memory cache that only run_etl/load_dimensions builds; without
        # it every fact row is skipped and the processor reports success
        # with zero rows. Build it explicitly for the incremental path.
        processor.cache_dimension_mappings()
        return processor.process_incremental_edw(set(changed_tables))
    return refresh


def run(args) -> int:
    if not args.database_url:
        logger.error("DATABASE_URL is required")
        return 2

    # Cheapest gate first: costs no database connection and no Yahoo call.
    if args.require_local_hour is not None and not local_hour_matches(
            args.require_local_hour, args.local_tz):
        logger.info("Not %02d:00 in %s - this is the off-DST twin of the "
                    "scheduled run. Exiting cleanly.",
                    args.require_local_hour, args.local_tz)
        return 0

    if not (args.force or args.season or args.weeks or is_fantasy_season()):
        logger.info("Outside the fantasy season window (Aug 18 - Jan 18); "
                    "use --force to run anyway. Exiting cleanly.")
        return 0

    state = PipelineState(args.database_url)
    try:
        try:
            lock_ctx = state.lock(wait=args.wait_for_lock)
            lock_ctx.__enter__()
        except LockHeld:
            logger.info("Another pipeline run is active - nothing to do.")
            return 0

        try:
            # Under the lock, and never on a dry run: the incremental path's
            # upserts need the constraints these migrations add, but a run
            # that promises to change nothing must not migrate a database.
            # The precondition applies to both paths: inspecting a
            # prospective target with --dry-run is the safest thing an
            # operator can do before pointing the pipeline at it, and it
            # must report the problem rather than die inside a query.
            try:
                require_warehouse(state.engine)
            except RuntimeError as e:
                # A precondition, not a crash: say what is wrong once.
                logger.error("%s", e)
                return 2

            if args.dry_run:
                pending = pending_migrations(state.engine)
                if pending:
                    logger.warning(
                        "%d migration(s) not applied to this database (%s). "
                        "A real run would apply them first.",
                        len(pending), ', '.join(pending))
            else:
                apply_pending_migrations(state.engine)

            return _run_locked(args, state)
        finally:
            lock_ctx.__exit__(None, None, None)
    finally:
        state.close()


def _run_locked(args, state) -> int:
    # A dry run reports; it does not prepare. Creating the pipeline tables
    # and indexes here made --dry-run write to a database it promised to
    # leave alone.
    if not args.dry_run:
        state.ensure_schema()
        with state.engine.begin() as conn:
            raw_loader.ensure_constraints(conn)
        pub.ensure_edw_serial_defaults(state.engine)
        pub.check_inbound_foreign_keys(state.engine)
    else:
        # Read-only: inspecting a prospective cutover target with --dry-run
        # is exactly when a missing app -> edw key needs to be reported.
        pub.check_inbound_foreign_keys(state.engine, validate=False)
        if not state.schema_exists():
            # Nothing to report a gap against, and a dry run must not create it.
            logger.warning("This database has no pipeline state tables yet; a "
                           "real run would create them. Reporting Yahoo-side "
                           "state only.")

    # 1. Repair publication BEFORE anything else: raw-complete periods the
    #    site cannot see yet need EDW work only - no Yahoo calls.
    stale = state.unpublished_raw()
    if stale and not args.dry_run:
        logger.info("Reconciliation: %d raw-complete period(s) unpublished "
                    "(raw_version > edw_version) - repairing EDW first", len(stale))
        pub.publish(state, stale,
                    make_edw_refresh(args.database_url, ALL_OPERATIONAL_TABLES))

    # 2. Authenticate and resolve scope.
    extractor = YahooFantasyExtractor()
    if not extractor.authenticate():
        logger.error("Yahoo authentication failed")
        return 1

    league_id, season, league, is_new = resolve_league(extractor, state, args)
    if league_id is None:
        return 0

    settings = league.settings()
    if settings.get('draft_status') != 'postdraft' and not args.weeks:
        logger.info("League %s is %s - clean no-op until the draft completes.",
                    league_id, settings.get('draft_status'))
        # A dry run reports this and stops: writing a ledger row here made
        # --dry-run mutate the database every off-season heartbeat, and fail
        # outright where the pipeline tables do not exist yet.
        if not args.dry_run:
            run_id = state.start_run(season)
            state.finish_run(run_id, 'success', weeks_loaded=[],
                             row_counts={'noop': 'predraft'})
        return 0

    # Weeks already recorded raw-complete need no re-confirmation from Yahoo.
    completed = yahoo_completed_weeks(
        league, state.raw_complete_weeks(league_id, season))

    # 3. Compute the gap (or take explicit scope).
    if args.weeks:
        gap = {'extract': sorted(args.weeks), 'publish_repair': []}
    elif args.full:
        gap = {'extract': completed, 'publish_repair': []}
    else:
        gap = state.compute_gap(league_id, season, completed,
                                reload_window=args.reload_window)

    logger.info("Season %s league %s: yahoo-complete=%s gap=%s",
                season, league_id, completed, gap)

    if args.dry_run:
        print(json.dumps({'league_id': league_id, 'season': season,
                          'is_new_league': is_new,
                          'yahoo_completed_weeks': completed,
                          'unpublished_raw': [list(p) for p in state.unpublished_raw()],
                          'gap': gap}, indent=2))
        return 0

    weeks = gap['extract']
    if not weeks and not is_new:
        logger.info("Nothing to load - all completed periods are published. "
                    "Recording clean no-op.")
        run_id = state.start_run(season)
        state.finish_run(run_id, 'success', weeks_loaded=[],
                         row_counts={'noop': 'up-to-date'})
        return 0

    run_id = state.start_run(season)
    try:
        # 4. Extract the scoped delta (draft-only for a new pre-week-1 league).
        league_info_rows = [{
            'league_id': league_id, 'name': settings.get('name'),
            'season': str(season), 'game_code': settings.get('game_code', 'nfl'),
            'game_id': str(extractor.game.game_id()),
            'num_teams': int(settings.get('num_teams', 0)),
            'current_week': str(settings.get('current_week', '')),
            'start_week': str(settings.get('start_week', '')),
            'end_week': str(settings.get('end_week', '')),
            'league_type': settings.get('league_type', 'private'),
            'draft_status': settings.get('draft_status'),
            'is_pro_league': str(settings.get('is_pro_league', '0')) == '1',
            'is_cash_league': str(settings.get('is_cash_league', '0')) == '1',
            'url': settings.get('url'), 'logo_url': settings.get('logo_url') or '',
            'extracted_at': datetime.now(),
        }]
        since = state.last_successful_run_start()
        data = extract_scope(extractor, league_id, weeks, args.stats_only,
                             is_new, since, league_info_rows)

        # 5. One transaction: the full raw delta + period flags.
        with state.engine.begin() as conn:
            counts = raw_loader.load_delta(conn, state, league_id, season,
                                           weeks, data)

        # 6. Publish: snapshot-guarded EDW refresh + verification gate.
        #    Only periods the load actually recorded are published. A week
        #    that fetched nothing has no pipeline_periods row, and publishing
        #    it would trip the gate's unverifiable-period check with an error
        #    about bookkeeping rather than the real condition.
        recorded = state.recorded_weeks(league_id, season, weeks)
        empty = [w for w in weeks if w not in recorded]
        if empty:
            logger.warning("No data fetched for week(s) %s - nothing to "
                           "publish for them", empty)

        # An empty period list is NOT the same as nothing to publish: a new
        # league's draft-only load (draft done, week 1 not yet complete)
        # records no period while still landing leagues, teams and draft
        # picks, and those must reach the EDW. Only a run that loaded
        # nothing at all skips publication.
        if not recorded and not any(counts.values()):
            logger.warning("Nothing was loaded for weeks %s; skipping publish.",
                           weeks)
            state.finish_run(run_id, 'success', weeks_loaded=[],
                             row_counts={'noop': 'no-data', 'weeks': weeks})
            return 0

        changed = {t for t, n in counts.items() if n} or ALL_OPERATIONAL_TABLES
        periods = [(league_id, season, w) for w in sorted(recorded)]
        pub.publish(state, periods,
                    make_edw_refresh(args.database_url, changed))

        # 7. Audit-trail snapshot (sanitized, gzipped; never load-bearing).
        write_run_snapshot(data, league_id, season)

        if not state.lock_still_held():
            # Not fatal - the work is committed and verified - but the run
            # was not serialised for its whole duration, so say so.
            logger.warning("Advisory lock was lost during this run (the "
                           "database closed its connection). The load "
                           "completed, but a concurrent run was possible.")
        state.finish_run(run_id, 'success', weeks_loaded=weeks,
                         row_counts=counts)
        logger.info("Run %d complete: weeks %s, %s", run_id, weeks, counts)
        return 0

    except Exception as e:
        state.finish_run(run_id, 'failed', error=str(e)[:2000])
        logger.exception("Run %d failed", run_id)
        return 1


if __name__ == '__main__':
    sys.exit(run(parse_args()))
