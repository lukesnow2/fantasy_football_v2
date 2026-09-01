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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.comprehensive_data_extractor import (  # noqa: E402
    YahooFantasyExtractor, completed_weeks)
from src.pipeline import raw_loader, publish as pub  # noqa: E402
from src.pipeline.state import PipelineState, LockHeld  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('incremental_load')

RUNS_DIR = 'data/runs'

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


def yahoo_completed_weeks(league) -> list:
    settings = league.settings()
    return completed_weeks(int(settings.get('current_week', 1)),
                           int(settings.get('end_week', 17)),
                           settings.get('is_finished', ''))


def extract_scope(extractor, league_id, weeks, stats_only, is_new,
                  since, league_info_rows):
    """Fetch the scoped delta from Yahoo. Errors raise - never empty-on-fail."""
    data = {'leagues': [], 'teams': [], 'rosters': [], 'matchups': [],
            'transactions': [], 'draft_picks': [], 'statistics': []}

    data['statistics'] = [s.__dict__ for s in
                          extractor.extract_statistics_for_league(league_id, weeks)]
    if stats_only:
        return data

    data['leagues'] = league_info_rows
    data['teams'] = [t.__dict__ for t in
                     extractor.extract_teams_for_league(league_id)]
    data['rosters'] = [r.__dict__ for r in
                       extractor.extract_rosters_for_league(league_id, weeks)]
    data['matchups'] = extractor.extract_matchups_for_league(league_id, weeks)

    txns = extractor.extract_transactions_for_league(league_id)
    if since is not None:
        txns = [t for t in txns if t.timestamp >= since.replace(tzinfo=None)]
    data['transactions'] = [t.__dict__ for t in txns]

    if is_new:
        data['draft_picks'] = [d.__dict__ for d in
                               extractor.extract_draft_for_league(league_id)]
    return data


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
    processor reads public.*, never a run-scoped JSON."""
    def refresh():
        from src.edw_schema.edw_etl_processor import EdwEtlProcessor
        processor = EdwEtlProcessor(database_url, None)
        return processor.process_incremental_edw(set(changed_tables))
    return refresh


def run(args) -> int:
    if not args.database_url:
        logger.error("DATABASE_URL is required")
        return 2

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
            return _run_locked(args, state)
        finally:
            lock_ctx.__exit__(None, None, None)
    finally:
        state.close()


def _run_locked(args, state) -> int:
    state.ensure_schema()
    with state.engine.begin() as conn:
        raw_loader.ensure_constraints(conn)

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
        run_id = state.start_run(season)
        state.finish_run(run_id, 'success', weeks_loaded=[],
                         row_counts={'noop': 'predraft'})
        return 0

    completed = yahoo_completed_weeks(league)

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
        changed = {t for t, n in counts.items() if n} or ALL_OPERATIONAL_TABLES
        periods = [(league_id, season, w) for w in weeks]
        pub.publish(state, periods,
                    make_edw_refresh(args.database_url, changed))

        # 7. Audit-trail snapshot (sanitized, gzipped; never load-bearing).
        write_run_snapshot(data, league_id, season)

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
