#!/usr/bin/env python3
"""Load every season's full Yahoo league settings: scoring, bonuses, lineup.

The weekly load captures the current season's settings from migration 012
on. Past seasons need one settings call each, which this script makes. It
also replaces the playoff format migration 011 inferred from the brackets
with Yahoo's own values; 2007's field was recorded as 8 teams (the
consolation bracket counted in) where Yahoo says 4.

    export DATABASE_URL="$DB"     # env, not a flag: keeps the password out of ps
    python scripts/backfill_league_settings.py                  # every season on record
    python scripts/backfill_league_settings.py --seasons 2007 2025
    python scripts/backfill_league_settings.py --no-publish     # raw tables only

One Yahoo call per season, under the pipeline lock. Applies pending
migrations first, writes public.leagues and the three settings tables, then
publishes them to the warehouse with the usual snapshot guard (a failed
refresh restores the previous generation).
"""
import argparse
import logging
import os
import sys
from datetime import datetime

from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.incremental_load import (  # noqa: E402
    apply_pending_migrations, league_for_season, make_edw_refresh)
from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor  # noqa: E402
from src.extractors.league_settings import parse_league_settings  # noqa: E402
from src.pipeline import publish as pub, raw_loader  # noqa: E402
from src.pipeline.state import LockHeld, PipelineState  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('backfill_league_settings')

# Columns set on the held league row. Yahoo is the authority for a finished
# season, so its values replace the bracket inference; one it doesn't send
# is stored NULL here and the warehouse keeps what it has.
LEAGUE_COLS = ['playoff_start_week', 'num_playoff_teams', 'trade_end_date',
               *raw_loader.LEAGUE_SETTINGS_COLS]


def update_league_row(conn, league_id: str, league: dict) -> int:
    sets = ', '.join(f'{c} = :{c}' for c in LEAGUE_COLS)
    return conn.execute(
        text(f'UPDATE public.leagues SET {sets} WHERE league_id = :league_id'),
        {**{c: league.get(c) for c in LEAGUE_COLS}, 'league_id': league_id}).rowcount


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--seasons', type=int, nargs='+',
                        help='Seasons to load (default: every season on record)')
    parser.add_argument('--no-publish', action='store_true',
                        help='Write the raw tables only; the next pipeline run publishes')
    args = parser.parse_args(argv)

    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL is required")
        return 2

    state = PipelineState(database_url)
    try:
        try:
            lock = state.lock(wait=True)
            lock.__enter__()
        except LockHeld:
            logger.error("Could not take the pipeline lock")
            return 1
        try:
            applied = apply_pending_migrations(state.engine)
            if applied:
                logger.info("Applied %d pending migration(s)", applied)

            # Leagues of record only (see backfill_faab).
            with state.engine.connect() as conn:
                seasons = args.seasons or [r[0] for r in conn.execute(text(
                    "SELECT DISTINCT season::int FROM public.leagues ORDER BY 1"))]
                leagues = [(s, league_for_season(conn, s)) for s in sorted(seasons)]

            extractor = YahooFantasyExtractor()
            if not extractor.authenticate():
                logger.error("Yahoo authentication failed")
                return 1

            for season, league_id in leagues:
                league = extractor.game.to_league(league_id)
                parsed = parse_league_settings(
                    league.yhandler.get_settings_raw(league_id), league_id)
                with state.engine.begin() as conn:
                    if not update_league_row(conn, league_id, parsed['league']):
                        raise RuntimeError(f"{league_id} is not in public.leagues")
                    written = raw_loader.replace_league_settings(
                        conn, league_id, parsed, extracted_at=datetime.now())
                lg = parsed['league']
                logger.info(
                    "%s %s: %d scoring rules, %d bonuses, %d lineup slots (%d rows); "
                    "playoffs week %s, %s teams; waivers %s", season, league_id,
                    len(parsed['scoring']), len(parsed['bonuses']),
                    len(parsed['roster_slots']), written, lg['playoff_start_week'],
                    lg['num_playoff_teams'], lg['waiver_type'])

            if not args.no_publish:
                pub.publish(state, [], make_edw_refresh(
                    database_url, {'leagues', 'league_settings'}))
                logger.info("Published league settings to the warehouse")
        finally:
            lock.__exit__(None, None, None)
    finally:
        state.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
