#!/usr/bin/env python3
"""Fill in the FAAB bids missing from transactions already loaded.

Until 2026-11 the transaction parser looked for faab_bid on each player's
transaction_data. Yahoo carries it on the transaction itself, so every
claim ever loaded holds NULL and the site's FAAB Efficiency read 0.0 for
every manager. The parser is fixed, and the loader now fills a NULL bid
from a later fetch (raw_loader.append_only fill_missing), so the current
season heals on its next weekly run. This script does the same for past
seasons: it re-fetches each season's transactions and fills only the bids,
inserting no rows that were not already held.

    export DATABASE_URL="$DB"     # env, not a flag: keeps the password out of ps
    python scripts/backfill_faab.py                   # every season on record
    python scripts/backfill_faab.py --seasons 2024 2025

One Yahoo call per transaction type per season, under the pipeline lock.
It writes public.transactions only. edw.fact_transaction re-reads that table
in full on every EDW refresh, so the bids reach the site with the next
pipeline run that refreshes transactions (any weekly run that loads a new
one), or straight away with any refresh run by hand.
"""
import argparse
import logging
import os
import sys

from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor  # noqa: E402
from src.pipeline import raw_loader  # noqa: E402
from src.pipeline.state import LockHeld, PipelineState  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('backfill_faab')

COUNT_BIDS = text(
    "SELECT count(faab_bid) FROM public.transactions WHERE league_id = :l")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--seasons', type=int, nargs='+',
                        help='Seasons to fill (default: every season on record)')
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
            with state.engine.connect() as conn:
                leagues = conn.execute(text(
                    "SELECT season::int, league_id FROM public.leagues "
                    "ORDER BY season::int")).fetchall()
            if args.seasons:
                leagues = [r for r in leagues if r[0] in set(args.seasons)]

            extractor = YahooFantasyExtractor()
            if not extractor.authenticate():
                logger.error("Yahoo authentication failed")
                return 1

            for season, league_id in leagues:
                rows = [t.__dict__ for t in
                        extractor.extract_transactions_for_league(league_id)]
                offered = sum(1 for r in rows if r['faab_bid'] is not None)
                with state.engine.begin() as conn:
                    before = conn.execute(COUNT_BIDS, {'l': league_id}).scalar()
                    new_rows = raw_loader.append_only(
                        conn, 'transactions', rows, 'transaction_id, player_id',
                        fill_missing=('faab_bid',))
                    after = conn.execute(COUNT_BIDS, {'l': league_id}).scalar()
                logger.info("%s %s: Yahoo has %d bids, %d filled, %d held in all, "
                            "%d new transactions", season, league_id, offered,
                            after - before, after, new_rows)
        finally:
            lock.__exit__(None, None, None)
    finally:
        state.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
