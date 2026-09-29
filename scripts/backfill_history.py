#!/usr/bin/env python3
"""Reload past seasons through the incremental pipeline, one season at a time.

This is the history backfill that fixed 2005-2025 in dev on 2026-09-28. The
original loads fetched each week's stats for whoever was rostered at the END
of the season (and, for 2016-2021, only players Yahoo still knew), never
stored weekly rosters, and left every championship week without stats. A
reload through scripts/incremental_load.py fetches each week's rosters and
then that week's stats for exactly those players.

Run it after any rebuild from the tracked snapshot, which predates the fix:

    python scripts/backfill_history.py --database-url "$DB"              # all past seasons
    python scripts/backfill_history.py --database-url "$DB" --seasons 2016 2017
    python scripts/backfill_history.py --database-url "$DB" --dry-run    # print the plan

About 8 minutes of Yahoo calls per season (~2.7 hours for all 21). Each
season is one ordinary incremental_load run - locked, verified, published or
restored - so it is safe to stop and resume.

It stops at the first failure on purpose: every run first republishes any
period an earlier run loaded but could not publish, so after one failed
season every later season would fail the same way in seconds.
"""
import argparse
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.incremental_load import current_season_year  # noqa: E402


def season_plan(rows, only=None):
    """(season, league_id, first_week, last_week) per season to reload.

    rows: (season, league_id, start_week, end_week) for the leagues of record.
    Weeks run from the league's own start week - 2006 and 2007 began in week
    2, and asking for week 1 is refused by incremental_load.
    """
    plan = []
    for season, league_id, start_week, end_week in rows:
        season = int(season)
        if only and season not in only:
            continue
        plan.append((season, league_id, int(start_week or 1), int(end_week)))
    return sorted(plan)


def leagues_of_record(database_url, before_season):
    from sqlalchemy import create_engine, text
    engine = create_engine(database_url.replace('postgres://', 'postgresql://', 1))
    try:
        with engine.connect() as conn:
            return conn.execute(text("""
                SELECT l.season, l.league_id, NULLIF(l.start_week, ''), NULLIF(l.end_week, '')
                FROM public.leagues l
                JOIN edw.dim_league dl
                  ON dl.league_id = l.league_id AND dl.season_year = l.season::int
                WHERE l.season::int < :before AND NULLIF(l.end_week, '') IS NOT NULL
            """), {'before': before_season}).fetchall()
    finally:
        engine.dispose()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--database-url', default=os.getenv('DATABASE_URL'))
    p.add_argument('--seasons', type=int, nargs='+',
                   help='Only these seasons (default: every past season)')
    p.add_argument('--dry-run', action='store_true', help='Print the plan and exit')
    args = p.parse_args(argv)
    if not args.database_url:
        p.error('--database-url or DATABASE_URL is required')

    plan = season_plan(leagues_of_record(args.database_url, current_season_year()),
                       set(args.seasons) if args.seasons else None)
    for season, league_id, first, last in plan:
        print(f"{season}  {league_id}  weeks {first}-{last}")
    if args.dry_run or not plan:
        return 0

    loader = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'incremental_load.py')
    for season, league_id, first, last in plan:
        started = time.time()
        rc = subprocess.run([
            sys.executable, loader, '--database-url', args.database_url,
            '--season', str(season), '--league-id', league_id,
            '--weeks', *[str(w) for w in range(first, last + 1)],
        ]).returncode
        print(f"SEASON {season} exit={rc} secs={int(time.time() - started)}", flush=True)
        if rc != 0:
            print(f"Stopped at {season}: every later season would repeat this failure "
                  "(runs republish held-back periods first). Fix it, then rerun with "
                  f"--seasons {' '.join(str(s) for s, *_ in plan if s >= season)}")
            return rc
    print("BACKFILL DONE")
    return 0


if __name__ == '__main__':
    sys.exit(main())
