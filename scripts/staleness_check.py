#!/usr/bin/env python3
"""Dead-man's check for the weekly pipeline. RUN THIS OUTSIDE GITHUB ACTIONS.

The failure mode that went unnoticed for 11 straight weeks was not a
failing run - it was NO run: GitHub silently disabled the scheduled
workflow, so there was never a failure event to alert on. Any watcher
scheduled inside the same repository's workflows shares that exact
failure domain (and a disabled workflow cannot re-enable itself).

This script therefore runs from an external host - a launchd/cron job on
a laptop, a scheduled cloud task, any box with psql access - and asks
one question: did a successful pipeline run finish recently? In-season
(Aug 18 - Jan 18 + a small grace window), staleness beyond
--max-age-days files a GitHub issue via `gh` (or just exits non-zero
with a message if gh is unavailable, for hosts that alert on exit code).

Usage:
    DATABASE_URL=postgres://... python scripts/staleness_check.py
    python scripts/staleness_check.py --max-age-days 8 --repo lukesnow2/fantasy_football_v2

Example launchd/cron (weekly, Fridays 9am - two days after the Wednesday
load should have landed):
    0 9 * * 5  cd ~/Desktop/the-league && DATABASE_URL=... .venv/bin/python scripts/staleness_check.py
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

DEFAULT_REPO = 'lukesnow2/fantasy_football_v2'


def in_season_window(now):
    """Aug 18 - Jan 25 (season + one week of grace for the last load)."""
    year = now.year
    if now.month == 1:
        return now.date() <= datetime(year, 1, 25).date()
    return now.date() >= datetime(year, 8, 18).date()


def file_issue(repo: str, age_days: float) -> bool:
    title = "Weekly pipeline has stopped running"
    body = (f"The dead-man's check found no successful pipeline run in "
            f"{age_days:.1f} days (in-season threshold exceeded).\n\n"
            "This is the silent failure mode: the workflow may be disabled "
            "(GitHub auto-disables scheduled workflows after 60 days of repo "
            "inactivity), a secret may have expired, or runs may be failing "
            "before the failure-issue step. Check: \n"
            f"- https://github.com/{repo}/actions (is the workflow enabled? "
            "are runs happening?)\n"
            "- `SELECT * FROM pipeline_runs ORDER BY run_id DESC LIMIT 5`\n\n"
            "The pipeline is self-healing once re-enabled: the next run "
            "loads every missing period automatically.")
    try:
        existing = subprocess.run(
            ['gh', 'issue', 'list', '--repo', repo, '--state', 'open',
             '--search', title, '--json', 'number'],
            capture_output=True, text=True, timeout=60)
        if existing.returncode == 0 and existing.stdout.strip() not in ('', '[]'):
            print(f"Open staleness issue already exists on {repo}; not duplicating.")
            return True
        created = subprocess.run(
            ['gh', 'issue', 'create', '--repo', repo, '--title', title,
             '--body', body], capture_output=True, text=True, timeout=60)
        if created.returncode == 0:
            print(f"Filed: {created.stdout.strip()}")
            return True
        print(f"gh issue create failed: {created.stderr.strip()}", file=sys.stderr)
    except FileNotFoundError:
        print("gh CLI not available on this host.", file=sys.stderr)
    except Exception as e:
        print(f"Issue filing failed: {e}", file=sys.stderr)
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database-url', default=os.getenv('DATABASE_URL'))
    p.add_argument('--max-age-days', type=float, default=8.0)
    p.add_argument('--repo', default=DEFAULT_REPO)
    p.add_argument('--always', action='store_true',
                   help='Check even outside the season window')
    args = p.parse_args()

    now = datetime.now(timezone.utc)
    if not args.always and not in_season_window(now):
        print("Off-season: staleness check skipped.")
        return 0

    if not args.database_url:
        print("DATABASE_URL required", file=sys.stderr)
        return 2

    url = args.database_url.replace('postgres://', 'postgresql://', 1)
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            last = conn.execute(text(
                "SELECT max(finished_at) FROM public.pipeline_runs "
                "WHERE status = 'success'")).scalar()
    finally:
        engine.dispose()

    if last is None:
        print("No successful run has EVER been recorded - treating as stale.")
        age_days = float('inf')
    else:
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age = now - last
        age_days = age.total_seconds() / 86400
        print(f"Last successful run finished {age_days:.1f} days ago ({last}).")

    if age_days <= args.max_age_days:
        print("Pipeline is healthy.")
        return 0

    print(f"STALE: exceeds {args.max_age_days} days.", file=sys.stderr)
    file_issue(args.repo, age_days if age_days != float('inf') else 999)
    return 1


if __name__ == '__main__':
    sys.exit(main())
