"""The record book counts finished seasons only (WP10 S10.9).

edw.vw_league_record_book listed three weeks of 2026 as "Fewest Points Scored
in a Season", and its career records added the season in progress through
mart_manager_performance. Season and career records now read a
finished_seasons CTE (a championship game with a winner); single-game records
keep the season in progress, since the extractor only loads completed weeks.

These tests read the view's SQL: the suite has no database to build it in.
"""
import re

from src.edw_schema.edw_etl_processor import EdwEtlProcessor

FINISHED = 'IN (SELECT season_year FROM finished_seasons)'

SEASON_RECORDS = [
    'Most Points Scored in a Season',
    'Fewest Points Scored in a Season',
    'Most Points Allowed in a Season (Unluckiest Team)',
    'Fewest Points Allowed in a Season (Luckiest Team)',
    'Best Regular-Season Record',
    'Worst Regular-Season Record',
    'Largest Point Differential in a Season',
    'Worst Point Differential in a Season',
    'Most Weeks as Top Scorer in a Season',
    'Most Trades in a Season',
    'Most Waiver Pickups in a Season',
    'Most FAAB Spent in a Season',
]

CAREER_RECORDS = [
    'Most Championships (Career)',
    'Most Championship Appearances (Career)',
    'Most Playoff Appearances (Career)',
    'Best Career Win Percentage (Min 3 Seasons)',
    'Most Career Points',
    'Most Career Points Allowed (Unluckiest Manager)',
    'Most Career Transactions',
]

SINGLE_GAME_RECORDS = [
    'Highest Single Game Score',
    'Lowest Single Game Score',
    'Biggest Blowout (Single Game)',
    'Closest Win (Single Game)',
    'Highest Scoring Game (Combined Points)',
    'Lowest Scoring Game (Combined Points)',
]


def _view_sql() -> str:
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    return proc._get_view_definition('vw_league_record_book')


def _parts():
    """The CTEs before all_records, and one SELECT per record name."""
    sql = _view_sql()
    head, records = sql.split('all_records AS (', 1)
    selects = {}
    for chunk in records.split('UNION ALL'):
        name = re.search(r"'([^']+)' as record_name", chunk)
        if name:
            selects[name.group(1)] = chunk
    return head, selects


def test_finished_seasons_need_a_decided_championship():
    head, _ = _parts()
    cte = head.split('finished_seasons AS (', 1)[1].split(')', 1)[0]
    assert 'FROM edw.fact_matchup' in cte
    assert 'is_championship' in cte and 'winner_team_key IS NOT NULL' in cte


def test_every_record_the_view_lists_is_covered_here():
    _, selects = _parts()
    assert set(selects) == set(SEASON_RECORDS + CAREER_RECORDS + SINGLE_GAME_RECORDS)


def test_season_records_count_finished_seasons_only():
    _, selects = _parts()
    for name in SEASON_RECORDS:
        assert FINISHED in selects[name], name


def test_career_records_count_finished_seasons_only():
    head, selects = _parts()
    for name in CAREER_RECORDS:
        chunk = selects[name]
        assert 'mart_manager_performance' not in chunk, name
        # Read from a finished-only CTE, or filtered in place.
        assert FINISHED in chunk or re.search(r'FROM career(_transactions)?\b', chunk), name
    for cte in ('career_team_seasons', 'career_playoffs', 'career_transactions'):
        body = head.split(f'{cte} AS (', 1)[1].split('\n                ),', 1)[0]
        assert FINISHED in body, cte


def test_single_game_records_keep_the_season_in_progress():
    _, selects = _parts()
    for name in SINGLE_GAME_RECORDS:
        assert 'finished_seasons' not in selects[name], name
