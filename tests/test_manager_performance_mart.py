"""mart_manager_performance: no value is NULL, not 0 (metric card audit, 2026-11-17).

Year-to-year swing (season_consistency_score) is the spread of a manager's
finished-season win rates. With one finished season there is no spread, but
the mart stored 0, which printed as "±.000", the steadiest in the league.
Playoff win % fell back to 0 for a manager with no bracket games, which
printed as 0.0%. Both are NULL now, and the pages print them as no value.

These build a three-manager league in a scratch database and run the real SQL.
"""
import subprocess
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from src.edw_schema.edw_etl_processor import EdwEtlProcessor

TEST_DB = 'manager_performance_mart_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'

# league -> season
LEAGUES = {1: 2023, 2: 2024, 3: 2025}
# team -> (league, manager)
TEAMS = {
    11: (1, 'Ann'), 12: (1, 'Bo'),
    21: (2, 'Ann'), 22: (2, 'Bo'), 23: (2, 'Cy'),
    31: (3, 'Ann'), 33: (3, 'Cy'),
}
# (league, team1, team2, winner, type). 2025 has no title game yet.
GAMES = [
    (1, 11, 12, 11, 'regular'),
    (1, 11, 12, 11, 'championship'),
    (2, 21, 23, 21, 'regular'),
    (2, 22, 21, 22, 'regular'),
    (2, 21, 22, 22, 'championship'),
    (3, 33, 31, 33, 'regular'),
]


@pytest.fixture(scope='module')
def rows():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    eng = create_engine(TEST_URL)
    with eng.begin() as conn:
        conn.execute(text("CREATE SCHEMA edw"))
        conn.execute(text("""CREATE TABLE edw.dim_team (team_key int, league_key int,
            manager_name text, is_active boolean)"""))
        conn.execute(text("CREATE TABLE edw.dim_league (league_key int, season_year int)"))
        conn.execute(text("""CREATE TABLE edw.dim_manager (manager_key serial, manager_name text,
            is_active boolean, include_in_analysis boolean)"""))
        conn.execute(text("""CREATE TABLE edw.fact_matchup (matchup_key serial, league_key int,
            season_year int, team1_key int, team2_key int, team1_points numeric,
            team2_points numeric, winner_team_key int, is_tie boolean, matchup_type text,
            is_championship boolean)"""))
        conn.execute(text("""CREATE TABLE edw.fact_team_performance (team_key int,
            season_year int, is_playoff_team boolean)"""))
        conn.execute(text("""CREATE TABLE edw.fact_transaction (transaction_key serial,
            to_manager_key int, to_team_key int, player_key int, transaction_week int,
            faab_bid numeric, transaction_date date)"""))
        conn.execute(text("""CREATE TABLE edw.fact_roster (team_key int, player_key int,
            week_key int, weekly_points numeric, is_starter boolean)"""))
        conn.execute(text("CREATE TABLE edw.dim_week (week_key int, week_number int)"))
        conn.execute(text("""CREATE TABLE edw.fact_draft (draft_key serial, manager_key int,
            overall_pick int, season_year int, season_points numeric)"""))

        for league, season in LEAGUES.items():
            conn.execute(text("INSERT INTO edw.dim_league VALUES (:l, :s)"), {'l': league, 's': season})
        for name in ('Ann', 'Bo', 'Cy'):
            conn.execute(text("""INSERT INTO edw.dim_manager (manager_name, is_active, include_in_analysis)
                VALUES (:n, TRUE, TRUE)"""), {'n': name})
        for team, (league, name) in TEAMS.items():
            conn.execute(text("INSERT INTO edw.dim_team VALUES (:t, :l, :n, TRUE)"),
                         {'t': team, 'l': league, 'n': name})
        for (league, t1, t2, winner, kind) in GAMES:
            conn.execute(text("""INSERT INTO edw.fact_matchup (league_key, season_year, team1_key,
                team2_key, team1_points, team2_points, winner_team_key, is_tie, matchup_type,
                is_championship) VALUES (:l, :s, :t1, :t2, 100, 90, :w, FALSE, :k, :c)"""),
                {'l': league, 's': LEAGUES[league], 't1': t1, 't2': t2, 'w': winner,
                 'k': kind, 'c': kind == 'championship'})
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.engine = eng
    out = {r['manager_name']: r for r in proc.transform_mart_manager_performance()}
    eng.dispose()
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    return out


def test_one_finished_season_has_no_swing(rows):
    # Cy has 2024 and the 2025 season being played: one finished season.
    assert rows['Cy']['total_seasons'] == 2
    assert rows['Cy']['season_consistency_score'] is None


def test_two_finished_seasons_have_a_swing(rows):
    # Ann went 2-0 then 1-2; Bo 0-2 then 2-0. Sample standard deviations.
    assert rows['Ann']['season_consistency_score'] == Decimal('0.4714')
    assert rows['Bo']['season_consistency_score'] == Decimal('0.7071')


def test_no_bracket_games_is_no_playoff_win_rate(rows):
    assert rows['Cy']['playoff_win_percentage'] is None


def test_a_bracket_record_has_a_rate(rows):
    assert rows['Ann']['playoff_win_percentage'] == Decimal('0.5000')
    assert rows['Bo']['playoff_win_percentage'] == Decimal('0.5000')
