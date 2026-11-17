"""Rivalry luck and win rate count a tie as half a win for each side.

Expected wins split every game in a series between the two managers, ties
included, so the two sides' expected wins add up to the games played. Actual
wins have to add up the same way for the two lucks to cancel, which means a
tie is half a win for each side, as in season Luck and on Yahoo.

Builds one rivalry in a scratch database and runs the real mart SQL.
"""
import subprocess

import pytest
from sqlalchemy import create_engine, text

from src.edw_schema.edw_etl_processor import EdwEtlProcessor

TEST_DB = 'rivalry_luck_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'

LEAGUE, SEASON = 1, 2024
ANN, BOB = 1, 2  # team keys
# week -> (Ann's points, Bob's points). Ann wins two, Bob one, one tie.
GAMES = {1: (120.0, 100.0), 2: (90.0, 110.0), 3: (105.0, 105.0), 4: (130.0, 95.0)}


@pytest.fixture(scope='module')
def rivalry():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    engine = create_engine(TEST_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA edw"))
            conn.execute(text("""CREATE TABLE edw.dim_team (team_key int, manager_id text,
                manager_name text, is_active boolean)"""))
            conn.execute(text("""CREATE TABLE edw.dim_manager (manager_name text,
                include_in_analysis boolean)"""))
            conn.execute(text("CREATE TABLE edw.dim_league (league_key int, league_name text)"))
            conn.execute(text("CREATE TABLE edw.dim_week (week_key int, week_number int)"))
            conn.execute(text("CREATE TABLE edw.dim_season (season_year int)"))
            conn.execute(text("""CREATE TABLE edw.fact_matchup (league_key int, season_year int,
                week_key int, team1_key int, team2_key int, team1_points numeric,
                team2_points numeric, point_difference numeric, matchup_type text,
                winner_team_key int, is_playoffs boolean, is_championship boolean,
                is_semifinal boolean)"""))
            conn.execute(text("""INSERT INTO edw.dim_team VALUES
                (:a, 'm1', 'Ann', true), (:b, 'm2', 'Bob', true)"""), {'a': ANN, 'b': BOB})
            conn.execute(text("INSERT INTO edw.dim_manager VALUES ('Ann', true), ('Bob', true)"))
            conn.execute(text("INSERT INTO edw.dim_league VALUES (:l, 'The League')"), {'l': LEAGUE})
            conn.execute(text("INSERT INTO edw.dim_season VALUES (:s)"), {'s': SEASON})
            for week, (ann, bob) in GAMES.items():
                conn.execute(text("INSERT INTO edw.dim_week VALUES (:k, :w)"),
                             {'k': 10 + week, 'w': week})
                conn.execute(text("""INSERT INTO edw.fact_matchup VALUES
                    (:l, :s, :k, :a, :b, :pa, :pb, :diff, 'regular', :win, false, false, false)"""),
                    {'l': LEAGUE, 's': SEASON, 'k': 10 + week, 'a': ANN, 'b': BOB,
                     'pa': ann, 'pb': bob, 'diff': abs(ann - bob),
                     'win': ANN if ann > bob else BOB if bob > ann else None})
        proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
        proc.engine = engine
        rows = proc.transform_mart_manager_h2h()
        pair = [r for r in rows if {r['manager_a_name'], r['manager_b_name']} == {'Ann', 'Bob'}]
        assert len(pair) == 1, f'expected one Ann-Bob rivalry row, got {len(pair)}'
        yield pair[0]
    finally:
        engine.dispose()
        subprocess.run(['psql', 'postgres', '-qc',
                        f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)


def test_the_series_has_a_tie(rivalry):
    assert rivalry['manager_a_ties'] == 1
    assert rivalry['total_matchups'] == 4


def test_a_tie_is_half_a_win_in_each_sides_luck(rivalry):
    ann_points = sum(a for a, _ in GAMES.values())
    bob_points = sum(b for _, b in GAMES.values())
    share = ann_points ** 2 / (ann_points ** 2 + bob_points ** 2)
    sides = {rivalry['manager_a_name']: 'a', rivalry['manager_b_name']: 'b'}
    ann, bob = sides['Ann'], sides['Bob']
    # Expected wins are stored to two places, and luck is taken from them.
    ann_expected = float(rivalry[f'manager_{ann}_pythagorean_wins'])
    bob_expected = float(rivalry[f'manager_{bob}_pythagorean_wins'])
    assert ann_expected == pytest.approx(4 * share, abs=0.005)
    assert bob_expected == pytest.approx(4 * (1 - share), abs=0.005)
    # Ann: 2 wins and a tie; Bob: 1 win and the same tie.
    assert float(rivalry[f'manager_{ann}_luck_factor']) == pytest.approx(2.5 - ann_expected, abs=0.0001)
    assert float(rivalry[f'manager_{bob}_luck_factor']) == pytest.approx(1.5 - bob_expected, abs=0.0001)


def test_the_two_lucks_cancel(rivalry):
    total = float(rivalry['manager_a_luck_factor']) + float(rivalry['manager_b_luck_factor'])
    assert total == pytest.approx(0.0, abs=0.0002)


def test_win_rate_counts_a_tie_as_half(rivalry):
    sides = {rivalry['manager_a_name']: 'a', rivalry['manager_b_name']: 'b'}
    assert float(rivalry[f"manager_{sides['Ann']}_win_percentage"]) == pytest.approx(0.625)
    assert float(rivalry[f"manager_{sides['Bob']}_win_percentage"]) == pytest.approx(0.375)
