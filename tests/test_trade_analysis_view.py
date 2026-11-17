"""edw.vw_trade_analysis: the trade score and its verdict (migration 022).

The view scores each side of a trade on four 0-100 components (production
40%, playoffs 30%, opportunity cost 20%, timing 10%) and names a winner when
the final scores are 8 or more apart. The metric card audit of 2026-11-17
found four bugs:

1. Timing added the same amount to both sides, so it never moved a verdict.
2. The winner test used uncapped component scores; the page shows them held
   to 0-100.
3. The title join compared fm.season_year with itself, so a title in another
   season counted.
4. "In Progress" went by calendar year, so a season whose title game hadn't
   been played read Complete from January.

These build a small league in a scratch database and run the real view SQL.
"""
import re
import subprocess
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from src.edw_schema.edw_etl_processor import EdwEtlProcessor

TEST_DB = 'trade_analysis_view_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'
MIGRATION = 'src/edw_schema/migrations/022_trade_analysis_fixes.sql'

FINISHED, UNSETTLED = 1, 2  # league keys: 2024 (title decided), 2025 (title game unplayed)

# (league, season, week, team giving A's player, team giving B's player,
#  A's player, B's player). Team A is the lower team key.
TRADES = {
    'early': (FINISHED, 2024, 2, 1, 2, 101, 102),
    'mid': (FINISHED, 2024, 7, 3, 4, 103, 104),
    'late': (FINISHED, 2024, 11, 5, 6, 105, 106),
    'capped': (FINISHED, 2024, 3, 7, 8, 107, 108),
    'unsettled': (UNSETTLED, 2025, 5, 21, 22, 121, 122),
}

# (player, league, season, week, points)
STATS = [
    # early/mid/late: A gains 50 more points than B, nothing else differs.
    (102, FINISHED, 2024, 5, 50.0),
    (104, FINISHED, 2024, 8, 50.0),
    (106, FINISHED, 2024, 12, 50.0),
    # capped: A gains 400 (production 50 + 400 x 0.15 = 110, held to 100) but
    # gave up a 25-point player for one who'd scored nothing (opportunity 0).
    (107, FINISHED, 2024, 1, 25.0),
    (107, FINISHED, 2024, 2, 25.0),
    (108, FINISHED, 2024, 4, 400.0),
]


def view_sql() -> str:
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    return proc._get_view_definition('vw_trade_analysis')


@pytest.fixture(scope='module')
def engine():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    eng = create_engine(TEST_URL)
    with eng.begin() as conn:
        conn.execute(text("CREATE SCHEMA edw"))
        conn.execute(text("""CREATE TABLE edw.fact_transaction (transaction_key serial,
            league_key int, season_year int, transaction_date date, transaction_week int,
            transaction_type text, from_team_key int, to_team_key int,
            from_manager_key int, to_manager_key int, player_key int)"""))
        conn.execute(text("CREATE TABLE edw.dim_player (player_key int, player_name text)"))
        conn.execute(text("""CREATE TABLE edw.fact_player_statistics (player_key int,
            league_key int, season_year int, week_number int, weekly_fantasy_points numeric)"""))
        conn.execute(text("""CREATE TABLE edw.fact_team_performance (league_key int,
            season_year int, team_key int, is_playoff_team boolean)"""))
        conn.execute(text("""CREATE TABLE edw.fact_matchup (league_key int, season_year int,
            winner_team_key int, is_championship boolean)"""))
        conn.execute(text("CREATE TABLE edw.dim_league (league_key int, league_name text)"))
        conn.execute(text("CREATE TABLE edw.dim_team (team_key int, team_name text)"))
        conn.execute(text("CREATE TABLE edw.dim_manager (manager_key int, manager_name text)"))

        conn.execute(text("INSERT INTO edw.dim_league VALUES (1, 'L'), (2, 'L')"))
        for team in (1, 2, 3, 4, 5, 6, 7, 8, 21, 22):
            conn.execute(text("INSERT INTO edw.dim_team VALUES (:t, :n)"), {'t': team, 'n': f'T{team}'})
            conn.execute(text("INSERT INTO edw.dim_manager VALUES (:t, :n)"), {'t': team, 'n': f'M{team}'})
        for (league, season, week, a, b, pa, pb) in TRADES.values():
            for player in (pa, pb):
                conn.execute(text("INSERT INTO edw.dim_player VALUES (:p, :n)"),
                             {'p': player, 'n': f'P{player}'})
            for (giver, taker, player) in ((a, b, pa), (b, a, pb)):
                conn.execute(text("""INSERT INTO edw.fact_transaction (league_key, season_year,
                    transaction_date, transaction_week, transaction_type, from_team_key,
                    to_team_key, from_manager_key, to_manager_key, player_key)
                    VALUES (:l, :s, make_date(:s, 9, 1) + :w * 7, :w, 'trade', :f, :t, :f, :t, :p)"""),
                    {'l': league, 's': season, 'w': week, 'f': giver, 't': taker, 'p': player})
        for row in STATS:
            conn.execute(text("INSERT INTO edw.fact_player_statistics VALUES (:p, :l, :s, :w, :pts)"),
                         dict(zip(('p', 'l', 's', 'w', 'pts'), row)))
        # 2024: team 8 made the playoffs and won the title; nobody else made it.
        for team in (1, 2, 3, 4, 5, 6, 7, 8):
            conn.execute(text("INSERT INTO edw.fact_team_performance VALUES (1, 2024, :t, :p)"),
                         {'t': team, 'p': team == 8})
        conn.execute(text("INSERT INTO edw.fact_matchup VALUES (1, 2024, 8, TRUE)"))
        # Team 1 won a title in 2023 under the same league key: not this trade's title.
        conn.execute(text("INSERT INTO edw.fact_matchup VALUES (1, 2023, 1, TRUE)"))
        # 2025: the title game is on the schedule but hasn't been played.
        conn.execute(text("INSERT INTO edw.fact_matchup VALUES (2, 2025, NULL, TRUE)"))
        conn.execute(text(view_sql()))
    yield eng
    eng.dispose()
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)


@pytest.fixture(scope='module')
def trades(engine):
    """The view's rows by trade name, keyed on side A's manager."""
    by_a = {f'M{a}': name for name, (_, _, _, a, _, _, _) in TRADES.items()}
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM edw.vw_trade_analysis")).mappings().all()
    return {by_a[r['team_a_manager']]: dict(r) for r in rows}


def test_timing_moves_the_verdict_toward_the_side_ahead(trades):
    # The same 50-point swing: 6 apart in week 2 (even), 8 in week 7, 11 in week 11.
    early, mid, late = trades['early'], trades['mid'], trades['late']
    assert (early['team_a_final_score'], early['team_b_final_score']) == (Decimal('53.0'), Decimal('47.0'))
    assert early['trade_winner'] == 'Even Trade'
    assert early['team_a_context_score'] == Decimal('50.0')
    assert (mid['team_a_final_score'], mid['team_b_final_score']) == (Decimal('54.0'), Decimal('46.0'))
    assert mid['trade_winner'] == 'M3'
    assert mid['team_a_context_score'] == Decimal('60.0')
    assert (late['team_a_final_score'], late['team_b_final_score']) == (Decimal('55.5'), Decimal('44.5'))
    assert late['trade_winner'] == 'M5'
    assert late['team_a_context_score'] == Decimal('75.0')


def test_final_scores_and_winner_use_the_scores_shown(trades):
    t = trades['capped']
    assert t['production_differential'] == Decimal('400.0')
    assert t['team_a_production_score'] == Decimal('100.0')
    assert t['team_a_playoff_score'] == Decimal('30.0')
    assert t['team_a_opportunity_score'] == Decimal('0.0')
    # 100 x .4 + 30 x .3 + 0 x .2 + 50 x .1 against 0 x .4 + 85 x .3 + 100 x .2 + 50 x .1.
    # Uncapped, production alone (110 vs -10) put A 11.5 ahead and called it A's trade.
    assert (t['team_a_final_score'], t['team_b_final_score']) == (Decimal('54.0'), Decimal('50.5'))
    assert t['trade_winner'] == 'Even Trade'


def test_every_winner_agrees_with_the_final_scores(trades):
    for t in trades.values():
        a, b = t['team_a_final_score'], t['team_b_final_score']
        if abs(a - b) >= 8:
            assert t['trade_winner'] == (t['team_a_manager'] if a > b else t['team_b_manager'])
        else:
            assert t['trade_winner'] == 'Even Trade'
        assert 0 <= a <= 100 and 0 <= b <= 100


def test_a_title_counts_only_in_the_trades_season(trades):
    assert trades['early']['team_a_champion'] == 0
    assert trades['capped']['team_b_champion'] == 1


def test_a_season_is_in_progress_until_its_title_game_has_a_winner(trades):
    assert trades['unsettled']['evaluation_status'] == 'In Progress'
    for name in ('early', 'mid', 'late', 'capped'):
        assert trades[name]['evaluation_status'] == 'Complete'


def _squeeze(sql: str) -> str:
    return re.sub(r'\s+', ' ', sql).strip().rstrip(';').strip()


def test_migration_carries_the_etl_definition():
    with open(MIGRATION) as f:
        body = f.read()
    migrated = body.split('CREATE VIEW', 1)[1].rsplit('COMMIT;', 1)[0]
    etl = view_sql().split('CREATE VIEW', 1)[1]
    assert _squeeze(migrated) == _squeeze(etl)


def test_migration_applies_twice(engine):
    with open(MIGRATION) as f:
        sql = f.read()
    for _ in range(2):
        # A raw cursor with no parameters, as incremental_load applies it.
        with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
            cursor = conn.connection.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM edw.vw_trade_analysis")).scalar() == len(TRADES)
