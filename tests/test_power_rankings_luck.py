"""Luck, schedule and the power score in the weekly power rankings (WP15).

Points for is the only skill signal in a league with no defense; points
against is the schedule. So luck is wins minus ALL-PLAY wins (the share of
the field a team outscored each week), not wins minus Pythagorean wins, which
counted points against as earned. Points vs league and schedule compare a
team's score, and its opponent's, with every other team that week. The power
score (D4 "R") reads season scoring, not the week's score, and has no
schedule term.

These build a four-team league in a scratch database and run the real SQL.
"""
import subprocess

import pytest
from sqlalchemy import create_engine, text

from src.edw_schema.edw_etl_processor import EdwEtlProcessor

TEST_DB = 'power_rankings_luck_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'
MIGRATION = 'src/edw_schema/migrations/019_luck_schedule_and_power_score.sql'

LEAGUE, SEASON = 1, 2024
# week -> [(team, points, opponent, opponent points)], one row per game.
# Ties in weeks 1 and 3, so all-play and wins both have to count halves.
# Four weeks, so by week 4 the last three weeks and the season differ.
GAMES = {
    1: [(1, 120.0, 2, 100.0), (3, 110.0, 4, 110.0)],
    2: [(1, 90.0, 3, 130.0), (2, 105.0, 4, 95.0)],
    3: [(1, 150.0, 4, 80.0), (2, 100.0, 3, 100.0)],
    4: [(1, 95.0, 2, 140.0), (3, 125.0, 4, 105.0)],
}
TEAMS = [1, 2, 3, 4]


def week_key(week):
    return 10 + week


def scores(week):
    """team -> (its points, its opponent's points) for one week."""
    out = {}
    for a, pa, b, pb in GAMES[week]:
        out[a] = (pa, pb)
        out[b] = (pb, pa)
    return out


def expected(team, through):
    """Season-to-date all-play wins, wins, points vs league and schedule,
    worked out the long way."""
    all_play = wins = pvl = sched = 0.0
    for week in range(1, through + 1):
        s = scores(week)
        mine, theirs = s[team]
        others = [s[t][0] for t in TEAMS if t != team]
        all_play += sum(1.0 if o < mine else 0.5 if o == mine else 0.0 for o in others) / len(others)
        wins += 1.0 if mine > theirs else 0.5 if mine == theirs else 0.0
        pvl += mine - sum(others) / len(others)
        sched += theirs - sum(others) / len(others)
    return all_play, wins, pvl / through, sched / through


@pytest.fixture(scope='module')
def engine():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    eng = create_engine(TEST_URL)
    with eng.begin() as conn:
        conn.execute(text("CREATE SCHEMA edw"))
        conn.execute(text("""CREATE TABLE edw.dim_week (week_key int, season_year int,
            week_number int, week_type text)"""))
        conn.execute(text("CREATE TABLE edw.dim_season (season_year int, playoff_start_week int)"))
        conn.execute(text("""CREATE TABLE edw.dim_league (league_key int, season_year int,
            num_teams int, playoff_start_week int, num_playoff_teams int)"""))
        conn.execute(text("""CREATE TABLE edw.fact_matchup (league_key int, week_key int,
            season_year int, team1_key int, team2_key int, team1_points numeric,
            team2_points numeric, winner_team_key int, is_tie boolean,
            is_playoffs boolean DEFAULT false, is_championship boolean DEFAULT false)"""))
        conn.execute(text("""CREATE TABLE edw.fact_team_performance (league_key int,
            season_year int, week_key int, team_key int, win_percentage numeric,
            points_for numeric, points_against numeric, season_rank int,
            weekly_points numeric, playoff_probability numeric)"""))
        # The mart as it stood before 019, so the migration's ALTER is tested.
        conn.execute(text("""CREATE TABLE edw.mart_weekly_power_rankings (
            league_key int, week_key int, team_key int, power_rank int,
            luck_factor numeric(8,4), pythagorean_wins numeric(8,2))"""))
        conn.execute(text("INSERT INTO edw.dim_season VALUES (:s, 15)"), {'s': SEASON})
        conn.execute(text("INSERT INTO edw.dim_league VALUES (:l, :s, 4, 15, 2)"),
                     {'l': LEAGUE, 's': SEASON})
        totals = {t: [0.0, 0.0, 0] for t in TEAMS}  # wins, points, games
        for week in GAMES:
            conn.execute(text("INSERT INTO edw.dim_week VALUES (:k, :s, :w, 'regular')"),
                         {'k': week_key(week), 's': SEASON, 'w': week})
            for a, pa, b, pb in GAMES[week]:
                conn.execute(text("""INSERT INTO edw.fact_matchup
                    (league_key, week_key, season_year, team1_key, team2_key,
                     team1_points, team2_points, winner_team_key, is_tie)
                    VALUES (:l, :k, :s, :a, :b, :pa, :pb, :win, :tie)"""),
                    {'l': LEAGUE, 'k': week_key(week), 's': SEASON, 'a': a, 'b': b,
                     'pa': pa, 'pb': pb, 'tie': pa == pb,
                     'win': a if pa > pb else b if pb > pa else None})
            s = scores(week)
            for t in TEAMS:
                mine, theirs = s[t]
                totals[t][0] += 1.0 if mine > theirs else 0.5 if mine == theirs else 0.0
                totals[t][1] += mine
                totals[t][2] += 1
            order = sorted(TEAMS, key=lambda t: -totals[t][0])
            for t in TEAMS:
                w, pts, g = totals[t]
                conn.execute(text("""INSERT INTO edw.fact_team_performance VALUES
                    (:l, :s, :k, :t, :wp, :pf, :pa, :rank, :avg, null)"""),
                    {'l': LEAGUE, 's': SEASON, 'k': week_key(week), 't': t,
                     'wp': w / g, 'pf': s[t][0], 'pa': s[t][1],
                     'rank': order.index(t) + 1, 'avg': pts / g})
        conn.execute(text("CREATE SCHEMA meta_data"))
    with open('src/edw_schema/meta_data_schema.sql') as f:
        with eng.begin() as conn:
            conn.exec_driver_sql(f.read())
    yield eng
    eng.dispose()
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)


@pytest.fixture(scope='module')
def rows(engine):
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.engine = engine
    return {(r['team_key'], r['week_key'] - 10): r
            for r in proc.transform_mart_weekly_power_rankings()}


def test_all_play_wins_count_ties_as_half(rows):
    # Week 1: team 3 beat team 2 and tied team 4: 1.5 of 3.
    assert float(rows[(3, 1)]['all_play_wins']) == pytest.approx(0.5)
    for team in TEAMS:
        all_play, _, _, _ = expected(team, 3)
        assert float(rows[(team, 3)]['all_play_wins']) == pytest.approx(all_play, abs=0.005)


def test_luck_is_wins_minus_all_play_and_sums_to_zero(rows):
    for week in GAMES:
        total = 0.0
        for team in TEAMS:
            all_play, wins, _, _ = expected(team, week)
            luck = float(rows[(team, week)]['luck_factor'])
            assert luck == pytest.approx(wins - all_play, abs=0.0001)
            total += luck
        assert total == pytest.approx(0.0, abs=0.0005)


def test_points_vs_league_and_schedule_leave_the_team_out(rows):
    # Week 1, team 1: 120 against the other three's 106.67; its opponent's
    # 100 against the same 106.67.
    assert float(rows[(1, 1)]['points_vs_league']) == pytest.approx(13.33, abs=0.01)
    assert float(rows[(1, 1)]['schedule_vs_league']) == pytest.approx(-6.67, abs=0.01)
    for team in TEAMS:
        _, _, pvl, sched = expected(team, 3)
        assert float(rows[(team, 3)]['points_vs_league']) == pytest.approx(pvl, abs=0.005)
        assert float(rows[(team, 3)]['schedule_vs_league']) == pytest.approx(sched, abs=0.005)


def test_power_score_reads_season_scoring_with_no_schedule_term(rows):
    # 45% record, 35% season points per game, 20% the last three weeks, each
    # scoring term over the week's best. Week 2 is where the old build went
    # wrong (team 1 scored 90 that week but averages 105, and the old points
    # term read the 90); week 4 is where the season and the last three weeks
    # part, so a wrong form window or swapped weights would show.
    for week in (2, 4):
        played = range(1, week + 1)
        recent = range(max(1, week - 2), week + 1)
        avg = {t: sum(scores(w)[t][0] for w in played) / len(played) for t in TEAMS}
        form = {t: sum(scores(w)[t][0] for w in recent) / len(recent) for t in TEAMS}
        for team in TEAMS:
            _, wins, _, _ = expected(team, week)
            want = (0.45 * wins / week + 0.35 * avg[team] / max(avg.values())
                    + 0.20 * form[team] / max(form.values()))
            assert float(rows[(team, week)]['power_score']) == pytest.approx(want, abs=0.0005)
    assert max(abs(a - f) for a, f in zip(avg.values(), form.values())) > 3  # they do part


def test_migration_adds_the_columns_and_dictionary_rows_twice_over(engine):
    with open(MIGRATION) as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO meta_data.metric_categories (category_id, category_name)
            VALUES ('team_performance', 'Team'), ('head_to_head', 'H2H'),
                   ('manager_performance', 'Manager')"""))
        conn.execute(text("""INSERT INTO meta_data.metric_definitions
            (metric_id, metric_name, metric_category, short_description, data_type)
            VALUES ('luck_factor', 'Luck Factor', 'head_to_head', 'Actual minus Pythagorean', 'decimal'),
                   ('pythagorean_wins', 'Pythagorean Wins', 'head_to_head', 'old', 'decimal'),
                   ('strength_of_schedule', 'SOS', 'team_performance', 'old', 'decimal'),
                   ('power_score', 'Power Score', 'team_performance', 'old', 'decimal')"""))
        conn.execute(text("""INSERT INTO meta_data.api_definitions (endpoint_path, endpoint_name)
            VALUES ('/api/head-to-head', 'Head to head')"""))
        conn.execute(text("""INSERT INTO meta_data.field_definitions
            (field_id, field_name, api_endpoint, metric_id)
            VALUES ('h2h_luck_factor', 'luck_factor', '/api/head-to-head', 'luck_factor')"""))
        conn.execute(text("""INSERT INTO meta_data.metric_relationships
            (primary_metric_id, related_metric_id, relationship_type)
            VALUES ('luck_factor', 'pythagorean_wins', 'derived_from'),
                   ('power_score', 'strength_of_schedule', 'related_to')"""))
    for _ in range(2):  # idempotent
        # A raw cursor with no parameters, as incremental_load applies it:
        # the copy has "%" in it, which exec_driver_sql would read as a
        # placeholder.
        with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
            cursor = conn.connection.cursor()
            try:
                cursor.execute(sql)
            finally:
                cursor.close()
    with engine.connect() as conn:
        cols = {r[0] for r in conn.execute(text("""SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'edw' AND table_name = 'mart_weekly_power_rankings'"""))}
        assert {'all_play_wins', 'points_vs_league', 'schedule_vs_league'} <= cols
        defs = dict(conn.execute(text(
            "SELECT metric_id, calculation_formula FROM meta_data.metric_definitions")).fetchall())
        for metric in ('all_play_wins', 'points_vs_league', 'schedule_vs_league',
                       'rivalry_luck', 'career_luck'):
            assert defs.get(metric)
        assert 'expected wins' in defs['luck_factor'] and 'Pythagorean' not in defs['luck_factor']
        assert conn.execute(text("""SELECT metric_id FROM meta_data.field_definitions
            WHERE field_id = 'h2h_luck_factor'""")).scalar() == 'rivalry_luck'
        rels = set(conn.execute(text("""SELECT primary_metric_id, related_metric_id
            FROM meta_data.metric_relationships""")).fetchall())
        assert ('luck_factor', 'all_play_wins') in rels
        assert ('luck_factor', 'pythagorean_wins') not in rels
        assert ('power_score', 'strength_of_schedule') not in rels
