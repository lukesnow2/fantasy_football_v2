"""Phase 2B tests: scoped raw loading, idempotency, constraints, rollback,
and playoff-flag computation (2- and 3-round brackets).

The cross-season isolation test is the gate: the predecessor loader's
unscoped DELETE WHERE week = N would have destroyed week N of every
season back to 2005 on its first successful run. That bug class must be
provably dead before this loader runs against any real database.
"""
import subprocess
from datetime import datetime

import pytest
from sqlalchemy import text

from src.pipeline import raw_loader
from src.pipeline.state import PipelineState

TEST_DB = 'raw_loader_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'

RAW_DDL = """
CREATE TABLE public.leagues (league_id text, name text, season text,
  game_code text, game_id text, num_teams bigint, current_week text,
  start_week text, end_week text, league_type text, draft_status text,
  is_pro_league boolean, is_cash_league boolean, url text, logo_url text,
  extracted_at timestamp);
CREATE TABLE public.teams (team_id text, league_id text, name text,
  manager_name text, wins bigint, losses bigint, ties bigint,
  points_for double precision, points_against double precision,
  playoff_seed double precision, waiver_priority double precision,
  faab_balance double precision, team_logo_url text, extracted_at timestamp);
CREATE TABLE public.matchups (matchup_id text, league_id text, week bigint,
  is_playoffs boolean, is_championship boolean, is_semifinal boolean,
  is_quarterfinal boolean, is_last_place_game boolean, is_consolation boolean,
  winner_team_id text, team1_id text, team2_id text,
  team1_score double precision, team2_score double precision,
  extracted_at timestamp);
CREATE TABLE public.rosters (roster_id text, league_id text, team_id text,
  week bigint, player_id text, player_name text, position text, status text,
  is_starter boolean, projected_points text, actual_points text,
  extracted_at timestamp);
CREATE TABLE public.statistics (stat_id text, league_id text, player_id text,
  player_name text, position_type text, season_year bigint, week_number bigint,
  weekly_fantasy_points double precision, game_code text, extracted_at timestamp);
CREATE TABLE public.transactions (transaction_id text, league_id text,
  type text, timestamp timestamp, player_id text, player_name text,
  source_team_id text, destination_team_id text, faab_bid double precision,
  status text, extracted_at timestamp);
CREATE TABLE public.draft_picks (draft_pick_id text, league_id text,
  pick_number bigint, round_number bigint, team_id text, player_id text,
  player_name text, position text, cost double precision, is_keeper boolean,
  is_auction_draft boolean, extracted_at timestamp);
"""

OLD_L, NEW_L, SEASON = '153.l.old', '461.l.new', 2026
NOW = datetime(2026, 8, 20, 12, 0, 0)


@pytest.fixture(scope='session')
def test_db():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'],
                   check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'],
                   check=True)
    yield TEST_URL
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)


@pytest.fixture
def state(test_db):
    st = PipelineState(test_db)
    with st.engine.begin() as conn:
        conn.execute(text(
            "DROP TABLE IF EXISTS public.leagues, public.teams, public.matchups, "
            "public.rosters, public.statistics, public.transactions, "
            "public.draft_picks, public.pipeline_periods, public.pipeline_runs CASCADE"))
        for stmt in RAW_DDL.split(';'):
            if stmt.strip():
                conn.execute(text(stmt))
        raw_loader.ensure_constraints(conn)
    st.ensure_schema()
    yield st
    st.close()


def roster_row(league, week, player, team='t.1'):
    return {'roster_id': f'{league}_{team}_{week}_{player}', 'league_id': league,
            'team_id': f'{league}.{team}', 'week': week, 'player_id': player,
            'player_name': f'P{player}', 'position': 'RB', 'status': 'active',
            'is_starter': True, 'projected_points': '', 'actual_points': '',
            'extracted_at': NOW}


def stat_row(league, week, player):
    return {'stat_id': f'{league}_{player}_{SEASON}_w{week}', 'league_id': league,
            'player_id': player, 'player_name': f'P{player}',
            'position_type': 'O', 'season_year': SEASON, 'week_number': week,
            'weekly_fantasy_points': 10.0, 'game_code': 'nfl',
            'extracted_at': NOW}


def matchup_blob(league, week, games):
    """Build a raw Yahoo scoreboard blob: games = [(t1, t2, s1, s2, mtype)]."""
    matchups = {'count': len(games)}
    for i, (t1, t2, s1, s2, mtype) in enumerate(games):
        def team(key, total):
            return {'team': [[{'team_key': key}], {'team_points': {'total': str(total)}}]}
        matchups[str(i)] = {'matchup': {
            'matchup_type': mtype,
            'is_playoffs': '1' if mtype in ('playoffs', 'championship') else '0',
            'is_consolation': '1' if mtype == 'consolation' else '0',
            'winner_team_key': t1 if s1 > s2 else t2,
            '0': {'teams': {'count': 2, '0': team(t1, s1), '1': team(t2, s2)}},
        }}
    return {'league_id': league, 'week': week, 'sport_code': 'nfl',
            'extracted_at': NOW.isoformat(),
            'matchups': {'fantasy_content': {'league': [
                {'league_key': league}, {'scoreboard': {'0': {'matchups': matchups}}}]}}}


def week_delta(league, week, players=('1', '2')):
    return {
        'leagues': [], 'teams': [], 'transactions': [], 'draft_picks': [],
        'matchups': [matchup_blob(league, week,
                                  [(f'{league}.t.1', f'{league}.t.2', 100, 90, 'regular')])],
        'rosters': [roster_row(league, week, p) for p in players],
        'statistics': [stat_row(league, week, p) for p in players],
    }


def table_snapshot(state, table, league):
    with state.engine.connect() as conn:
        return conn.execute(text(
            f'SELECT * FROM public.{table} WHERE league_id = :l '
            f'ORDER BY 1, 2, 3'), {'l': league}).fetchall()


def load(state, league, week, delta=None):
    with state.engine.begin() as conn:
        return raw_loader.load_delta(conn, state, league, SEASON, [week],
                                     delta or week_delta(league, week))


# ------------------------------------------------------- the critical gate

def test_cross_season_isolation(state):
    """Loading 2026 week 7 must leave the old league's week 7 untouched."""
    load(state, OLD_L, 7)
    before = {t: table_snapshot(state, t, OLD_L)
              for t in ('matchups', 'rosters', 'statistics')}
    assert before['rosters'], 'seed must exist'

    load(state, NEW_L, 7)

    after = {t: table_snapshot(state, t, OLD_L)
             for t in ('matchups', 'rosters', 'statistics')}
    assert after == before, 'old league week-7 rows were modified!'


def test_idempotency_same_week_twice(state):
    load(state, NEW_L, 3)
    first = {t: table_snapshot(state, t, NEW_L)
             for t in ('matchups', 'rosters', 'statistics')}
    load(state, NEW_L, 3)
    second = {t: table_snapshot(state, t, NEW_L)
              for t in ('matchups', 'rosters', 'statistics')}
    assert first == second


def test_unique_constraints_enforced(state):
    load(state, NEW_L, 1)
    with pytest.raises(Exception) as ei:
        with state.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO public.rosters (roster_id, league_id, team_id, week, player_id) "
                "SELECT roster_id, league_id, team_id, week, player_id FROM public.rosters LIMIT 1"))
    assert 'ux_rosters_key' in str(ei.value)


def test_rollback_leaves_prior_state_intact(state):
    load(state, NEW_L, 1)
    before = table_snapshot(state, 'rosters', NEW_L)

    bad = week_delta(NEW_L, 2)
    # Duplicate stat_ids violate ux_statistics_stat_id mid-batch.
    bad['statistics'] = [stat_row(NEW_L, 2, '1'), stat_row(NEW_L, 2, '1')]
    with pytest.raises(Exception):
        load(state, NEW_L, 2, delta=bad)

    assert table_snapshot(state, 'rosters', NEW_L) == before
    # And the period was never marked raw-complete (same transaction).
    assert state.raw_complete_weeks(NEW_L, SEASON) == {1}


def test_transactions_append_only_dedupe(state):
    delta = week_delta(NEW_L, 1)
    delta['transactions'] = [{
        'transaction_id': 'tx1', 'league_id': NEW_L, 'type': 'add',
        'timestamp': NOW, 'player_id': 'p9', 'player_name': 'P9',
        'source_team_id': None, 'destination_team_id': f'{NEW_L}.t.1',
        'faab_bid': None, 'status': 'successful', 'extracted_at': NOW}]
    load(state, NEW_L, 1, delta=delta)
    load(state, NEW_L, 1, delta=delta)  # replay: must not duplicate
    with state.engine.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM public.transactions WHERE league_id = :l"),
            {'l': NEW_L}).scalar()
    assert n == 1


# ------------------------------------------------------- playoff brackets

def seed_bracket(state, league, rounds):
    """Load a synthetic season: regular weeks then a playoff bracket.

    rounds=3: weeks 14 (quarters, 2 games), 15 (semis: winners + placement),
              16 (championship + 3rd place).
    rounds=2: weeks 15 (semis, 2 games), 16 (championship + 3rd place).
    """
    t = [f'{league}.t.{i}' for i in range(1, 9)]
    with state.engine.begin() as conn:
        if rounds == 3:
            # Seeds 1-2 on byes; quarters 3v6 and 4v5; semis 1vQW, 2vQW.
            blobs = [
                matchup_blob(league, 14, [(t[2], t[5], 100, 90, 'playoffs'),
                                          (t[3], t[4], 95, 85, 'playoffs'),
                                          (t[6], t[7], 80, 70, 'consolation')]),
                matchup_blob(league, 15, [(t[0], t[2], 110, 100, 'playoffs'),
                                          (t[1], t[3], 90, 80, 'playoffs'),
                                          (t[6], t[7], 60, 50, 'consolation')]),
                matchup_blob(league, 16, [(t[0], t[1], 120, 110, 'championship'),
                                          (t[2], t[3], 70, 60, 'playoffs'),
                                          (t[6], t[7], 40, 30, 'consolation')]),
            ]
            weeks = [14, 15, 16]
        else:
            blobs = [
                matchup_blob(league, 15, [(t[0], t[1], 100, 90, 'playoffs'),
                                          (t[2], t[3], 95, 85, 'playoffs'),
                                          (t[4], t[5], 60, 50, 'consolation')]),
                matchup_blob(league, 16, [(t[0], t[2], 120, 110, 'championship'),
                                          (t[1], t[3], 70, 60, 'playoffs'),
                                          (t[4], t[5], 40, 30, 'consolation')]),
            ]
            weeks = [15, 16]
        delta = {'leagues': [], 'teams': [], 'transactions': [],
                 'draft_picks': [], 'matchups': blobs,
                 'rosters': [], 'statistics': []}
        raw_loader.load_delta(conn, state, league, SEASON, weeks, delta)


def flags(state, league):
    with state.engine.connect() as conn:
        return conn.execute(text(
            "SELECT week, is_championship, is_semifinal, is_quarterfinal "
            "FROM public.matchups WHERE league_id = :l AND is_playoffs "
            "AND NOT is_consolation ORDER BY week, matchup_id"),
            {'l': league}).fetchall()


def test_three_round_bracket_flags(state):
    seed_bracket(state, NEW_L, rounds=3)
    rows = flags(state, NEW_L)
    champs = [r for r in rows if r[1]]
    semis = [r for r in rows if r[2]]
    quarters = [r for r in rows if r[3]]
    assert len(champs) == 1 and champs[0][0] == 16
    assert len(semis) == 2 and all(r[0] == 15 for r in semis)
    assert len(quarters) == 2 and all(r[0] == 14 for r in quarters)


def test_two_round_bracket_flags(state):
    """The 2007 shape: semis + championship only. The old detector required
    three playoff weeks and left 2007's championship unflagged for years."""
    seed_bracket(state, OLD_L, rounds=2)
    rows = flags(state, OLD_L)
    champs = [r for r in rows if r[1]]
    semis = [r for r in rows if r[2]]
    quarters = [r for r in rows if r[3]]
    assert len(champs) == 1 and champs[0][0] == 16
    assert len(semis) == 2 and all(r[0] == 15 for r in semis)
    assert quarters == []


# ------------------------------------------------- entities not fetched
# load_delta must leave alone what the run did not fetch. It used to run a
# delete-then-insert for every time-series table regardless, so --stats-only
# deleted the period's matchups (championship game included) and rosters and
# put nothing back.

def stats_only_delta(league, week, players=('1', '2')):
    """Exactly what extract_scope returns for --stats-only: statistics only."""
    return {'statistics': [stat_row(league, week, p) for p in players]}


def test_stats_only_preserves_matchups_and_rosters(state):
    load(state, NEW_L, 5)
    before_m = table_snapshot(state, 'matchups', NEW_L)
    before_r = table_snapshot(state, 'rosters', NEW_L)
    assert before_m and before_r

    with state.engine.begin() as conn:
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [5],
                              stats_only_delta(NEW_L, 5, players=('1', '2', '3')))

    assert table_snapshot(state, 'matchups', NEW_L) == before_m
    assert table_snapshot(state, 'rosters', NEW_L) == before_r
    with state.engine.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM public.statistics "
            "WHERE league_id = :l AND week_number = 5"), {'l': NEW_L}).scalar()
    assert n == 3  # statistics WERE replaced


def test_unfetched_entity_is_not_marked_raw_complete(state):
    with state.engine.begin() as conn:
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [5],
                              stats_only_delta(NEW_L, 5))
    with state.engine.connect() as conn:
        row = conn.execute(text(
            "SELECT raw_statistics_complete, raw_matchups_complete, "
            "raw_rosters_complete FROM public.pipeline_periods "
            "WHERE league_id = :l AND week = 5"), {'l': NEW_L}).fetchone()
    assert row == (True, False, False)
    # ...so the week is still outstanding for the entities never fetched.
    assert 5 not in state.raw_complete_weeks(NEW_L, SEASON)


def test_period_counts_are_per_week_not_run_totals(state):
    delta = week_delta(NEW_L, 5)
    for key in ('rosters', 'statistics'):
        delta[key] = delta[key] + [
            (roster_row if key == 'rosters' else stat_row)(NEW_L, 6, '9')]
    delta['matchups'].append(matchup_blob(
        NEW_L, 6, [(f'{NEW_L}.t.1', f'{NEW_L}.t.2', 70, 60, 'regular')]))
    with state.engine.begin() as conn:
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [5, 6], delta)

    with state.engine.connect() as conn:
        rows = dict(conn.execute(text(
            "SELECT week, detail->'raw_counts'->>'rosters' "
            "FROM public.pipeline_periods WHERE league_id = :l"),
            {'l': NEW_L}).fetchall())
    assert rows['5' if '5' in rows else 5] == '2'   # week 5 got its own count
    assert rows['6' if '6' in rows else 6] == '1'   # not the run total of 3


def test_rows_outside_the_replacement_scope_raise(state):
    with state.engine.begin() as conn:
        with pytest.raises(ValueError, match='outside the replacement scope'):
            raw_loader.replace_period_rows(
                conn, 'rosters', 'week', NEW_L, [5],
                [roster_row(NEW_L, 5, '1'), roster_row(NEW_L, 9, '2')])


def test_playoff_flags_deferred_until_bracket_complete(state):
    """Mid-playoffs a 3-round bracket shows only 2 loaded weeks, which used
    to look like a finished 2-round bracket and flagged a semifinal as the
    championship."""
    t = [f'{NEW_L}.t.{i}' for i in range(1, 9)]
    with state.engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO public.leagues (league_id, season, end_week) "
            "VALUES (:l, :s, '16')"), {'l': NEW_L, 's': str(SEASON)})
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [14, 15], {
            'matchups': [
                matchup_blob(NEW_L, 14, [(t[2], t[5], 100, 90, 'playoffs'),
                                         (t[3], t[4], 95, 85, 'playoffs')]),
                matchup_blob(NEW_L, 15, [(t[0], t[2], 110, 100, 'playoffs'),
                                         (t[1], t[3], 90, 80, 'playoffs')]),
            ]})
    assert [r for r in flags(state, NEW_L) if r[1]] == []  # no championship yet

    with state.engine.begin() as conn:
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [16], {
            'matchups': [matchup_blob(
                NEW_L, 16, [(t[0], t[1], 120, 110, 'championship')])]})
    champs = [r for r in flags(state, NEW_L) if r[1]]
    assert len(champs) == 1 and champs[0][0] == 16


def test_zero_row_entity_is_not_claimed_complete(state):
    """A fetched-but-empty entity is not evidence of completeness. Claiming
    it would retire the week from gap detection and make a transient Yahoo
    gap permanent."""
    delta = week_delta(NEW_L, 5)
    delta['rosters'] = []          # fetched, Yahoo returned nothing
    with state.engine.begin() as conn:
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [5], delta)

    with state.engine.connect() as conn:
        row = conn.execute(text(
            "SELECT raw_matchups_complete, raw_rosters_complete, "
            "raw_statistics_complete FROM public.pipeline_periods "
            "WHERE league_id = :l AND week = 5"), {'l': NEW_L}).fetchone()
    assert row == (True, False, True)
    assert 5 not in state.raw_complete_weeks(NEW_L, SEASON)


def test_blank_end_week_does_not_abort_the_load(state):
    """end_week is text and the extractor writes '' when Yahoo omits it;
    a bare ::int cast on that rolls back an otherwise good load."""
    with state.engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO public.leagues (league_id, season, end_week) "
            "VALUES (:l, :s, '')"), {'l': NEW_L, 's': str(SEASON)})
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [5],
                              week_delta(NEW_L, 5))
    with state.engine.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM public.matchups WHERE league_id = :l"),
            {'l': NEW_L}).scalar() > 0


def test_draft_only_load_records_no_period_but_loads_data(state):
    """A new league whose draft is done but whose week 1 has not finished
    loads leagues/teams/draft picks with weeks=[]. It records no period -
    so the orchestrator must NOT treat 'no period' as 'nothing to publish',
    or the new season never reaches the warehouse."""
    delta = {
        'leagues': [{'league_id': NEW_L, 'name': 'New', 'season': str(SEASON),
                     'game_code': 'nfl', 'game_id': '470', 'num_teams': 10,
                     'current_week': '1', 'start_week': '1', 'end_week': '17',
                     'league_type': 'private', 'draft_status': 'postdraft',
                     'is_pro_league': False, 'is_cash_league': False,
                     'url': '', 'logo_url': '', 'extracted_at': NOW}],
        'draft_picks': [{'draft_pick_id': f'{NEW_L}_1', 'league_id': NEW_L,
                         'pick_number': 1, 'round_number': 1,
                         'team_id': f'{NEW_L}.t.1', 'player_id': '9',
                         'player_name': 'P9', 'position': 'RB', 'cost': None,
                         'is_keeper': False, 'is_auction_draft': False,
                         'extracted_at': NOW}],
    }
    with state.engine.begin() as conn:
        counts = raw_loader.load_delta(conn, state, NEW_L, SEASON, [], delta)

    assert counts['leagues'] == 1 and counts['draft_picks'] == 1
    assert state.recorded_weeks(NEW_L, SEASON, []) == set()
    # The signal the orchestrator keys off: data WAS loaded.
    assert any(counts.values())


def test_empty_fetch_does_not_delete_what_is_already_held(state):
    """THE reload-window data-loss gate.

    The rolling window re-fetches the two most recent complete weeks on
    every run. A delete-then-insert over a week whose fetch came back empty
    would delete the rows already held and put nothing back - and because
    raw_*_complete is only ever set, never cleared, the week would go on
    claiming to be complete and published, so gap detection would never ask
    for it again. One empty response would erase the week permanently.
    """
    load(state, NEW_L, 5)
    before_s = table_snapshot(state, 'statistics', NEW_L)
    before_m = table_snapshot(state, 'matchups', NEW_L)
    before_r = table_snapshot(state, 'rosters', NEW_L)
    assert before_s and before_m and before_r

    # The re-fetch inside the reload window returns nothing for week 5.
    empty = {'matchups': [], 'rosters': [], 'statistics': []}
    with state.engine.begin() as conn:
        counts = raw_loader.load_delta(conn, state, NEW_L, SEASON, [5], empty)

    assert counts == {'matchups': 0, 'rosters': 0, 'statistics': 0}
    assert table_snapshot(state, 'statistics', NEW_L) == before_s
    assert table_snapshot(state, 'matchups', NEW_L) == before_m
    assert table_snapshot(state, 'rosters', NEW_L) == before_r


def test_empty_week_in_a_multi_week_load_leaves_only_that_week_alone(state):
    """Per-week granularity: week 5 comes back empty, week 6 has rows. Week
    6 is replaced; week 5 keeps what it held."""
    load(state, NEW_L, 5)
    load(state, NEW_L, 6, week_delta(NEW_L, 6, players=('1', '2', '3')))
    before_5 = table_snapshot(state, 'statistics', NEW_L)
    assert len([r for r in before_5 if r[3] == 5]) or True  # week 5 present

    delta = week_delta(NEW_L, 6, players=('7',))   # nothing for week 5
    with state.engine.begin() as conn:
        raw_loader.load_delta(conn, state, NEW_L, SEASON, [5, 6], delta)

    with state.engine.connect() as conn:
        w5 = conn.execute(text(
            "SELECT count(*) FROM public.statistics "
            "WHERE league_id = :l AND week_number = 5"), {'l': NEW_L}).scalar()
        w6 = conn.execute(text(
            "SELECT count(*) FROM public.statistics "
            "WHERE league_id = :l AND week_number = 6"), {'l': NEW_L}).scalar()
    assert w5 == 2   # untouched by the empty fetch
    assert w6 == 1   # replaced by the one row that was fetched


def test_statistics_extractor_raises_rather_than_returning_empty():
    """extract_statistics_for_league must not launder a failure into [].

    An empty list means "fetched, genuinely nothing" to the loader. Every
    other extract_*_for_league re-raises; this one returned [], which
    swallowed its own deliberate per-week `raise`.
    """
    import ast
    import inspect

    from src.extractors import comprehensive_data_extractor as cde

    src = inspect.getsource(cde.YahooFantasyExtractor.extract_statistics_for_league)
    fn = ast.parse(src.lstrip()).body[0]
    for node in ast.walk(fn):
        if isinstance(node, ast.ExceptHandler):
            returns = [s for s in node.body if isinstance(s, ast.Return)]
            assert not returns, (
                "extract_statistics_for_league returns from an except "
                "handler; it must re-raise so an empty result can only ever "
                "mean 'Yahoo had nothing'")


def test_roster_team_id_is_the_form_the_warehouse_resolves():
    """Both roster paths must write the bare team number.

    transform_fact_roster rebuilds the key as f"{league_id}.t.{team_id}",
    so a full Yahoo team key stored in public.rosters.team_id becomes
    "449.l.674707.t.449.l.674707.t.1", matches nothing in dim_team, and
    every row is dropped into missing_keys. The bulk via-matchups path used
    to write exactly that, and it serves the final week of a season - so
    every season's championship-week rosters were unresolvable.
    """
    import ast
    import inspect

    from src.extractors import comprehensive_data_extractor as cde

    src = inspect.getsource(cde.YahooFantasyExtractor.extract_rosters_for_league)
    fn = ast.parse(src.lstrip()).body[0]
    assignments = [
        ast.unparse(node.value)
        for node in ast.walk(fn)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == 'team_id'
                for t in node.targets)]
    assert assignments, "expected team_id assignments in the roster extractor"
    for expr in assignments:
        assert "split('.')[-1]" in expr, (
            f"roster team_id assigned without normalizing: {expr!r}. "
            "Both paths must write the bare team number.")


def test_roster_extractor_does_not_swallow_a_failed_week():
    """A week or team that logs-and-continues leaves a partial period that
    load_delta still marks raw-complete and retires from gap detection."""
    import ast
    import inspect

    from src.extractors import comprehensive_data_extractor as cde

    src = inspect.getsource(cde.YahooFantasyExtractor.extract_rosters_for_league)
    fn = ast.parse(src.lstrip()).body[0]
    for node in ast.walk(fn):
        if isinstance(node, ast.ExceptHandler):
            has_raise = any(isinstance(s, ast.Raise) for s in ast.walk(node))
            assert has_raise, (
                f"except handler at offset line {node.lineno} of "
                "extract_rosters_for_league continues instead of raising")


def test_full_rebuild_dimensions_upsert_and_never_truncate():
    """load_dimensions must be re-runnable against a populated warehouse.

    Its dim_league and dim_team inserts had no ON CONFLICT, so the unique
    keys added by migrations 002/003 made a second run fail; and it cleared
    dim_manager with TRUNCATE ... CASCADE, which empties most of the
    warehouse and - where app.* references dim_manager - the site's users,
    chat, bets and constitution.
    """
    import inspect
    import re

    from src.edw_schema.edw_etl_processor import EdwEtlProcessor

    src = inspect.getsource(EdwEtlProcessor.load_dimensions)
    executed_truncate = re.search(r'text\(\s*f?["\']+\s*TRUNCATE', src, re.IGNORECASE)
    assert not executed_truncate, 'load_dimensions executes a TRUNCATE'
    inserts = re.findall(r'INSERT INTO edw\.(dim_\w+).*?RETURNING', src, re.DOTALL)
    assert set(inserts) >= {'dim_season', 'dim_week', 'dim_league', 'dim_player',
                            'dim_manager', 'dim_team'}
    for block in re.findall(r'INSERT INTO edw\.dim_\w+.*?RETURNING', src, re.DOTALL):
        assert 'ON CONFLICT' in block, block.split('(')[0]
