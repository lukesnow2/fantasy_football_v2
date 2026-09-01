"""Phase 2C tests: atomic EDW publication - snapshot, restore, verify gate.

The property under test: a failed refresh can NEVER leave the site
reading a mixed generation. Refresh callables are injected so the tests
control exactly how and when things break.
"""
import subprocess

import pytest
from sqlalchemy import text

from src.pipeline import publish as pub
from src.pipeline.state import PipelineState

TEST_DB = 'publish_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'
L, S = '461.l.1', 2026


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
    st.ensure_schema()
    with st.engine.begin() as conn:
        conn.execute(text('TRUNCATE public.pipeline_periods, public.pipeline_runs '
                          'RESTART IDENTITY'))
        for schema in ('edw', 'edw_prev', 'edw_broken'):
            conn.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
        conn.execute(text('CREATE SCHEMA edw'))
        conn.execute(text(
            'CREATE TABLE edw.fact_player_statistics ('
            ' league_key text, season_year int, week_number int,'
            ' weekly_fantasy_points float)'))
        conn.execute(text(
            'CREATE TABLE edw.fact_matchup (league_key text, season_year int,'
            ' week int, team1 text, team2 text)'))
        conn.execute(text(
            'CREATE VIEW edw.vw_totals AS SELECT season_year,'
            ' sum(weekly_fantasy_points) AS pts'
            ' FROM edw.fact_player_statistics GROUP BY season_year'))
        conn.execute(text(
            'CREATE VIEW edw.vw_totals_plus AS SELECT season_year, pts + 0 AS pts2'
            ' FROM edw.vw_totals'))  # view-on-view: exercises dependency order
        for w in (1, 2):
            conn.execute(text(
                'INSERT INTO edw.fact_player_statistics VALUES '
                f"('{L}', {S}, {w}, 10.0), ('{L}', {S}, {w}, 12.0)"))
            conn.execute(text(
                f"INSERT INTO edw.fact_matchup VALUES ('{L}', {S}, {w}, 'a', 'b')"))
    # Mark weeks 1-2 as raw-complete so publish flags have rows to update.
    with st.engine.begin() as conn:
        for w in (1, 2):
            st.mark_raw_complete(conn, L, S, w,
                                 {'matchups': 1, 'rosters': 1, 'statistics': 2})
    yield st
    st.close()


def edw_state(state):
    with state.engine.connect() as conn:
        stats = conn.execute(text(
            'SELECT * FROM edw.fact_player_statistics ORDER BY 1,2,3,4')).fetchall()
        views = sorted(r[0] for r in conn.execute(text(
            "SELECT viewname FROM pg_views WHERE schemaname='edw'")))
        return stats, views


def add_week3(state):
    """A refresh that correctly lands week 3."""
    def refresh():
        with state.engine.begin() as conn:
            conn.execute(text(
                'INSERT INTO edw.fact_player_statistics VALUES '
                f"('{L}', {S}, 3, 9.0)"))
            conn.execute(text(
                f"INSERT INTO edw.fact_matchup VALUES ('{L}', {S}, 3, 'a', 'b')"))
        return True
    return refresh


def test_successful_publish_marks_periods_and_drops_snapshot(state):
    with state.engine.begin() as conn:
        state.mark_raw_complete(conn, L, S, 3, {'matchups': 1, 'rosters': 1,
                                                'statistics': 1})
    report = pub.publish(state, [(L, S, 3)], add_week3(state))
    assert report['fact_player_statistics']['after'] == 5
    assert state.published_weeks(L, S) == {3}
    assert state.unpublished_raw() == [(L, S, 1), (L, S, 2)]  # untouched here
    with state.engine.connect() as conn:
        n = conn.execute(text(
            "SELECT count(*) FROM pg_namespace WHERE nspname IN "
            "('edw_prev', 'edw_broken')")).scalar()
    assert n == 0, 'snapshot must be dropped after success'


def test_refresh_crash_restores_previous_generation(state):
    before = edw_state(state)

    def exploding_refresh():
        with state.engine.begin() as conn:
            # Do visible damage, then die mid-run (processor commits
            # per-table, so damage IS committed when the crash hits).
            conn.execute(text('DELETE FROM edw.fact_player_statistics'))
        raise RuntimeError('processor died mid-refresh')

    with pytest.raises(RuntimeError, match='died mid-refresh'):
        pub.publish(state, [(L, S, 2)], exploding_refresh)

    assert edw_state(state) == before, 'previous generation must be restored intact'
    assert state.published_weeks(L, S) == set()
    # Raw stays ahead of EDW: next run repairs publication, no Yahoo calls.
    assert (L, S, 1) in state.unpublished_raw()


def test_wipe_class_bug_caught_by_shrink_gate(state):
    before = edw_state(state)

    def season_wipe_refresh():
        # The classic: something deletes far more than it reinserts.
        with state.engine.begin() as conn:
            conn.execute(text(
                'DELETE FROM edw.fact_player_statistics WHERE season_year = :s'),
                {'s': S})
        return True

    # Seed enough rows that the >20-row guard applies.
    with state.engine.begin() as conn:
        conn.execute(text(
            'INSERT INTO edw.fact_player_statistics '
            f"SELECT '{L}', {S}, 1, 1.0 FROM generate_series(1, 40)"))
    before = edw_state(state)

    with pytest.raises(pub.PublishVerificationError, match='shrank'):
        pub.publish(state, [(L, S, 2)], season_wipe_refresh)

    assert edw_state(state) == before


def test_invisible_period_fails_verification(state):
    with state.engine.begin() as conn:
        state.mark_raw_complete(conn, L, S, 9, {'matchups': 1, 'rosters': 1,
                                                'statistics': 1})

    def refresh_that_skips_week9():
        return True  # commits nothing for week 9

    before = edw_state(state)
    with pytest.raises(pub.PublishVerificationError, match='w9 not visible'):
        pub.publish(state, [(L, S, 9)], refresh_that_skips_week9)
    assert edw_state(state) == before
    assert 9 not in state.published_weeks(L, S)


def test_snapshot_clones_views_in_dependency_order(state):
    tables = pub.clone_edw_snapshot(state.engine)
    assert set(tables) == {'fact_player_statistics', 'fact_matchup'}
    with state.engine.connect() as conn:
        views = sorted(r[0] for r in conn.execute(text(
            "SELECT viewname FROM pg_views WHERE schemaname='edw_prev'")))
        # view-on-view survived the rewrite; totals match source data
        pts = conn.execute(text(
            'SELECT pts2 FROM edw_prev.vw_totals_plus')).scalar()
    assert views == ['vw_totals', 'vw_totals_plus']
    assert pts == 44.0
    pub.drop_snapshot(state.engine)


def test_republish_after_restore_heals(state):
    """The full failure-then-heal cycle: crash, restore, then a later run
    publishes the still-unpublished periods without re-extraction."""
    def bad(): raise RuntimeError('boom')
    with pytest.raises(RuntimeError):
        pub.publish(state, [(L, S, 1), (L, S, 2)], bad)
    assert state.published_weeks(L, S) == set()

    pub.publish(state, [(L, S, 1), (L, S, 2)], lambda: True)
    assert state.published_weeks(L, S) == {1, 2}
    assert state.unpublished_raw() == []
