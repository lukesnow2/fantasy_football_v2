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
            'CREATE TABLE edw.dim_week (week_key serial primary key,'
            ' season_year int, week_number int)'))
        conn.execute(text(
            'CREATE TABLE edw.fact_matchup (league_key text, season_year int,'
            ' week_key int, team1 text, team2 text)'))
        conn.execute(text(
            'CREATE TABLE edw.fact_roster (week_key int, player_key int,'
            ' league_key text)'))
        # The verification gate resolves league_id through dim_league so it
        # checks the right league's rows, not just the right season/week.
        conn.execute(text(
            'CREATE TABLE edw.dim_league (league_key text, league_id text)'))
        conn.execute(text(
            f"INSERT INTO edw.dim_league VALUES ('{L}', '{L}')"))
        # Raw sources the verification gate compares the EDW against.
        for ddl in (
            'CREATE TABLE IF NOT EXISTS public.statistics ('
            ' league_id text, week_number int)',
            'CREATE TABLE IF NOT EXISTS public.matchups (league_id text, week int)',
            'CREATE TABLE IF NOT EXISTS public.rosters (league_id text, week int)',
        ):
            conn.execute(text(ddl))
        conn.execute(text('TRUNCATE public.statistics, public.matchups,'
                          ' public.rosters'))
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
            wk = conn.execute(text(
                'INSERT INTO edw.dim_week (season_year, week_number)'
                f' VALUES ({S}, {w}) RETURNING week_key')).scalar()
            conn.execute(text(
                f"INSERT INTO edw.fact_matchup VALUES ('{L}', {S}, {wk}, 'a', 'b')"))
            conn.execute(text(
                f"INSERT INTO edw.fact_roster VALUES ({wk}, 1, '{L}')"))
            conn.execute(text(
                f"INSERT INTO public.statistics VALUES ('{L}', {w})"))
            conn.execute(text(f"INSERT INTO public.matchups VALUES ('{L}', {w})"))
            conn.execute(text(f"INSERT INTO public.rosters VALUES ('{L}', {w})"))
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

    # Seed enough rows that the >20-row guard applies, plus rows from another
    # season that survive the delete - so this exercises the FRACTIONAL guard
    # rather than the emptied-table one.
    with state.engine.begin() as conn:
        conn.execute(text(
            'INSERT INTO edw.fact_player_statistics '
            f"SELECT '{L}', {S}, 1, 1.0 FROM generate_series(1, 40)"))
        conn.execute(text(
            'INSERT INTO edw.fact_player_statistics '
            f"SELECT '{L}', {S - 1}, 1, 1.0 FROM generate_series(1, 5)"))
    before = edw_state(state)

    with pytest.raises(pub.PublishVerificationError, match='shrank'):
        pub.publish(state, [(L, S, 2)], season_wipe_refresh)

    assert edw_state(state) == before


def test_invisible_period_fails_verification(state):
    with state.engine.begin() as conn:
        state.mark_raw_complete(conn, L, S, 9, {'matchups': 1, 'rosters': 1,
                                                'statistics': 1})
        # Raw rows exist for week 9, so a refresh that publishes nothing for
        # it is the silent-drop case the gate must catch.
        conn.execute(text(f"INSERT INTO public.statistics VALUES ('{L}', 9)"))
        conn.execute(text(f"INSERT INTO public.matchups VALUES ('{L}', 9)"))
        conn.execute(text(f"INSERT INTO public.rosters VALUES ('{L}', 9)"))

    def refresh_that_skips_week9():
        return True  # commits nothing for week 9

    before = edw_state(state)
    with pytest.raises(pub.PublishVerificationError,
                       match='w9: public.statistics has rows'):
        pub.publish(state, [(L, S, 9)], refresh_that_skips_week9)
    assert edw_state(state) == before
    assert 9 not in state.published_weeks(L, S)


def test_snapshot_clones_views_in_dependency_order(state):
    tables = pub.clone_edw_snapshot(state.engine)
    assert set(tables) == {'fact_player_statistics', 'fact_matchup',
                           'fact_roster', 'dim_week', 'dim_league'}
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


# --------------------------------------------------- snapshot fidelity
# CREATE TABLE (LIKE ... INCLUDING ALL) does NOT copy foreign keys, so a
# restore used to promote a schema stripped of referential integrity and
# drop the original - permanently. Production carried 36 FKs; a database
# that had been through restores carried none.

def _fk_count(conn, schema):
    return conn.execute(text(
        "SELECT count(*) FROM pg_constraint "
        "WHERE contype='f' AND connamespace = CAST(:s AS regnamespace)"),
        {'s': schema}).scalar()


@pytest.fixture
def state_with_fk(state):
    with state.engine.begin() as conn:
        conn.execute(text(
            'ALTER TABLE edw.dim_week ADD CONSTRAINT dim_week_pk_u '
            'UNIQUE (week_key)'))
        conn.execute(text(
            'ALTER TABLE edw.fact_matchup ADD CONSTRAINT fact_matchup_week_fk '
            'FOREIGN KEY (week_key) REFERENCES edw.dim_week(week_key)'))
    return state


def test_snapshot_preserves_foreign_keys(state_with_fk):
    state = state_with_fk
    with state.engine.connect() as conn:
        assert _fk_count(conn, 'edw') == 1
    pub.clone_edw_snapshot(state.engine)
    with state.engine.connect() as conn:
        assert _fk_count(conn, pub.SNAPSHOT_SCHEMA) == 1, \
            "snapshot must carry the source's foreign keys"
    pub.drop_snapshot(state.engine)


def test_failed_publish_keeps_foreign_keys(state_with_fk):
    state = state_with_fk

    def failing_refresh():
        return False

    with pytest.raises(RuntimeError):
        pub.publish(state, [(L, S, 1)], failing_refresh)

    with state.engine.connect() as conn:
        assert _fk_count(conn, 'edw') == 1, \
            "restore must not strip referential integrity"


def test_emptied_small_table_fails_verification(state):
    """dim_manager holds exactly 20 rows, so the fractional shrink guard
    (before > 20) never covered it; a wipe published silently."""
    with state.engine.begin() as conn:
        conn.execute(text('CREATE TABLE edw.dim_small (id int)'))
        conn.execute(text(
            'INSERT INTO edw.dim_small SELECT generate_series(1, 5)'))

    def refresh_that_empties_it():
        with state.engine.begin() as conn:
            conn.execute(text('DELETE FROM edw.dim_small'))
        return True

    with pytest.raises(pub.PublishVerificationError, match='emptied'):
        pub.publish(state, [(L, S, 1)], refresh_that_empties_it)
    with state.engine.connect() as conn:
        assert conn.execute(text(
            'SELECT count(*) FROM edw.dim_small')).scalar() == 5


def test_batch_upsert_survives_commit(state):
    """A batched upsert runs on the raw DBAPI cursor, which SQLAlchemy does
    not see. Without an explicit transaction its commit() is a no-op and
    every 'upserted' row is silently rolled back - the upsert still reports
    success, so only the verification gate catches it."""
    from src.edw_schema.edw_etl_processor import EdwEtlProcessor
    import pandas as pd

    with state.engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS edw.batch_probe'))
        conn.execute(text(
            'CREATE TABLE edw.batch_probe (id int primary key, v text)'))

    df = pd.DataFrame([{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'b'}])
    with state.engine.connect() as conn:
        n = EdwEtlProcessor._batch_upsert(
            conn, 'batch_probe', ['id', 'v'], 'id', ['v'], df)
        conn.commit()
    assert n == 2

    with state.engine.connect() as conn:
        assert conn.execute(text(
            'SELECT count(*) FROM edw.batch_probe')).scalar() == 2, \
            'batched rows must survive the commit'

    # And it must still upsert rather than duplicate.
    df2 = pd.DataFrame([{'id': 2, 'v': 'B2'}, {'id': 3, 'v': 'c'}])
    with state.engine.connect() as conn:
        EdwEtlProcessor._batch_upsert(
            conn, 'batch_probe', ['id', 'v'], 'id', ['v'], df2)
        conn.commit()
    with state.engine.connect() as conn:
        rows = dict(conn.execute(text(
            'SELECT id, v FROM edw.batch_probe ORDER BY id')).fetchall())
    assert rows == {1: 'a', 2: 'B2', 3: 'c'}
