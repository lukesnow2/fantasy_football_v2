"""Phase 2A tests: period completeness, gap detection, advisory lock, ledger.

Run against a real local Postgres (created/dropped per session) because the
guarantees under test - advisory locks, transactional flag commits, ledger
autocommit survival - are database behaviors, not Python behaviors.
"""
import subprocess

import pytest
from sqlalchemy import text

from src.pipeline.state import PipelineState, LockHeld

TEST_DB = 'pipeline_state_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'


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
    yield st
    st.close()


L, S = '461.l.1', 2026


def _load_week(state, week, entities=None):
    """Simulate a committed raw load of one week."""
    with state.engine.begin() as conn:
        state.mark_raw_complete(conn, L, S, week, entities or
                                {'matchups': 5, 'rosters': 150, 'statistics': 170})


def _publish_week(state, week):
    with state.engine.begin() as conn:
        state.mark_published(conn, L, S, week)


# ---------------------------------------------------------------- gap logic

def test_empty_db_full_gap(state):
    gap = state.compute_gap(L, S, [1, 2, 3])
    assert gap == {'extract': [1, 2, 3], 'publish_repair': []}


def test_up_to_date_extracts_only_reload_window(state):
    for w in (1, 2, 3):
        _load_week(state, w)
        _publish_week(state, w)
    gap = state.compute_gap(L, S, [1, 2, 3])
    # Rolling window: the two most recent complete weeks re-fetch for
    # Yahoo stat corrections; nothing else.
    assert gap == {'extract': [2, 3], 'publish_repair': []}


def test_no_reload_window_is_a_true_noop(state):
    for w in (1, 2, 3):
        _load_week(state, w)
        _publish_week(state, w)
    gap = state.compute_gap(L, S, [1, 2, 3], reload_window=0)
    assert gap == {'extract': [], 'publish_repair': []}


def test_interior_hole_detected(state):
    for w in (1, 2, 4, 5):
        _load_week(state, w)
        _publish_week(state, w)
    gap = state.compute_gap(L, S, [1, 2, 3, 4, 5], reload_window=0)
    # max(week) reasoning would say "up to date at 5"; period reasoning
    # finds the hole at 3.
    assert gap['extract'] == [3]


def test_partially_loaded_week_detected(state):
    _load_week(state, 1)
    _publish_week(state, 1)
    # Week 2: matchups landed, rosters/statistics did not.
    with state.engine.begin() as conn:
        state.mark_raw_complete(conn, L, S, 2, {'matchups': 5})
    gap = state.compute_gap(L, S, [1, 2], reload_window=0)
    assert gap['extract'] == [2]


def test_raw_complete_unpublished_routes_to_publish_repair(state):
    _load_week(state, 1)
    _publish_week(state, 1)
    _load_week(state, 2)  # raw committed, EDW refresh died before publish
    gap = state.compute_gap(L, S, [1, 2], reload_window=0)
    # Week 2 must NOT be re-extracted - we already possess it.
    assert gap == {'extract': [], 'publish_repair': [2]}
    assert state.unpublished_raw() == [(L, S, 2)]


def test_new_raw_data_unpublishes_a_period(state):
    _load_week(state, 1)
    _publish_week(state, 1)
    assert state.published_weeks(L, S) == {1}
    _load_week(state, 1)  # reload-window refetch commits new raw data
    assert state.published_weeks(L, S) == set()
    assert state.unpublished_raw() == [(L, S, 1)]
    _publish_week(state, 1)
    assert state.unpublished_raw() == []


# ---------------------------------------------------------------- locking

def test_lock_mutual_exclusion(test_db, state):
    other = PipelineState(test_db)
    try:
        assert state.try_lock() is True
        assert other.try_lock() is False  # fail-fast, no queueing
        state.release_lock()
        assert other.try_lock() is True
    finally:
        other.close()


def test_lock_context_manager_raises_lockheld(test_db, state):
    other = PipelineState(test_db)
    try:
        with state.lock():
            with pytest.raises(LockHeld):
                with other.lock():
                    pass
        # released on exit:
        with other.lock():
            pass
    finally:
        other.close()


# ---------------------------------------------------------------- ledger

def test_ledger_survives_data_rollback(state):
    run_id = state.start_run(season=S)
    try:
        with state.engine.begin() as conn:
            state.mark_raw_complete(conn, L, S, 1, {'matchups': 5})
            raise RuntimeError('simulated mid-load crash')
    except RuntimeError:
        state.finish_run(run_id, 'failed', error='simulated mid-load crash')

    # The data transaction rolled back...
    assert state.raw_complete_weeks(L, S) == set()
    # ...but the failure record survived on the autocommit ledger.
    with state.engine.connect() as conn:
        row = conn.execute(text(
            'SELECT status, error FROM public.pipeline_runs '
            'WHERE run_id = :r'), {'r': run_id}).fetchone()
    assert row[0] == 'failed'
    assert 'simulated' in row[1]


def test_last_successful_run_start(state):
    assert state.last_successful_run_start() is None
    r1 = state.start_run(season=S)
    state.finish_run(r1, 'success')
    assert state.last_successful_run_start() is not None


def test_readers_tolerate_missing_schema(test_db):
    """A dry run must be able to report against a database whose pipeline
    tables do not exist yet, without creating them."""
    import subprocess
    from src.pipeline.state import PipelineState
    subprocess.run(['psql', test_db.rsplit('/', 1)[-1], '-qc',
                    'DROP TABLE IF EXISTS public.pipeline_periods CASCADE'],
                   check=True)
    st = PipelineState(test_db)
    try:
        assert st.schema_exists() is False
        assert st.unpublished_raw() == []
        assert st.published_weeks('x', 2026) == set()
        assert st.raw_complete_weeks('x', 2026) == set()
        assert st.recorded_weeks('x', 2026, [1, 2]) == set()
        assert st.schema_exists() is False, 'reading must not create anything'
    finally:
        st.close()
