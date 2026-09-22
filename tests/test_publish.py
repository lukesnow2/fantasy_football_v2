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
            ' weekly_fantasy_points float, player_key int)'))
        conn.execute(text(
            'CREATE TABLE edw.dim_player (player_key int, player_id text)'))
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
            ' league_id text, week_number int, player_id text)',
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
                           'fact_roster', 'dim_week', 'dim_league',
                           'dim_player'}
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


def test_batch_upsert_collapses_duplicate_keys(state):
    """One statement per row tolerated a repeated business key; a single
    multi-row ON CONFLICT raises 'cannot affect row a second time' and takes
    the whole refresh down. Duplicates must be collapsed, last write wins."""
    from src.edw_schema.edw_etl_processor import EdwEtlProcessor
    import pandas as pd

    with state.engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS edw.dup_probe'))
        conn.execute(text(
            'CREATE TABLE edw.dup_probe (id int primary key, v text)'))

    df = pd.DataFrame([{'id': 1, 'v': 'first'},
                       {'id': 1, 'v': 'last'},
                       {'id': 2, 'v': 'other'}])
    with state.engine.connect() as conn:
        EdwEtlProcessor._batch_upsert(
            conn, 'dup_probe', ['id', 'v'], 'id', ['v'], df)
        conn.commit()

    with state.engine.connect() as conn:
        rows = dict(conn.execute(text(
            'SELECT id, v FROM edw.dup_probe ORDER BY id')).fetchall())
    assert rows == {1: 'last', 2: 'other'}


def _setup_player_level(state, players=(11, 12, 13)):
    """Give the fixture's statistics tables the player grain reconciliation
    works at: EDW rows keyed by player_key, raw rows by player_id."""
    with state.engine.begin() as conn:
        conn.execute(text('TRUNCATE edw.dim_player'))
        conn.execute(text('DELETE FROM edw.fact_player_statistics WHERE week_number = 1'))
        conn.execute(text('DELETE FROM public.statistics WHERE week_number = 1'))
        for pk in players:
            conn.execute(text('INSERT INTO edw.dim_player VALUES (:k, :i)'),
                         {'k': pk, 'i': str(pk)})
            conn.execute(text(
                'INSERT INTO edw.fact_player_statistics '
                '(league_key, season_year, week_number, weekly_fantasy_points, player_key) '
                f"VALUES ('{L}', {S}, 1, 5.0, :k)"), {'k': pk})
            conn.execute(text(
                'INSERT INTO public.statistics (league_id, week_number, player_id) '
                f"VALUES ('{L}', 1, :i)"), {'i': str(pk)})


def _edw_stat_count(state, week=1):
    with state.engine.connect() as conn:
        return conn.execute(text(
            'SELECT count(*) FROM edw.fact_player_statistics '
            f"WHERE league_key = '{L}' AND week_number = :w"), {'w': week}).scalar()


def test_reconcile_deletions_removes_rows_raw_no_longer_has(state):
    """The upsert-only statistics path never removes a retracted row.

    The rolling reload window re-fetches the two most recent weeks to pick
    up Yahoo's corrections. Additions and revisions propagate through the
    business-key upsert; retractions do not, so a voided stat line stays
    in the warehouse - and on the site - for good.
    """
    _setup_player_level(state)
    assert _edw_stat_count(state) == 3
    assert pub.reconcile_deletions(state.engine, [(L, S, 1)]) == 0

    # Yahoo retracts one player: the raw period is replaced with two rows,
    # the EDW upsert leaves all three in place.
    with state.engine.begin() as conn:
        conn.execute(text("DELETE FROM public.statistics "
                          f"WHERE league_id = '{L}' AND week_number = 1 "
                          "AND player_id = '13'"))
    assert _edw_stat_count(state) == 3

    assert pub.reconcile_deletions(state.engine, [(L, S, 1)]) == 1
    assert _edw_stat_count(state) == 2


def test_reconcile_deletions_will_not_act_on_an_empty_raw_side(state):
    """An empty raw side is not a retraction - the same rule load_delta
    follows. Deleting here would turn one failed fetch into warehouse
    data loss."""
    _setup_player_level(state)
    with state.engine.begin() as conn:
        conn.execute(text("DELETE FROM public.statistics "
                          f"WHERE league_id = '{L}' AND week_number = 1"))

    assert pub.reconcile_deletions(state.engine, [(L, S, 1)]) == 0
    assert _edw_stat_count(state) == 3


def test_reconcile_deletions_skips_a_period_that_claims_nothing(state):
    """A period whose statistics were never fetched must not be reconciled
    against a raw table it never populated."""
    _setup_player_level(state)
    with state.engine.begin() as conn:
        conn.execute(text('UPDATE public.pipeline_periods SET '
                          'raw_statistics_complete = false '
                          f"WHERE league_id = '{L}' AND week = 1"))
        conn.execute(text("DELETE FROM public.statistics "
                          f"WHERE league_id = '{L}' AND week_number = 1 "
                          "AND player_id = '13'"))

    assert pub.reconcile_deletions(state.engine, [(L, S, 1)]) == 0
    assert _edw_stat_count(state) == 3


def _default_expr(state, schema, table, col):
    with state.engine.connect() as conn:
        return conn.execute(text("""
            SELECT pg_get_expr(d.adbin, d.adrelid)
            FROM pg_class c
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = :c
            LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE c.relnamespace = CAST(:s AS regnamespace) AND c.relname = :t
        """), {'s': schema, 't': table, 'c': col}).scalar()


def test_restored_snapshot_can_still_insert(state):
    """A restore must not strip the SERIAL defaults.

    CREATE TABLE (LIKE ... INCLUDING ALL) copies the default text, so the
    clone's keys default to nextval() on the SOURCE schema's sequence.
    restore_snapshot then drops that schema CASCADE, destroying those
    sequences and the defaults with them - leaving a restored warehouse
    that cannot insert a single dimension row. The pipeline hid this by
    repairing defaults at the top of every run; deploy_complete_edw.py
    does not, so the documented rebuild died on a NOT NULL violation.
    """
    pub.ensure_edw_serial_defaults(state.engine)
    assert _default_expr(state, 'edw', 'dim_week', 'week_key')

    pub.clone_edw_snapshot(state.engine)
    # The snapshot owns its sequence, so it survives the old schema's drop.
    assert 'edw_prev' in _default_expr(state, 'edw_prev', 'dim_week', 'week_key')

    pub.restore_snapshot(state.engine)
    assert _default_expr(state, 'edw', 'dim_week', 'week_key')

    # The real proof: the restored warehouse accepts an insert.
    with state.engine.begin() as conn:
        key = conn.execute(text(
            'INSERT INTO edw.dim_week (season_year, week_number) '
            f'VALUES ({S}, 9) RETURNING week_key')).scalar()
    assert key is not None


def test_restore_keeps_foreign_keys_held_by_other_schemas(state):
    """app.user and app.league_member reference edw.dim_manager. Those keys
    follow the renamed tables by OID, so without re-pointing them the
    restore's DROP SCHEMA edw_broken CASCADE deleted them - silently, on
    every failed publish."""
    with state.engine.begin() as conn:
        conn.execute(text('DROP SCHEMA IF EXISTS ext CASCADE'))
        conn.execute(text('CREATE SCHEMA ext'))
        conn.execute(text(
            'CREATE TABLE ext.member (id serial primary key, week_key int, '
            'CONSTRAINT member_week_fk FOREIGN KEY (week_key) '
            'REFERENCES edw.dim_week(week_key))'))
        wk = conn.execute(text('SELECT min(week_key) FROM edw.dim_week')).scalar()
        conn.execute(text('INSERT INTO ext.member (week_key) VALUES (:w)'), {'w': wk})

    pub.clone_edw_snapshot(state.engine)
    pub.restore_snapshot(state.engine)

    with state.engine.connect() as conn:
        target = conn.execute(text(
            "SELECT confrelid::regclass::text, convalidated FROM pg_constraint "
            "WHERE conname = 'member_week_fk'")).fetchone()
    assert target == ('edw.dim_week', True)
    with pytest.raises(Exception, match='member_week_fk'):
        with state.engine.begin() as conn:
            conn.execute(text('INSERT INTO ext.member (week_key) VALUES (-1)'))
    with state.engine.begin() as conn:
        conn.execute(text('DROP SCHEMA ext CASCADE'))


def test_preflight_finishes_not_valid_inbound_keys(state):
    """A restore re-adds inbound keys NOT VALID and validates once. If that
    one attempt fails, nothing retried it - the key stayed NOT VALID
    forever. The per-run preflight finishes it once the data allows."""
    with state.engine.begin() as conn:
        conn.execute(text('DROP SCHEMA IF EXISTS ext CASCADE'))
        conn.execute(text('CREATE SCHEMA ext'))
        conn.execute(text('CREATE TABLE ext.member (week_key int)'))
        wk = conn.execute(text('SELECT min(week_key) FROM edw.dim_week')).scalar()
        conn.execute(text('INSERT INTO ext.member VALUES (:w)'), {'w': wk})
        conn.execute(text(
            'ALTER TABLE ext.member ADD CONSTRAINT member_week_fk FOREIGN KEY '
            '(week_key) REFERENCES edw.dim_week(week_key) NOT VALID'))

    pub.check_inbound_foreign_keys(state.engine)

    with state.engine.connect() as conn:
        assert conn.execute(text(
            "SELECT convalidated FROM pg_constraint "
            "WHERE conname = 'member_week_fk'")).scalar() is True
    with state.engine.begin() as conn:
        conn.execute(text('DROP SCHEMA ext CASCADE'))


def test_preflight_names_missing_app_keys(state):
    """dev had lost both drizzle-declared app -> edw.dim_manager keys and
    nothing said so, which is why every dev verification ran against a
    different key graph from production's."""
    with state.engine.begin() as conn:
        conn.execute(text('DROP SCHEMA IF EXISTS app CASCADE'))
        conn.execute(text('CREATE SCHEMA app'))
        conn.execute(text('CREATE TABLE app.league_member (manager_key int)'))
        conn.execute(text('CREATE TABLE app."user" (manager_key int)'))
    try:
        missing = pub.check_inbound_foreign_keys(state.engine)
        assert set(missing) == {c for _, c in pub.EXPECTED_INBOUND_FKS}
    finally:
        with state.engine.begin() as conn:
            conn.execute(text('DROP SCHEMA app CASCADE'))


def test_preflight_is_silent_without_an_app_schema(state):
    assert pub.check_inbound_foreign_keys(state.engine) == []


def test_restore_keeps_views_when_edw_is_on_the_search_path(test_db):
    """Production's database default is 'app, edw, public' (RUNBOOK). There,
    pg_get_viewdef returned unqualified table names, the snapshot's views
    were created reading the LIVE edw tables, and restore's DROP ... CASCADE
    of the old schema took every view with it."""
    from sqlalchemy import create_engine

    db = 'publish_searchpath_test'
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {db} WITH (FORCE)'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {db}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc',
                    f'ALTER DATABASE {db} SET search_path TO app, edw, public'],
                   check=True)
    engine = create_engine(f'postgresql://localhost:5432/{db}')
    try:
        with engine.begin() as conn:
            conn.execute(text('CREATE SCHEMA edw'))
            conn.execute(text('CREATE TABLE edw.fact_x (pts int)'))
            conn.execute(text('INSERT INTO edw.fact_x VALUES (10)'))
            conn.execute(text('CREATE VIEW edw.vw_pts AS SELECT sum(pts) AS pts '
                              'FROM edw.fact_x'))
            conn.execute(text('CREATE VIEW edw.vw_pts2 AS SELECT pts FROM edw.vw_pts'))

        pub.clone_edw_snapshot(engine)
        with engine.connect() as conn:
            reads = {r[0] for r in conn.execute(text("""
                SELECT DISTINCT t.relnamespace::regnamespace::text
                FROM pg_rewrite r JOIN pg_depend d ON d.objid = r.oid
                JOIN pg_class t ON t.oid = d.refobjid
                WHERE r.ev_class = CAST('edw_prev.vw_pts' AS regclass)
                  AND t.oid <> r.ev_class"""))}
        assert reads == {'edw_prev'}, 'snapshot view reads the live schema'

        pub.restore_snapshot(engine)
        with engine.connect() as conn:
            views = sorted(r[0] for r in conn.execute(text(
                "SELECT viewname FROM pg_views WHERE schemaname = 'edw'")))
            assert views == ['vw_pts', 'vw_pts2']
            assert conn.execute(text('SELECT pts FROM edw.vw_pts2')).scalar() == 10
    finally:
        engine.dispose()
        subprocess.run(['psql', 'postgres', '-qc',
                        f'DROP DATABASE IF EXISTS {db} WITH (FORCE)'], check=True)


def test_retarget_rewrites_only_whole_qualifiers():
    assert pub._retarget('FROM edw.fact_x fedw JOIN edw.dim y ON fedw.k = y.k',
                         'edw', 'edw_prev') == \
        'FROM edw_prev.fact_x fedw JOIN edw_prev.dim y ON fedw.k = y.k'


def test_preflight_accepts_an_app_key_under_any_name(state):
    """A key re-added by hand under Postgres's default name protects the rows
    just the same; reporting it missing every run trains people to ignore
    the warning."""
    with state.engine.begin() as conn:
        conn.execute(text('DROP SCHEMA IF EXISTS app CASCADE'))
        conn.execute(text('CREATE SCHEMA app'))
        conn.execute(text('CREATE TABLE edw.dim_manager (manager_key int primary key)'))
        conn.execute(text('CREATE TABLE app.league_member (manager_key int '
                          'REFERENCES edw.dim_manager(manager_key))'))
        conn.execute(text('CREATE TABLE app."user" (manager_key int)'))
    try:
        missing = pub.check_inbound_foreign_keys(state.engine, validate=False)
        assert missing == ['user_manager_key_dim_manager_manager_key_fk']
    finally:
        with state.engine.begin() as conn:
            conn.execute(text('DROP SCHEMA app CASCADE'))
            conn.execute(text('DROP TABLE edw.dim_manager'))
