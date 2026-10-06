"""Batched INSERTs (src/utils/batch_sql.py) keep one-row-at-a-time semantics.

The raw loader and the EDW dimension upserts used to send one statement per
row; batching them cut the round trips to Neon but must not change results:
a repeated key in one batch still ends with the last row, the writes stay in
the caller's transaction, and dim_team still skips teams of unknown leagues.
Runs against a real local Postgres, like the other loader tests.
"""
import subprocess

import pytest
from sqlalchemy import create_engine, text

from src.edw_schema.edw_etl_processor import EdwEtlProcessor
from src.pipeline import raw_loader
from src.utils.batch_sql import insert_batched

TEST_DB = 'batch_writes_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'


@pytest.fixture(scope='module')
def engine():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    eng = create_engine(TEST_URL)
    with eng.begin() as conn:
        conn.execute(text("CREATE SCHEMA edw"))
        conn.execute(text("CREATE TABLE public.nums (n int PRIMARY KEY, label text)"))
        conn.execute(text("CREATE TABLE public.teams (team_id text PRIMARY KEY, team_name text)"))
        conn.execute(text("""CREATE TABLE edw.dim_league (league_key serial PRIMARY KEY,
            league_id text, season_year int)"""))
        conn.execute(text("""CREATE TABLE edw.dim_team (team_key serial PRIMARY KEY,
            team_id text UNIQUE, league_key int, manager_key int, team_name text,
            manager_name text, manager_id text, team_logo_url text,
            is_active boolean DEFAULT true, valid_from timestamp, valid_to timestamp)"""))
    yield eng
    eng.dispose()
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'],
                   check=True)


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nums, public.teams, edw.dim_team, edw.dim_league "
                          "RESTART IDENTITY"))


def count(engine, table):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()


def test_every_page_lands_and_commits_with_the_caller(engine):
    rows = [(i, f'n{i}') for i in range(2500)]
    with engine.begin() as conn:
        assert insert_batched(conn, 'public.nums', ['n', 'label'], rows, page_size=1000) == 2500
    assert count(engine, 'public.nums') == 2500


def test_a_rolled_back_caller_takes_the_batch_with_it(engine):
    with pytest.raises(RuntimeError):
        with engine.begin() as conn:
            insert_batched(conn, 'public.nums', ['n', 'label'], [(1, 'a'), (2, 'b')])
            raise RuntimeError('load failed after the insert')
    assert count(engine, 'public.nums') == 0


def test_raw_upsert_keeps_the_last_row_for_a_repeated_key(engine):
    rows = [{'team_id': 't1', 'team_name': 'first'},
            {'team_id': 't2', 'team_name': 'other'},
            {'team_id': 't1', 'team_name': 'last'}]
    with engine.begin() as conn:
        n = raw_loader.upsert_dimension(conn, 'teams', 'team_id', rows, ['team_name'])
    assert n == 3   # rows offered, as before batching
    with engine.connect() as conn:
        got = dict(conn.execute(text("SELECT team_id, team_name FROM public.teams")).fetchall())
    assert got == {'t1': 'last', 't2': 'other'}


def test_raw_insert_still_rejects_a_row_missing_a_column(engine):
    with pytest.raises(KeyError):
        with engine.begin() as conn:
            raw_loader._insert_rows(conn, 'nums', [{'n': 1, 'label': 'a'}, {'n': 2}], '')
    assert count(engine, 'public.nums') == 0


def test_dim_team_skips_unknown_leagues_and_keeps_last_per_team(engine):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO edw.dim_league (league_id, season_year) "
                          "VALUES ('L1', 2026)"))
    p = EdwEtlProcessor(database_url=TEST_URL)
    p.engine = engine
    team = dict(manager_name='m', manager_id='1', team_logo_url='', is_active=True,
                valid_from=None, valid_to=None)
    assert p.load_dimension_table('dim_team', [
        {**team, 'team_id': 'T1', 'league_id': 'L1', 'team_name': 'old name'},
        {**team, 'team_id': 'T2', 'league_id': 'NOPE', 'team_name': 'orphan'},
        {**team, 'team_id': 'T1', 'league_id': 'L1', 'team_name': 'new name'},
    ])
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT team_id, league_key, team_name FROM edw.dim_team")).fetchall()
    assert [tuple(r) for r in rows] == [('T1', 1, 'new name')]
