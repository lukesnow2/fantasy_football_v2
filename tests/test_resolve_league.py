"""--season must pick the league of record, never an arbitrary row.

Five seasons carry more than one league id in public.leagues, and the old
lookup took whichever row came back first (no ORDER BY) - a repair of one of
those seasons could reload a league the warehouse excludes.
"""
import subprocess

import pytest
from sqlalchemy import create_engine, text

from scripts.incremental_load import check_league_season, league_for_season

TEST_DB = 'resolve_league_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'


@pytest.fixture
def conn():
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    engine = create_engine(TEST_URL)
    with engine.begin() as c:
        c.execute(text('CREATE SCHEMA edw'))
        c.execute(text('CREATE TABLE public.leagues (league_id text, season text)'))
        c.execute(text('CREATE TABLE edw.dim_league (league_id text, season_year int)'))
        c.execute(text("INSERT INTO public.leagues VALUES "
                       "('359.l.1', '2016'), "
                       "('175.l.86092', '2007'), ('175.l.658531', '2007'), "
                       "('242.l.1', '2010'), ('242.l.2', '2010')"))
        c.execute(text("INSERT INTO edw.dim_league VALUES "
                       "('359.l.1', 2016), ('175.l.658531', 2007)"))
    with engine.connect() as c:
        yield c
    engine.dispose()
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)


def test_single_league_season(conn):
    assert league_for_season(conn, 2016) == '359.l.1'


def test_two_leagues_picks_the_league_of_record(conn):
    # '175.l.658531' sorts after '175.l.86092', so "first row" would be wrong.
    assert league_for_season(conn, 2007) == '175.l.658531'


def test_ambiguity_is_refused_not_guessed(conn):
    with pytest.raises(SystemExit, match='2 leagues'):
        league_for_season(conn, 2010)


def test_unknown_season_is_refused(conn):
    with pytest.raises(SystemExit, match='no league found'):
        league_for_season(conn, 1999)


def test_league_and_season_must_agree(conn):
    check_league_season(conn, '359.l.1', 2016)
    with pytest.raises(SystemExit, match='is season 2016, not 2017'):
        check_league_season(conn, '359.l.1', 2017)
