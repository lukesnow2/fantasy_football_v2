"""The documented full rebuild must never reach outside the warehouse.

RUNBOOK's rebuild runs deploy_complete_edw.py --force-rebuild. It used to
TRUNCATE edw.dim_manager RESTART IDENTITY CASCADE, and the web app's
app.user / app.league_member hold foreign keys into dim_manager - so the
cascade emptied the sign-in allowlist and, through league_member, chat,
bets, rule votes and the constitution.
"""
import subprocess

import pytest
from sqlalchemy import text

from scripts.deploy_complete_edw import EdwDeployment

TEST_DB = 'deploy_truncate_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'

GRAPH = """
CREATE SCHEMA edw; CREATE SCHEMA app;
CREATE TABLE edw.dim_manager (manager_key serial primary key, manager_name text unique);
CREATE TABLE edw.dim_team (team_key serial primary key,
  manager_key int REFERENCES edw.dim_manager(manager_key));
CREATE TABLE edw.fact_roster (roster_key serial primary key,
  team_key int REFERENCES edw.dim_team(team_key));
CREATE TABLE app.league_member (member_key serial primary key,
  manager_key int REFERENCES edw.dim_manager(manager_key));
CREATE TABLE app.chat_message (id serial primary key,
  author_key int REFERENCES app.league_member(member_key));
INSERT INTO edw.dim_manager (manager_name) VALUES ('A'), ('B');
INSERT INTO edw.dim_team (manager_key) VALUES (1), (2);
INSERT INTO edw.fact_roster (team_key) VALUES (1), (2);
INSERT INTO app.league_member (manager_key) VALUES (1), (2);
INSERT INTO app.chat_message (author_key) VALUES (1), (2);
"""


@pytest.fixture
def deployment():
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    subprocess.run(['psql', TEST_URL, '-qc', GRAPH], check=True)
    d = EdwDeployment(TEST_URL, force_rebuild=True)
    assert d.connect_database()
    yield d
    d.engine.dispose()
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)


def _count(d, table):
    with d.engine.connect() as conn:
        return conn.execute(text(f'SELECT count(*) FROM {table}')).scalar()


def test_force_rebuild_truncate_leaves_app_data_and_manager_keys(deployment):
    assert deployment.truncate_edw_tables()

    assert _count(deployment, 'app.league_member') == 2
    assert _count(deployment, 'app.chat_message') == 2
    assert _count(deployment, 'edw.dim_manager') == 2   # keys the app stores
    assert _count(deployment, 'edw.dim_team') == 0      # the warehouse is cleared
    assert _count(deployment, 'edw.fact_roster') == 0
