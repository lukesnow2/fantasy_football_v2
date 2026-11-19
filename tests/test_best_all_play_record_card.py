"""The Best all-play record card (migration 023).

This Season's all-play tile opens best_all_play_record. Its copy is written
three times: the migration, populate_metric_definitions.sql (a rebuild's seed)
and add_metric_limitations.sql (the limitations it sets after the seed). These
check the three agree and that the migration applies twice, as the pipeline
may re-apply it, including on a database without the Expected wins card it
links to.
"""
import subprocess

import pytest
from sqlalchemy import create_engine, text

TEST_DB = 'best_all_play_record_card_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'
SCHEMA = 'src/edw_schema/meta_data_schema.sql'
SEED = 'src/edw_schema/populate_metric_definitions.sql'
LIMITATIONS = 'src/edw_schema/add_metric_limitations.sql'
MIGRATION = 'src/edw_schema/migrations/023_best_all_play_record_definition.sql'

CARD = """SELECT metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide, limitations,
    data_type, unit_of_measure, typical_range, display_format, sort_order
    FROM meta_data.metric_definitions WHERE metric_id = 'best_all_play_record'"""
LINKS = """SELECT COUNT(*) FROM meta_data.metric_relationships
    WHERE primary_metric_id = 'best_all_play_record' AND related_metric_id = 'all_play_wins'"""


def _drop():
    # FORCE, so a connection a failed test left open can't block the drop.
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'],
                   check=True)


def _fresh(*files):
    _drop()
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    for path in files:
        subprocess.run(['psql', TEST_DB, '-q', '-v', 'ON_ERROR_STOP=1', '-f', path],
                       check=True, capture_output=True)
    return create_engine(TEST_URL)


def _migrate(engine):
    with open(MIGRATION) as f:
        sql = f.read()
    # A raw cursor with no parameters, as incremental_load applies it.
    with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
        cursor = conn.connection.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()


@pytest.fixture
def engine():
    """Builds the scratch database from the given files; drops it afterwards."""
    engines = []

    def build(*files):
        engines.append(_fresh(*files))
        return engines[-1]

    yield build
    for eng in engines:
        eng.dispose()
    _drop()


def test_the_seed_and_the_migration_carry_the_same_card(engine):
    eng = engine(SCHEMA, SEED, LIMITATIONS)
    with eng.connect() as conn:
        seeded = conn.execute(text(CARD)).one()
        assert conn.execute(text(LINKS)).scalar() == 1
    assert seeded.limitations

    for _ in range(2):
        _migrate(eng)
    with eng.connect() as conn:
        assert conn.execute(text(CARD)).one() == seeded
        assert conn.execute(text(LINKS)).scalar() == 1


def test_the_migration_applies_without_the_expected_wins_card(engine):
    eng = engine(SCHEMA)
    for _ in range(2):
        _migrate(eng)
    with eng.connect() as conn:
        card = conn.execute(text(CARD)).one()
        assert card.metric_name == 'Best all-play record'
        assert conn.execute(text(LINKS)).scalar() == 0
