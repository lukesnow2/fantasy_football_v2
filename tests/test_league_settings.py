"""The league's playoff format, carried from Yahoo's settings into the warehouse.

Before migration 011 the pipeline kept neither the playoff start week nor the
number of playoff teams, so the season record guessed a three-round bracket
and the web app's odds guessed the regular-season length.
"""
from src.edw_schema.edw_etl_processor import (
    DIMENSION_UPSERTS, LEAGUE_FORMAT_SET, EdwEtlProcessor)
from src.extractors.comprehensive_data_extractor import playoff_settings
from src.pipeline.raw_loader import LEAGUE_UPDATE_COLS
from src.utils.batch_sql import upsert_set


def test_playoff_settings_reads_yahoo_strings_as_ints():
    assert playoff_settings({'playoff_start_week': '15', 'num_playoff_teams': '6',
                             'uses_playoff': '1'}) == {
        'playoff_start_week': 15, 'num_playoff_teams': 6}


def test_missing_or_blank_settings_are_unknown_not_guessed():
    assert playoff_settings({}) == {'playoff_start_week': None, 'num_playoff_teams': None}
    assert playoff_settings({'playoff_start_week': '', 'num_playoff_teams': '0'}) == {
        'playoff_start_week': None, 'num_playoff_teams': None}


def test_league_without_playoffs_has_no_format():
    assert playoff_settings({'uses_playoff': '0', 'playoff_start_week': '15',
                             'num_playoff_teams': '6'}) == {
        'playoff_start_week': None, 'num_playoff_teams': None}


def _processor(leagues, matchups=()):
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.data = {'leagues': leagues, 'matchups': list(matchups)}
    return proc


def test_dim_league_rows_carry_the_format():
    proc = _processor([{'league_id': 'L26', 'name': 'The League', 'season': '2026',
                        'num_teams': 10, 'end_week': '17',
                        'playoff_start_week': 15, 'num_playoff_teams': 6}])
    [row] = proc.transform_leagues()
    assert (row['end_week'], row['playoff_start_week'], row['num_playoff_teams']) == (17, 15, 6)
    # Every column the batched upsert writes is present on the row.
    columns = DIMENSION_UPSERTS['dim_league'][0]
    assert all(c in row for c in columns)


def test_raw_rows_from_before_migration_011_carry_nulls():
    proc = _processor([{'league_id': 'L26', 'name': 'The League', 'season': '2026',
                        'num_teams': 10, 'end_week': '17'}])
    [row] = proc.transform_leagues()
    assert row['playoff_start_week'] is None and row['num_playoff_teams'] is None


def test_upsert_never_replaces_a_stored_format_with_null():
    """Re-publishing a season extracted before migration 011 must keep the
    values the migration backfilled from its bracket."""
    _, _, update_cols, extra = DIMENSION_UPSERTS['dim_league']
    sql = upsert_set(update_cols, extra)
    for col in ('end_week', 'playoff_start_week', 'num_playoff_teams'):
        assert col not in update_cols
        assert f'{col} = COALESCE(EXCLUDED.{col}, dim_league.{col})' in sql
    assert extra == LEAGUE_FORMAT_SET


def test_weekly_load_refreshes_the_format_on_the_raw_row():
    for col in ('end_week', 'playoff_start_week', 'num_playoff_teams'):
        assert col in LEAGUE_UPDATE_COLS


def test_in_progress_season_takes_the_configured_playoff_start():
    """A league that starts its playoffs in week 16 of 17 (a two-round
    bracket) used to be recorded as starting in week 15."""
    proc = _processor(
        [{'league_id': 'L26', 'season': '2026', 'start_week': '1', 'end_week': '17',
          'current_week': '4', 'playoff_start_week': 16}],
        [{'league_id': 'L26', 'week': w, 'is_playoffs': False, 'is_championship': False}
         for w in (1, 2, 3)])
    season = {s['season_year']: s for s in proc.extract_seasons()}[2026]
    assert season['playoff_start_week'] == 16
    assert season['championship_week'] == 17


def test_in_progress_season_without_settings_keeps_the_old_fallback():
    proc = _processor(
        [{'league_id': 'L26', 'season': '2026', 'start_week': '1', 'end_week': '17',
          'current_week': '4'}],
        [{'league_id': 'L26', 'week': w, 'is_playoffs': False, 'is_championship': False}
         for w in (1, 2, 3)])
    season = {s['season_year']: s for s in proc.extract_seasons()}[2026]
    assert season['playoff_start_week'] == 15
