"""The league's playoff format, carried from Yahoo's settings into the warehouse.

Before migration 011 the pipeline kept neither the playoff start week nor the
number of playoff teams, so the season record guessed a three-round bracket
and the web app's odds guessed the regular-season length.
"""
from src.edw_schema.edw_etl_processor import (
    DIMENSION_UPSERTS, LEAGUE_FORMAT_SET, EdwEtlProcessor)
from src.extractors.comprehensive_data_extractor import season_format
from src.pipeline.raw_loader import LEAGUE_UPDATE_COLS
from src.utils.batch_sql import upsert_set


UNKNOWN = {'playoff_start_week': None, 'num_playoff_teams': None, 'trade_end_date': None}


def test_season_format_reads_yahoo_strings():
    assert season_format({'playoff_start_week': '15', 'num_playoff_teams': '6',
                          'uses_playoff': '1', 'trade_end_date': '2026-11-22'}) == {
        'playoff_start_week': 15, 'num_playoff_teams': 6, 'trade_end_date': '2026-11-22'}


def test_missing_or_blank_settings_are_unknown_not_guessed():
    assert season_format({}) == UNKNOWN
    assert season_format({'playoff_start_week': '', 'num_playoff_teams': '0',
                          'trade_end_date': 'soon'}) == UNKNOWN


def test_league_without_playoffs_has_no_playoff_format():
    assert season_format({'uses_playoff': '0', 'playoff_start_week': '15',
                          'num_playoff_teams': '6', 'trade_end_date': '2026-11-22'}) == {
        **UNKNOWN, 'trade_end_date': '2026-11-22'}


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
    for col in ('end_week', 'playoff_start_week', 'num_playoff_teams', 'trade_end_date'):
        assert col not in update_cols
        assert f'{col} = COALESCE(EXCLUDED.{col}, dim_league.{col})' in sql
    assert extra == LEAGUE_FORMAT_SET


def test_weekly_load_refreshes_the_format_on_the_raw_row():
    for col in ('end_week', 'playoff_start_week', 'num_playoff_teams', 'trade_end_date'):
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


def _playoff_game(league_id, week, a, b):
    return {'league_id': league_id, 'week': week, 'is_playoffs': True,
            'is_consolation': False, 'team1_id': a, 'team2_id': b}


def test_league_format_prefers_the_stored_settings():
    proc = _processor([{'league_id': 'L26', 'season': '2026', 'end_week': '17',
                        'playoff_start_week': 16, 'num_playoff_teams': 4}])
    assert proc.league_format('L26') == {'playoff_start_week': 16, 'num_playoff_teams': 4}


def test_league_format_reads_a_finished_bracket_for_its_own_season_only():
    """The old lookup took the earliest playoff week across every season
    loaded, so 2010-2021's week-14 brackets put 2026's playoffs in week 14."""
    leagues = [{'league_id': 'L12', 'season': '2012', 'end_week': '16'},
               {'league_id': 'L26', 'season': '2026', 'end_week': '17'}]
    games = ([_playoff_game('L12', 14, f't{i}', f't{i + 1}') for i in (3, 5)]
             + [_playoff_game('L12', 15, 't1', 't3'), _playoff_game('L12', 15, 't2', 't5')])
    proc = _processor(leagues, games)
    assert proc.league_format('L12') == {'playoff_start_week': 14, 'num_playoff_teams': 6}
    # 2026 has no bracket yet: three rounds ending at its own end week.
    assert proc.league_format('L26')['playoff_start_week'] == 15


def test_consolation_games_are_not_counted_as_the_playoff_field():
    games = [_playoff_game('L25', 15, 'a', 'b'),
             {**_playoff_game('L25', 15, 'c', 'd'), 'is_consolation': True}]
    proc = _processor([{'league_id': 'L25', 'season': '2025', 'end_week': '17'}], games)
    assert proc.league_format('L25')['num_playoff_teams'] == 2


def test_playoff_probability_uses_the_leagues_field_size():
    """max(4, teams // 3) gave this ten-team, six-spot league four spots. Once
    the regular season is over the field size alone decides: fifth is in."""
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    after_week_14 = dict(win_percentage=0.5, current_rank=5, total_teams=10,
                         current_week=15, weekly_points=120, playoff_start_week=15)
    in_six = proc._calculate_playoff_probability(**after_week_14, playoff_teams=6)
    in_four = proc._calculate_playoff_probability(**after_week_14, playoff_teams=4)
    assert in_six > in_four
