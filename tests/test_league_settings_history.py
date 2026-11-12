"""A league-season's full Yahoo settings: scoring, bonuses, lineup (migration 012).

Before 012 the pipeline kept three settings and threw the rest away, so a
21-season trend could not tell a scoring change (receptions from 2008,
six-point passing touchdowns from 2025) from a change in how the league played.
"""
import subprocess

import pytest
from sqlalchemy import create_engine, text

from src.edw_schema.edw_etl_processor import (
    DIMENSION_UPSERTS, LEAGUE_SETTINGS_COLUMNS, EdwEtlProcessor)
from src.extractors.league_settings import parse_league_settings
from src.pipeline import raw_loader
from src.utils.batch_sql import upsert_set

L26, L25 = '470.l.744846', '461.l.654923'


def stat(stat_id, name, **extra):
    return {'stat': {'stat_id': stat_id, 'enabled': '1', 'name': name,
                     'display_name': name[:8], 'group': 'passing',
                     'position_type': 'O', **extra}}


def settings_response(**overrides):
    """Yahoo's league/{key}/settings shape, trimmed to what matters."""
    settings = {
        'draft_type': 'self', 'is_auction_draft': '0', 'scoring_type': 'head',
        'uses_playoff': '1', 'playoff_start_week': '15', 'num_playoff_teams': '6',
        'num_playoff_consolation_teams': 6, 'uses_playoff_reseeding': 0,
        'has_multiweek_championship': 0, 'uses_median_score': False,
        'waiver_type': 'FR', 'waiver_rule': 'gametime', 'uses_faab': '1',
        'waiver_time': '2', 'trade_end_date': '2026-11-28',
        'trade_ratify_type': 'commish', 'trade_reject_time': '2',
        'post_draft_players': 'W', 'uses_fractional_points': '1',
        'uses_negative_points': '1', 'draft_time': '1788054408',
        'sendbird_channel_url': 'secret-channel',
        'roster_positions': [
            {'roster_position': {'position': 'QB', 'position_type': 'O', 'count': 1,
                                 'is_starting_position': 1}},
            {'roster_position': {'position': 'BN', 'count': 6, 'is_starting_position': 0}},
        ],
        'stat_categories': {'stats': [
            stat(4, 'Passing Yards'),
            stat(5, 'Passing Touchdowns'),
            stat(78, 'Targets', is_only_display_stat='1'),
        ]},
        'stat_modifiers': {'stats': [
            {'stat': {'stat_id': 4, 'value': '0.05', 'bonuses': [
                {'bonus': {'target': 150, 'points': '1'}},
                {'bonus': {'target': 250, 'points': '2'}}]}},
            # A single bonus arrives as an object, not a one-item list.
            {'stat': {'stat_id': 5, 'value': '6', 'bonuses': {'bonus': {'target': 3, 'points': '1'}}}},
        ]},
        **overrides,
    }
    meta = {'league_key': L26, 'name': 'The League', 'password': 'hunter2',
            'short_invitation_url': 'https://x/invitation?key=k', 'renew': '461_654923',
            'start_date': '2026-09-09', 'end_date': '2027-01-04', 'season': '2026'}
    return {'fantasy_content': {'league': [meta, {'settings': [settings]}]}}


# ---------------------------------------------------------------- parsing

def test_scoring_rules_carry_points_names_and_display_only_stats():
    parsed = parse_league_settings(settings_response(), L26)
    rules = {r['stat_id']: r for r in parsed['scoring']}
    assert rules[4]['points'] == 0.05 and rules[4]['stat_name'] == 'Passing Yards'
    assert rules[5]['points'] == 6.0
    # Targets are tracked but not scored: kept, with no points.
    assert rules[78]['points'] is None and rules[78]['is_display_only'] is True
    assert [r['stat_id'] for r in parsed['scoring']] == [4, 5, 78]  # Yahoo's order


def test_bonuses_from_a_list_and_from_a_lone_object():
    parsed = parse_league_settings(settings_response(), L26)
    assert sorted((b['stat_id'], b['target'], b['points']) for b in parsed['bonuses']) == [
        (4, 150.0, 1.0), (4, 250.0, 2.0), (5, 3.0, 1.0)]


def test_lineup_slots_in_order_with_starters_flagged():
    parsed = parse_league_settings(settings_response(), L26)
    assert [(s['position'], s['slot_count'], s['is_starting']) for s in parsed['roster_slots']] == [
        ('QB', 1, True), ('BN', 6, False)]


def test_league_scalars_are_typed():
    league = parse_league_settings(settings_response(), L26)['league']
    assert league['previous_league_id'] == L25
    assert (league['playoff_start_week'], league['num_playoff_teams'],
            league['num_playoff_consolation_teams']) == (15, 6, 6)
    assert league['uses_faab'] is True and league['uses_playoff_reseeding'] is False
    assert league['waiver_time'] == 2 and league['trade_end_date'] == '2026-11-28'
    assert league['start_date'] == '2026-09-09'
    assert league['draft_time'].year == 2026


def test_secrets_in_the_response_are_never_kept():
    parsed = parse_league_settings(settings_response(), L26)
    flat = repr(parsed)
    for secret in ('hunter2', 'invitation?key', 'secret-channel'):
        assert secret not in flat


def test_unsent_settings_are_unknown_not_guessed():
    raw = settings_response()
    for key in ('uses_faab', 'waiver_type', 'trade_end_date'):
        del raw['fantasy_content']['league'][1]['settings'][0][key]
    raw['fantasy_content']['league'][0]['renew'] = ''
    league = parse_league_settings(raw, L26)['league']
    assert league['uses_faab'] is None and league['waiver_type'] is None
    assert league['trade_end_date'] is None and league['previous_league_id'] is None


def test_league_without_playoffs_has_no_playoff_field():
    league = parse_league_settings(settings_response(uses_playoff='0'), L26)['league']
    assert league['uses_playoff'] is False
    assert league['playoff_start_week'] is None and league['num_playoff_teams'] is None


def test_a_response_without_settings_raises():
    with pytest.raises(ValueError):
        parse_league_settings({'fantasy_content': {'league': [{'league_key': L26}]}}, L26)


# ---------------------------------------------------------------- warehouse rules

def test_dim_league_takes_yahoos_values_and_never_nulls_a_stored_one():
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.data = {'leagues': [{'league_id': L26, 'name': 'The League', 'season': '2026',
                              'num_teams': 10, 'scoring_type': 'head', 'waiver_type': 'FR',
                              'uses_faab': True, 'draft_type': ''}]}
    [row] = proc.transform_leagues()
    assert row['scoring_type'] == 'head' and row['uses_faab'] is True
    assert row['draft_type'] is None  # blank is unknown, not ''
    assert row['waiver_rule'] is None  # not on a raw row loaded before 012
    columns, _, update_cols, extra = DIMENSION_UPSERTS['dim_league']
    assert all(c in row for c in columns)
    sql = upsert_set(update_cols, extra)
    for col in LEAGUE_SETTINGS_COLUMNS:
        assert col not in update_cols
        assert f'{col} = COALESCE(EXCLUDED.{col}, dim_league.{col})' in sql


def test_weekly_load_refreshes_every_setting_on_the_raw_row():
    for col in raw_loader.LEAGUE_SETTINGS_COLS:
        assert col in raw_loader.LEAGUE_UPDATE_COLS


# ---------------------------------------------------------------- database

TEST_DB = 'league_settings_test'
TEST_URL = f'postgresql://localhost:5432/{TEST_DB}'
MIGRATION = 'src/edw_schema/migrations/012_league_settings_history.sql'


@pytest.fixture(scope='module')
def engine():
    subprocess.run(['psql', 'postgres', '-qc', f'DROP DATABASE IF EXISTS {TEST_DB}'], check=True)
    subprocess.run(['psql', 'postgres', '-qc', f'CREATE DATABASE {TEST_DB}'], check=True)
    eng = create_engine(TEST_URL)
    with eng.begin() as conn:
        conn.execute(text("CREATE SCHEMA edw"))
        conn.execute(text("CREATE TABLE public.leagues (league_id text, season text)"))
        conn.execute(text("CREATE TABLE edw.dim_league (league_id varchar(50), season_year int)"))
    # The real migration, so the tests exercise the real tables.
    with open(MIGRATION) as f:
        sql = f.read()
    with eng.begin() as conn:
        conn.exec_driver_sql(sql.replace('BEGIN;', '').replace('COMMIT;', ''))
    yield eng
    eng.dispose()
    subprocess.run(['psql', 'postgres', '-qc',
                    f'DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)'], check=True)


def count(conn, table, league):
    return conn.execute(text(f'SELECT count(*) FROM {table} WHERE league_id = :l'),
                        {'l': league}).scalar()


def test_a_fetch_replaces_only_that_leagues_rules(engine):
    parsed = parse_league_settings(settings_response(), L26)
    with engine.begin() as conn:
        raw_loader.replace_league_settings(conn, L25, parsed)
        raw_loader.replace_league_settings(conn, L26, parsed)
        # The commissioner drops the passing-TD bonus; the next fetch has 2 bonuses.
        fewer = {**parsed, 'bonuses': parsed['bonuses'][:2]}
        written = raw_loader.replace_league_settings(conn, L26, fewer)
        assert written == 3 + 2 + 2
        assert count(conn, 'public.league_scoring_bonuses', L26) == 2
        assert count(conn, 'public.league_scoring_bonuses', L25) == 3  # untouched


def test_an_empty_fetch_deletes_nothing(engine):
    parsed = parse_league_settings(settings_response(), L26)
    with engine.begin() as conn:
        raw_loader.replace_league_settings(conn, L26, parsed)
        assert raw_loader.replace_league_settings(
            conn, L26, {'scoring': [], 'bonuses': [], 'roster_slots': []}) == 0
        assert count(conn, 'public.league_scoring', L26) == 3


def _processor(engine, data):
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.engine = engine
    proc.refreshed_tables = set()
    proc.data = data
    return proc


def test_warehouse_replaces_fetched_leagues_and_keeps_the_rest(engine):
    parsed = parse_league_settings(settings_response(), L26)
    rows = lambda key, league: [{**r, 'league_id': league} for r in parsed[key]]  # noqa: E731
    leagues = [{'league_id': L26, 'season': '2026'}, {'league_id': L25, 'season': '2025'},
               {'league_id': '999.l.1', 'season': '2019'}]  # not a league of record
    full = {'leagues': leagues,
            'league_scoring': rows('scoring', L26) + rows('scoring', L25) + rows('scoring', '999.l.1'),
            'league_scoring_bonuses': rows('bonuses', L26) + rows('bonuses', L25),
            'league_roster_slots': rows('roster_slots', L26) + rows('roster_slots', L25)}
    assert _processor(engine, full).load_league_settings_tables()
    with engine.connect() as conn:
        assert count(conn, 'edw.dim_scoring_rule', L26) == 3
        assert count(conn, 'edw.dim_scoring_rule', '999.l.1') == 0
        assert conn.execute(text(
            "SELECT season_year FROM edw.dim_roster_slot WHERE league_id = :l LIMIT 1"),
            {'l': L25}).scalar() == 2025

    # A later refresh where raw holds 2026's settings but not 2025's (not
    # fetched yet) replaces 2026 and leaves 2025 alone.
    later = {'leagues': leagues,
             'league_scoring': rows('scoring', L26)[:2],
             'league_scoring_bonuses': [],
             'league_roster_slots': rows('roster_slots', L26)}
    proc = _processor(engine, later)
    assert proc.load_league_settings_tables()
    assert {'dim_scoring_rule', 'dim_scoring_bonus', 'dim_roster_slot'} <= proc.refreshed_tables
    with engine.connect() as conn:
        assert count(conn, 'edw.dim_scoring_rule', L26) == 2
        assert count(conn, 'edw.dim_scoring_bonus', L26) == 0
        assert count(conn, 'edw.dim_scoring_rule', L25) == 3
        assert count(conn, 'edw.dim_scoring_bonus', L25) == 3


# ---------------------------------------------------------------- season weeks

L07, SIDE_07 = '175.l.658531', '175.l.86092'  # 2007: league of record, side league


def _games(league, weeks, playoff_from, championship=None):
    return [{'league_id': league, 'week': w, 'is_playoffs': w >= playoff_from,
             'is_championship': w == championship} for w in weeks]


def _season_2007(playoff_start_setting=None):
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    record = {'league_id': L07, 'season': '2007', 'start_week': '2', 'end_week': '16'}
    if playoff_start_setting:
        record['playoff_start_week'] = playoff_start_setting
    proc.data = {
        'leagues': [record,
                    {'league_id': SIDE_07, 'season': '2007', 'start_week': '2', 'end_week': '16'}],
        # The real 2007 bracket starts in week 15; the side league's in week 14.
        'matchups': (_games(L07, range(2, 17), 15, championship=16)
                     + _games(SIDE_07, range(2, 17), 14, championship=16)),
    }
    return proc


def test_a_side_leagues_bracket_does_not_move_the_seasons_playoffs():
    season = {s['season_year']: s for s in _season_2007().extract_seasons()}[2007]
    assert (season['playoff_start_week'], season['championship_week']) == (15, 16)


def test_week_14_of_2007_is_regular_season():
    weeks = {w['week_number']: w['week_type']
             for w in _season_2007().extract_weeks() if w['season_year'] == 2007}
    assert (weeks[14], weeks[15], weeks[16]) == ('regular', 'playoffs', 'championship')


def test_weeks_and_season_agree_on_the_playoff_start():
    proc = _season_2007(playoff_start_setting=15)
    [season] = [s for s in proc.extract_seasons() if s['season_year'] == 2007]
    first_playoff = min(w['week_number'] for w in proc.extract_weeks()
                        if w['season_year'] == 2007 and w['week_type'] != 'regular')
    assert first_playoff == season['playoff_start_week'] == 15


def test_yahoos_setting_wins_over_the_bracket_flags():
    """Flags are inferred at load time; the setting is the league's own."""
    proc = _season_2007(playoff_start_setting=16)
    season = {s['season_year']: s for s in proc.extract_seasons()}[2007]
    assert season['playoff_start_week'] == 16
