"""Regression tests for the issues closed on 2026-09-29.

- NFL team: captured per roster row, kept only for the season being played.
- Power rankings / head-to-head: rebuilt by the weekly path.
- Republish step: recorded in the run ledger, failures included.
"""
from datetime import datetime
from types import SimpleNamespace

import pytest

from scripts.incremental_load import repair_unpublished, scrub_historical_teams
from src.edw_schema.edw_etl_processor import EdwEtlProcessor
from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor


def test_roster_row_carries_the_nfl_team():
    ex = YahooFantasyExtractor.__new__(YahooFantasyExtractor)
    row = ex._extract_roster_player_data(
        {'player_id': 41825, 'name': 'Tyler Shough', 'eligible_positions': ['QB'],
         'selected_position': 'BN', 'editorial_team_abbr': 'NO'},
        '470.l.744846', '3', 3)
    assert row.nfl_team == 'NO'
    assert ex._extract_roster_player_data(
        {'player_id': 1, 'name': 'X', 'eligible_positions': ['WR'],
         'selected_position': 'WR'}, '470.l.744846', '3', 3).nfl_team is None


def test_team_is_kept_only_for_the_season_being_played():
    """For a past season Yahoo returns today's team (2017 Aaron Rodgers comes
    back as Pittsburgh), so a historical load must not record it."""
    sept_2026 = datetime(2026, 9, 29)
    live = {'rosters': [{'nfl_team': 'CIN'}]}
    scrub_historical_teams(live, 2026, now=sept_2026)
    assert live['rosters'][0]['nfl_team'] == 'CIN'

    past = {'rosters': [{'nfl_team': 'PIT'}]}
    scrub_historical_teams(past, 2017, now=sept_2026)
    assert past['rosters'][0]['nfl_team'] is None

    # January is still the 2026 season (playoffs).
    jan = {'rosters': [{'nfl_team': 'KC'}]}
    scrub_historical_teams(jan, 2026, now=datetime(2027, 1, 5))
    assert jan['rosters'][0]['nfl_team'] == 'KC'


def test_dim_player_takes_the_most_recent_team_seen():
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.data = {
        'leagues': [{'league_id': 'L25', 'season': '2025'},
                    {'league_id': 'L26', 'season': '2026'}],
        'transactions': [], 'draft_picks': [], 'statistics': [],
        'rosters': [
            {'league_id': 'L26', 'week': 1, 'player_id': '7', 'player_name': 'P', 'nfl_team': 'DAL'},
            {'league_id': 'L26', 'week': 3, 'player_id': '7', 'player_name': 'P', 'nfl_team': 'NYJ'},
            {'league_id': 'L25', 'week': 17, 'player_id': '7', 'player_name': 'P', 'nfl_team': 'SF'},
            {'league_id': 'L25', 'week': 5, 'player_id': '8', 'player_name': 'Q', 'nfl_team': None},
        ],
    }
    players = {p['player_id']: p for p in proc.transform_players()}
    assert players['7']['nfl_team'] == 'NYJ'        # 2026 week 3 beats 2025 week 17
    assert players['8']['nfl_team'] == 'Unknown'


def test_weekly_path_rebuilds_power_rankings_and_head_to_head():
    """Neither mart was listed under any raw table, so only a full rebuild
    refreshed them - the site kept showing 2025's power rankings in 2026."""
    triggers = EdwEtlProcessor.EDW_PROCESSING_STRATEGIES['matchups']['triggers_refresh']
    assert 'mart_weekly_power_rankings' in triggers
    assert 'mart_manager_h2h' in triggers


class LedgerStub:
    def __init__(self):
        self.started, self.finished = [], []

    def start_run(self, season):
        self.started.append(season)
        return 99

    def finish_run(self, run_id, status, weeks_loaded=None, row_counts=None, error=None):
        self.finished.append((run_id, status, row_counts, error))


def test_failed_republish_is_recorded_in_the_ledger():
    """It used to run before a ledger row existed: 2007-2009 failed on 2006's
    week 1 and pipeline_runs showed nothing."""
    state = LedgerStub()

    def boom(*_):
        raise RuntimeError('verification failed')

    with pytest.raises(RuntimeError):
        repair_unpublished(state, [('153.l.76788', 2006, 1)], refresh=None, publish=boom)
    assert state.started == [2006]
    (run_id, status, counts, error), = state.finished
    assert status == 'failed' and 'verification failed' in error
    assert counts['periods'] == [['153.l.76788', 2006, 1]]


def test_successful_republish_is_recorded_too():
    state = LedgerStub()
    repair_unpublished(state, [('L', 2026, 2)], refresh=None, publish=lambda *_: None)
    assert state.finished[0][1] == 'success'


def test_in_progress_season_uses_the_configured_length():
    """Mid-season, no championship game exists yet. The season record used to
    read '3 weeks loaded' as a 3-week season (championship week 3, playoffs
    from week 1), so the power rankings gave a 3-0 team 0% playoff odds."""
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.data = {
        'leagues': [{'league_id': 'L26', 'season': '2026', 'start_week': '1',
                     'end_week': '17', 'current_week': '4'}],
        'matchups': [{'league_id': 'L26', 'week': w, 'is_playoffs': False,
                      'is_championship': False} for w in (1, 2, 3)],
    }
    season = {s['season_year']: s for s in proc.extract_seasons()}[2026]
    assert season['championship_week'] == 17
    assert season['total_weeks'] == 17
    assert season['playoff_start_week'] == 15


def test_completed_season_still_reads_its_own_bracket():
    proc = EdwEtlProcessor.__new__(EdwEtlProcessor)
    proc.data = {
        'leagues': [{'league_id': 'L07', 'season': '2007', 'start_week': '2', 'end_week': '16'}],
        'matchups': ([{'league_id': 'L07', 'week': w, 'is_playoffs': False,
                       'is_championship': False} for w in range(2, 15)]
                     + [{'league_id': 'L07', 'week': 15, 'is_playoffs': True, 'is_championship': False},
                        {'league_id': 'L07', 'week': 16, 'is_playoffs': True, 'is_championship': True}]),
    }
    season = {s['season_year']: s for s in proc.extract_seasons()}[2007]
    assert season['championship_week'] == 16
    assert season['playoff_start_week'] == 15


def test_standings_rank_is_record_then_points():
    """season_rank was NULL for every row of every season, so the playoff-odds
    model never knew who held a playoff spot."""
    def fact(team, pct, avg, week=3):
        return {'league_key': 1, 'week_key': week, 'team': team,
                'win_percentage': pct, 'weekly_points': avg, 'season_rank': None}
    facts = [fact('a', 2 / 3, 120.0), fact('b', 1.0, 100.0),
             fact('c', 2 / 3, 130.0), fact('d', 0.0, 150.0),
             fact('e', 2 / 3, 130.0), fact('x', 0.0, 1.0, week=4)]
    EdwEtlProcessor._assign_standings_ranks(facts)
    rank = {f['team']: f['season_rank'] for f in facts}
    assert rank['b'] == 1                       # 3-0 leads regardless of points
    assert rank['c'] == rank['e'] == 2          # same record and points: shared
    assert rank['a'] == 4                       # 2-1 but fewer points
    assert rank['d'] == 5                       # most points, worst record
    assert rank['x'] == 1                       # ranked within its own week
