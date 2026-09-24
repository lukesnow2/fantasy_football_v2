"""Each week's statistics must be fetched for THAT week's rostered players.

The extractor used league.taken_players() - who is rostered now - for every
week. The weekly load re-fetches the two most recent completed weeks, so a
past week's stats were swapped for today's rosters: after one reload in
2026, 16 of 155 week-1 players had no week-1 stats (cut since), and 16
players who weren't rostered in week 1 had some.
"""
from types import SimpleNamespace

import pytest

from scripts.incremental_load import extract_scope
from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor


class FakeLeague:
    def __init__(self):
        self.calls = []

    def settings(self):
        return {'season': '2026', 'game_code': 'nfl', 'name': 'Test',
                'current_week': '3', 'end_week': '17'}

    def taken_players(self):
        return [{'player_id': '99'}]

    def player_stats(self, ids, req_type, week=None):
        self.calls.append((week, list(ids)))
        return [{'player_id': i, 'name': f'P{i}', 'position_type': 'O',
                 'total_points': 1.5} for i in ids]


def _extractor(league):
    ex = YahooFantasyExtractor.__new__(YahooFantasyExtractor)
    ex.game = SimpleNamespace(to_league=lambda league_id: league)
    ex._rate_limited_request = lambda fn, *a, **k: fn(*a, **k)
    return ex


def test_each_week_uses_its_own_roster():
    league = FakeLeague()
    stats = _extractor(league).extract_statistics_for_league(
        '470.l.1', [1, 2], players_by_week={1: ['11', '12'], 2: {'13'}})

    assert league.calls == [(1, [11, 12]), (2, [13])]
    assert {(s.week_number, s.player_id) for s in stats} == {
        (1, '11'), (1, '12'), (2, '13')}


def test_a_week_without_rostered_players_is_refused():
    with pytest.raises(ValueError, match=r'week\(s\) \[2\]'):
        _extractor(FakeLeague()).extract_statistics_for_league(
            '470.l.1', [1, 2], players_by_week={1: ['11']})


def test_backfill_path_still_uses_taken_players():
    league = FakeLeague()
    _extractor(league).extract_statistics_for_league('470.l.1', [1])
    assert league.calls == [(1, [99])]


class FakeExtractor:
    """Records how extract_scope drives the extractor."""
    def __init__(self):
        self.stats_players = None

    def extract_rosters_for_league(self, league_id, weeks):
        return [SimpleNamespace(week=w, player_id=p, team_id='1')
                for w, players in {1: ['11', '12'], 2: ['13']}.items()
                if w in weeks for p in players]

    def extract_statistics_for_league(self, league_id, weeks, players_by_week=None):
        self.stats_players = players_by_week
        return []

    def extract_teams_for_league(self, league_id):
        return []

    def extract_matchups_for_league(self, league_id, weeks):
        return []

    def extract_transactions_for_league(self, league_id):
        return []


def test_scope_passes_each_weeks_roster_to_statistics():
    ex = FakeExtractor()
    data = extract_scope(ex, '470.l.1', [1, 2], stats_only=False, is_new=False,
                         since=None, league_info_rows=[])
    assert ex.stats_players == {1: {'11', '12'}, 2: {'13'}}
    assert len(data['rosters']) == 3


def test_stats_only_uses_rosters_but_does_not_load_them():
    """A stats-only repair needs each week's players, but must leave the
    rosters table alone - an absent key means 'not fetched' to the loader."""
    ex = FakeExtractor()
    data = extract_scope(ex, '470.l.1', [1], stats_only=True, is_new=False,
                         since=None, league_info_rows=[])
    assert ex.stats_players == {1: {'11', '12'}}
    assert set(data) == {'statistics'}
