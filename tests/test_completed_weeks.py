"""Phase 1 tests: completed-week calculation semantics.

The production data bug this guards against: every season 2005-2025 was
missing its final (championship) week of player statistics because the
old range expression stopped one week short for finished seasons.
"""
import pytest

from src.extractors.comprehensive_data_extractor import (
    completed_weeks, matchup_status, week_is_complete)


def test_finished_season_includes_final_week():
    # 2024: current_week == end_week == 17, is_finished = 1.
    assert completed_weeks(17, 17, 1) == list(range(1, 18))


def test_finished_16_week_era_includes_week_16():
    # 2010-2021 era leagues ended at week 16.
    assert completed_weeks(16, 16, '1') == list(range(1, 17))


def test_midseason_excludes_in_progress_week():
    # Week 8 in progress: weeks 1-7 are complete.
    assert completed_weeks(8, 17, '') == list(range(1, 8))


def test_final_week_in_progress_excludes_it():
    # current_week == end_week but season not finished: the championship
    # week is being played right now and must NOT be ingested.
    assert completed_weeks(17, 17, '') == list(range(1, 17))
    assert completed_weeks(17, 17, 0) == list(range(1, 17))


def test_week_one_in_progress_yields_nothing():
    assert completed_weeks(1, 17, '') == []


@pytest.mark.parametrize("truthy", [1, '1', 'True', 'true', True])
def test_is_finished_representations(truthy):
    # Yahoo returns is_finished variously as int and string.
    assert completed_weeks(17, 17, truthy)[-1] == 17


# Per-week status. This is what makes a Tuesday-morning schedule safe:
# Yahoo advances current_week at an unspecified time after Monday night, so
# completeness is read from the week's own scoreboard instead.

def _league_with(statuses):
    """Fake league whose matchups(week) returns a Yahoo-shaped scoreboard."""
    class FakeLeague:
        def __init__(self):
            self.asked = []

        def matchups(self, week):
            self.asked.append(week)
            status = statuses.get(week)
            if status is None:
                return {'fantasy_content': {'league': [{}, {
                    'scoreboard': {'0': {'matchups': {'count': 0}}}}]}}
            return {'fantasy_content': {'league': [{}, {'scoreboard': {'0': {
                'matchups': {'count': 1,
                             '0': {'matchup': {'status': status}}}}}}]}}
    return FakeLeague()


def test_postevent_week_is_complete():
    assert week_is_complete(_league_with({1: 'postevent'}), 1) is True


@pytest.mark.parametrize("status", ['midevent', 'preevent'])
def test_unfinished_week_is_not_complete(status):
    assert week_is_complete(_league_with({1: status}), 1) is False


def test_week_without_scoreboard_is_not_complete():
    assert week_is_complete(_league_with({}), 1) is False


def test_matchup_status_reads_the_week():
    lg = _league_with({3: 'postevent'})
    assert matchup_status(lg, 3) == 'postevent'
    assert lg.asked == [3]


def test_finished_week_detected_before_current_week_advances():
    # Monday night is over but Yahoo still reports current_week == 1. The
    # settings-level inference yields nothing; the per-week status does not.
    assert completed_weeks(1, 17, '') == []
    assert week_is_complete(_league_with({1: 'postevent'}), 1) is True
