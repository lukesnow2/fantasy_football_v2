"""Phase 1 tests: completed-week calculation semantics.

The production data bug this guards against: every season 2005-2025 was
missing its final (championship) week of player statistics because the
old range expression stopped one week short for finished seasons.
"""
import pytest

from src.extractors.comprehensive_data_extractor import completed_weeks


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
