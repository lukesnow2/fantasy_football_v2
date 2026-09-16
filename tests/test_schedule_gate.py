"""The DST gate that holds the weekly run at 08:00 America/Denver.

GitHub cron is UTC-only and does not follow daylight saving, so the workflow
schedules both candidate hours (14:00 and 15:00 UTC) and this gate admits
exactly one. Without it the run drifts to 07:00 or 09:00 local each November.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from scripts.incremental_load import local_hour_matches

UTC = ZoneInfo('UTC')
DENVER = 'America/Denver'

# (UTC instant, which cron entry, should it run)
SEASON_TUESDAYS = [
    (datetime(2026, 9, 15, 14, tzinfo=UTC), 'MDT twin', True),
    (datetime(2026, 9, 15, 15, tzinfo=UTC), 'MST twin', False),
    (datetime(2026, 10, 20, 14, tzinfo=UTC), 'MDT twin', True),
    (datetime(2026, 10, 20, 15, tzinfo=UTC), 'MST twin', False),
    # After DST ends (first Sunday of November) the pair swaps roles.
    (datetime(2026, 11, 10, 14, tzinfo=UTC), 'MDT twin', False),
    (datetime(2026, 11, 10, 15, tzinfo=UTC), 'MST twin', True),
    (datetime(2027, 1, 12, 14, tzinfo=UTC), 'MDT twin', False),
    (datetime(2027, 1, 12, 15, tzinfo=UTC), 'MST twin', True),
]


@pytest.mark.parametrize("instant,label,should_run", SEASON_TUESDAYS)
def test_gate_admits_only_the_local_eight_am(instant, label, should_run):
    assert local_hour_matches(8, DENVER, instant) is should_run


def test_exactly_one_twin_runs_each_tuesday():
    by_date = {}
    for instant, _, _ in SEASON_TUESDAYS:
        key = instant.astimezone(ZoneInfo(DENVER)).date()
        by_date.setdefault(key, []).append(instant)
    for date, instants in by_date.items():
        admitted = [i for i in instants if local_hour_matches(8, DENVER, i)]
        assert len(admitted) == 1, f"{date}: {len(admitted)} runs, expected 1"


def test_admitted_run_is_always_eight_local():
    for instant, _, _ in SEASON_TUESDAYS:
        if local_hour_matches(8, DENVER, instant):
            local = instant.astimezone(ZoneInfo(DENVER))
            assert (local.hour, local.weekday()) == (8, 1)  # 8am, Tuesday


def test_gate_is_timezone_aware_not_utc_offset_math():
    # 15:00 UTC is 08:00 in Denver only during MST; in July it is 09:00.
    july = datetime(2026, 7, 14, 15, tzinfo=UTC)
    assert local_hour_matches(8, DENVER, july) is False
    assert local_hour_matches(9, DENVER, july) is True
