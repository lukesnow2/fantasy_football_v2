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


# ---------------------------------------------------------------- slot gate
# Scheduled runs are judged by the cron entry that fired, not the start-up
# clock: GitHub can start them hours late. On 2026-10-13 both entries started
# after 13:00 Mountain, the wall-clock gate skipped both, and nothing loaded.

from datetime import timedelta  # noqa: E402

from scripts.incremental_load import parse_args, scheduled_slot_matches  # noqa: E402

MDT_ENTRY = '0 14 * 9,10,11,12,1 2'   # 08:00 while daylight saving is on
MST_ENTRY = '0 15 * 9,10,11,12,1 2'   # 08:00 after it ends


@pytest.mark.parametrize("instant,label,should_run", SEASON_TUESDAYS)
def test_slot_gate_agrees_with_wall_clock_for_on_time_runs(instant, label, should_run):
    entry = MDT_ENTRY if instant.hour == 14 else MST_ENTRY
    assert scheduled_slot_matches(entry, 8, DENVER, instant) is should_run


@pytest.mark.parametrize("entry,started,should_run", [
    # The real 2026-10-13 runs: 19:59 and 20:31 UTC.
    (MDT_ENTRY, datetime(2026, 10, 13, 19, 59, tzinfo=UTC), True),
    (MST_ENTRY, datetime(2026, 10, 13, 20, 31, tzinfo=UTC), False),
    # After DST ends the roles swap, late or not.
    (MDT_ENTRY, datetime(2026, 11, 10, 21, 5, tzinfo=UTC), False),
    (MST_ENTRY, datetime(2026, 11, 10, 21, 40, tzinfo=UTC), True),
    # So late it starts after midnight UTC: still Tuesday's slot.
    (MST_ENTRY, datetime(2026, 11, 11, 2, 15, tzinfo=UTC), True),
    (MDT_ENTRY, datetime(2026, 10, 14, 1, 0, tzinfo=UTC), True),
])
def test_late_runs_are_judged_by_their_slot(entry, started, should_run):
    assert scheduled_slot_matches(entry, 8, DENVER, started) is should_run
    if should_run:
        assert local_hour_matches(8, DENVER, started) is False, \
            'the wall-clock gate would have skipped this run'


def test_exactly_one_entry_loads_each_tuesday_even_when_both_are_late():
    for tuesday in (datetime(2026, 9, 15, tzinfo=UTC), datetime(2026, 10, 13, tzinfo=UTC),
                    datetime(2026, 11, 10, tzinfo=UTC), datetime(2027, 1, 12, tzinfo=UTC)):
        late = timedelta(hours=6, minutes=30)
        admitted = [e for e, h in ((MDT_ENTRY, 14), (MST_ENTRY, 15))
                    if scheduled_slot_matches(e, 8, DENVER, tuesday.replace(hour=h) + late)]
        assert len(admitted) == 1, f"{tuesday.date()}: {admitted}"


def test_slot_gate_rejects_an_entry_without_a_fixed_hour():
    with pytest.raises(ValueError):
        scheduled_slot_matches('*/5 * * * *', 8, DENVER)


def test_scheduled_cron_comes_from_the_environment(monkeypatch):
    # The workflow passes it as env: the cron string has spaces, and the
    # workflow's ARGS string is word-split.
    monkeypatch.setenv('SCHEDULED_CRON', MDT_ENTRY)
    assert parse_args([]).scheduled_cron == MDT_ENTRY
    monkeypatch.setenv('SCHEDULED_CRON', '')   # manual runs: empty
    assert parse_args([]).scheduled_cron is None
