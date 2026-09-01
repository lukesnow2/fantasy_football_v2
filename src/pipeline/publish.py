#!/usr/bin/env python3
"""Atomically-safe EDW publication.

The web app reads ``edw.*`` live, so the boundary that matters is visible
EDW state. The EDW processor commits per-table internally (9+ commit
sites across 6,300 lines), so running it inside one outer transaction is
not cleanly possible - this module implements the plan's sanctioned
fallback: snapshot + refresh + verify + ATOMIC RESTORE on failure.

Guarantee: the persistent EDW state is always a complete generation.

    1. Snapshot: clone every edw table (+ views, definitions rewritten)
       into ``edw_prev`` - the site keeps reading ``edw`` untouched.
    2. Refresh: run the (internally-committing) EDW processor.
    3. Verify: the gate below must pass - loaded periods visible,
       no table shrank suspiciously.
    4. Success: mark periods published, drop the snapshot.
       Failure at ANY point: one transaction renames the broken schema
       away and renames ``edw_prev`` back to ``edw`` - Postgres DDL is
       transactional and views follow their OID-bound tables, so the
       site is instantly back on the previous complete generation.

Residual, documented honestly: while a refresh is RUNNING, a reader can
see a transiently mixed state for the seconds the refresh takes (weekly,
off-peak). A failed refresh can never leave one behind - restoration is
atomic and total, and the raw_version > edw_version reconciliation
re-publishes on the next invocation. Eliminating the transient window
requires refactoring the processor's internal commit seams; revisit if
it ever matters at this site's traffic.
"""
import logging
from typing import Callable, Dict, List, Tuple

from sqlalchemy import text

logger = logging.getLogger(__name__)

SNAPSHOT_SCHEMA = 'edw_prev'
BROKEN_SCHEMA = 'edw_broken'
# A weekly load appends one week and re-loads two; nothing legitimate
# shrinks a table by more than a few percent. A big shrink is the
# signature of wipe-class bugs (e.g. an unscoped season delete).
MAX_SHRINK_FRACTION = 0.10


class PublishVerificationError(RuntimeError):
    """The refreshed EDW failed the verification gate."""


def clone_edw_snapshot(engine, src: str = 'edw', dst: str = SNAPSHOT_SCHEMA):
    """Clone src's tables (data + constraints) and views into dst.

    View definitions are textually rewritten src. -> dst. and created in
    dependency order (retry passes until fixed point). Materialized views
    are asserted absent - this pipeline has none; if one appears, this
    must be extended deliberately rather than silently skipping it.
    """
    with engine.begin() as conn:
        n_mat = conn.execute(text(
            "SELECT count(*) FROM pg_matviews WHERE schemaname = :s"),
            {'s': src}).scalar()
        if n_mat:
            raise RuntimeError(
                f"{n_mat} materialized view(s) in {src}: snapshot cloning "
                "does not support them - extend clone_edw_snapshot first")

        conn.execute(text(f'DROP SCHEMA IF EXISTS {dst} CASCADE'))
        conn.execute(text(f'CREATE SCHEMA {dst}'))

        tables = [r[0] for r in conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = :s"),
            {'s': src})]
        for t in tables:
            conn.execute(text(
                f'CREATE TABLE {dst}."{t}" (LIKE {src}."{t}" INCLUDING ALL)'))
            conn.execute(text(
                f'INSERT INTO {dst}."{t}" SELECT * FROM {src}."{t}"'))

        views = {r[0]: r[1] for r in conn.execute(text(
            "SELECT viewname, pg_get_viewdef(schemaname || '.' || viewname) "
            "FROM pg_views WHERE schemaname = :s"), {'s': src})}
        pending = dict(views)
        while pending:
            progressed = []
            for name, definition in list(pending.items()):
                rewritten = definition.replace(f'{src}.', f'{dst}.')
                try:
                    with conn.begin_nested():
                        conn.execute(text(
                            f'CREATE VIEW {dst}."{name}" AS {rewritten}'))
                    progressed.append(name)
                except Exception:
                    continue  # depends on a view not yet created; next pass
            if not progressed:
                raise RuntimeError(
                    f"could not clone views (circular or external "
                    f"dependency?): {sorted(pending)}")
            for name in progressed:
                del pending[name]
        logger.info("Snapshot %s: %d tables, %d views cloned from %s",
                    dst, len(tables), len(views), src)
        return tables


def restore_snapshot(engine, src: str = 'edw', snapshot: str = SNAPSHOT_SCHEMA):
    """Atomically put the previous complete generation back.

    One transaction: broken schema renamed away, snapshot renamed to live.
    Views follow their tables (OID-bound), so the site flips generations
    in a single commit.
    """
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS {BROKEN_SCHEMA} CASCADE'))
        conn.execute(text(f'ALTER SCHEMA {src} RENAME TO {BROKEN_SCHEMA}'))
        conn.execute(text(f'ALTER SCHEMA {snapshot} RENAME TO {src}'))
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS {BROKEN_SCHEMA} CASCADE'))
    logger.warning("EDW restored to previous generation from %s", snapshot)


def drop_snapshot(engine, snapshot: str = SNAPSHOT_SCHEMA):
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS {snapshot} CASCADE'))


def verify_refresh(engine, periods: List[Tuple[str, int, int]],
                   snapshot: str = SNAPSHOT_SCHEMA) -> Dict[str, Dict]:
    """The verification gate. Raises PublishVerificationError on failure.

    (a) Every loaded period is visible in the refreshed EDW
        (fact_player_statistics has rows at that season/week when the
        raw statistics landed; season presence otherwise).
    (b) No EDW table shrank more than MAX_SHRINK_FRACTION vs the
        snapshot - the signature of wipe-class bugs.
    Returns per-table before/after counts for the run ledger.
    """
    report = {}
    with engine.connect() as conn:
        tables = [r[0] for r in conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = :s"),
            {'s': snapshot})]
        for t in tables:
            before = conn.execute(text(f'SELECT count(*) FROM {snapshot}."{t}"')).scalar()
            after = conn.execute(text(f'SELECT count(*) FROM edw."{t}"')).scalar()
            report[t] = {'before': before, 'after': after}
            if before > 20 and after < before * (1 - MAX_SHRINK_FRACTION):
                raise PublishVerificationError(
                    f"edw.{t} shrank {before} -> {after} "
                    f"(more than {MAX_SHRINK_FRACTION:.0%}) - refusing to publish")

        for league_id, season, week in periods:
            visible = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM edw.fact_player_statistics "
                "WHERE season_year = :s AND week_number = :w)"),
                {'s': season, 'w': week}).scalar()
            if not visible:
                # Statistics may legitimately be empty for a period whose
                # raw load carried no statistics rows (e.g. pre-first-week
                # draft-only loads never reach here; but a bye-shaped
                # anomaly should fail loudly rather than publish blind).
                raise PublishVerificationError(
                    f"period {league_id} {season} w{week} not visible in "
                    "edw.fact_player_statistics after refresh")
    return report


def publish(state, periods: List[Tuple[str, int, int]],
            refresh: Callable[[], bool],
            verify: Callable = verify_refresh) -> Dict[str, Dict]:
    """Snapshot -> refresh -> verify -> mark published; atomic restore on
    any failure. `refresh` is the EDW processor invocation (injected for
    testability); it must return True on success.

    On success the snapshot is dropped and every period's edw_version
    catches up to raw_version (published) in one transaction. On failure
    the previous generation is restored and the periods stay unpublished:
    the next run's raw_version > edw_version reconciliation re-publishes
    without touching Yahoo.
    """
    engine = state.engine
    clone_edw_snapshot(engine)
    try:
        ok = refresh()
        if not ok:
            raise RuntimeError("EDW refresh reported failure")
        report = verify(engine, periods)
    except BaseException:
        restore_snapshot(engine)
        raise

    with engine.begin() as conn:
        for league_id, season, week in periods:
            state.mark_published(conn, league_id, season, week)
    drop_snapshot(engine)
    logger.info("Published %d period(s); EDW generation advanced", len(periods))
    return report
