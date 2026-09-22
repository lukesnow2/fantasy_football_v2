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


# fantasy_edw_schema.sql declares these as SERIAL PRIMARY KEY, but the
# live tables lost their defaults somewhere along the way (a to_sql
# 'replace' recreates a table bare). Incremental inserts that omit the
# key column then hit NOT NULL violations. This repair is idempotent.
EDW_SERIAL_KEYS = [
    ('dim_season', 'season_key'), ('dim_week', 'week_key'),
    ('dim_league', 'league_key'), ('dim_team', 'team_key'),
    ('dim_player', 'player_key'), ('dim_manager', 'manager_key'),
    ('fact_matchup', 'matchup_key'), ('fact_roster', 'roster_key'),
    ('fact_transaction', 'transaction_key'), ('fact_draft', 'draft_key'),
    ('fact_player_statistics', 'stat_key'),
    ('fact_team_performance', 'performance_key'),
    ('mart_manager_h2h', 'h2h_key'),
]


def ensure_edw_serial_defaults(engine, schema: str = 'edw', force: bool = False):
    """Give EDW surrogate keys a SERIAL default owned by `schema`, idempotently.

    `force` rewrites the default even when one is already present. That is
    what a freshly cloned snapshot needs: CREATE TABLE (LIKE ... INCLUDING
    ALL) copies the column default verbatim, so the clone's keys default to
    nextval() on the SOURCE schema's sequence. restore_snapshot then renames
    the old schema aside and DROPs it CASCADE - which destroys exactly those
    sequences, and the defaults with them. A restored warehouse therefore
    comes back unable to insert a single dimension row.

    The pipeline masked this by calling ensure_edw_serial_defaults at the top
    of every run. Nothing else does: deploy_complete_edw.py, the RUNBOOK's
    full rebuild and an operator's natural response to a broken warehouse,
    goes straight to load_dimensions and dies on
    'null value in column "season_key"'. Pointing the clone at its own
    sequences fixes it at the source, the same way foreign keys are replayed.
    """
    with engine.begin() as conn:
        _set_serial_defaults(conn, schema, force)


def _set_serial_defaults(conn, schema: str, force: bool):
    for table, col in EDW_SERIAL_KEYS:
        # One catalog read covers existence, type and current default. A
        # sequence default only makes sense on an integer key; guarding the
        # type matters because force=True runs this on every clone, where a
        # column of another type would fail COALESCE(max(col), 0) and take
        # the whole publish down with it.
        row = conn.execute(text(
            "SELECT data_type, column_default FROM information_schema.columns "
            "WHERE table_schema=:s AND table_name=:t AND column_name=:c"),
            {'s': schema, 't': table, 'c': col}).fetchone()
        if row is None or row[0] not in ('integer', 'bigint', 'smallint'):
            continue
        if row[1] is not None and not force:
            continue
        seq = f'{schema}.{table}_{col}_seq'
        conn.execute(text(f'CREATE SEQUENCE IF NOT EXISTS {seq}'))
        conn.execute(text(
            f'ALTER TABLE {schema}."{table}" ALTER COLUMN "{col}" '
            f"SET DEFAULT nextval('{seq}')"))
        conn.execute(text(
            f"SELECT setval('{seq}', COALESCE((SELECT max(\"{col}\") "
            f'FROM {schema}."{table}"), 0) + 1, false)'))
        conn.execute(text(
            f'ALTER SEQUENCE {seq} OWNED BY {schema}."{table}"."{col}"'))
        logger.info("Set SERIAL default on %s.%s.%s", schema, table, col)


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

        # LIKE ... INCLUDING ALL does NOT copy foreign keys, despite the
        # name. Restoring such a snapshot promoted a schema with no
        # referential integrity at all and silently dropped it for good -
        # production carried 36 FKs while a restored database carried none.
        # Replay them explicitly, after the data is in place so they validate.
        fkeys = conn.execute(text("""
            SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE contype = 'f' AND connamespace = CAST(:s AS regnamespace)
            ORDER BY conname
        """), {'s': src}).fetchall()
        for qualified_table, conname, definition in fkeys:
            table_only = qualified_table.split('.')[-1].strip('"')
            # Definitions reference the source schema either explicitly or
            # via search_path; rewrite the former and set the latter.
            rewritten = definition.replace(f'{src}.', f'{dst}.')
            conn.execute(text(f'SET LOCAL search_path TO {dst}'))
            conn.execute(text(
                f'ALTER TABLE {dst}."{table_only}" '
                f'ADD CONSTRAINT "{conname}" {rewritten}'))
        conn.execute(text('SET LOCAL search_path TO DEFAULT'))
        if fkeys:
            logger.info("Snapshot %s: replayed %d foreign keys", dst, len(fkeys))

        # Repoint the cloned SERIAL defaults at sequences the snapshot owns.
        # LIKE copies the default text, so without this the clone's keys
        # depend on src's sequences - which restore_snapshot destroys when it
        # drops the old schema, leaving the restored warehouse unable to
        # insert. See ensure_edw_serial_defaults.
        _set_serial_defaults(conn, dst, force=True)

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
        inbound = _inbound_foreign_keys(conn, src, (snapshot, BROKEN_SCHEMA))
        conn.execute(text(f'DROP SCHEMA IF EXISTS {BROKEN_SCHEMA} CASCADE'))
        conn.execute(text(f'ALTER SCHEMA {src} RENAME TO {BROKEN_SCHEMA}'))
        conn.execute(text(f'ALTER SCHEMA {snapshot} RENAME TO {src}'))
        # Foreign keys held by OTHER schemas follow the renamed tables by OID,
        # so they now point into the broken generation - and the DROP ...
        # CASCADE below would silently delete them. That is not hypothetical:
        # app.user and app.league_member reference edw.dim_manager, and a
        # scratch run of this function removed both. Re-point them at the
        # restored tables in the same transaction as the swap. NOT VALID so a
        # restore can never fail on a validation scan; validated afterwards.
        for child, conname, definition in inbound:
            conn.execute(text(f'ALTER TABLE {child} DROP CONSTRAINT "{conname}"'))
            conn.execute(text(
                f'ALTER TABLE {child} ADD CONSTRAINT "{conname}" '
                f'{definition.removesuffix(" NOT VALID")} NOT VALID'))
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS {BROKEN_SCHEMA} CASCADE'))
    for child, conname, _ in inbound:
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    f'ALTER TABLE {child} VALIDATE CONSTRAINT "{conname}"'))
        except Exception as e:
            logger.warning("Restored FK %s on %s is enforced for new rows but "
                           "could not be validated against existing ones (%s)",
                           conname, child, type(e).__name__)
    logger.warning("EDW restored to previous generation from %s", snapshot)


def _inbound_foreign_keys(conn, schema: str, also_exclude=()) -> list:
    """Foreign keys held by tables outside `schema` that reference into it.

    Returned as (child table, constraint name, definition). The definition
    is captured under a pg_catalog-only search_path so pg_get_constraintdef
    schema-qualifies the referenced table (e.g. REFERENCES edw.dim_manager),
    which is what lets it resolve to the restored generation once re-added.
    """
    excluded = [schema, *also_exclude]
    conn.execute(text('SET LOCAL search_path TO pg_catalog'))
    rows = conn.execute(text("""
        SELECT c.conrelid::regclass::text, c.conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.confrelid
        JOIN pg_namespace rn ON rn.oid = r.relnamespace
        JOIN pg_namespace cn ON cn.oid = c.connamespace
        WHERE c.contype = 'f' AND rn.nspname = :s
          AND cn.nspname <> ALL(:excluded)
        ORDER BY 1, 2
    """), {'s': schema, 'excluded': excluded}).fetchall()
    conn.execute(text('SET LOCAL search_path TO DEFAULT'))
    return rows


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
            # A table that had rows and now has none is always a wipe, at
            # any size. The fractional test needs enough rows to be
            # meaningful, but gating the zero case on it too left small
            # dimensions (dim_manager holds exactly 20) entirely unguarded.
            if before > 0 and after == 0:
                raise PublishVerificationError(
                    f"edw.{t} emptied ({before} -> 0) - refusing to publish")
            if before > 20 and after < before * (1 - MAX_SHRINK_FRACTION):
                raise PublishVerificationError(
                    f"edw.{t} shrank {before} -> {after} "
                    f"(more than {MAX_SHRINK_FRACTION:.0%}) - refusing to publish")

        # Every entity the raw layer holds for a period must be visible in the
        # EDW after refresh. Checking statistics alone once let a whole
        # season's matchups vanish (missing dim_week rows) while the refresh
        # reported success, so each entity is compared against its own raw
        # source: raw rows but no published rows means the transform dropped
        # them, almost always an unresolved dimension key.
        # Both sides filter on the same league. Checking the EDW side on
        # season/week alone meant another league's rows at the same week
        # could satisfy the gate for a league whose data had been dropped -
        # the very failure this gate exists to catch.
        #
        # Only entities this period actually holds raw data FOR are checked,
        # read from its raw_*_complete flags. Checking every entity with any
        # raw rows failed on pre-existing historical gaps that no refresh
        # ever claimed to fill - edw.fact_roster was never built for old
        # seasons - which is a different problem from a transform silently
        # dropping what we just loaded.
        entity_checks = (
            ('statistics', 'public.statistics',
             "SELECT EXISTS (SELECT 1 FROM edw.fact_player_statistics f "
             "JOIN edw.dim_league dl ON dl.league_key = f.league_key "
             "WHERE dl.league_id = :l AND f.season_year = :s "
             "AND f.week_number = :w)",
             "SELECT EXISTS (SELECT 1 FROM public.statistics "
             "WHERE league_id = :l AND week_number = :w)"),
            ('matchups', 'public.matchups',
             "SELECT EXISTS (SELECT 1 FROM edw.fact_matchup fm "
             "JOIN edw.dim_week dw ON fm.week_key = dw.week_key "
             "JOIN edw.dim_league dl ON dl.league_key = fm.league_key "
             "WHERE dl.league_id = :l AND fm.season_year = :s "
             "AND dw.week_number = :w)",
             "SELECT EXISTS (SELECT 1 FROM public.matchups "
             "WHERE league_id = :l AND week = :w)"),
            ('rosters', 'public.rosters',
             "SELECT EXISTS (SELECT 1 FROM edw.fact_roster fr "
             "JOIN edw.dim_week dw ON fr.week_key = dw.week_key "
             "JOIN edw.dim_league dl ON dl.league_key = fr.league_key "
             "WHERE dl.league_id = :l AND dw.season_year = :s "
             "AND dw.week_number = :w)",
             "SELECT EXISTS (SELECT 1 FROM public.rosters "
             "WHERE league_id = :l AND week = :w)"),
        )

        for league_id, season, week in periods:
            params = {'l': league_id, 's': season, 'w': week}
            claimed = conn.execute(text(
                "SELECT raw_matchups_complete, raw_rosters_complete, "
                "raw_statistics_complete FROM public.pipeline_periods "
                "WHERE league_id = :l AND season = :s AND week = :w"),
                params).fetchone()
            if claimed is None:
                # An unverifiable claim is a failed claim: publishing a
                # period the state model has no record of would mark it
                # published against nothing and verify nothing.
                raise PublishVerificationError(
                    f"period {league_id} {season} w{week} has no "
                    "pipeline_periods row - refusing to publish an "
                    "unverifiable period")
            holds = {'matchups': claimed[0], 'rosters': claimed[1],
                     'statistics': claimed[2]}

            for entity, raw_table, edw_sql, raw_sql in entity_checks:
                if not holds.get(entity):
                    continue  # this period never claimed this entity
                if not conn.execute(text(raw_sql), params).scalar():
                    continue  # nothing raw to publish for this entity
                if not conn.execute(text(edw_sql), params).scalar():
                    raise PublishVerificationError(
                        f"period {league_id} {season} w{week}: {raw_table} has "
                        f"rows but none are visible in the EDW after refresh "
                        f"(check dimension coverage for {entity})")
    return report


def reconcile_deletions(engine, periods: List[Tuple[str, int, int]]) -> int:
    """Drop EDW rows for published periods that raw no longer holds.

    fact_player_statistics is loaded with a business-key UPSERT while its
    raw period is delete-and-replace. Additions and revisions therefore
    propagate, but RETRACTIONS never do: a row Yahoo stops returning is
    simply never touched again, and stays visible on the site for good.
    The rolling reload window re-fetches the two most recent weeks
    precisely to pick up Yahoo's corrections, so this is the common path,
    not an edge case. Confirmed on dev: 23 orphaned rows, all of them in
    the two league-weeks that had been reloaded most often.

    The other fact tables do not need this. fact_roster / fact_matchup /
    fact_team_performance delete their week before inserting, and
    fact_draft / fact_transaction are append-only against raw tables that
    never shrink - all three verified at zero orphans.

    Deliberately conservative, for the same reason load_delta is: a
    period is reconciled only when it CLAIMS the entity complete and raw
    actually holds rows for it. If raw is empty, nothing is deleted - an
    empty raw side must never be read as a retraction.
    """
    removed = 0
    with engine.begin() as conn:
        for league_id, season, week in periods:
            params = {'l': league_id, 's': season, 'w': week}
            claimed = conn.execute(text(
                "SELECT raw_statistics_complete FROM public.pipeline_periods "
                "WHERE league_id = :l AND season = :s AND week = :w"),
                params).scalar()
            if not claimed:
                continue
            if not conn.execute(text(
                    "SELECT EXISTS (SELECT 1 FROM public.statistics "
                    "WHERE league_id = :l AND week_number = :w)"),
                    params).scalar():
                continue
            n = conn.execute(text("""
                DELETE FROM edw.fact_player_statistics f
                USING edw.dim_league dl, edw.dim_player dp
                WHERE dl.league_key = f.league_key
                  AND dp.player_key = f.player_key
                  AND dl.league_id = :l
                  AND f.season_year = :s
                  AND f.week_number = :w
                  AND NOT EXISTS (
                      SELECT 1 FROM public.statistics s
                      WHERE s.league_id = :l AND s.week_number = :w
                        AND s.player_id = dp.player_id)
            """), params).rowcount
            if n:
                logger.info(
                    "Reconciled %d statistics row(s) out of %s %s w%s - raw "
                    "no longer has them", n, league_id, season, week)
                removed += n
    return removed


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
        # Before verifying, not after: reconciliation is part of the
        # generation being published, so it lives inside the snapshot
        # guard and the gate sees its result.
        reconcile_deletions(engine, periods)
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
