#!/usr/bin/env python3
"""Pipeline state: period completeness, run ledger, and the advisory lock.

The database is the state. A period (league, season, week) passes through
two distinct stages that are never conflated:

    Yahoo  ->  RAW complete  ->  EDW published

``raw_*_complete`` flags flip when the raw transaction commits;
``published`` only when the EDW refresh covering the period commits.
After "raw committed, EDW failed, process died", the next invocation reads
unambiguously "we already possess a complete raw week - do not hit Yahoo
again; repair publication first". ``raw_version > edw_version`` is the
reconciliation signal for exactly that state.

The gap is: every completed Yahoo week for which there is no published
period. That detects never-loaded weeks, partially loaded weeks, and
interior holes identically - self-healing is literal, not approximate.

Concurrency correctness belongs to the database: a fail-fast
``pg_try_advisory_lock`` covers GitHub runs, laptop runs, and anything
else. The run ledger is written on a separate autocommit connection so
failure records survive the data transaction's rollback.
"""
import json
import logging
from contextlib import contextmanager
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Fixed key for pg_try_advisory_lock: 'theleague' pipeline, arbitrary but
# stable. Session-scoped: released on disconnect even after a crash.
ADVISORY_LOCK_KEY = 815_2005_17

RAW_ENTITIES = ('matchups', 'rosters', 'statistics')

DDL = """
CREATE TABLE IF NOT EXISTS public.pipeline_periods (
    league_id                text        NOT NULL,
    season                   integer     NOT NULL,
    week                     integer     NOT NULL,
    source_completed         boolean     NOT NULL DEFAULT false,
    raw_matchups_complete    boolean     NOT NULL DEFAULT false,
    raw_rosters_complete     boolean     NOT NULL DEFAULT false,
    raw_statistics_complete  boolean     NOT NULL DEFAULT false,
    raw_version              integer     NOT NULL DEFAULT 0,
    edw_version              integer     NOT NULL DEFAULT 0,
    published                boolean     NOT NULL DEFAULT false,
    verified_at              timestamptz,
    detail                   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    updated_at               timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (league_id, season, week)
);

CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    status       text NOT NULL DEFAULT 'running',
    season       integer,
    weeks_loaded integer[] NOT NULL DEFAULT '{}',
    row_counts   jsonb     NOT NULL DEFAULT '{}'::jsonb,
    error        text
);
"""


class LockHeld(Exception):
    """Another pipeline run holds the advisory lock."""


class PipelineState:
    """Period completeness + run ledger over a SQLAlchemy engine.

    The ledger uses its own AUTOCOMMIT connection; period mutations happen
    on the caller's transaction connection so they commit or roll back with
    the data they describe.
    """

    def __init__(self, database_url: str):
        url = database_url.replace('postgres://', 'postgresql://', 1)
        self.engine = create_engine(url)
        self._ledger = self.engine.connect().execution_options(
            isolation_level='AUTOCOMMIT')
        self._lock_conn = None

    def ensure_schema(self):
        with self.engine.begin() as conn:
            for stmt in DDL.split(';'):
                if stmt.strip():
                    conn.execute(text(stmt))

    def close(self):
        if self._lock_conn is not None:
            self.release_lock()
        self._ledger.close()
        self.engine.dispose()

    # ------------------------------------------------------------------
    # Advisory lock (fail-fast; DB-owned concurrency)
    # ------------------------------------------------------------------

    def try_lock(self) -> bool:
        """Acquire the pipeline advisory lock, fail-fast.

        Returns True on acquisition. The lock is session-scoped on a
        dedicated connection held until release_lock()/close().
        """
        conn = self.engine.connect()
        got = conn.execute(
            text("SELECT pg_try_advisory_lock(:k)"), {'k': ADVISORY_LOCK_KEY}
        ).scalar()
        if got:
            self._lock_conn = conn
            return True
        conn.close()
        return False

    def wait_lock(self):
        """Blocking acquisition, for manual repairs that want queueing."""
        conn = self.engine.connect()
        conn.execute(text("SELECT pg_advisory_lock(:k)"),
                     {'k': ADVISORY_LOCK_KEY})
        self._lock_conn = conn

    def release_lock(self):
        if self._lock_conn is not None:
            try:
                self._lock_conn.execute(
                    text("SELECT pg_advisory_unlock(:k)"),
                    {'k': ADVISORY_LOCK_KEY})
            finally:
                self._lock_conn.close()
                self._lock_conn = None

    @contextmanager
    def lock(self, wait: bool = False):
        """Context manager: acquire (fail-fast unless wait), always release.

        Raises LockHeld when fail-fast acquisition loses the race - callers
        exit 0 with "another run is active" so a benign overlap never
        looks like a failure.
        """
        if wait:
            self.wait_lock()
        elif not self.try_lock():
            raise LockHeld("another pipeline run is active")
        try:
            yield
        finally:
            self.release_lock()

    # ------------------------------------------------------------------
    # Run ledger (autocommit - survives data-transaction rollback)
    # ------------------------------------------------------------------

    def start_run(self, season: Optional[int] = None) -> int:
        row = self._ledger.execute(
            text("INSERT INTO public.pipeline_runs (season, status) "
                 "VALUES (:season, 'running') RETURNING run_id"),
            {'season': season}).fetchone()
        return row[0]

    def finish_run(self, run_id: int, status: str,
                   weeks_loaded: Optional[List[int]] = None,
                   row_counts: Optional[Dict] = None,
                   error: Optional[str] = None):
        self._ledger.execute(
            text("UPDATE public.pipeline_runs SET finished_at = now(), "
                 "status = :status, weeks_loaded = :weeks, "
                 "row_counts = :counts, error = :error "
                 "WHERE run_id = :run_id"),
            {'status': status, 'weeks': weeks_loaded or [],
             'counts': json.dumps(row_counts or {}), 'error': error,
             'run_id': run_id})

    def last_successful_run_start(self):
        """Start timestamp of the most recent successful run (or None).

        The transactions since-filter uses the run START, not finish -
        transactions landing mid-run would otherwise be skipped forever
        (overlap is harmless; the loader dedupes on transaction_id).
        """
        return self._ledger.execute(
            text("SELECT max(started_at) FROM public.pipeline_runs "
                 "WHERE status = 'success'")).scalar()

    # ------------------------------------------------------------------
    # Period completeness
    # ------------------------------------------------------------------

    def upsert_period_source(self, conn, league_id: str, season: int,
                             week: int, source_completed: bool):
        """Record what Yahoo says about a period (postevent or not)."""
        conn.execute(text("""
            INSERT INTO public.pipeline_periods (league_id, season, week, source_completed)
            VALUES (:l, :s, :w, :c)
            ON CONFLICT (league_id, season, week)
            DO UPDATE SET source_completed = EXCLUDED.source_completed,
                          updated_at = now()
        """), {'l': league_id, 's': season, 'w': week, 'c': source_completed})

    def mark_raw_complete(self, conn, league_id: str, season: int, week: int,
                          entities: Dict[str, int]):
        """Flip raw_*_complete for the given entities and bump raw_version.

        Must run on the SAME connection/transaction as the raw data load,
        so the flags commit (or roll back) with the rows they describe.
        entities maps entity name -> row count, e.g. {'matchups': 5}.
        """
        unknown = set(entities) - set(RAW_ENTITIES)
        if unknown:
            raise ValueError(f"unknown raw entities: {unknown}")
        if not entities:
            # Joining over an empty dict produced "SET , raw_version = ..."
            # - a syntax error that aborts the whole raw transaction and
            # discards data that had already loaded successfully.
            raise ValueError(
                "mark_raw_complete requires at least one entity; "
                "a period with nothing loaded must not be marked complete")
        sets = ", ".join(
            f"raw_{e}_complete = true" for e in entities)
        conn.execute(text(f"""
            INSERT INTO public.pipeline_periods (league_id, season, week)
            VALUES (:l, :s, :w)
            ON CONFLICT (league_id, season, week) DO NOTHING
        """), {'l': league_id, 's': season, 'w': week})
        conn.execute(text(f"""
            UPDATE public.pipeline_periods
            SET {sets},
                raw_version = raw_version + 1,
                published = false,
                detail = detail || CAST(:detail AS jsonb),
                updated_at = now()
            WHERE league_id = :l AND season = :s AND week = :w
        """), {'l': league_id, 's': season, 'w': week,
               'detail': json.dumps({'raw_counts': entities})})

    def mark_published(self, conn, league_id: str, season: int, week: int):
        """Mark a period published: edw_version catches up to raw_version.

        Must run on the SAME connection/transaction as the EDW refresh.
        """
        conn.execute(text("""
            UPDATE public.pipeline_periods
            SET edw_version = raw_version,
                published = true,
                verified_at = now(),
                updated_at = now()
            WHERE league_id = :l AND season = :s AND week = :w
        """), {'l': league_id, 's': season, 'w': week})

    def unpublished_raw(self, league_id: Optional[str] = None
                        ) -> List[Tuple[str, int, int]]:
        """Periods with raw_version > edw_version: committed raw data the
        site cannot see yet. These are repaired by re-publishing, never by
        re-extracting."""
        q = ("SELECT league_id, season, week FROM public.pipeline_periods "
             "WHERE raw_version > edw_version")
        params = {}
        if league_id:
            q += " AND league_id = :l"
            params['l'] = league_id
        q += " ORDER BY season, week"
        with self.engine.connect() as conn:
            return [tuple(r) for r in conn.execute(text(q), params)]

    def published_weeks(self, league_id: str, season: int) -> Set[int]:
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT week FROM public.pipeline_periods "
                "WHERE league_id = :l AND season = :s AND published"),
                {'l': league_id, 's': season})
            return {r[0] for r in rows}

    def raw_complete_weeks(self, league_id: str, season: int) -> Set[int]:
        """Weeks where every raw entity is complete (published or not)."""
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT week FROM public.pipeline_periods "
                "WHERE league_id = :l AND season = :s "
                "AND raw_matchups_complete AND raw_rosters_complete "
                "AND raw_statistics_complete"),
                {'l': league_id, 's': season})
            return {r[0] for r in rows}

    def compute_gap(self, league_id: str, season: int,
                    yahoo_completed_weeks: List[int],
                    reload_window: int = 2) -> Dict[str, List[int]]:
        """The heart of database-as-state.

        Returns:
          extract: completed Yahoo weeks needing extraction - never loaded,
                   partially loaded, or interior holes - PLUS the rolling
                   reload window (the most recent `reload_window` already-
                   complete weeks, re-fetched so Yahoo stat corrections
                   within ~two weeks reconcile automatically).
          publish_repair: weeks whose raw data is complete but unpublished
                   (raw_version > edw_version) - EDW repair only, no Yahoo.
        """
        completed = sorted(set(yahoo_completed_weeks))
        raw_done = self.raw_complete_weeks(league_id, season)
        published = self.published_weeks(league_id, season)

        missing = [w for w in completed if w not in raw_done]
        reload_tail = [w for w in completed
                       if w in raw_done][-reload_window:] if reload_window else []
        extract = sorted(set(missing) | set(reload_tail))

        publish_repair = sorted(
            w for w in raw_done
            if w not in published and w not in extract)

        return {'extract': extract, 'publish_repair': publish_repair}
