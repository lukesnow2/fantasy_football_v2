"""Multi-row INSERTs: one network round trip per page instead of per row.

One statement per row is invisible against a local database and dominates
against a hosted one: each statement waits on a round trip to Neon (~70 ms
from GitHub's runners). Writing ~1,100 raw rows took 82 s and the EDW
dimension upserts nearly 3 minutes of a 7.4-minute weekly run (2026-10-06).
"""
import logging
from typing import Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)


def last_per_key(rows: List[dict], key_cols: Sequence[str], table: str) -> List[dict]:
    """Keep the last row per business key, in first-seen key order.

    One statement per row let a repeated key through (the last write won);
    a single multi-row INSERT ... ON CONFLICT DO UPDATE instead raises
    "cannot affect row a second time". Collapsing here keeps the old result.
    Only needed for DO UPDATE: DO NOTHING and plain inserts behave the same
    either way.
    """
    latest = {}
    for row in rows:
        latest[tuple(row[c] for c in key_cols)] = row
    if len(latest) != len(rows):
        logger.warning("%s: collapsed %d row(s) sharing a business key (%s)",
                       table, len(rows) - len(latest), ', '.join(key_cols))
    return list(latest.values())


def insert_batched(conn, table: str, columns: Sequence[str],
                   rows: Iterable[Sequence], suffix: str = '',
                   page_size: int = 1000) -> int:
    """INSERT INTO <table> (<columns>) VALUES ... <suffix>, in pages.

    `table` is schema-qualified; `rows` are value tuples in `columns` order,
    passed to psycopg2 unchanged (as the one-row-at-a-time statements did);
    `suffix` is the ON CONFLICT clause, if any. Runs on the caller's
    SQLAlchemy connection and transaction. Returns the number of rows sent.
    """
    from psycopg2.extras import execute_values

    rows = list(rows)
    if not rows:
        return 0
    col_list = ', '.join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO {table} ({col_list}) VALUES %s {suffix}'.rstrip()

    # The batch runs on the raw DBAPI cursor, which SQLAlchemy does not see.
    # If SQLAlchemy has no transaction of its own, its later commit() is a
    # no-op and every row written here is rolled back when the connection
    # returns to the pool - silently. Opening the transaction explicitly
    # makes the caller's commit cover this work.
    if not conn.in_transaction():
        conn.begin()
    cursor = conn.connection.cursor()
    try:
        execute_values(cursor, sql, rows, page_size=page_size)
    finally:
        cursor.close()
    return len(rows)


def upsert_set(update_cols: Sequence[str], extra: Optional[List[str]] = None) -> str:
    """The DO UPDATE SET list: each column from EXCLUDED, plus raw expressions."""
    return ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols] + (extra or []))
