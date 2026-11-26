"""Small psycopg2 helpers shared by ingestion, ML eval and the triage agent."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from itertools import islice
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from common.config import Settings, get_settings


@contextmanager
def connect(settings: Settings | None = None, autocommit: bool = False) -> Iterator[Any]:
    settings = settings or get_settings()
    conn = psycopg2.connect(settings.dsn)
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        conn.close()


def execute_values(
    conn: Any,
    sql: str,
    rows: Iterable[Sequence[Any]],
    page_size: int = 2000,
) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)


def copy_rows(
    conn: Any,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    null: str = r"\N",
) -> int:
    """Bulk load rows with COPY FROM STDIN (fast path for 1M+ row batches)."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL {})").format(
        sql.Identifier(*table.split(".")),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.Literal(null),
    )
    count = 0
    pending = iter(rows)
    with conn.cursor() as cur:
        command = statement.as_string(conn)
        while batch := list(islice(pending, 10_000)):
            for row in batch:
                if any(value == null for value in row):
                    raise ValueError("Bulk load value equals the configured NULL marker")
                writer.writerow([null if value is None else value for value in row])
            buffer.seek(0)
            cur.copy_expert(command, buffer)
            buffer.seek(0)
            buffer.truncate()
            count += len(batch)
    return count


def read_sql(conn: Any, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def replace_batch(conn: Any, table: str, batch_id: str) -> None:
    """Idempotent re-runs: remove this batch's raw rows before reload."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE batch_id = %s").format(sql.Identifier(*table.split("."))),
            (batch_id,),
        )


def table_exists(conn: Any, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s)",
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def parquet_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.parquet"))
