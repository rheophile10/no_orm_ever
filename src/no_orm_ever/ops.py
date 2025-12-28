from contextlib import contextmanager
import sqlite3
from pathlib import Path
import sqlite_vec
from typing import Any, Iterable, Mapping, TypedDict, Literal
import numpy as np


def die(message: str):
    raise ValueError(f"\n\nTHIS SQL.YAML IS BULLSHIT:\n    {message}\n")


@contextmanager
def db(path: Path | str):
    path = Path(path)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    try:
        sqlite_vec.load(conn)
    finally:
        conn.enable_load_extension(False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run(
    sql: str,
    db_path: Path,
    data: Any = None,
    atomic: bool = False,
) -> list[dict] | None:
    with db(db_path) as conn:
        if atomic:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = execute_with_data(conn, sql, data)
                if cur.description:
                    results = [dict(row) for row in cur.fetchall()]
                else:
                    results = None

                conn.commit()
            except Exception:
                conn.rollback()
                raise
        else:
            cur = execute_with_data(conn, sql, data)
            if cur.description:
                results = [dict(row) for row in cur.fetchall()]
            else:
                results = None

        return results


def execute_with_data(conn, sql: str, data: Any):
    """Helper to avoid code duplication"""
    if data is None:
        return conn.execute(sql)
    elif isinstance(data, Mapping):
        return conn.execute(sql, data)
    elif isinstance(data, (list, tuple)):
        return conn.executemany(sql, data)
    else:
        return conn.execute(sql, (data,))


BulkStrategy = Literal["upsert", "deflect"]


def bulk(
    db_path: Path,
    table: str,
    data: Iterable[Mapping[str, Any]],
    strategy: BulkStrategy = "deflect",
    *,
    batch_size: int = 10_000,
) -> int:
    it = iter(data)
    try:
        first = next(it)
    except StopIteration:
        return 0

    columns = tuple(first.keys())
    placeholders = ",".join(f":{col}" for col in columns)
    column_list = ", ".join(columns)

    if strategy == "upsert":
        sql = f"INSERT OR REPLACE INTO {table} ({column_list}) VALUES ({placeholders})"
    elif strategy == "deflect":
        sql = (
            f"INSERT INTO {table} ({column_list}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    batch: list[tuple] = []
    total = 0

    batch.append(tuple(first.values()))
    total += 1
    for row in it:
        batch.append(tuple(row.values()))
        total += 1
        if len(batch) >= batch_size:
            run(sql, db_path, batch)
            batch.clear()

    if batch:
        run(sql, db_path, batch, atomic=True)

    return total


class Vec0Row(TypedDict):
    id: int
    embedding: np.ndarray


def bulk_vec0(
    db_path: Path,
    table: str,
    rows: Iterable[Vec0Row],
    *,
    batch_size: int = 500,
) -> int:
    it = iter(rows)
    try:
        first = next(it)
    except StopIteration:
        return 0

    id_column = "id"
    keys = first.keys()
    embedding_column = next(k for k in keys if k != id_column)

    sql = f"""
        INSERT INTO {table} ({id_column}, {embedding_column})
        VALUES (?, ?)
    """

    batch: list[tuple[int, bytes]] = []
    total = 0

    def _prep(row: Vec0Row):
        vec = row[embedding_column]
        if isinstance(vec, (list, tuple)):
            vec = np.array(vec, dtype=np.float32)
        elif isinstance(vec, np.ndarray):
            vec = vec.astype(np.float32)
        else:
            die(f"embedding must be list/tuple/np.array, got {type(vec)}")
        return (row[id_column], vec.tobytes())

    batch.append(_prep(first))
    total += 1

    for row in it:
        batch.append(_prep(row))
        total += 1
        if len(batch) >= batch_size:
            run(sql, db_path, batch)
            batch.clear()

    if batch:
        run(sql, db_path, batch, atomic=True)

    return total
