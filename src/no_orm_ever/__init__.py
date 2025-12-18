from pathlib import Path
from contextlib import contextmanager
import sqlite3
import csv
from types import SimpleNamespace
from typing import Iterable, Mapping, Any, TypedDict, Literal, Dict
import numpy as np
import sqlite_vec
from functools import partial
import yaml


class Vec0Row(TypedDict):
    id: int
    embedding: np.ndarray


InterfaceType = Literal["namespace", "dict"]


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


def validate_sql_yaml(yaml_path: Path | str) -> dict[str, dict[str, str]]:
    path = Path(yaml_path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        die(f"Invalid YAML syntax: {e}")

    if raw is None:
        raw = {}  # empty file
    if not isinstance(raw, dict):
        die("root must be a mapping of tables")

    for table, section in raw.items():
        if not isinstance(section, dict):
            die(f"[{table}] must be a mapping")

        for name, sql in section.items():
            if not isinstance(sql, str) or not sql.strip():
                die(f"[{table}.{name}] must be a non-empty SQL string")

    return raw


def die(message: str):
    raise ValueError(f"\n\nTHIS SQL.YAML IS BULLSHIT:\n    {message}\n")


def _run(sql: str, db_path: Path, data: Any = None) -> list[dict] | None:
    with db(db_path) as conn:
        if data is None:
            cur = conn.execute(sql)
        elif isinstance(data, Mapping):
            cur = conn.execute(sql, data)
        elif isinstance(data, (list, tuple)):
            cur = conn.executemany(sql, data)
        else:
            cur = conn.execute(sql, (data,))

        if cur.description:
            return [dict(row) for row in cur.fetchall()]
        return None


def _bulk(
    db_path: Path,
    table: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    replace: bool = False,
    batch_size: int = 10_000,
) -> int:
    it = iter(rows)
    try:
        first = next(it)
    except StopIteration:
        return 0

    columns = tuple(first.keys())
    placeholders = ",".join("?" for _ in columns)
    prefix = "REPLACE INTO" if replace else "INSERT INTO"
    sql = f"{prefix} {table} ({', '.join(columns)}) VALUES ({placeholders})"

    batch: list[tuple] = []
    total = 0

    batch.append(tuple(first.values()))
    total += 1
    for row in it:
        batch.append(tuple(row.values()))
        total += 1
        if len(batch) >= batch_size:
            _run(sql, db_path, batch)
            batch.clear()

    if batch:
        _run(sql, db_path, batch)

    return total


def _bulk_vec0(
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
            _run(sql, db_path, batch)
            batch.clear()

    if batch:
        _run(sql, db_path, batch)

    return total


def _seed_from_csv(
    db_path: Path, seeds_dir: Path, sql_yaml: dict[str, dict[str, str]]
) -> None:
    if not seeds_dir.exists():
        return

    for csv_path in sorted(seeds_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime):
        table_name = csv_path.stem
        if table_name not in sql_yaml:
            continue

        with csv_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if rows:
            _bulk(db_path, table_name, rows, replace=True)


def load(
    db_path: Path | str,
    sql_yaml_path: Path | str,
    seeds_dir: Path | None = None,
    interface_type: InterfaceType = "namespace",
) -> SimpleNamespace | Dict[str, Any]:
    db_interface = {}
    db_path = Path(db_path)
    sql_yaml = validate_sql_yaml(sql_yaml_path)
    vec_tables = []

    if not db_path.exists() or db_path.stat().st_size == 0:
        with db(db_path) as conn:
            for section in sql_yaml.values():
                if "create" in section:
                    conn.executescript(section["create"])
                    if "vec0" in section["create"].lower():
                        vec_tables.append(section)

    for table_name, section in sql_yaml.items():
        ns = {}
        is_vec_table = any(
            isinstance(s, str) and ("vec0" in s.lower() or "vec(" in s.lower())
            for s in section.values()
        )
        for stmt_name, sql in section.items():
            if stmt_name == "create":
                continue
            ns[stmt_name] = partial(_run, sql, db_path)

        if is_vec_table:
            ns["bulk"] = partial(_bulk_vec0, db_path, table_name)
        else:
            ns["bulk"] = partial(_bulk, db_path, table_name)

        if interface_type == "namespace":
            db_interface[table_name] = SimpleNamespace(**ns)
        else:
            db_interface[table_name] = ns

    if seeds_dir:
        _seed_from_csv(db_path, seeds_dir, sql_yaml)

    if interface_type == "dict":
        final_interface = db_interface
    else:
        final_interface = SimpleNamespace(**db_interface)

    return final_interface


__all__ = ["validate_sql_yaml", "load"]
