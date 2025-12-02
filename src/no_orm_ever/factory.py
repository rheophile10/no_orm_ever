from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, Sequence
import csv
import tomllib

from no_orm_ever.connect import open_db

NotAnORM = dict[str, SimpleNamespace]


def make_sql_runner(db_path: Path, sql: str):
    """Returns a function that executes `sql` against `db_path`."""
    sql = sql.strip()
    is_write = sql.split(maxsplit=1)[0].upper() in {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "UPSERT",
        "CREATE",
        "DROP",
        "PRAGMA",
    }

    def run(data: Any = None, /):
        with open_db(db_path) as conn:
            if data is None:
                conn.executescript(sql)

            else:

                if isinstance(data, Mapping):
                    conn.execute(sql, tuple(data.values()))
                elif (
                    isinstance(data, (list, tuple))
                    and data
                    and isinstance(data[0], (Mapping, Sequence))
                ):
                    rows = [
                        tuple(r.values()) if isinstance(r, Mapping) else tuple(r)
                        for r in data
                    ]
                    conn.executemany(sql, rows)
                else:
                    conn.execute(
                        sql, data if isinstance(data, (list, tuple)) else (data,)
                    )

                conn.commit()

            return (
                None if is_write else [dict(row) for row in conn.fetchall()]
            )  # ← nice dicts

    return run


def bulk_insert(
    db_path: Path,
    table: str,
    rows: Iterable[Mapping | Sequence],
    *,
    replace: bool = False,
    or_ignore: bool = False,
    batch_size: int = 10_000,
) -> int:
    it = iter(rows)
    try:
        first = next(it)
    except StopIteration:
        return 0

    if isinstance(first, Mapping):
        columns = tuple(first.keys())
        to_values = lambda r: tuple(r[c] for c in columns)
    else:
        columns = None
        to_values = tuple

    placeholders = ",".join("?" for _ in (columns or first))
    prefix = (
        "REPLACE INTO"
        if replace
        else ("INSERT OR IGNORE INTO" if or_ignore else "INSERT INTO")
    )
    sql = (
        f"{prefix} {table} ({','.join(columns)})"
        if columns
        else f"{prefix} {table} VALUES ({placeholders})"
    )

    total = 0
    batch = [to_values(first)]

    with open_db(db_path) as conn:
        for row in it:
            batch.append(to_values(row))
            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)
            total += len(batch)

    return total


def seed_database(db_path: Path, seeds_dir: Path, table_interfaces: NotAnORM) -> None:
    if not seeds_dir.exists():
        return

    for csv_path in sorted(
        seeds_dir.glob("*_seed.csv"), key=lambda p: p.stat().st_mtime
    ):
        table_name = csv_path.stem.removesuffix("_seed")
        if table_name not in table_interfaces:
            continue

        with csv_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if rows:
            bulk_insert(db_path, table_name, rows, replace=True)


def create_db_interface(
    *,
    db_path: Path,
    sql_toml_path: Path,
    seeds_dir: Path | None = None,
) -> NotAnORM:
    raw_sql = tomllib.loads(sql_toml_path.read_text(encoding="utf-8"))["sql"]
    interface: NotAnORM = {}
    create_scripts: list[str] = []

    for table_name, statements in raw_sql.items():
        table_ns: dict[str, Any] = {}

        for stmt_name, sql_text in statements.items():
            if not isinstance(sql_text, str):
                continue

            if stmt_name == "create":
                create_scripts.append(sql_text)
                continue

            table_ns[stmt_name] = make_sql_runner(db_path, sql_text)

        table_ns["bulk_insert"] = lambda rows, **kw: bulk_insert(
            db_path, table_name, rows, **kw
        )

        interface[table_name] = SimpleNamespace(**table_ns)

    if create_scripts and (not db_path.exists() or db_path.stat().st_size == 0):
        with open_db(db_path) as conn:
            for script in create_scripts:
                conn.executescript(script)

    if seeds_dir:
        seed_database(db_path, seeds_dir, interface)

    return interface
