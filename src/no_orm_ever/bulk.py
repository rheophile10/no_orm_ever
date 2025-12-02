from typing import Iterable, Mapping, Sequence
from no_orm_ever.connect import open_db
from pathlib import Path


def bulk_insert(
    db_path: Path,
    table: str,
    rows: Iterable[Mapping | Sequence],
    *,
    replace=False,
    or_ignore=False,
    batch_size=10_000,
) -> int:
    it = iter(rows)
    try:
        first = next(it)
    except StopIteration:
        return 0

    if isinstance(first, Mapping):
        columns = tuple(first.keys())
        to_tuple = lambda r: tuple(r[c] for c in columns)
    else:
        columns = None
        to_tuple = tuple

    placeholders = ",".join("?" for _ in (columns or first))
    prefix = (
        "REPLACE INTO"
        if replace
        else "INSERT OR IGNORE INTO" if or_ignore else "INSERT INTO"
    )
    sql = (
        f"{prefix} {table} ({','.join(columns)})"
        if columns
        else f"{prefix} {table} VALUES ({placeholders})"
    )

    total = 0
    batch = [to_tuple(first)]

    with open_db(db_path) as conn:
        for row in it:
            batch.append(to_tuple(row))
            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)
            total += len(batch)
    return total
