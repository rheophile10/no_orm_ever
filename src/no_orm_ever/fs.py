import yaml
from pathlib import Path
import csv
from types import SimpleNamespace
from functools import partial
from typing import Dict, Any
from typing import Literal
from no_orm_ever.ops import run, bulk, bulk_vec0, db, die
from no_orm_ever.sql import is_vec_sql, is_write_sql


InterfaceType = Literal["namespace", "dict"]


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
            bulk(db_path, table_name, rows, replace=True)


def load(
    db_path: Path | str,
    sql_yaml_path: Path | str,
    seeds_dir: Path | None = None,
    interface_type: InterfaceType = "namespace",
) -> SimpleNamespace | Dict[str, Any]:
    db_interface = {}
    db_path = Path(db_path)
    sql_yaml = validate_sql_yaml(sql_yaml_path)

    if not db_path.exists() or db_path.stat().st_size == 0:
        with db(db_path) as conn:
            for section in sql_yaml.values():
                if "create" in section and is_write_sql(section["create"]):
                    conn.executescript(section["create"])

    for table_name, section in sql_yaml.items():
        ns = {}
        is_vec_table = any(is_vec_sql(s) for s in section.values())

        if is_vec_table:
            ns["bulk"] = partial(bulk_vec0, db_path, table_name)
        else:
            ns["bulk"] = partial(bulk, db_path, table_name)

        for stmt_name, sql in section.items():
            if stmt_name == "create":
                continue
            if is_write_sql(sql):
                ns[stmt_name] = partial(run, sql, db_path, atomic=True)
            else:
                ns[stmt_name] = partial(run, sql, db_path)

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
