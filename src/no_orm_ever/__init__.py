from pathlib import Path
from no_orm_ever.factory import create_db_interface, NotAnORM

DATA_DIR = Path(__file__).parent.parent / "data" / "main"
DB: NotAnORM = create_db_interface(
    db_path=DATA_DIR / "sqlite_dict.db",
    sql_toml_path=DATA_DIR / "sql.toml",
    seeds_dir=DATA_DIR / "seeds",
)


def get_db(name: str = "main") -> NotAnORM:
    data_dir = Path(__file__).parent.parent / "data" / name
    return create_db_interface(
        db_path=data_dir / f"{name}.db",
        sql_toml_path=data_dir / "sql.toml",
        seeds_dir=data_dir / "seeds",
    )
