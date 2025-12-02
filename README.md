
plain SQL in a sql.toml file.
```toml
[sql]

[sql.videos]
create = """
CREATE TABLE IF NOT EXISTS videos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    title TEXT
);

CREATE INDEX idx_pipelines_name ON pipelines(name);
CREATE INDEX idx_pipelines_active ON pipelines(is_active);
"""

insert = """
INSERT INTO videos (url, title)
VALUES (?, ?)
"""
```
creates tables on first run
dict of functions
```python
from no_orm_ever import DB

DB.videos.insert({"url": "https://...", "title": "cat video"})
recent = DB.videos.list_recent(limit=10)
DB.videos.bulk_insert(big_list_of_dicts, replace=True)
```
optional CSV seed files (*_seed.csv) loaded once
some sqlite_vec but needs work

