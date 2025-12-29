from no_orm_ever import load
from pathlib import Path

if Path("dev.db").exists():
    Path("dev.db").unlink()


db = load("dev.db", "example/example-sql.yaml")

db.users.bulk(
    [
        {"name": "Alice", "email": "a@example.com"},
        {"name": "Bob", "email": None},
    ]
)

new_user = db.users.insert([{"name": "Charlie", "email": "c@example.com"}])[0]
print("Created:", dict(new_user))

query_result = db.query("select * from users where name = :name", {"name": "Alice"})
print("Query result:", [dict(row) for row in query_result])

all_users = db.users.all()
print("All users:", [dict(u) for u in all_users])

import numpy as np

db.embeddings.bulk(
    [
        {"id": 1, "embedding": np.random.rand(1536)},
        {"id": 2, "embedding": np.random.rand(1536).tolist()},
    ]
)
