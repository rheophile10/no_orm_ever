import re
from typing import Iterator


def clean_sql_statements(sql: str) -> Iterator[str]:
    """
    Yield individual SQL statements from a multi-statement string,
    stripping comments and empty statements.
    """
    if not sql:
        return

    sql_no_line_comments = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)

    sql_clean = re.sub(r"/\*.*?\*/", "", sql_no_line_comments, flags=re.DOTALL)

    for raw_stmt in sql_clean.split(";"):
        stmt = raw_stmt.strip()
        if stmt:
            yield stmt


def sql_starts_with_keyword(sql: str, keywords: set[str]) -> bool:
    """
    Return True if any cleaned statement in the SQL starts with one of the keywords.
    """
    for stmt in clean_sql_statements(sql):
        match = re.match(r"\s*(\w+)", stmt, re.IGNORECASE)
        if match and match.group(1).upper() in keywords:
            return True
    return False


WRITE_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "REPLACE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "PRAGMA",
}


def is_write_sql(sql: str) -> bool:
    return sql_starts_with_keyword(sql, WRITE_KEYWORDS)


VEC_KEYWORDS = {"VEC0", "VEC("}


def is_vec_sql(sql: str) -> bool:
    """True if SQL mentions vec0 or vec( — used for vector tables."""
    sql_clean = "\n".join(clean_sql_statements(sql))
    sql_lower = sql_clean.lower()
    return "vec0" in sql_lower or "vec(" in sql_lower
