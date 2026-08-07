"""Runs a caller's SQL under their Postgres role.

The service never inspects the SQL. It switches to the caller's role and lets
Postgres accept or reject the statement, because determining what a statement
writes to requires a full SQL parser plus search_path resolution plus following
functions and triggers -- and every gap in that is a permission bypass.
"""

from dataclasses import dataclass

import psycopg2
from psycopg2 import sql as pgsql


class PermissionDenied(Exception):
    """Postgres refused the statement for the caller's role."""


@dataclass
class QueryResult:
    columns: list
    rows: list
    row_count: int
    truncated: bool


def execute_as(conn, caller, sql: str, row_limit: int, params=None) -> QueryResult:
    """Execute `sql` as `caller`, returning at most `row_limit` rows.

    `params` is for SQL this service constructs itself, such as the metadata
    endpoints. Caller-supplied SQL arrives fully formed and passes params=None;
    it needs no parameterisation because the caller's role, not string hygiene,
    is what bounds it.

    Raises PermissionDenied if the role lacks permission, or psycopg2.Error for
    anything else (syntax errors, timeouts, constraint violations).
    """
    try:
        with conn:  # transaction: commits on success, rolls back on exception
            with conn.cursor() as cur:
                # SET LOCAL, not SET: both revert when the transaction ends, so
                # an exception cannot leave a pooled connection holding an
                # elevated role for the next caller.
                cur.execute(
                    pgsql.SQL("SET LOCAL ROLE {}").format(
                        pgsql.Identifier(caller.db_role)
                    )
                )
                cur.execute(
                    "SET LOCAL statement_timeout = %s",
                    (caller.statement_timeout_ms,),
                )

                cur.execute(sql, params) if params else cur.execute(sql)

                if cur.description is None:
                    # No result set: INSERT, UPDATE, DELETE, DDL.
                    return QueryResult([], [], cur.rowcount, False)

                columns = [d.name for d in cur.description]
                # Fetch one extra to detect truncation without counting the
                # whole result, which would defeat the limit's purpose.
                fetched = cur.fetchmany(row_limit + 1)
                truncated = len(fetched) > row_limit
                rows = [list(r) for r in fetched[:row_limit]]
                return QueryResult(columns, rows, len(rows), truncated)

    except psycopg2.errors.InsufficientPrivilege as exc:
        raise PermissionDenied(str(exc).strip()) from exc
