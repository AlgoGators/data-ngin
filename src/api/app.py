"""The database API gateway.

Authenticates a key, takes a concurrency slot, runs the caller's SQL under their
Postgres role, and records the outcome. Enforcement is Postgres's throughout --
this module never inspects the SQL it is given.

There is one login per role rather than one shared service login. Postgres
authorises SET ROLE against session_user, not current_user, so a single login
that was a member of all three roles could be escalated by any caller prefixing
their SQL with `SET ROLE db_readwrite_all;` -- SET LOCAL ROLE would narrow
current_user while session_user stayed a member of everything. Because this
service deliberately never parses the SQL it is given, no layer here would catch
that. Choosing the connection by role removes it: api_service_ro is not a member
of db_readwrite_all, so Postgres refuses the SET ROLE outright.
"""

import ipaddress
import os
import time

import psycopg2
from fastapi import FastAPI, Header, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.api import audit
from src.api.executor import PermissionDenied, execute_as
from src.api.keys import PREFIX_LENGTH, authenticate
from src.api.limits import AtCapacity, ConcurrencyLimiter

ROW_LIMIT = int(os.environ.get("API_ROW_LIMIT", "100000"))
GLOBAL_CONCURRENCY = int(os.environ.get("API_GLOBAL_CONCURRENCY", "2"))

# Each caller role has exactly one login, and each login is a member of exactly
# that one role. The separation is the escalation defence described above, so
# this mapping is a security boundary rather than configuration convenience.
LOGIN_BY_ROLE = {
    "db_readonly": ("api_service_ro", "API_DB_PASSWORD_RO"),
    "db_readwrite": ("api_service_rw", "API_DB_PASSWORD_RW"),
    "db_readwrite_all": ("api_service_all", "API_DB_PASSWORD_ALL"),
}

# Authentication happens before the caller's role is known, so it needs a
# connection of its own. The read-only login is used because it is the only one
# holding the direct grants on auth.api_keys and auth.audit_log -- direct, so
# they survive NOINHERIT and are reachable without any SET ROLE. Using the
# lowest-privileged of the three means a leaked password reaches no further than
# read-only access plus the auth tables.
AUTH_ROLE = "db_readonly"

app = FastAPI(title="data-ngin database API", version="1.0")
limiter = ConcurrencyLimiter(global_limit=GLOBAL_CONCURRENCY)


class QueryRequest(BaseModel):
    sql: str


def _connect(db_role: str):
    """Open a connection as the login belonging to `db_role`.

    Raises ValueError for an unrecognised role. It must never fall back to a
    default login: doing so would run that caller's SQL with privileges chosen
    by accident rather than by their key.

    Each login is NOINHERIT, so the connection holds none of its role's
    privileges until the caller's transaction issues SET LOCAL ROLE. A code path
    that forgot to switch would fail closed rather than run with that access.
    """
    if db_role not in LOGIN_BY_ROLE:
        raise ValueError(f"no service login is configured for role {db_role!r}")
    user, password_var = LOGIN_BY_ROLE[db_role]
    try:
        return psycopg2.connect(
            host=os.environ["API_DB_HOST"],
            port=os.environ.get("API_DB_PORT", "5432"),
            user=user,
            password=os.environ[password_var],
            dbname=os.environ["API_DB_NAME"],
        )
    except KeyError as exc:
        raise ValueError(f"missing required environment variable {exc}") from exc


def _client_ip(request) -> str | None:
    """Return the peer address only if it is a real IP, otherwise None.

    audit_log.client_ip is INET, so a value Postgres cannot parse makes the
    whole INSERT fail -- and audit writes are deliberately swallowed so a
    logging failure cannot fail a caller's request. Together those would mean an
    unparseable peer address silently discards the entire audit row: who ran
    what, and whether it was denied. Dropping just the address keeps the row.
    """
    if request.client is None:
        return None
    try:
        return str(ipaddress.ip_address(request.client.host))
    except ValueError:
        return None


def _bearer(authorization: str | None) -> str:
    if not authorization:
        return ""
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return authorization.strip()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/v1/query")
async def query(
    body: QueryRequest,
    request: Request,
    authorization: str | None = Header(default=None),
):
    key = _bearer(authorization)
    client_ip = _client_ip(request)

    auth_conn = await run_in_threadpool(_connect, AUTH_ROLE)
    auth_conn.autocommit = True

    try:
        caller = await run_in_threadpool(authenticate, auth_conn, key)
        if caller is None:
            # The prefix identifies which key was tried without storing it.
            await run_in_threadpool(
                audit.record_anonymous,
                auth_conn, "denied",
                key_prefix=key[:PREFIX_LENGTH] or None,
                error_message="unknown or revoked key",
                client_ip=client_ip,
            )
            return JSONResponse({"detail": "invalid API key"}, status_code=401)

        try:
            async with limiter.slot(caller.email, caller.max_concurrent):
                started = time.monotonic()

                # A second connection, opened as this caller's login. Two per
                # request, and the global cap is 2, so at most four backends.
                try:
                    exec_conn = await run_in_threadpool(_connect, caller.db_role)
                except (ValueError, psycopg2.OperationalError) as exc:
                    # Misconfiguration or an unreachable database: the caller
                    # did nothing wrong, so this is not a 4xx.
                    await run_in_threadpool(
                        audit.record,
                        auth_conn, caller, body.sql, "error",
                        error_message=str(exc).strip(),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        client_ip=client_ip,
                    )
                    return JSONResponse(
                        {"detail": "service misconfigured", "code": "misconfigured"},
                        status_code=500,
                    )

                try:
                    # Deliberately not autocommit. execute_as relies on a real
                    # transaction: SET LOCAL is silently ignored outside one, so
                    # an autocommit connection would run every statement as the
                    # bare NOINHERIT login and refuse everything.
                    result = await run_in_threadpool(
                        execute_as, exec_conn, caller, body.sql, ROW_LIMIT
                    )
                except PermissionDenied as exc:
                    await run_in_threadpool(
                        audit.record,
                        auth_conn, caller, body.sql, "denied",
                        error_message=str(exc),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        client_ip=client_ip,
                    )
                    return JSONResponse(
                        {
                            "detail": "the caller's role does not have "
                            "permission for this statement",
                            "code": "permission_denied",
                        },
                        status_code=403,
                    )
                except psycopg2.Error as exc:
                    await run_in_threadpool(
                        audit.record,
                        auth_conn, caller, body.sql, "error",
                        error_message=str(exc).strip(),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        client_ip=client_ip,
                    )
                    return JSONResponse(
                        {
                            "detail": "the statement could not be executed",
                            "code": "query_failed",
                        },
                        status_code=400,
                    )
                finally:
                    await run_in_threadpool(exec_conn.close)

                await run_in_threadpool(
                    audit.record,
                    auth_conn, caller, body.sql, "success",
                    row_count=result.row_count,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    client_ip=client_ip,
                )
                return {
                    "columns": result.columns,
                    "rows": result.rows,
                    "row_count": result.row_count,
                    "truncated": result.truncated,
                }

        except AtCapacity as exc:
            await run_in_threadpool(
                audit.record,
                auth_conn, caller, body.sql, "rate_limited",
                error_message=str(exc), client_ip=client_ip,
            )
            return JSONResponse({"detail": str(exc)}, status_code=429)

    finally:
        await run_in_threadpool(auth_conn.close)


async def _metadata_query(authorization, sql, params=None, client_ip=None):
    """Run a service-constructed metadata query as the caller.

    information_schema is filtered by Postgres to what the calling role may see,
    so these endpoints need no permission logic of their own -- a general member
    simply does not see tables they cannot read. That is also why the statement
    runs on the caller's own login rather than on the authentication connection:
    reusing the latter would answer every caller with api_service_ro's view.

    Query parameters are passed to psycopg2 rather than interpolated. The
    caller's role would bound the damage either way, but building SQL out of
    user input by hand is a habit worth not having.

    Returns (result, error_response); exactly one of the two is None.

    These endpoints are audited and rate limited on the same terms as /v1/query.
    They read less, but they still authenticate, still consume two connections,
    and are still a way to probe the service with a stolen or guessed key. An
    endpoint that leaves no trace is the one worth probing against.
    """
    key = _bearer(authorization)
    auth_conn = await run_in_threadpool(_connect, AUTH_ROLE)
    auth_conn.autocommit = True
    try:
        caller = await run_in_threadpool(authenticate, auth_conn, key)
        if caller is None:
            await run_in_threadpool(
                audit.record_anonymous,
                auth_conn, "denied",
                key_prefix=key[:PREFIX_LENGTH] or None,
                error_message="unknown or revoked key",
                client_ip=client_ip,
            )
            return None, JSONResponse(
                {"detail": "invalid API key"}, status_code=401
            )

        try:
            async with limiter.slot(caller.email, caller.max_concurrent):
                started = time.monotonic()
                try:
                    exec_conn = await run_in_threadpool(_connect, caller.db_role)
                except (ValueError, psycopg2.OperationalError) as exc:
                    # Misconfiguration or an unreachable database. The message
                    # is not echoed: it names logins and hosts, and the caller
                    # did nothing wrong, so there is nothing here to act on.
                    await run_in_threadpool(
                        audit.record,
                        auth_conn, caller, sql, "error",
                        error_message=str(exc).strip(), client_ip=client_ip,
                    )
                    return None, JSONResponse(
                        {"detail": "service misconfigured", "code": "misconfigured"},
                        status_code=500,
                    )

                try:
                    # Not autocommit, for the reason execute_as needs a real
                    # transaction: SET LOCAL is ignored outside one, so the
                    # query would run as the bare NOINHERIT login and see
                    # nothing at all.
                    result = await run_in_threadpool(
                        execute_as, exec_conn, caller, sql, ROW_LIMIT, params
                    )
                except psycopg2.Error as exc:
                    await run_in_threadpool(
                        audit.record,
                        auth_conn, caller, sql, "error",
                        error_message=str(exc).strip(),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        client_ip=client_ip,
                    )
                    return None, JSONResponse(
                        {
                            "detail": "the statement could not be executed",
                            "code": "query_failed",
                        },
                        status_code=400,
                    )
                finally:
                    await run_in_threadpool(exec_conn.close)

                await run_in_threadpool(
                    audit.record,
                    auth_conn, caller, sql, "success",
                    row_count=result.row_count,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    client_ip=client_ip,
                )
                return result, None

        except AtCapacity as exc:
            await run_in_threadpool(
                audit.record,
                auth_conn, caller, sql, "rate_limited",
                error_message=str(exc), client_ip=client_ip,
            )
            return None, JSONResponse({"detail": str(exc)}, status_code=429)
    finally:
        await run_in_threadpool(auth_conn.close)


@app.get("/v1/schemas")
async def list_schemas(
    request: Request, authorization: str | None = Header(default=None)
):
    result, error = await _metadata_query(
        authorization,
        "SELECT schema_name FROM information_schema.schemata"
        " WHERE schema_name NOT LIKE 'pg\\_%'"
        "   AND schema_name <> 'information_schema'"
        " ORDER BY schema_name",
        client_ip=_client_ip(request),
    )
    if error is not None:
        return error
    return {"schemas": [r[0] for r in result.rows]}


@app.get("/v1/tables")
async def list_tables(
    schema: str, request: Request,
    authorization: str | None = Header(default=None),
):
    result, error = await _metadata_query(
        authorization,
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = %s ORDER BY table_name",
        (schema,),
        client_ip=_client_ip(request),
    )
    if error is not None:
        return error
    return {"schema": schema, "tables": [r[0] for r in result.rows]}


@app.get("/v1/columns")
async def list_columns(
    schema: str, table: str, request: Request,
    authorization: str | None = Header(default=None),
):
    result, error = await _metadata_query(
        authorization,
        "SELECT column_name, data_type FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s"
        " ORDER BY ordinal_position",
        (schema, table),
        client_ip=_client_ip(request),
    )
    if error is not None:
        return error
    return {
        "schema": schema,
        "table": table,
        "columns": [{"name": r[0], "type": r[1]} for r in result.rows],
    }
