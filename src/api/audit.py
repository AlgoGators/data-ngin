"""Writes audit rows.

Every request produces one, including authentication failures, permission
denials and rate limits. A log recording only successes says nothing about
someone probing for access.

Actor identity is written onto each row rather than joined from api_keys, so
removing someone does not erase them from history.
"""

import logging

logger = logging.getLogger(__name__)

_INSERT = (
    "INSERT INTO auth.audit_log"
    " (actor_email, actor_name, actor_role, key_prefix, statement,"
    "  row_count, outcome, error_message, duration_ms, client_ip)"
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)


def record(
    conn,
    caller,
    statement: str,
    outcome: str,
    *,
    row_count=None,
    error_message=None,
    duration_ms=None,
    client_ip=None,
) -> None:
    """Record a request made by an authenticated caller."""
    _write(
        conn,
        (
            caller.email,
            caller.name,
            caller.db_role,
            caller.key_prefix,
            statement,
            row_count,
            outcome,
            error_message,
            duration_ms,
            client_ip,
        ),
    )


def record_anonymous(
    conn,
    outcome: str,
    *,
    key_prefix=None,
    error_message=None,
    client_ip=None,
) -> None:
    """Record an attempt that never authenticated.

    key_prefix identifies which key was presented. The key itself is never
    stored -- only the caller-supplied prefix, which is not usable alone.
    """
    _write(
        conn,
        (
            "unknown",
            "unknown",
            "none",
            key_prefix,
            None,
            None,
            outcome,
            error_message,
            None,
            client_ip,
        ),
    )


def _write(conn, params) -> None:
    """Insert one row. Never raises.

    A failure to log must not turn a successful query into an error for the
    caller, so the exception is logged locally and swallowed. That is a
    deliberate trade: losing one audit row is preferable to failing the request
    that produced it.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT, params)
        if not conn.autocommit:
            conn.commit()
    except Exception:  # noqa: BLE001 - deliberately swallowed
        logger.exception("failed to write audit row")
