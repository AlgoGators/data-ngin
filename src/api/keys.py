"""API key generation and hashing.

Keys are stored only as SHA-256 hashes. SHA-256 rather than bcrypt because
bcrypt exists to slow brute-forcing of guessable secrets such as human-chosen
passwords; a 32-character random token has enough entropy that brute force is
irrelevant, and this runs on every request.
"""

import hashlib
import secrets
from dataclasses import dataclass

ROLE_PREFIXES = {
    "db_readonly": "ro",
    "db_readwrite": "rw",
    "db_readwrite_all": "ad",
}

# Long enough to identify a key in a log line, short enough to be useless alone.
PREFIX_LENGTH = 10


def hash_key(plaintext: str) -> str:
    """Return the hex SHA-256 of a key. The only form ever persisted."""
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def generate_key(db_role: str) -> tuple[str, str, str]:
    """Generate a new API key.

    Returns (plaintext, key_hash, key_prefix). The plaintext is the only copy
    that will ever exist -- callers must display it and discard it.
    """
    if db_role not in ROLE_PREFIXES:
        raise ValueError(
            f"unknown role {db_role!r}; expected one of {sorted(ROLE_PREFIXES)}"
        )
    plaintext = f"ag_{ROLE_PREFIXES[db_role]}_{secrets.token_urlsafe(24)}"
    return plaintext, hash_key(plaintext), plaintext[:PREFIX_LENGTH]


@dataclass(frozen=True)
class Caller:
    """An authenticated person. Carries everything the request needs, so no
    later stage has to query the key table again."""

    email: str
    name: str
    db_role: str
    key_prefix: str
    max_concurrent: int
    statement_timeout_ms: int


def authenticate(conn, plaintext: str) -> "Caller | None":
    """Resolve an API key to a Caller, or None if it is unknown or revoked.

    Looks up by hash, so the plaintext key is never compared against stored
    data and never appears in a query log.
    """
    if not plaintext:
        return None

    with conn.cursor() as cur:
        cur.execute(
            "SELECT email, name, db_role, key_prefix, max_concurrent,"
            "       statement_timeout_ms"
            "  FROM auth.api_keys"
            " WHERE key_hash = %s AND active",
            (hash_key(plaintext),),
        )
        row = cur.fetchone()
        if row is None:
            return None

        # Best-effort: a failure here must not deny an otherwise valid request.
        try:
            cur.execute(
                "UPDATE auth.api_keys SET last_used_at = now() WHERE email = %s",
                (row[0],),
            )
        except Exception:  # noqa: BLE001 - deliberately swallowed
            pass

    return Caller(
        email=row[0],
        name=row[1],
        db_role=row[2],
        key_prefix=row[3],
        max_concurrent=row[4],
        statement_timeout_ms=row[5],
    )
