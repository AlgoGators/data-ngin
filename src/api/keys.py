"""API key generation and hashing.

Keys are stored only as SHA-256 hashes. SHA-256 rather than bcrypt because
bcrypt exists to slow brute-forcing of guessable secrets such as human-chosen
passwords; a 32-character random token has enough entropy that brute force is
irrelevant, and this runs on every request.
"""

import hashlib
import secrets

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
