#!/usr/bin/env python3
"""Admin CLI for API keys.

A script rather than an endpoint, to avoid a bootstrap problem: an endpoint
would require a key in order to create the first key.

Usage:
    python -m scripts.manage_api_keys create --email a@b.com --name "A B" --role db_readwrite
    python -m scripts.manage_api_keys revoke --email a@b.com
    python -m scripts.manage_api_keys list
"""

import argparse
import os
import sys

import psycopg2

from src.api.keys import ROLE_PREFIXES, generate_key


def _connect():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


def create_key(conn, email: str, name: str, db_role: str, created_by: str) -> str:
    """Create or rotate a key. Returns the plaintext, which is the only copy.

    Rotating overwrites the hash, so the previous key stops working at once.
    """
    if db_role not in ROLE_PREFIXES:
        raise ValueError(
            f"unknown role {db_role!r}; expected one of {sorted(ROLE_PREFIXES)}"
        )
    plaintext, key_hash, prefix = generate_key(db_role)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO auth.api_keys"
            " (email, name, db_role, key_hash, key_prefix, created_by,"
            "  active, revoked_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, TRUE, NULL)"
            " ON CONFLICT (email) DO UPDATE SET"
            "   name = EXCLUDED.name,"
            "   db_role = EXCLUDED.db_role,"
            "   key_hash = EXCLUDED.key_hash,"
            "   key_prefix = EXCLUDED.key_prefix,"
            "   active = TRUE,"
            "   revoked_at = NULL",
            (email, name, db_role, key_hash, prefix, created_by),
        )
    if not conn.autocommit:
        conn.commit()
    return plaintext


def revoke_key(conn, email: str) -> bool:
    """Deactivate a key. Returns False if there was no such person.

    The row is kept rather than deleted, so 'who had access in March?' stays
    answerable.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE auth.api_keys"
            "   SET active = FALSE, revoked_at = now()"
            " WHERE email = %s AND active",
            (email,),
        )
        changed = cur.rowcount
    if not conn.autocommit:
        conn.commit()
    return changed > 0


def list_keys(conn) -> list:
    """List keys. Never returns key_hash."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT email, name, db_role, key_prefix, active, created_at,"
            "       last_used_at"
            "  FROM auth.api_keys ORDER BY email"
        )
        return [
            {
                "email": r[0], "name": r[1], "db_role": r[2], "key_prefix": r[3],
                "active": r[4], "created_at": r[5], "last_used_at": r[6],
            }
            for r in cur.fetchall()
        ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage data-ngin API keys")
    sub = parser.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="create or rotate a key")
    c.add_argument("--email", required=True)
    c.add_argument("--name", required=True)
    c.add_argument("--role", required=True, choices=sorted(ROLE_PREFIXES))
    c.add_argument("--created-by", default=os.environ.get("USER", "unknown"))

    r = sub.add_parser("revoke", help="deactivate a key")
    r.add_argument("--email", required=True)

    sub.add_parser("list", help="list keys")

    args = parser.parse_args()
    conn = _connect()
    conn.autocommit = True
    try:
        if args.command == "create":
            plaintext = create_key(
                conn, args.email, args.name, args.role, args.created_by
            )
            print(f"Key for {args.email} ({args.role}):\n")
            print(f"    {plaintext}\n")
            print("This is the only time it will be shown. Send it to them")
            print("privately -- not in the repo, not in a public channel.")
        elif args.command == "revoke":
            if revoke_key(conn, args.email):
                print(f"Revoked {args.email}")
            else:
                print(f"No active key for {args.email}")
                return 1
        else:
            for row in list_keys(conn):
                state = "active" if row["active"] else "revoked"
                print(
                    f"{row['email']:<32} {row['name']:<20} {row['db_role']:<18} "
                    f"{row['key_prefix']:<12} {state}"
                )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
