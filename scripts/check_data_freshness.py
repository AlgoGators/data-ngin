"""Scheduled data-freshness check for the market-data tables data-ngin owns.

Standalone (no Airflow/DAG context needed) so it can run from a plain GitHub
Actions runner on a schedule. Connects directly to Postgres via DB_HOST/DB_PORT/
DB_USER/DB_PASSWORD/DB_NAME env vars and checks the latest ingested timestamp in
each canonical OHLCV table (see ADR-000 C-2). Files/updates a GitHub issue if any
table is staler than STALE_THRESHOLD_HOURS.

IMPORTANT reachability caveat: this only works if DB_HOST is reachable from
GitHub-hosted runners. Per ADR-000 C-1, the production DB's documented private
address (172.31.23.190) is an AWS PRIVATE IP and will NOT be reachable this way --
a public endpoint, VPN/tunnel, or self-hosted runner inside the VPC is needed for
this to actually run against production. Point DB_HOST at whatever address is
genuinely reachable, or run this workflow on a self-hosted runner.

Threshold defaults to 72 hours (3 days) rather than 24 -- OHLCV data does not
update on weekends/holidays, so a naive 24h check would false-alarm every
Saturday/Sunday morning after a normal Friday close. 72h absorbs a routine
Friday-close-to-Monday-morning gap while still catching a genuine multi-day
pipeline outage.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

TABLES = [
    "futures_data.ohlcv_1d",
    "equities_data.ohlcv_1d",
]

STALE_THRESHOLD_HOURS = int(os.environ.get("STALE_THRESHOLD_HOURS", "72"))
ISSUE_LABEL = "data-freshness"
DEFAULT_REPO = "AlgoGators/data-ngin"


def get_latest_timestamps(conn):
    latest = {}
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute(f"SELECT MAX(time) FROM {table}")
            row = cur.fetchone()
            latest[table] = row[0] if row else None
    return latest


def find_stale_tables(latest, now, threshold_hours):
    """Pure function: given {table: latest_timestamp_or_None}, return the list of
    (table, reason) pairs that are missing data or older than threshold_hours."""
    stale = []
    for table, ts in latest.items():
        if ts is None:
            stale.append((table, "no rows found"))
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = now - ts
        if age > timedelta(hours=threshold_hours):
            stale.append(
                (table, f"latest row is {age} old (threshold {threshold_hours}h)")
            )
    return stale


def file_or_update_issue(stale, repo, token):
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    title = "[data-freshness] Market data is stale"
    body = "\n".join(f"- `{table}`: {reason}" for table, reason in stale)

    query = f'repo:{repo} is:issue is:open label:{ISSUE_LABEL} in:title "Market data is stale"'
    resp = requests.get(
        "https://api.github.com/search/issues",
        params={"q": query},
        headers=headers,
        timeout=10,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])

    if items:
        number = items[0]["number"]
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/issues/{number}/comments",
            json={"body": body},
            headers=headers,
            timeout=10,
        )
    else:
        resp = requests.post(
            f"https://api.github.com/repos/{repo}/issues",
            json={"title": title, "body": body, "labels": [ISSUE_LABEL]},
            headers=headers,
            timeout=10,
        )
    resp.raise_for_status()


def main():
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
        connect_timeout=10,
    )
    try:
        latest = get_latest_timestamps(conn)
    finally:
        conn.close()

    now = datetime.now(timezone.utc)
    stale = find_stale_tables(latest, now, STALE_THRESHOLD_HOURS)

    for table, ts in latest.items():
        print(f"{table}: latest={ts}")

    if not stale:
        print("OK: all tables within freshness threshold.")
        return 0

    print("STALE:")
    for table, reason in stale:
        print(f"  {table}: {reason}")

    token = os.environ.get("GITHUB_TOKEN")
    if token:
        repo = os.environ.get("GITHUB_REPO", DEFAULT_REPO)
        file_or_update_issue(stale, repo, token)
    else:
        print("GITHUB_TOKEN not set; skipping issue filing.", file=sys.stderr)

    return 1


if __name__ == "__main__":
    sys.exit(main())
