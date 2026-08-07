from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging, pendulum

from src.modules.notifications.github_issue_notifier import notify_dag_failure

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    # Files/updates a GitHub issue on failure -- see github_issue_notifier for why
    # (email_on_failure above has no SMTP configured in this deployment).
    "on_failure_callback": notify_dag_failure,
}


def check_universe_drift(**kwargs):
    """Fail if the committed S&P snapshots have drifted from the live index.

    The universe is built from COMMITTED snapshots, which is what makes it
    reproducible. The cost is that they go stale, and stale snapshots are exactly
    how survivorship bias creeps back after it has been fixed:

      * a company joins the index -> never fetched at all. MRVL, FLEX, HONA and
        FERG sat in the index unfetched because the snapshot lagged.
      * a company leaves -> unless the removal is recorded it never becomes a
        former member either; it just disappears from the universe.
      * a company delists -> still marked active, so the daily run spends a
        request on a security that returns nothing. That is the EA case.

    This deliberately only DETECTS. Rewriting committed snapshots from a scheduled
    run would make the universe depend on when a DAG last fired rather than on
    what is in git, and would quietly re-point the daily pipeline with no review.
    The failure message carries the exact commands to resolve it.

    Monthly is the right cadence: the S&P sees roughly 20-25 changes a year, and
    the daily gap check already covers per-symbol data loss between runs.
    """
    import io
    from contextlib import redirect_stdout
    from scripts.check_sp500_drift import (
        fetch_live_constituents, fetch_live_status, index_drift,
        delisted_but_active, tiingo_ticker, _read,
        SP500_SNAPSHOT, CONTRACT,
    )

    committed = {tiingo_ticker(r["Symbol"]) for r in _read(SP500_SNAPSHOT)
                 if (r.get("Symbol") or "").strip()}
    live = fetch_live_constituents()
    last_day, as_of = fetch_live_status()

    joined, left = index_drift(committed, live)
    stale_active = delisted_but_active(_read(CONTRACT), last_day, as_of)

    logging.info("committed=%d live=%d manifest_as_of=%s", len(committed), len(live), as_of)
    if not (joined or left or stale_active):
        logging.info("No drift: committed snapshots match the live index.")
        return

    for label, items in (("JOINED the index, not in the snapshot (NOT being fetched)", joined),
                         ("LEFT the index, still in the snapshot", left),
                         ("marked active but the vendor stopped updating", stale_active)):
        if items:
            logging.error("%s (%d): %s", label, len(items), items)

    raise ValueError(
        f"S&P universe drift: {len(joined)} joined, {len(left)} left, "
        f"{len(stale_active)} stale-active. Resolve with:\n"
        "  curl -sS https://raw.githubusercontent.com/datasets/s-and-p-500-companies"
        "/main/data/constituents.csv -o scripts/sp500_constituents.csv\n"
        "  PYTHONPATH=. python3 scripts/build_sp500_history.py\n"
        "  PYTHONPATH=. python3 scripts/validate_sp500_entities.py\n"
        "  PYTHONPATH=. python3 scripts/build_tiingo_universe.py\n"
        "  # then backfill any newly-former members and commit the regenerated files"
    )


with DAG(
    "tiingo_universe_drift_dag",
    default_args=default_args,
    description="Monthly check that the committed S&P snapshots match the live index",
    schedule_interval="0 10 1 * *",   # 1st of the month, 10:00 AM ET
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    tags=["tiingo", "equity", "data_quality"],
    max_active_runs=1,
) as dag:

    check_universe_drift_task = PythonOperator(
        task_id="check_universe_drift",
        python_callable=check_universe_drift,
    )
