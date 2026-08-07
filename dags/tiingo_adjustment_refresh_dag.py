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
    "retry_delay": timedelta(minutes=30),
    # Files/updates a GitHub issue on failure -- see github_issue_notifier for why
    # (email_on_failure above has no SMTP configured in this deployment).
    "on_failure_callback": notify_dag_failure,
}

CONFIG_PATH = "/opt/airflow/data_engine/src/config/config_tiingo.yaml"

# One vendor request per symbol. The free tier allows 50 per key per hour, so a
# slice of 450 keeps a run inside budget even if a couple of keys are disabled.
# The script checkpoints, so successive runs continue the sweep and restart it once
# every symbol has been visited.
SYMBOLS_PER_RUN = 450


def refresh_adjustments(**kwargs):
    """Re-sync split/dividend-adjusted prices with the vendor.

    Adjusted prices are not fixed: every dividend and split retroactively rewrites
    the adjusted series for all prior bars. The daily pipeline never revisits a bar
    -- it only requests days after MAX(time), and the inserter uses ON CONFLICT DO
    NOTHING -- so adjusted_close rots from the moment a corporate action happens.

    Measured against the vendor on 2026-08-07: 29% of a 21-symbol sample had every
    2025 bar stale, and CRWD was wrong by exactly 4x after an unapplied 4-for-1
    split. A total-return backtest crossing that date saw a fabricated -75% move.
    Raw OHLCV is unaffected; only the adjustment columns rot.

    Runs weekly rather than daily: corporate actions are infrequent per symbol, and
    a full pass costs one request per symbol. It fails loudly if any symbol errors,
    so a systematic problem (expired key, vendor outage) surfaces instead of the job
    quietly re-adjusting nothing week after week.
    """
    import asyncio
    from utils.dynamic_loader import load_config
    from scripts.refresh_adjustments import refresh

    summary = asyncio.run(refresh(load_config(CONFIG_PATH), limit=SYMBOLS_PER_RUN))
    logging.info("adjustment refresh summary: %s", summary)

    if summary["failed"]:
        raise ValueError(
            f"{len(summary['failed'])} symbol(s) failed the adjustment refresh: "
            f"{summary['failed'][:20]}. They stay in the checkpoint as FAILED and are "
            f"retried next run, but a large count usually means a dead API key or a "
            f"vendor outage rather than anything symbol-specific."
        )

    logging.info("%d symbol(s) re-adjusted, %d bar(s) corrected",
                 summary["updated_symbols"], summary["updated_rows"])


with DAG(
    "tiingo_adjustment_refresh_dag",
    default_args=default_args,
    description="Weekly re-sync of split/dividend-adjusted equity prices",
    schedule_interval="0 11 * * 0",   # Sundays 11:00 AM ET -- market closed, no
                                      # contention with the 07:15 weekday ingestion
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    tags=["tiingo", "equity", "data_quality"],
    max_active_runs=1,
) as dag:

    refresh_adjustments_task = PythonOperator(
        task_id="refresh_adjustments",
        python_callable=refresh_adjustments,
    )
