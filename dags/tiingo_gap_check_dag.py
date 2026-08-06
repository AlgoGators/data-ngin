from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging, pendulum

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

CONFIG_PATH = "/opt/airflow/data_engine/src/config/config_tiingo.yaml"

# Only look at recent history. Older holes are already cached in
# verified_absent_bars, and a full-history scan is pointless weekly work.
LOOKBACK_DAYS = 120


def check_for_gaps(**kwargs):
    """Fail the task if any unexplained hole exists in the recent window.

    Failing loudly is the point: a symbol previously went missing for 24
    consecutive days without anyone noticing, because the orchestrator logs
    per-symbol failures and moves on.

    Known limitation -- a TOTAL outage is invisible to this check. A day only
    counts as a trading day if enough symbols have a bar on it, so if a pipeline
    run fails for EVERY symbol, that day has zero bars, is not recognised as a
    trading day, and produces no candidate holes for anyone. This check finds
    per-symbol gaps, not missing days. Detecting a whole missing session needs an
    authoritative market calendar to compare against, which is deliberately out of
    scope here.
    """
    from datetime import date, timedelta as td
    from utils.dynamic_loader import load_config
    from src.modules.data_quality import DataQuality

    config = load_config(CONFIG_PATH)
    schema = config["database"]["target_schema"]
    table = config["database"]["table"]
    since = (date.today() - td(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    holes = DataQuality(config=config).find_missing_bars(schema, table, since=since)
    if not holes:
        logging.info("Gap check clean: no missing bars since %s", since)
        return

    for symbol, day in holes[:50]:
        logging.error("MISSING BAR %s %s", symbol, day)
    raise ValueError(
        f"{len(holes)} missing bar(s) in {schema}.{table} since {since}. "
        "Run from the repo root: PYTHONPATH=. python3 scripts/repair_missing_bars.py"
    )


with DAG(
    "tiingo_gap_check_dag",
    default_args=default_args,
    description="Weekly check for permanently-missing equity bars",
    schedule_interval="0 9 * * 6",   # Saturdays 9:00 AM ET, after the week's runs
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    tags=["tiingo", "equity", "data_quality"],
    max_active_runs=1,
) as dag:

    check_for_gaps_task = PythonOperator(
        task_id="check_for_gaps",
        python_callable=check_for_gaps,
    )
