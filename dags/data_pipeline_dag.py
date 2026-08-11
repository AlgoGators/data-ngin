from datetime import datetime, timedelta
import logging
import asyncio
import os

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.utils.dynamic_loader import load_config
from src.application.orchestrator import Orchestrator
from src.domain.services import StalenessChecker
from src.infrastructure.repository.ohlcv_repository import OhlcvRepository

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Was a hardcoded absolute container path; now resolved from an env var (set
# in the Airflow deployment's container/compose config) with the same
# repo-relative default used for local runs, matching the loader's
# contracts CSV path pattern in config.yaml.
CONFIG_PATH = os.getenv("DATA_NGIN_CONFIG_PATH", "src/config/config.yaml")


def _log_run_type() -> None:
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    run_type = conf.get("run_type", "scheduled")
    logging.info(f"Running pipeline, type={run_type}")


@task(multiple_outputs=True)
def resolve_date_range() -> dict:
    """
    Stage 1a: resolve the date range to process. Split from
    resolve_symbols() (rather than one task returning both) because Airflow's
    dynamic task mapping (.expand(), used below) can only map over a task's
    raw return value, not a subscript/key of it -- so the symbol list has to
    come back from its own task with nothing else attached.

    Calls Orchestrator.get_date_range() specifically (not the combined
    get_symbols_and_date_range()) so this task's DB round-trip (via
    determine_date_range(), when time_range.start_date isn't set in config)
    doesn't also reload the symbols CSV -- that's resolve_symbols()'s job,
    running independently below.
    """
    _log_run_type()
    config = load_config(CONFIG_PATH)
    orchestrator = Orchestrator(config=config)
    return orchestrator.get_date_range()


@task
def resolve_symbols() -> list:
    """
    Stage 1b: resolve the list of symbols to process. See resolve_date_range()
    for why this is a separate task -- calls Orchestrator.get_symbols()
    specifically so this task never needs the DB reachable, only the CSV
    symbol source.
    """
    config = load_config(CONFIG_PATH)
    orchestrator = Orchestrator(config=config)
    return orchestrator.get_symbols()


@task(trigger_rule="all_done")
def check_staleness() -> None:
    """
    Stage 3: warns (does not fail the DAG -- `trigger_rule="all_done"` runs
    this regardless of upstream failures) if the latest date in the DB is
    older than the configured threshold. Rebuilt replacement for the deleted
    src/modules/data_staleness.py -- see StalenessChecker's docstring for why
    that file was removed with nothing wired in its place.
    """
    config = load_config(CONFIG_PATH)
    repository = OhlcvRepository(config=config)
    repository.connect()
    try:
        latest_date = repository.get_latest_date()
    finally:
        repository.close()

    threshold_days = int(os.getenv("DATA_NGIN_STALENESS_THRESHOLD_DAYS", "1"))
    checker = StalenessChecker(max_staleness=timedelta(days=threshold_days))
    report = checker.check_staleness(latest_date)

    if report.is_stale:
        logging.warning(report.message)
    else:
        logging.info(report.message)


@task
def process_symbol(symbol: dict, start_date: str, end_date: str) -> bool:
    """
    Stage 2: fetch, clean, and insert data for a single symbol. Dynamically
    mapped over every symbol resolve_symbols() returns, so each symbol is its
    own Airflow task instance -- a failure on one symbol is visible and
    retryable in the UI without hiding the other symbols' results, unlike the
    previous single opaque `run_pipeline` task.

    (Per-symbol mapping rather than per-pipeline-stage tasks: the
    fetch/clean/insert steps within a symbol pass a pandas DataFrame between
    them, which is a poor fit for Airflow XCom. Per-symbol is the natural
    task boundary here -- it's also exactly what
    Orchestrator.retrieve_and_process_data already represents.)
    """
    config = load_config(CONFIG_PATH)
    orchestrator = Orchestrator(config=config)
    return asyncio.run(orchestrator.retrieve_and_process_data(symbol, start_date, end_date))


@dag(
    dag_id="data_pipeline_dag",
    default_args=default_args,
    description="Daily data pipeline for market data ingestion",
    schedule="0 7 * * *",
    start_date=datetime(2024, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=["data_pipeline"],
    max_active_runs=1,
)
def data_pipeline_dag():
    date_range = resolve_date_range()
    symbols = resolve_symbols()
    processed = process_symbol.partial(
        start_date=date_range["start_date"],
        end_date=date_range["end_date"],
    ).expand(symbol=symbols)
    processed >> check_staleness()


data_pipeline_dag()
