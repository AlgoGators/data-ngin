from datetime import datetime, timedelta
from airflow import DAG

# Airflow 3: PythonOperator moved from airflow.operators.python (removed) to the
# standard provider, which ships bundled with the apache-airflow 3.x meta-package.
from airflow.providers.standard.operators.python import PythonOperator
import logging, pendulum

# Heavy imports (pandas, sqlalchemy, dotenv via src.orchestrator/dynamic_loader)
# are deferred into run_tiingo_pipeline() so DAG parsing stays fast and avoids the
# AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT under host memory pressure (t2.micro).

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CONFIG_PATH = "/opt/airflow/data_engine/src/config/config_tiingo.yaml"


def run_tiingo_pipeline(**kwargs):
    import asyncio
    from utils.dynamic_loader import load_config
    from src.orchestrator import Orchestrator

    try:
        dag_run = kwargs.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        run_type = conf.get("run_type", "scheduled")
        logging.info(f"Running Tiingo pipeline, type={run_type}")

        # Build orchestrator at task runtime (not parse time)
        config = load_config(CONFIG_PATH)
        orchestrator = Orchestrator(config=config)

        asyncio.run(orchestrator.run())
        logging.info("Tiingo pipeline execution completed successfully.")
    except Exception as e:
        logging.error(f"Tiingo pipeline execution failed: {e}")
        raise


with DAG(
    "tiingo_data_dag",
    default_args=default_args,
    description="Daily Tiingo equity OHLCV ingestion",
    # Airflow 3: schedule_interval was removed; use schedule.
    # Weekdays 7:15 AM ET — staggered after the Databento run (7:00) to ease
    # memory pressure on the t2.micro.
    schedule="15 7 * * 1-5",
    start_date=datetime(2024, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=["tiingo", "equity", "data_pipeline"],
    max_active_runs=1,
) as dag:
    run_tiingo_pipeline_task = PythonOperator(
        task_id="run_tiingo_pipeline",
        python_callable=run_tiingo_pipeline,
    )
