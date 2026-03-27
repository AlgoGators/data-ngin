from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.dynamic_loader import load_config
from src.orchestrator import Orchestrator
import logging, asyncio, pendulum

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

CONFIG_PATH = "/opt/airflow/data_engine/src/config/config_eodhd.yaml"


def run_eodhd_pipeline(**kwargs):
    try:
        dag_run = kwargs.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        run_type = conf.get("run_type", "scheduled")
        logging.info(f"Running EODHD pipeline, type={run_type}")

        config = load_config(CONFIG_PATH)
        orchestrator = Orchestrator(config=config)

        asyncio.run(orchestrator.run())
        logging.info("EODHD pipeline execution completed successfully.")
    except Exception as e:
        logging.error(f"EODHD pipeline execution failed: {e}")
        raise


with DAG(
    "eodhd_data_dag",
    default_args=default_args,
    description="Daily EODHD equity data ingestion",
    schedule_interval="0 7 * * 1-5",  # Weekdays at 7 AM ET (markets closed weekends)
    start_date=datetime(2024, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=["eodhd", "equity", "data_pipeline"],
    max_active_runs=1,
) as dag:

    fetch_eodhd_data = PythonOperator(
        task_id="fetch_eodhd_data",
        python_callable=run_eodhd_pipeline,
    )
