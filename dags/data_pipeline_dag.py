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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CONFIG_PATH = "/opt/airflow/data_engine/src/config/config.yaml"

def run_pipeline(**kwargs):
    try:
        dag_run = kwargs.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        run_type = conf.get("run_type", "scheduled")
        logging.info(f"Running pipeline, type={run_type}")

        # Build orchestrator at task runtime (not parse time)
        config = load_config(CONFIG_PATH)
        orchestrator = Orchestrator(config=config)

        asyncio.run(orchestrator.run())
        logging.info("Pipeline execution completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        raise

with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="Daily data pipeline for market data ingestion",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=["data_pipeline"],
    max_active_runs=1,
) as dag:

    run_pipeline_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
