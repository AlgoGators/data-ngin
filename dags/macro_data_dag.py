from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pendulum

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def run_macro_pipeline(**kwargs):
    try:
        from src.modules.macro.fred_pipeline import run_pipeline

        logging.info("Starting FRED macro data pipeline.")
        stats = run_pipeline()
        logging.info("FRED macro pipeline complete. Stats: %s", stats)
    except Exception as e:
        logging.error("FRED macro pipeline failed: %s", e)
        raise


with DAG(
    "macro_data_dag",
    default_args=default_args,
    description="Weekly FRED macro data ingestion for DFM",
    schedule_interval="0 8 * * 0",  # Sundays at 8 AM ET
    start_date=datetime(2024, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=["macro", "fred", "data_pipeline"],
    max_active_runs=1,
) as dag:

    fetch_macro_data = PythonOperator(
        task_id="fetch_fred_macro_data",
        python_callable=run_macro_pipeline,
    )
