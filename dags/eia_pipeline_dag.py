from datetime import datetime, timedelta
import logging
import asyncio
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.orchestrator import Orchestrator
from utils.dynamic_loader import load_config

# Define ET timezone
local_tz = pendulum.timezone("America/New_York")

# Config path relative to Airflow's container mount
EIA_CONFIG_PATH = "/opt/airflow/data_engine/src/config/eia_config.yaml"

default_args = {
    "owner": "AlgoGators",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "eia_data_pipeline",
    default_args=default_args,
    description="EIA multi-dimensional energy data ingestion",
    schedule_interval="30 10 * * 3",  # Wed at 10:30 AM ET
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["energy", "eia", "quant"],
    max_active_runs=1,
)

def run_eia_pipeline(**kwargs):
    """Executes the EIA-specific orchestrator run."""
    try:
        # Load the specific EIA configuration
        config = load_config(EIA_CONFIG_PATH)
        orchestrator = Orchestrator(config=config)
        
        # Run the async orchestrator
        asyncio.run(orchestrator.run())
        logging.info("EIA Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"EIA Pipeline failed: {e}")
        raise

run_task = PythonOperator(
    task_id="fetch_eia_data",
    python_callable=run_eia_pipeline,
    provide_context=True,
    dag=dag,
)

run_task