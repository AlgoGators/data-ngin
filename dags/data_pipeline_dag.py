from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.orchestrator import Orchestrator
from utils.dynamic_loader import load_config
import logging
import pendulum

# Define ET timezone using pendulum
local_tz = pendulum.timezone("America/New_York")

# Define default arguments for the DAG
default_args: dict[str, any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Load configuration from config.yaml
CONFIG_PATH: str = "/opt/airflow/data_engine/src/config/config.yaml"
config: dict[str, any] = load_config(CONFIG_PATH)

# Initialize orchestrator
orchestrator: Orchestrator = Orchestrator(config=config)

# Define Airflow DAG
dag: DAG = DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description="Daily data pipeline for market data ingestion",
    schedule_interval="0 7 * * *", # Run daily at 7:00 AM
    start_date=datetime(2024, 12, 1, tzinfo=local_tz),
    catchup=False,
    tags=["data_pipeline"],
    max_active_runs=1,
)

def run_pipeline(**kwargs) -> None:
    """
    Task to run the entire data pipeline using the orchestrator.
    """
    try:
        run_type = kwargs.get('dag_run', {}).get('conf', {}).get('run_type', 'scheduled')
        if run_type == 'manual':
            logging.info("Running the pipeline in manual mode.")
        elif run_type == "scheduled":
            logging.info("Running the pipeline in scheduled mode.")
        else:
            logging.info(f"Unknown runtype {run_type}.")
        import asyncio
        asyncio.run(orchestrator.run())
        logging.info("Pipeline execution completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        raise

# Define tasks in the DAG
run_pipeline_task: PythonOperator = PythonOperator(
    task_id="run_pipeline",
    python_callable=run_pipeline,
    provide_context = True,
    dag=dag,
)

# Set the task as the only task in the DAG
run_pipeline_task
