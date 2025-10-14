# Total pipeline runs

from prometheus_client import Counter
from .prometheus_registry import get_registry

REG = get_registry()

PIPELINE_RUNS = Counter(
    "data_ngin_pipeline_runs_total",
    "Total number of data-ngin pipeline runs",
    ["pipeline"],  
    registry=REG,
)


# Exectution time

from prometheus_client import Histogram

EXECUTION_TIME = Histogram(
    "data_ngin_stage_execution_seconds",
    "Execution time of each pipeline stage and total pipeline",
    ["stage"],  # stage = loader|fetcher|cleaner|inserter|total
    registry=REG,
)


# Errors per stage

from prometheus_client import Counter
from .prometheus_registry import get_registry

REG = get_registry()

STAGE_ERRORS = Counter(
    "data_ngin_stage_errors_total",
    "Number of exceptions per stage",
    ["stage", "pipeline", "exc_type"],  # stage: loader|fetcher|cleaner|inserter
    registry=REG,
)


# Total records processed

from prometheus_client import Counter
from .prometheus_registry import get_registry

REG = get_registry()

RECORDS_PROCESSED = Counter(
    "data_ngin_records_processed_total",
    "Total number of records successfully processed (inserted) by the pipeline",
    ["dataset", "pipeline"],  # dataset: raw|cleaned
    registry=REG,
)


# Data completeness per asset

from prometheus_client import Gauge
from .prometheus_registry import get_registry

REG = get_registry()

DATA_COMPLETENESS = Gauge(
    "data_ngin_data_completeness_ratio",
    "Data completeness ratio per asset (actual rows / expected rows)",
    ["symbol", "pipeline"],
    registry=REG,
)


# Last successful run

from prometheus_client import Gauge
from .prometheus_registry import get_registry

REG = get_registry()

LAST_SUCCESSFUL_RUN = Gauge(
    "data_ngin_last_successful_run_timestamp",
    "Timestamp (in UNIX time) of the last successful pipeline run",
    ["pipeline"],
    registry=REG,
)

