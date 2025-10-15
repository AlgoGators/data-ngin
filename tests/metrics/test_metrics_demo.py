import os
import time
import re
import requests
from dashboard.metrics.prometheus_registry import start_metrics_server
from dashboard.metrics.metrics_definitions import (
    PIPELINE_RUNS,
    EXECUTION_TIME,
    STAGE_ERRORS,
    RECORDS_PROCESSED,
    DATA_COMPLETENESS,
    LAST_SUCCESSFUL_RUN,
)

def _fetch_metrics(port: int) -> str:
    """Helper: fetch Prometheus metrics from the running test server."""
    url = f"http://localhost:{port}/metrics"
    for _ in range(10):
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return r.text
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("Failed to connect to metrics endpoint")

def test_demo_metrics_endpoint_exposes_values():
    """
    Integration-style test to confirm that the Prometheus metrics endpoint
    exposes all major data-ngin metrics with expected labels.
    """
    port = int(os.getenv("METRICS_PORT", "8013"))
    start_metrics_server(port)

    # --- Simulate one "pipeline run" and stage metrics ---
    PIPELINE_RUNS.labels(pipeline="default").inc()

    EXECUTION_TIME.labels(stage="loader").observe(0.123)
    EXECUTION_TIME.labels(stage="fetcher").observe(0.456)
    EXECUTION_TIME.labels(stage="cleaner").observe(0.078)
    EXECUTION_TIME.labels(stage="inserter").observe(0.210)
    EXECUTION_TIME.labels(stage="total").observe(0.867)

    STAGE_ERRORS.labels(stage="fetcher", pipeline="default", exc_type="TimeoutError").inc()

    RECORDS_PROCESSED.labels(dataset="raw", pipeline="default").inc(12)
    RECORDS_PROCESSED.labels(dataset="cleaned", pipeline="default").inc(10)

    DATA_COMPLETENESS.labels(symbol="ES", pipeline="default").set(0.95)

    LAST_SUCCESSFUL_RUN.labels(pipeline="default").set(time.time())

    # --- Validate /metrics output ---
    body = _fetch_metrics(port)

    # Core metrics exist
    assert 'data_ngin_pipeline_runs_total{pipeline="default"}' in body

    # Histogram: tolerate label order differences
    assert re.search(r"data_ngin_stage_execution_seconds_bucket\{[^}]*stage=\"loader\"", body)

    # Error metric present
    assert 'data_ngin_stage_errors_total{exc_type="TimeoutError",pipeline="default",stage="fetcher"}' in body

    # Records processed
    assert 'data_ngin_records_processed_total{dataset="cleaned",pipeline="default"}' in body

    # Data completeness
    assert 'data_ngin_data_completeness_ratio{pipeline="default",symbol="ES"}' in body

    # Last successful run
    assert 'data_ngin_last_successful_run_timestamp{pipeline="default"}' in body
