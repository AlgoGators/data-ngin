from dashboard.metrics.prometheus_registry import get_registry
from dashboard.metrics.metrics_definitions import PIPELINE_RUNS

def test_pipeline_runs_counter_increments_once():
    reg = get_registry()

    before = reg.get_sample_value(
        "data_ngin_pipeline_runs_total", {"pipeline": "default"}
    ) or 0.0

    # simulate one run
    PIPELINE_RUNS.labels(pipeline="default").inc()

    after = reg.get_sample_value(
        "data_ngin_pipeline_runs_total", {"pipeline": "default"}
    ) or 0.0

    assert after == before + 1.0
    