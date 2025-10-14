import time
from dashboard.metrics.prometheus_registry import get_registry
from dashboard.metrics.metrics_definitions import LAST_SUCCESSFUL_RUN

def test_last_successful_run_sets_timestamp():
    reg = get_registry()
    now = time.time()

    LAST_SUCCESSFUL_RUN.labels(pipeline="default").set(now)

    value = reg.get_sample_value(
        "data_ngin_last_successful_run_timestamp",
        {"pipeline": "default"}
    )

    assert abs(value - now) < 1
