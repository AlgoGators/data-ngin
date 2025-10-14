from dashboard.metrics.prometheus_registry import get_registry
from dashboard.metrics.metrics_definitions import STAGE_ERRORS

def test_stage_errors_counter_increments():
    reg = get_registry()
    before = reg.get_sample_value(
        "data_ngin_stage_errors_total",
        {"stage": "fetcher", "pipeline": "default", "exc_type": "ValueError"},
    ) or 0.0

    STAGE_ERRORS.labels(stage="fetcher", pipeline="default", exc_type="ValueError").inc()

    after = reg.get_sample_value(
        "data_ngin_stage_errors_total",
        {"stage": "fetcher", "pipeline": "default", "exc_type": "ValueError"},
    ) or 0.0

    assert after == before + 1.0
