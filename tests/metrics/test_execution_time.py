from dashboard.metrics.prometheus_registry import get_registry
from dashboard.metrics.metrics_definitions import EXECUTION_TIME

def test_stage_duration_histogram_observes_once():
    reg = get_registry()

    # Read current values (default to 0.0 if unset)
    before_sum = reg.get_sample_value(
        "data_ngin_stage_execution_seconds_sum", {"stage": "loader"}
    ) or 0.0
    before_count = reg.get_sample_value(
        "data_ngin_stage_execution_seconds_count", {"stage": "loader"}
    ) or 0.0

    # Simulate one loader timing event (e.g., 2.5 seconds)
    EXECUTION_TIME.labels(stage="loader").observe(2.5)

    after_sum = reg.get_sample_value(
        "data_ngin_stage_execution_seconds_sum", {"stage": "loader"}
    ) or 0.0
    after_count = reg.get_sample_value(
        "data_ngin_stage_execution_seconds_count", {"stage": "loader"}
    ) or 0.0

    # Sum should increase by ~2.5 and count by 1
    assert after_count == before_count + 1.0
    assert after_sum >= before_sum + 2.5
