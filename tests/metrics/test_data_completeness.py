from dashboard.metrics.prometheus_registry import get_registry
from dashboard.metrics.metrics_definitions import DATA_COMPLETENESS

def test_data_completeness_sets_value():
    reg = get_registry()

    # Set completeness ratio for a fake symbol
    DATA_COMPLETENESS.labels(symbol="ES", pipeline="default").set(0.9)

    # Retrieve value
    value = reg.get_sample_value(
        "data_ngin_data_completeness_ratio",
        {"symbol": "ES", "pipeline": "default"}
    )

    assert value == 0.9
