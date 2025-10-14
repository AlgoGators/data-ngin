from dashboard.metrics.prometheus_registry import get_registry
from dashboard.metrics.metrics_definitions import RECORDS_PROCESSED

def test_records_processed_counter_increments_by_n():
    reg = get_registry()
    before = reg.get_sample_value(
        "data_ngin_records_processed_total", {"dataset": "cleaned", "pipeline": "default"}
    ) or 0.0

    RECORDS_PROCESSED.labels(dataset="cleaned", pipeline="default").inc(42)

    after = reg.get_sample_value(
        "data_ngin_records_processed_total", {"dataset": "cleaned", "pipeline": "default"}
    ) or 0.0

    assert after == before + 42.0
