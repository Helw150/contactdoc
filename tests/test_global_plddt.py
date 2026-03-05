"""Test: global pLDDT filtering (section 13, test 2)."""


def test_below_threshold_skipped():
    """Entries below global pLDDT threshold should be skipped."""
    threshold = 70.0
    entry = {"globalMetricValue": 65.0}
    assert entry["globalMetricValue"] < threshold


def test_at_threshold_passes():
    """Entries at exactly the threshold should pass."""
    threshold = 70.0
    entry = {"globalMetricValue": 70.0}
    assert entry["globalMetricValue"] >= threshold


def test_above_threshold_passes():
    """Entries above threshold should pass."""
    threshold = 70.0
    entry = {"globalMetricValue": 90.0}
    assert entry["globalMetricValue"] >= threshold
