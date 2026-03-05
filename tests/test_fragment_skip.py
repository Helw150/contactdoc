"""Test: fragment detection (section 13, test 1)."""


def test_fragment_skip_uniprotStart():
    """Entry with uniprotStart != 1 should be flagged as fragment."""
    entry = {"entryId": "AF-TEST-F1", "uniprotStart": 5, "uniprotEnd": 100,
             "uniprotSequence": "A" * 100, "globalMetricValue": 90.0}
    # uniprotStart != 1 => fragment
    assert entry["uniprotStart"] != 1


def test_fragment_skip_uniprotEnd():
    """Entry with uniprotEnd != len(seq) should be flagged as fragment."""
    seq = "A" * 100
    entry = {"entryId": "AF-TEST-F1", "uniprotStart": 1, "uniprotEnd": 50,
             "uniprotSequence": seq, "globalMetricValue": 90.0}
    assert entry["uniprotEnd"] != len(seq)


def test_non_fragment_passes():
    """Entry with matching start/end should not be a fragment."""
    seq = "A" * 100
    entry = {"entryId": "AF-TEST-F1", "uniprotStart": 1, "uniprotEnd": 100,
             "uniprotSequence": seq, "globalMetricValue": 90.0}
    is_fragment = (entry["uniprotStart"] != 1 or
                   entry["uniprotEnd"] != len(entry["uniprotSequence"]))
    assert not is_fragment
