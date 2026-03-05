"""Test: cluster -> split determinism (section 13, test 10)."""

from contactdoc.splits import assign_split


def test_same_cluster_same_split():
    """All members of same cluster_id must share the same split."""
    cluster_id = "AF-CLUSTER-001"
    seed = "contactdoc-v1"
    split1 = assign_split(seed, cluster_id, 0.98, 0.01)
    split2 = assign_split(seed, cluster_id, 0.98, 0.01)
    assert split1 == split2


def test_determinism_across_calls():
    """Same inputs always produce same output."""
    results = set()
    for _ in range(100):
        s = assign_split("contactdoc-v1", "my-cluster-42", 0.98, 0.01)
        results.add(s)
    assert len(results) == 1


def test_different_clusters_can_differ():
    """Different cluster_ids can map to different splits."""
    splits = set()
    # Use enough clusters that we statistically see multiple splits
    for i in range(1000):
        s = assign_split("test-seed", f"cluster-{i}", 0.50, 0.25)
        splits.add(s)
    assert len(splits) == 3  # train, val, test


def test_split_fractions_approximate():
    """Split fractions should be approximately correct over many clusters."""
    counts = {"train": 0, "val": 0, "test": 0}
    n = 10000
    for i in range(n):
        s = assign_split("frac-test", f"c-{i}", 0.98, 0.01)
        counts[s] += 1
    # Train should be ~98%
    assert counts["train"] / n > 0.95
    assert counts["train"] / n < 1.0
    # Val and test small but nonzero
    assert counts["val"] > 0
    assert counts["test"] > 0


def test_seed_changes_assignments():
    """Different seeds produce different assignments for same cluster."""
    s1 = assign_split("seed-A", "cluster-1", 0.50, 0.25)
    s2 = assign_split("seed-B", "cluster-1", 0.50, 0.25)
    # Not guaranteed to differ for one cluster, but for many they should
    diffs = 0
    for i in range(100):
        a = assign_split("seed-A", f"c-{i}", 0.50, 0.25)
        b = assign_split("seed-B", f"c-{i}", 0.50, 0.25)
        if a != b:
            diffs += 1
    assert diffs > 0
