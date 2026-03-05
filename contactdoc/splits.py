"""Deterministic cluster -> split assignment via SHA1 hashing."""

import hashlib
import struct


def assign_split(
    seed: str,
    cluster_id: str,
    train_frac: float,
    val_frac: float,
) -> str:
    """Assign a split (train/val/test) deterministically based on cluster_id.

    h = sha1(seed + "::" + cluster_id)
    u = first 8 bytes as uint64 / 2^64 in [0, 1)
    """
    h = hashlib.sha1((seed + "::" + cluster_id).encode()).digest()
    u = struct.unpack(">Q", h[:8])[0] / (2**64)
    if u < train_frac:
        return "train"
    elif u < train_frac + val_frac:
        return "val"
    else:
        return "test"
