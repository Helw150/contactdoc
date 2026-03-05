"""Manifest building: enrich entries with cluster_id + split, write shards."""

import json
from pathlib import Path

from .clusters import get_cluster_id
from .config import PipelineConfig
from .splits import assign_split


def enrich_entries(
    entries: list[dict],
    cluster_map: dict[str, str] | None,
    cfg: PipelineConfig,
) -> list[dict]:
    """Attach split_cluster_id, split, and gcs_uri to each entry."""
    enriched = []
    for entry in entries:
        entry_id = entry["entryId"]
        cluster_id = get_cluster_id(entry_id, cluster_map)
        split = assign_split(
            cfg.splits.seed,
            cluster_id,
            cfg.splits.train_frac,
            cfg.splits.val_frac,
        )
        gcs_uri = f"{cfg.gcs_bucket_prefix}{entry_id}-model_v{cfg.afdb_version}.cif"

        enriched.append({
            **entry,
            "split_cluster_id": cluster_id,
            "split": split,
            "gcs_uri": gcs_uri,
        })
    return enriched


def write_manifest_shards(
    entries: list[dict],
    output_dir: str | Path,
    shard_size: int,
) -> list[str]:
    """Write manifest entries as sharded JSONL files. Returns paths written."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for shard_idx in range(0, len(entries), shard_size):
        shard_entries = entries[shard_idx:shard_idx + shard_size]
        shard_num = shard_idx // shard_size
        path = output_dir / f"manifest_shard_{shard_num:06d}.jsonl"
        with open(path, "w") as f:
            for entry in shard_entries:
                # Drop uniprotSequence from manifest to save space
                row = {k: v for k, v in entry.items() if k != "uniprotSequence"}
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
        paths.append(str(path))

    return paths


def read_manifest_shard(path: str | Path) -> list[dict]:
    """Read a manifest shard JSONL file."""
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries
