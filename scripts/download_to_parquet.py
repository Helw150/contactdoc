#!/usr/bin/env python3
"""CLI: download CIFs for one manifest shard and write a Parquet file.

Each row contains the raw mmCIF text plus metadata and split assignment.
This creates a local cache of AFDB structures that can be reused for
multiple downstream analyses without re-downloading from GCS.
"""

import json
import traceback
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.parquet as pq

from contactdoc.manifest import read_manifest_shard


_gcs_client = None


def _get_gcs_client():
    global _gcs_client
    if _gcs_client is None:
        from google.cloud import storage
        _gcs_client = storage.Client()
    return _gcs_client


def _download_from_gcs(gcs_uri: str) -> str:
    assert gcs_uri.startswith("gs://")
    parts = gcs_uri[5:].split("/", 1)
    client = _get_gcs_client()
    bucket = client.bucket(parts[0])
    blob = bucket.blob(parts[1])
    return blob.download_as_text()


SCHEMA = pa.schema([
    ("entry_id", pa.string()),
    ("uniprot_accession", pa.string()),
    ("tax_id", pa.int64()),
    ("organism_name", pa.string()),
    ("global_plddt", pa.float32()),
    ("seq_len", pa.int32()),
    ("seq_cluster_id", pa.string()),
    ("struct_cluster_id", pa.string()),
    ("split", pa.string()),
    ("gcs_uri", pa.string()),
    ("cif_content", pa.string()),
])


@click.command()
@click.option("--manifest-shard", required=True, help="Path to manifest shard JSONL")
@click.option("--shard-index", required=True, type=int, help="Output shard index")
@click.option("--output-dir", required=True, help="Output directory for Parquet files")
def main(manifest_shard: str, shard_index: int, output_dir: str):
    entries = read_manifest_shard(manifest_shard)
    click.echo(f"Downloading shard {shard_index} with {len(entries)} entries")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    rows = {col: [] for col in SCHEMA.names}
    errors = []
    success_count = 0

    for entry in entries:
        entry_id = entry["entryId"]
        try:
            cif_content = _download_from_gcs(entry["gcs_uri"])
            rows["entry_id"].append(entry_id)
            rows["uniprot_accession"].append(entry.get("uniprotAccession", ""))
            rows["tax_id"].append(entry.get("taxId", 0))
            rows["organism_name"].append(entry.get("organismScientificName", ""))
            rows["global_plddt"].append(entry.get("globalMetricValue", 0.0))
            rows["seq_len"].append(entry.get("seq_len", 0))
            rows["seq_cluster_id"].append(entry.get("seq_cluster_id", ""))
            rows["struct_cluster_id"].append(entry.get("struct_cluster_id", ""))
            rows["split"].append(entry.get("split", ""))
            rows["gcs_uri"].append(entry.get("gcs_uri", ""))
            rows["cif_content"].append(cif_content)
            success_count += 1
        except Exception:
            errors.append({
                "entryId": entry_id,
                "gcs_uri": entry.get("gcs_uri", ""),
                "exception": traceback.format_exc(),
            })

    # Write Parquet shard
    if rows["entry_id"]:
        table = pa.table(rows, schema=SCHEMA)
        parquet_path = output_path / f"shard_{shard_index:06d}.parquet"
        pq.write_table(table, parquet_path, compression="zstd")

    # Write errors sidecar
    if errors:
        error_path = output_path / f"shard_{shard_index:06d}.errors.jsonl"
        with open(error_path, "w") as f:
            for err in errors:
                f.write(json.dumps(err) + "\n")

    click.echo(f"Done: {success_count} downloaded, {len(errors)} errors")


if __name__ == "__main__":
    main()
