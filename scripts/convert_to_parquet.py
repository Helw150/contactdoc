#!/usr/bin/env python3
"""Convert txt.gz + metadata.jsonl.gz shards into Parquet format.

Reads the document shards and metadata, combines them into Parquet files
with one row per document. Keeps the same sharding structure.
"""

import gzip
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA = pa.schema([
    ("document", pa.string()),
    ("entry_id", pa.string()),
    ("uniprot_accession", pa.string()),
    ("tax_id", pa.int64()),
    ("organism_name", pa.string()),
    ("global_plddt", pa.float32()),
    ("seq_len", pa.int32()),
    ("contacts_pre_filter", pa.int32()),
    ("contacts_emitted", pa.int32()),
    ("residues_passing_plddt", pa.int32()),
    ("split", pa.string()),
    ("seq_cluster_id", pa.string()),
    ("struct_cluster_id", pa.string()),
    ("split_cluster_id", pa.string()),
    ("sha1", pa.string()),
])


def _convert_shard(args: tuple) -> str:
    """Convert one (split, shard) pair to parquet."""
    txt_path, meta_path, output_path = args

    # Read documents
    with gzip.open(txt_path, "rt") as f:
        text = f.read()
    docs = text.split("<end_of_document>\n")
    docs = [d for d in docs if d.strip()]

    # Read metadata
    with gzip.open(meta_path, "rt") as f:
        metas = [json.loads(line) for line in f if line.strip()]

    if len(docs) != len(metas):
        return f"FAIL {txt_path.name}: {len(docs)} docs vs {len(metas)} metadata records"

    rows = {col: [] for col in SCHEMA.names}
    for doc, meta in zip(docs, metas):
        rows["document"].append(doc)
        rows["entry_id"].append(meta["entryId"])
        rows["uniprot_accession"].append(meta.get("uniprotAccession", ""))
        rows["tax_id"].append(meta.get("taxId", 0))
        rows["organism_name"].append(meta.get("organismScientificName", ""))
        rows["global_plddt"].append(meta.get("globalMetricValue_mean_pLDDT", 0.0))
        rows["seq_len"].append(meta.get("seq_len", 0))
        rows["contacts_pre_filter"].append(meta.get("contacts_found_pre_confidence_filter", 0))
        rows["contacts_emitted"].append(meta.get("contacts_emitted", 0))
        rows["residues_passing_plddt"].append(meta.get("residues_passing_plddt", 0))
        rows["split"].append(meta.get("split", ""))
        rows["seq_cluster_id"].append(meta.get("seq_cluster_id", ""))
        rows["struct_cluster_id"].append(meta.get("struct_cluster_id", ""))
        rows["split_cluster_id"].append(meta.get("split_cluster_id", ""))
        rows["sha1"].append(meta.get("sha1_of_document_text", ""))

    table = pa.table(rows, schema=SCHEMA)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path, compression="zstd")

    return f"OK {output_path}: {len(docs)} rows"


@click.command()
@click.option("--input-dir", required=True, help="Directory with split=*/shard=*.txt.gz")
@click.option("--output-dir", required=True, help="Output directory for parquet files")
@click.option("--workers", default=16, type=int, help="Number of parallel workers")
def main(input_dir: str, output_dir: str, workers: int):
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    tasks = []
    for split_dir in sorted(input_path.glob("split=*")):
        split_name = split_dir.name.split("=")[1]
        txt_files = sorted(split_dir.glob("shard=*.txt.gz"))

        for txt_file in txt_files:
            shard_name = txt_file.name.replace(".txt.gz", "")
            # shard=000000 -> shard_000000
            shard_out_name = shard_name.replace("=", "_")
            meta_file = txt_file.parent / f"{shard_name}.metadata.jsonl.gz"
            if not meta_file.exists():
                click.echo(f"Warning: no metadata for {txt_file}, skipping")
                continue

            out_file = output_path / split_name / f"{shard_out_name}.parquet"
            tasks.append((txt_file, meta_file, str(out_file)))

    click.echo(f"Converting {len(tasks)} shards using {workers} workers")

    done = 0
    errors = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_convert_shard, t): t for t in tasks}
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if result.startswith("FAIL"):
                errors += 1
                click.echo(result)
            if done % 100 == 0:
                click.echo(f"  {done}/{len(tasks)} done")

    click.echo(f"Done: {done} shards converted, {errors} errors")


if __name__ == "__main__":
    main()
