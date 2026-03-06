#!/usr/bin/env python3
"""Convert sharded txt.gz + metadata.jsonl.gz output into a HuggingFace datasets Arrow format.

Produces one Arrow dataset per split with columns:
  - input_ids: list[int]    (tokenized document)
  - entry_id: string
  - seq_len: int
  - contacts_emitted: int
  - global_plddt: float
"""

import gzip
import json
from pathlib import Path

import click
import datasets

from contactdoc.tokenizer import encode, VOCAB_SIZE


def _split_documents(text: str) -> list[str]:
    """Split concatenated shard text into individual documents."""
    docs = []
    current = []
    for line in text.split("\n"):
        current.append(line)
        if line.strip() == "<end>":
            docs.append("\n".join(current) + "\n")
            current = []
    return docs


def _load_shard(txt_gz_path: Path, meta_gz_path: Path) -> list[dict]:
    """Load one shard's documents and metadata, tokenize, return rows."""
    with gzip.open(txt_gz_path, "rt") as f:
        text = f.read()
    docs = _split_documents(text)

    meta_records = []
    with gzip.open(meta_gz_path, "rt") as f:
        for line in f:
            line = line.strip()
            if line:
                meta_records.append(json.loads(line))

    if len(docs) != len(meta_records):
        raise ValueError(
            f"Doc/metadata count mismatch in {txt_gz_path}: "
            f"{len(docs)} docs vs {len(meta_records)} metadata records"
        )

    rows = []
    for doc, meta in zip(docs, meta_records):
        ids = encode(doc)
        rows.append({
            "input_ids": ids,
            "entry_id": meta["entryId"],
            "seq_len": meta["seq_len"],
            "contacts_emitted": meta["contacts_emitted"],
            "global_plddt": meta["globalMetricValue_mean_pLDDT"],
        })
    return rows


@click.command()
@click.option("--input-dir", required=True, help="Directory with split=*/shard=*.txt.gz output")
@click.option("--output-dir", required=True, help="Directory to write HF datasets (one subdir per split)")
@click.option("--num-proc", default=1, type=int, help="Parallel workers for Arrow writing")
def main(input_dir: str, output_dir: str, num_proc: int):
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    click.echo(f"Vocab size: {VOCAB_SIZE}")

    for split_dir in sorted(input_path.glob("split=*")):
        split_name = split_dir.name.split("=")[1]
        txt_files = sorted(split_dir.glob("shard=*.txt.gz"))
        meta_files = sorted(split_dir.glob("shard=*.metadata.jsonl.gz"))

        if not txt_files:
            continue

        click.echo(f"Processing split={split_name}: {len(txt_files)} shards")

        all_rows = []
        for txt_gz, meta_gz in zip(txt_files, meta_files):
            rows = _load_shard(txt_gz, meta_gz)
            all_rows.extend(rows)

        click.echo(f"  {len(all_rows)} documents, tokenizing...")

        ds = datasets.Dataset.from_dict({
            "input_ids": [r["input_ids"] for r in all_rows],
            "entry_id": [r["entry_id"] for r in all_rows],
            "seq_len": [r["seq_len"] for r in all_rows],
            "contacts_emitted": [r["contacts_emitted"] for r in all_rows],
            "global_plddt": [r["global_plddt"] for r in all_rows],
        })

        split_output = output_path / split_name
        ds.save_to_disk(str(split_output), num_proc=num_proc)
        click.echo(f"  Saved to {split_output} ({len(ds)} rows, {ds.data.nbytes / 1e6:.1f} MB)")

    click.echo("Done.")


if __name__ == "__main__":
    main()
