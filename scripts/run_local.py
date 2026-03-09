#!/usr/bin/env python3
"""CLI: orchestrate full pipeline locally with multiprocessing."""

import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import click

from contactdoc.config import config_sha, load_config
from contactdoc.manifest import read_manifest_shard


def _process_shard(args: tuple) -> str:
    """Worker function: run process_manifest_shard for one shard."""
    config_path, shard_path, shard_index, output_dir, use_gcs = args
    script = str(Path(__file__).parent / "process_manifest_shard.py")
    cmd = [
        sys.executable, script,
        "--config", config_path,
        "--manifest-shard", shard_path,
        "--shard-index", str(shard_index),
        "--output-dir", output_dir,
    ]
    if use_gcs:
        cmd.append("--use-gcs")
    else:
        cmd.append("--local")

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return f"FAIL shard {shard_index}: {result.stderr}"
    return f"OK shard {shard_index}: {result.stdout.strip()}"


@click.command()
@click.option("--config", "config_path", required=True, help="Path to YAML config")
@click.option("--manifest-dir", required=True, help="Directory with manifest shard JSONL files")
@click.option("--output-dir", default=None, help="Override output directory")
@click.option("--use-gcs/--local", default=False, help="Download from GCS or use local paths")
@click.option("--workers", default=None, type=int, help="Number of workers (default from config)")
@click.option("--retry-from", default=None, type=int, help="Only process shards with index >= this value")
@click.option("--retry-list", default=None, type=str, help="File with shard indices to retry (one per line)")
def main(config_path: str, manifest_dir: str, output_dir: str | None, use_gcs: bool, workers: int | None,
         retry_from: int | None, retry_list: str | None):
    cfg = load_config(config_path)
    sha = config_sha(cfg)

    if output_dir is None:
        output_dir = f"{cfg.output_prefix}config_sha={sha}"
    if workers is None:
        workers = cfg.parallelism.num_workers_local

    # Find manifest shards
    manifest_paths = sorted(Path(manifest_dir).glob("manifest_shard_*.jsonl"))
    if not manifest_paths:
        click.echo("No manifest shards found!")
        return

    # Build (index, path) pairs for all shards
    all_shards = list(enumerate(manifest_paths))

    # Filter to retry subset if requested
    if retry_list is not None:
        retry_indices = set()
        with open(retry_list) as f:
            for line in f:
                line = line.strip()
                if line:
                    retry_indices.add(int(line))
        all_shards = [(idx, p) for idx, p in all_shards if idx in retry_indices]
        click.echo(f"Retrying {len(all_shards)} shards from {retry_list}, using {workers} workers")
    elif retry_from is not None:
        all_shards = [(idx, p) for idx, p in all_shards if idx >= retry_from]
        click.echo(f"Retrying shards {retry_from}+ ({len(all_shards)} shards), using {workers} workers")
    else:
        click.echo(f"Found {len(manifest_paths)} manifest shards, using {workers} workers")

    tasks = [
        (config_path, str(p), idx, output_dir, use_gcs)
        for idx, p in all_shards
    ]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_shard, t): t for t in tasks}
        for future in as_completed(futures):
            click.echo(future.result())

    click.echo("Pipeline complete.")


if __name__ == "__main__":
    main()
