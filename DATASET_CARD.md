---
license: cc-by-4.0
task_categories:
  - text-generation
language:
  - en
tags:
  - protein-structure
  - alphafold
  - contact-map
  - structural-biology
  - protein-language-model
size_categories:
  - 10M<n<100M
---

# AFDB ContactDoc Parquet — AlphaFold Database Structures with Cluster-Based Splits

A curated subset of ~24 million [AlphaFold Database](https://alphafold.ebi.ac.uk/) (AFDB) v4 predicted protein structures, stored as sharded Parquet files. Each row contains the raw mmCIF structure text alongside metadata, precomputed cluster IDs, and leakage-resistant train/val/test split assignments.

This dataset is designed as a reusable intermediate for generating LLM training corpora from protein structures. Once downloaded, downstream document generation (contact maps, residue sequences, etc.) can be re-run with different parameters without re-downloading from Google Cloud Storage.

## Dataset Summary

| Property | Value |
|----------|-------|
| Source | [AlphaFold Database v4](https://alphafold.ebi.ac.uk/) (DeepMind / EMBL-EBI) |
| Total entries | ~24,009,002 |
| Shards | 12,005 (2,000 entries each) |
| Format | Apache Parquet, ZSTD compressed (level 12) |
| Estimated total size | ~1.2 TB |
| Splits | train (98%), val (1%), test (1%) |

## Selection Criteria

Entries were selected from the public BigQuery table `bigquery-public-data.deepmind_alphafold.metadata` with the following filters:

| Filter | Value | Description |
|--------|-------|-------------|
| `latestVersion` | `= 4` | Only AFDB v4 structures |
| `skip_fragments` | `true` | `uniprotStart = 1` AND `uniprotEnd = LENGTH(uniprotSequence)` — only full-length UniProt models, no fragments |
| `globalMetricValue` | `>= 70.0` | Minimum global mean pLDDT of 70 (confident or better predictions) |
| `LENGTH(uniprotSequence)` | `<= 2048` | Maximum sequence length of 2048 residues |

The BigQuery query scans ~178 million rows from the AFDB v4 metadata table. After applying the above filters, approximately 30 million entries are returned. These are then further filtered by cluster membership (see below), yielding the final ~24 million entries.

### Cluster-Based Filtering

Only entries present in **both** of the following precomputed cluster files are included:

1. **Sequence clusters (AFDB50)** — `7-AFDB50-repId_memId.tsv.gz` from [Steinegger lab AFDB cluster page](https://afdb-cluster.steineggerlab.workers.dev/) (Version 3). Groups proteins at 50% sequence identity using Foldseek.

2. **Structural clusters** — `5-allmembers-repId-entryId-cluFlag-taxId.tsv.gz` from the same source. Only entries with `cluFlag=2` (structurally clustered) are loaded; fragments, singletons, and sequence-only entries are excluded. This file groups proteins by 3D fold similarity, which is a stricter criterion than sequence identity.

Entries missing from either cluster file are dropped entirely — there are no singleton fallbacks. This ensures every entry has proper cluster assignments for clean split computation.

## Leakage-Resistant Splits

Split assignment uses the **structural cluster representative** as the hash key, ensuring all proteins sharing a 3D fold land in the same split. This prevents data leakage where structurally similar proteins (even with low sequence identity) end up in different splits.

The algorithm:

1. Look up the structural cluster representative ID for each entry
2. Compute `h = SHA1("contactdoc-v1" + "::" + cluster_id)`
3. Interpret the first 8 bytes as a uint64: `u = uint64 / 2^64` → uniform in [0, 1)
4. Assign split:
   - `train` if `u < 0.98`
   - `val` if `0.98 <= u < 0.99`
   - `test` if `u >= 0.99`

This is fully deterministic — the same cluster ID always maps to the same split across runs.

## Schema

Each Parquet file contains a flat table with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `entry_id` | `string` | AFDB entry ID (e.g., `AF-A0A1C0V126-F1`) |
| `uniprot_accession` | `string` | UniProt accession (e.g., `A0A1C0V126`) |
| `tax_id` | `int64` | NCBI taxonomy ID |
| `organism_name` | `string` | Scientific name of the organism |
| `global_plddt` | `float32` | Global mean pLDDT confidence score (70–100) |
| `seq_len` | `int32` | Sequence length in residues |
| `seq_cluster_id` | `string` | AFDB50 sequence cluster representative entry ID |
| `struct_cluster_id` | `string` | Structural cluster representative entry ID |
| `split` | `string` | `train`, `val`, or `test` |
| `gcs_uri` | `string` | Original GCS URI (e.g., `gs://public-datasets-deepmind-alphafold-v4/AF-...`) |
| `cif_content` | `string` | Complete raw mmCIF file text |

## File Structure

```
parquet/
  shard_000000.parquet
  shard_000001.parquet
  ...
  shard_012004.parquet
```

Each shard contains up to 2,000 rows (one row per protein structure). Files are compressed with ZSTD at level 12, averaging ~100 MB per shard.

Error sidecars (`shard_NNNNNN.errors.jsonl`) are written alongside any shard that had download failures, containing the entry ID, GCS URI, and exception traceback.

## Usage

### Loading with PyArrow

```python
import pyarrow.parquet as pq

# Read a single shard
table = pq.read_table("parquet/shard_000000.parquet")
print(table.schema)
print(f"{len(table)} rows")

# Access a specific column
entry_ids = table["entry_id"].to_pylist()
cif_texts = table["cif_content"].to_pylist()
```

### Loading with Pandas

```python
import pandas as pd

df = pd.read_parquet("parquet/shard_000000.parquet")
print(df[["entry_id", "organism_name", "global_plddt", "seq_len", "split"]].head())
```

### Filtering by Split

```python
import pyarrow.parquet as pq
import pyarrow.dataset as ds

dataset = ds.dataset("parquet/", format="parquet")
train = dataset.to_table(filter=ds.field("split") == "train")
```

### Parsing Structures with Gemmi

```python
import gemmi

row = table.to_pydict()
cif_text = row["cif_content"][0]
doc = gemmi.cif.read_string(cif_text)
structure = gemmi.make_structure_from_block(doc.sole_block())
model = structure[0]
chain = model[0]
print(f"{len(chain)} residues")
```

## Intended Use

This dataset is an intermediate artifact for generating protein structure training data for language models. The primary downstream task is producing "ContactDoc" documents that encode residue sequences and 3D contact maps as structured text:

```
<begin_sequence>
<MET> <LYS> <PHE> <CYS> <ASP> <TYR> <GLY> <LEU>
<begin_contacts>
<p1> <p8> <SD> <CD1>
<p1> <p7> <CG> <CA>
<p2> <p8> <NZ> <O>
<p1> <p6> <CE> <OH>
<end_contacts>
<end>
```

The Parquet format allows re-generating documents with different parameters (distance cutoffs, contact limits, pLDDT thresholds) without re-downloading ~24 million CIF files from GCS.

## Data Source and License

- **AlphaFold Database** structures are provided by DeepMind and EMBL-EBI under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- **Cluster files** are from the [Steinegger lab](https://afdb-cluster.steineggerlab.workers.dev/), based on Foldseek clustering of AFDB v4 (Version 3 clusters).

### Citation

If you use this dataset, please cite the AlphaFold Database:

```bibtex
@article{varadi2022alphafold,
  title={AlphaFold Protein Structure Database: massively expanding the structural coverage of protein-sequence space with high-accuracy models},
  author={Varadi, Mihaly and Anyango, Stephen and Deshpande, Mandar and others},
  journal={Nucleic Acids Research},
  volume={50},
  number={D1},
  pages={D439--D444},
  year={2022},
  doi={10.1093/nar/gkab1061}
}
```

And the AFDB cluster resource:

```bibtex
@article{barrio2024clustering,
  title={Clustering predicted structures at the scale of the known protein universe},
  author={Barrio-Hernandez, Inigo and Yeo, Jimin and Jänes, Jürgen and others},
  journal={Nature},
  volume={622},
  pages={637--645},
  year={2023},
  doi={10.1038/s41586-023-06510-w}
}
```

## Generation Pipeline

This dataset was generated using the [ContactDoc](https://github.com/TODO) pipeline. The full reproduction steps are:

1. Download cluster files from the Steinegger lab
2. Run `bq_make_manifest.py` to query BigQuery and produce sharded JSONL manifests
3. Run `run_local.py --stage download` to download CIFs from GCS into Parquet shards

See the repository README for complete instructions.
