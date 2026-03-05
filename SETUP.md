# ContactDoc — GCP Setup Guide

This pipeline uses two Google Cloud services on public datasets:
- **BigQuery** — to query AFDB metadata and select entries
- **Cloud Storage (GCS)** — to download mmCIF structure files

Both are free to access (public data), but Google requires an authenticated billing project to track usage.

---

## 1. Install the Google Cloud CLI

```bash
# On Ubuntu/Debian
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install -y google-cloud-cli

# Or via snap
sudo snap install google-cloud-cli --classic

# Or download directly
# https://cloud.google.com/sdk/docs/install
```

Verify:
```bash
gcloud version
```

---

## 2. Create a GCP project (if you don't have one)

```bash
gcloud projects create contactdoc-pipeline --name="ContactDoc Pipeline"
gcloud config set project contactdoc-pipeline
```

Or use an existing project:
```bash
gcloud config set project YOUR_PROJECT_ID
```

> **Cost note:** BigQuery gives 1 TB/month free queries. The AFDB metadata table is small (~2 GB), so a single full scan is well within free tier. GCS egress for mmCIF downloads depends on volume — expect ~1 KB per protein for the CIF file.

---

## 3. Enable required APIs

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

---

## 4. Authenticate

This sets up **Application Default Credentials (ADC)**, which the Python client libraries pick up automatically:

```bash
gcloud auth application-default login
```

This opens a browser for OAuth. After completing it, credentials are saved to `~/.config/gcloud/application_default_credentials.json`.

Verify it works:
```bash
# Test BigQuery — should print a count
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `bigquery-public-data.deepmind_alphafold.metadata` WHERE latestVersion = 4'

# Test GCS — should list CIF files
gcloud storage ls gs://public-datasets-deepmind-alphafold-v4/AF-P01308-F1-*
```

---

## 5. Install Python dependencies

```bash
uv sync --all-extras
```

---

## 6. Run the pipeline

### Quick smoke test (small query)

```bash
# Build manifest for first 100 entries
uv run python scripts/bq_make_manifest.py \
  --config config/default.yaml \
  --output-dir output/manifests

# Process one manifest shard
uv run python scripts/process_manifest_shard.py \
  --config config/default.yaml \
  --manifest-shard output/manifests/manifest_shard_000000.jsonl \
  --shard-index 0 \
  --output-dir output/results \
  --use-gcs
```

### Full local pipeline

```bash
uv run python scripts/run_local.py \
  --config config/default.yaml \
  --manifest-dir output/manifests \
  --output-dir output/results \
  --use-gcs \
  --workers 8
```

### Local mode (pre-downloaded CIFs)

If you've already downloaded CIF files locally, set `gcs_uri` in the manifest to local paths and use `--local` instead of `--use-gcs`.

---

## 7. Run tests (no GCP needed)

```bash
uv run pytest tests/ -v
```

Tests use synthetic CIF fixtures and don't require any cloud access.

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `DefaultCredentialsError` | Run `gcloud auth application-default login` |
| `403 Access Denied` on BigQuery | Enable the BigQuery API: `gcloud services enable bigquery.googleapis.com` |
| `403 Access Denied` on GCS | Enable the Storage API: `gcloud services enable storage.googleapis.com` |
| `Project not found` | Set project: `gcloud config set project YOUR_PROJECT_ID` |
| Billing not enabled | Link a billing account at https://console.cloud.google.com/billing |
