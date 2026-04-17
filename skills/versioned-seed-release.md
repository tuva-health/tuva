# Versioned Seed Release Sync

This runbook covers the recurring operator workflow for publishing Tuva seed asset releases after a human has already cut the DoltHub release tags.

It uses the existing repo tooling:
- `scripts/publish-dolthub-seeds`
- `scripts/mirror-seed-release`

Use this process any time a released seed asset needs to be refreshed in:
- S3
- GCS
- Azure Blob Storage

## Asset Map

The release tooling recognizes these 6 logical assets:

| Logical asset | Storage folder |
|---|---|
| `concept_library` | `concept-library` |
| `reference_data` | `reference-data` |
| `terminology` | `terminology` |
| `value_sets` | `value-sets` |
| `provider_data` | `provider-data` |
| `synthetic_data` | `synthetic-data` |

## Preconditions

Before starting:
- the DoltHub release tags already exist
- `aws`, `gsutil`, and `az` are installed
- AWS auth can read and write the Tuva public bucket
- `gsutil` auth can write `gs://tuva-public-resources`
- Azure auth can write to storage account `tuvapublicresources`, container `tuva-public-resources`
- you are running from the repo root: `/Users/aaronneiderhiser/code/tuva`

Recommended auth bootstrap:

```bash
aws sso login --profile tuva-dev-admin
export AWS_PROFILE=tuva-dev-admin
aws sts get-caller-identity
```

```bash
gcloud auth login aaron@tuvahealth.com
gcloud config set account aaron@tuvahealth.com
gcloud auth list
```

Recommended auth checks:

```bash
aws sts get-caller-identity
gsutil ls gs://tuva-public-resources
az storage container show \
  --account-name tuvapublicresources \
  --name tuva-public-resources \
  --auth-mode login
```

Recommended GCS write test:

```bash
echo ok >/tmp/gsutil-codex-write-test.txt
gsutil cp /tmp/gsutil-codex-write-test.txt gs://tuva-public-resources/_codex_test.txt
gsutil rm gs://tuva-public-resources/_codex_test.txt
```

If GCS write fails with `storage.objects.create` permission errors, grant the active
writer identity access before continuing. Example:

```bash
gcloud storage buckets add-iam-policy-binding gs://tuva-public-resources \
  --member="serviceAccount:codex-transfer-runner@tuva-datasets.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

Run that IAM grant as a human account with bucket admin / project admin access, not as
the service account itself.

## Standard Workflow

### 1. Dry-run the S3 publish

Use `scripts/publish-dolthub-seeds` first in dry-run mode to verify the release selection.
Always pin `--ref` to the DoltHub release tag you intend to mirror. Otherwise the script reads from DoltHub `main`.

Example for all 6 assets:

```bash
scripts/publish-dolthub-seeds --version vX.Y.Z --ref vX.Y.Z --dry-run
```

The script:
- reads the released DoltHub tables
- validates expected row counts from `asset_table_catalog`
- plans uploads to `s3://<bucket>/<database-folder>/<version>/<table>.csv.gz`

### 1a. If DoltHub CSV export is slow or failing

The public DoltHub CSV endpoints can timeout or return `503` for large tables. If that
happens, switch to local Dolt exports with `--source local --local-ref`.

Steps:
- get the release commit SHA from the DoltHub releases API
- compare it to candidate local refs
- publish from the matching local ref

Useful commands:

```bash
python - <<'PY'
import json, urllib.request
for repo in ['reference-data', 'terminology', 'value-sets', 'provider-data']:
    url = f'https://www.dolthub.com/api/v1alpha1/tuva-health/{repo}/releases'
    with urllib.request.urlopen(url, timeout=30) as r:
        releases = json.load(r)['releases']
    sha = next(rel['release_commit_sha'] for rel in releases if rel['release_tag'] == 'vX.Y.Z')
    print(repo, sha)
PY
```

```bash
dolt sql -q "select hashof('main') as h" -r csv
dolt sql -q "select hashof('tuva_health/main') as h" -r csv
```

Example local fallback publish:

```bash
scripts/publish-dolthub-seeds \
  --version vX.Y.Z \
  --source local \
  --local-ref main \
  --database reference_data \
  --stage-dir ./tmp/seed-release-vX.Y.Z
```

The local fallback uses the exact Dolt ref you specify and does not require checking out a
different branch in the local repo.

### 2. Publish to S3 and retain the manifest

Always provide `--stage-dir` for live publishes so the generated `publish-manifest.json` is retained as the audit artifact for the release.

Example for all 6 assets:

```bash
scripts/publish-dolthub-seeds \
  --version vX.Y.Z \
  --ref vX.Y.Z \
  --stage-dir ./tmp/seed-release-vX.Y.Z
```

Retain:
- `./tmp/seed-release-vX.Y.Z/publish-manifest.json`

That manifest records:
- logical database
- repo database
- source reference
- destination URI
- expected and actual row counts

### 3. Dry-run the cross-cloud mirror

After S3 publish succeeds, mirror from S3 to GCS and Azure.

```bash
scripts/mirror-seed-release \
  --version vX.Y.Z \
  --target gcs \
  --target azure \
  --stage-dir ./tmp/seed-release-vX.Y.Z-mirror \
  --dry-run
```

### 4. Mirror live to GCS and Azure

```bash
scripts/mirror-seed-release \
  --version vX.Y.Z \
  --target gcs \
  --target azure \
  --stage-dir ./tmp/seed-release-vX.Y.Z-mirror
```

Notes:
- S3 and GCS keep `.csv.gz`
- Azure uploads expanded `.csv` files for Fabric-compatible loading
- if only one target is blocked, rerun with just the other target:
  - `--target azure`
  - `--target gcs`

### 5. Verify cloud copies

Spot-check at least one representative file per updated asset in all three clouds.

Example checks:

```bash
aws s3 ls s3://tuva-public-resources/reference-data/vX.Y.Z/code_type.csv.gz
aws s3 ls s3://tuva-public-resources/terminology/vX.Y.Z/claim_type.csv.gz
aws s3 ls s3://tuva-public-resources/value-sets/vX.Y.Z/cms_hcc__adjustment_rates.csv.gz
aws s3 ls s3://tuva-public-resources/provider-data/vX.Y.Z/provider.csv.gz
```

```bash
gsutil ls gs://tuva-public-resources/reference-data/vX.Y.Z/code_type.csv.gz
gsutil ls gs://tuva-public-resources/terminology/vX.Y.Z/claim_type.csv.gz
gsutil ls gs://tuva-public-resources/value-sets/vX.Y.Z/cms_hcc__adjustment_rates.csv.gz
gsutil ls gs://tuva-public-resources/provider-data/vX.Y.Z/provider.csv.gz
```

```bash
az storage blob show \
  --account-name tuvapublicresources \
  --container-name tuva-public-resources \
  --name reference-data/vX.Y.Z/code_type.csv \
  --auth-mode login

az storage blob show \
  --account-name tuvapublicresources \
  --container-name tuva-public-resources \
  --name terminology/vX.Y.Z/claim_type.csv \
  --auth-mode login

az storage blob show \
  --account-name tuvapublicresources \
  --container-name tuva-public-resources \
  --name value-sets/vX.Y.Z/cms_hcc__adjustment_rates.csv \
  --auth-mode login

az storage blob show \
  --account-name tuvapublicresources \
  --container-name tuva-public-resources \
  --name provider-data/vX.Y.Z/provider.csv \
  --auth-mode login
```

### 6. Update project config and docs

After the cloud copies exist, update:
- `dbt_project.yml`
- `integration_tests/dbt_project.yml`
- `README.md`
- `docs/docs/dbt-variables.md`
- `integration_tests/README.md`

Use the per-asset version vars as the primary interface:
- `concept_library_version`
- `reference_data_version`
- `terminology_version`
- `value_sets_version`
- `provider_data_version`
- `synthetic_data_version`

The legacy fallback vars still work but should not be the primary interface:
- `tuva_seed_version`
- `tuva_seed_versions`

### 7. Run local validation

From the repo root:

```bash
dbt deps \
  --project-dir /Users/aaronneiderhiser/code/tuva \
  --profiles-dir ~/.dbt \
  --profile default
```

```bash
dbt parse \
  --project-dir /Users/aaronneiderhiser/code/tuva \
  --profiles-dir ~/.dbt \
  --profile default
```

```bash
scripts/dbt-local deps
```

```bash
scripts/dbt-local build \
  --select reference_data__calendar terminology__claim_type provider_data__provider cms_hcc__adjustment_rates \
  --vars '{use_synthetic_data: true, synthetic_data_size: small}'
```

```bash
cd docs && npm ci && npm run build
```

## Operational Notes

- Always export `AWS_PROFILE` before running `scripts/publish-dolthub-seeds` or
  `scripts/mirror-seed-release`. Both scripts call `aws sts get-caller-identity`.
- Keep the staged publish directory until the rollout is complete. It contains
  `publish-manifest.json` and allows safe reuse of already-generated gzip files.
- If a publish attempt used the wrong source ref or stopped mid-asset, remove the partial
  target folder from S3 before retrying.
- For very large assets, local fallback publish can be much more reliable than streaming
  DoltHub CSV exports directly.

## Current Release: v1.1.0

For the current rollout, only update these 4 assets to `v1.1.0`:
- `reference_data`
- `terminology`
- `value_sets`
- `provider_data`

Leave these unchanged:
- `concept_library` stays on `1.0.1`
- `synthetic_data` stays on `1.0.0`

### S3 publish dry-run

```bash
scripts/publish-dolthub-seeds \
  --version v1.1.0 \
  --ref v1.1.0 \
  --database reference_data \
  --database terminology \
  --database value_sets \
  --database provider_data \
  --stage-dir ./tmp/seed-release-v1.1.0 \
  --dry-run
```

### S3 publish live

```bash
scripts/publish-dolthub-seeds \
  --version v1.1.0 \
  --ref v1.1.0 \
  --database reference_data \
  --database terminology \
  --database value_sets \
  --database provider_data \
  --stage-dir ./tmp/seed-release-v1.1.0
```

### Mirror dry-run

```bash
scripts/mirror-seed-release \
  --version v1.1.0 \
  --database reference_data \
  --database terminology \
  --database value_sets \
  --database provider_data \
  --target gcs \
  --target azure \
  --stage-dir ./tmp/seed-release-v1.1.0-mirror \
  --dry-run
```

### Mirror live

```bash
scripts/mirror-seed-release \
  --version v1.1.0 \
  --database reference_data \
  --database terminology \
  --database value_sets \
  --database provider_data \
  --target gcs \
  --target azure \
  --stage-dir ./tmp/seed-release-v1.1.0-mirror
```

### Local fallback examples used in this rollout

These were the exact local refs that matched the released `v1.1.0` content during this
rollout:

- `reference_data`: `main`
- `terminology`: `main`
- `value_sets`: `tuva_health/main`
- `provider_data`: `merge-test-from-main`

Example fallback publishes:

```bash
scripts/publish-dolthub-seeds \
  --version v1.1.0 \
  --source local \
  --local-ref main \
  --database reference_data \
  --stage-dir ./tmp/seed-release-v1.1.0-release-tag
```

```bash
scripts/publish-dolthub-seeds \
  --version v1.1.0 \
  --source local \
  --local-ref main \
  --database terminology \
  --stage-dir ./tmp/seed-release-v1.1.0-no-reference-data
```

```bash
scripts/publish-dolthub-seeds \
  --version v1.1.0 \
  --source local \
  --local-ref tuva_health/main \
  --database value_sets \
  --stage-dir ./tmp/seed-release-v1.1.0-value-sets-local
```

```bash
scripts/publish-dolthub-seeds \
  --version v1.1.0 \
  --source local \
  --local-ref merge-test-from-main \
  --database provider_data \
  --stage-dir ./tmp/seed-release-v1.1.0-provider-data-local
```

## Expected Version State After This Rollout

- `concept_library_version: "1.0.1"`
- `reference_data_version: "1.1.0"`
- `terminology_version: "1.1.0"`
- `value_sets_version: "1.1.0"`
- `provider_data_version: "1.1.0"`
- `synthetic_data_version: "1.0.0"`
