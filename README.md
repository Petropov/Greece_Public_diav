# Greece_Public_diav

This repository builds a Diavgeia public-sector digest and intelligence layer. It currently supports the **Diavgeia Monthly Digest** workflow, which builds the monthly report, emails it using repository secrets, and uploads generated digest artifacts.

## Operational workflow

### GitHub Actions

The main operational workflow is:

- **Workflow name:** `Diavgeia Monthly Digest`
- **Workflow file:** `.github/workflows/digest.yml`
- **Schedule:** `15 6 1 * *` — runs at 06:15 UTC on the 1st day of each month. GitHub cron expressions use UTC.
- **Manual trigger:** enabled via `workflow_dispatch`.

Trigger the workflow manually from the GitHub CLI:

```bash
gh workflow run 203950153 --ref main
```

Monitor the latest run and return a non-zero exit code on failure:

```bash
gh run watch --exit-status
```

Inspect logs:

```bash
gh run view --log
gh run view --log-failed
```

### Node.js 24 GitHub Actions compatibility

The workflow has been updated for Node.js 24 compatibility:

- `actions/checkout@v4` → `actions/checkout@v5`
- `actions/setup-python@v5` → `actions/setup-python@v6`

The **Diavgeia Monthly Digest** workflow has been validated successfully with Node.js 24-compatible actions.

### CI sanity check

The previous Python syntax sanity check used:

```bash
python -m compileall -q .
```

It has been replaced with:

```bash
git ls-files '*.py' | xargs python -m py_compile
```

This validates only tracked Python files and avoids compiling files inside `.git/` or other untracked/local directories.


## Diavgeia Monthly Digest cache and backfills

The general monthly digest still supports the original one-month invocation:

```bash
python digest_monthly.py --org 6166 --year 2026 --month 4
```

Monthly export payloads are cached by default under:

```text
data/raw/diavgeia/organization_uid=<ORG>/year=<YYYY>/month=<MM>/search_export.json
```

Decision detail payloads fetched during monthly enrichment are cached under the same monthly directory as:

```text
data/raw/diavgeia/organization_uid=<ORG>/year=<YYYY>/month=<MM>/decisions/<ADA>.json
```

Use `--cache-dir` to point at a different raw-data root, and use `--force-refresh` to ignore existing cache files and refetch them from Diavgeia. Historical backfills can be run month-by-month with `--from YYYY-MM --to YYYY-MM`:

```bash
python digest_monthly.py --org 6166 --from 2026-01 --to 2026-04
```

For broad historical ingestion, use `--search-only` to fetch and cache only each monthly `search_export.json` without per-ADA decision detail enrichment:

```bash
python digest_monthly.py --org 6166 --from 2020-01 --to 2026-05 --search-only
```

In search-only mode, each month's `fetch_metadata.json` records `detail_enrichment: "skipped"` and a `fetch_status` of `success` or `cache_hit`; skipped details alone do not create an `INCOMPLETE` marker.

The digest keeps writing the legacy top-level files in `artifacts/` and also writes per-month copies under:

```text
artifacts/<org>/<YYYY-MM>/
```

## Normalized Diavgeia analytics tables

Cached monthly Diavgeia JSON can be converted into reusable normalized tables without re-querying Diavgeia. Parquet remains the default format:

```bash
python scripts/build_normalized_tables.py --org 6166
```

If your local environment does not have a parquet engine such as `pyarrow` or `fastparquet`, use CSV output instead:

```bash
python scripts/build_normalized_tables.py --org 6166 --format csv
```

The normalizer reads only local cache files under:

```text
data/raw/diavgeia/organization_uid=<ORG>/year=<YYYY>/month=<MM>/search_export.json
data/raw/diavgeia/organization_uid=<ORG>/year=<YYYY>/month=<MM>/decisions/*.json
```

It writes one organization partition under `data/normalized/`:

```text
data/normalized/org=6166/decisions.parquet
data/normalized/org=6166/suppliers.parquet
data/normalized/org=6166/procurements.parquet
data/normalized/org=6166/monthly_summary.parquet

# With --format csv:
data/normalized/org=6166/decisions.csv
data/normalized/org=6166/suppliers.csv
data/normalized/org=6166/procurements.csv
data/normalized/org=6166/monthly_summary.csv
```

The output tables are intended for local analytics:

- `decisions.<format>` contains one row per cached decision with normalized date, type, amount, supplier, signer, and unit fields.
- `suppliers.<format>` groups supplier names/tax ids across decisions and tracks first/last seen dates, decision counts, and total amounts.
- `procurements.<format>` keeps procurement-like financial rows with supplier keys for spend analysis.
- `monthly_summary.<format>` aggregates decision count, total amount, and unique supplier count by year/month.

Use alternate roots when testing or building from a copied cache:

```bash
python scripts/build_normalized_tables.py --org 6166 --raw-root /path/to/raw/diavgeia --output-root /path/to/normalized --format csv
```

The script is offline-only and does not call the Diavgeia API. When the default parquet output is requested without a parquet engine installed, it prints `Parquet engine missing. Re-run with --format csv` instead of a long traceback.

## Lamia Municipality pilot workflow

This repository also includes a separate Lamia-focused pilot pipeline. It does **not** replace or change the general monthly digest. The Lamia pipeline only queries Diavgeia decisions for the Municipality of Lamia.

- **Workflow name:** `Lamia Municipality Digest`
- **Workflow file:** `.github/workflows/lamia-digest.yml`
- **Script:** `src/lamia_digest.py`
- **Target organization:** `ΔΗΜΟΣ ΛΑΜΙΕΩΝ`
- **Slug:** `dhmos_lamieon`
- **Diavgeia organizationUid:** `6166`
- **Query scope:** `organizationUid:"6166"` plus an `issueDate` range
- **Default period:** previous calendar month
- **Schedule:** `30 7 5 * *` — runs at 07:30 UTC on the 5th day of each month. GitHub cron expressions use UTC.
- **Manual trigger:** enabled via `workflow_dispatch`.

Trigger the Lamia workflow manually from the GitHub CLI:

```bash
gh workflow run lamia-digest.yml --ref main
```

Monitor the latest run and return a non-zero exit code on failure:

```bash
gh run watch --exit-status
```

The Lamia workflow saves its generated files under:

```text
artifacts/lamia/
```

Expected Lamia artifacts are:

- `artifacts/lamia/lamia_digest.json` — structured JSON payload with metadata and normalized decisions.
- `artifacts/lamia/lamia_digest.md` — Markdown table for quick review.

Run the Lamia digest locally after installing requirements:

```bash
python src/lamia_digest.py --verbose
```

Optionally override the date range and limit:

```bash
python src/lamia_digest.py --from 2026-04-01 --to 2026-04-30 --limit 200 --verbose
```

The Lamia digest enriches missing amounts by fetching full records from the Diavgeia OpenData decision endpoint:

```text
https://diavgeia.gov.gr/opendata/decisions/{ada}
```

Before increasing limits for a bulk run, smoke-test enrichment with a small cap:

```bash
python src/lamia_digest.py --from 2026-04-01 --to 2026-04-30 --limit 10 --max-detail-fetches 1 --verbose
```

The Lamia pilot differs from the general `Diavgeia Monthly Digest` workflow in these ways:

- It is municipality-specific and always filters on Diavgeia organizationUid `6166`.
- It writes only Lamia artifacts under `artifacts/lamia/`.
- It does not send email.
- It does not modify or reuse the existing monthly digest business logic.

## Local development and validation

Create and activate a local virtual environment, install dependencies, and run the same Python syntax check used by CI:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
git ls-files '*.py' | xargs python -m py_compile
```

The virtual environment affects only the current shell session. Exit it with:

```bash
deactivate
```

## Codex / PR workflow

Useful commands while reviewing or continuing Codex-generated work:

```bash
gh pr list
gh pr checkout <PR_NUMBER>
git status
git diff main
```

## Generated and local files

`decision_labels.json` is generated/local output and is ignored through `.gitignore`. It should not be committed unless the repository intentionally changes how generated label data is managed.
