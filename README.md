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

## Search-wide, hydrate-narrow pipeline

The full procurement intelligence pipeline runs in six stages.  Steps 1 and 2
require network access to `diavgeia.gov.gr`; the rest are offline.

### Quick start (one command)

```bash
# Full pipeline for Lamia: fetch → hydrate → normalize → cluster → report
python scripts/pipeline.py --org 6166

# Skip the live-fetch steps if you already have a local cache
python scripts/pipeline.py --org 6166 --skip-fetch --skip-hydrate

# Include per-supplier dossiers (top 30 by spend)
python scripts/pipeline.py --org 6166 --skip-fetch --skip-hydrate --dossiers --dossier-top 30

# Dry-run: print every command without executing
python scripts/pipeline.py --org 6166 --dry-run
```

### Individual steps

**Step 1 — Fetch search exports** (`digest_monthly.py`)

Fetch and cache every month's `search_export.json`.  Prefer `--search-only`
for a broad historical pull — it skips per-ADA detail fetches:

```bash
python digest_monthly.py --org 6166 --from 2020-01 --to 2026-05 --search-only
```

**Step 2 — Selective hydration** (`scripts/hydrate_narrow.py`)

Score every decision in the local cache and fetch detail JSONs only for those
that look like genuine procurement decisions.  Payroll, admin noise, and
already-cached decisions are skipped automatically:

```bash
# Hydrate all cached months
python scripts/hydrate_narrow.py --org 6166

# Hydrate a specific range, print per-decision actions
python scripts/hydrate_narrow.py --org 6166 --months 2024-01:2024-12 --verbose

# Preview what would be fetched without hitting the API
python scripts/hydrate_narrow.py --org 6166 --dry-run

# Raise the score threshold to fetch only the highest-confidence decisions
python scripts/hydrate_narrow.py --org 6166 --min-score 4
```

Hydration score logic (higher = fetch detail):
- Decision type is a procurement type (`Δ.2.2`, `Δ.1`, `Β.2.1`, …) → +3
- Subject contains procurement keywords → up to +3
- Search export already has both amount and supplier → -2
- Payroll/admin noise tokens present → -10 (always skip)

**Step 3 — Normalize** (`scripts/build_normalized_tables.py`)

```bash
python scripts/build_normalized_tables.py --org 6166 --format csv
```

Outputs under `data/normalized/org=6166/`:

```
decisions.csv         — one row per decision
suppliers.csv         — grouped supplier entities with totals
procurements.csv      — procurement-classified rows only
monthly_summary.csv   — decision counts and amounts by month
```

**Step 4 — Cluster suppliers** (`scripts/cluster_suppliers.py`)

Deduplicate suppliers: first by tax ID, then by canonical name.

```bash
python scripts/cluster_suppliers.py --org 6166
# Writes: data/normalized/org=6166/supplier_clusters.csv
```

**Step 5 — Intelligence report** (`scripts/supplier_intelligence_report.py`)

```bash
python scripts/supplier_intelligence_report.py --org 6166
# Writes: reports/supplier_intelligence_org_6166.html
```

**Step 6 — Markdown intelligence report** (`scripts/build_markdown_report.py`)

```bash
python scripts/build_markdown_report.py --org 6166
# Writes: reports/intelligence_org_6166.md
```

Sections: executive summary, spend by year, monthly breakdown, top 30 procurements, top 30 suppliers, repeat suppliers, decision-type breakdown, data coverage notes.

**Step 7 — Per-supplier dossiers** (`scripts/build_dossier.py`)

Requires `supplier_clusters.csv` from step 4.

```bash
# Top 50 suppliers by spend (JSON + HTML each)
python scripts/build_dossier.py --org 6166

# Top 20, HTML only
python scripts/build_dossier.py --org 6166 --top 20 --format html

# One specific cluster
python scripts/build_dossier.py --org 6166 --cluster-id cluster:abc123def456

# Dossiers written to: reports/dossiers/org=6166/<supplier_name>.html
```

### Avoiding brute-force hydration

`hydrate_narrow.py` is intentionally narrow — it skips:

1. Decisions already in the local cache (`decisions/<ADA>.json` exists)
2. Payroll/HR decisions (employment contracts, overtime, leave, appointments)
3. Cancellations and reversals
4. Decisions where the search export already carries both amount and supplier
5. Any decision scoring below `--min-score` (default 2)

This keeps API load proportional to intelligence value.  A typical year for
Lamia (org 6166) has ~5 000 search-export rows but only ~1 000–1 500 decisions
worth hydrating.

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
