# Greece_Public_diav

> **A citizen-built AI pipeline for cross-referencing Greek public procurement data.**  
> Built by a Greek engineer living abroad, out of personal curiosity, using only open government databases.  
> ~80 hours of work. Fully open source. Replicable for any Greek city.

## 📊 Live Report

**[→ View the Lamia findings report](https://petropov.github.io/Greece_Public_diav/reports/overview.html)**

## What this found (Lamia pilot)

Using only public data — Diavgeia, ΚΗΜΔΗΣ, ΓΕΜΗ — the pipeline identified three patterns in Lamia's public spending:

| # | Finding | Amount |
|---|---------|--------|
| 1 | **ΙΝΤΕΡΚΑΤ ΑΕ**: 10 years of H/M maintenance contracts at Lamia Hospital, zero competitive tenders, no matching business activity code in ΓΕΜΗ | >€5M |
| 2 | **ΚΟΥΤΚΙΑΣ**: same individual operating under two legal identities (personal AFM + ΕΕ), receiving simultaneous security contracts from the Municipality — including 2 identical contracts on the same day | ~€345k |
| 3 | **ΚΑΝΑΒΕΤΑΣ-ΚΑΡΑΔΗΜΑΣ ΟΕ**: 5 confirmed contract-splitting bundles (2019–2023), identical supplies split by neighbourhood to stay under the €30k threshold | €587k |

All findings are verifiable from the linked sources. Nothing here is an accusation — it is a pattern that warrants scrutiny.

## Why this matters

This is a proof of concept: **one person + AI tools + open data = procurement audit in weeks, not months.**

The pipeline is designed to run on any Greek public entity. If you want to check what's happening in your city, region, or hospital — the code is here.

---

## Use it for your city

Every Greek public body has a Diavgeia `organizationUid`. Find yours at [diavgeia.gov.gr](https://diavgeia.gov.gr) and substitute it for `6166` (Lamia Municipality) in the commands below.

### 1. Install

```bash
git clone https://github.com/Petropov/Greece_Public_diav.git
cd Greece_Public_diav
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the full pipeline

```bash
# Fetch, normalize, enrich, and report — all 9 steps
python scripts/pipeline.py --org 6166

# If you already have a local cache (skip network calls)
python scripts/pipeline.py --org 6166 --skip-fetch --skip-refetch --skip-hydrate --skip-gemi

# With ΓΕΜΗ company enrichment (free API key at opendata.businessportal.gr)
python scripts/pipeline.py --org 6166 --gemi-key YOUR_KEY

# Preview every command without running it
python scripts/pipeline.py --org 6166 --dry-run
```

---

## Pipeline steps (reference)

### Step 1a — Fetch decisions from Diavgeia

```bash
python digest_monthly.py --org 6166 --from 2020-01 --to 2026-05 --search-only
```

`--search-only` fetches the monthly index without per-decision detail calls — fast for a first historical pull.

### Step 1b — Recover capped months

The Diavgeia API caps results at 500 per query. Busy months lose 30–40% of decisions. This step recovers them using weekly sub-windows:

```bash
python scripts/fetch_windowed.py --org 6166
# Target a specific range:
python scripts/fetch_windowed.py --org 6166 --months 2022-01:2024-12
```

### Step 2 — Selective hydration

Fetch full decision detail only for procurement-like decisions (skips payroll, admin noise, already-cached):

```bash
python scripts/hydrate_narrow.py --org 6166
python scripts/hydrate_narrow.py --org 6166 --dry-run   # preview first
```

### Step 3 — Normalize

```bash
python scripts/build_normalized_tables.py --org 6166 --format csv
```

Outputs to `data/normalized/org=<ORG>/`:

| File | Contents |
|------|----------|
| `decisions.csv` | One row per decision |
| `suppliers.csv` | Supplier entities with totals |
| `procurements.csv` | Procurement rows only |
| `monthly_summary.csv` | Counts and amounts by month |

### Step 4 — Cluster suppliers

Deduplicates supplier names by tax ID and canonical name:

```bash
python scripts/cluster_suppliers.py --org 6166
# → data/normalized/org=6166/supplier_clusters.csv
```

### Step 5 — Lifecycle deduplication

Greek contracts generate multiple Diavgeia decisions (award, signing, payment). This links them to avoid ~35% double-counting:

```bash
python scripts/link_procurement_lifecycle.py --org 6166
# → data/normalized/org=6166/contracts.csv
# → data/normalized/org=6166/lifecycle.csv
```

### Step 6 — ΓΕΜΗ enrichment

Look up each supplier in the Greek Commercial Registry. Flags: inactive companies, recently registered, low capital, no record found:

```bash
# Free API key: https://opendata.businessportal.gr/register/
python scripts/enrich_gemi.py --org 6166 --api-key YOUR_KEY
# or: export GEMI_API_KEY=YOUR_KEY
# → data/normalized/org=6166/gemi_enrichment.csv
```

### Step 7 — HTML intelligence report

```bash
python scripts/supplier_intelligence_report.py --org 6166
# → reports/supplier_intelligence_org_6166.html
```

### Step 8 — Markdown report

```bash
python scripts/build_markdown_report.py --org 6166
# → reports/intelligence_org_6166.md
```

### Step 9 — Per-supplier dossiers

```bash
python scripts/build_dossier.py --org 6166             # top 50 by spend
python scripts/build_dossier.py --org 6166 --top 20    # top 20 only
# → reports/dossiers/org=6166/<supplier_name>.html
```

---

## Data sources

| Source | What it provides | URL |
|--------|-----------------|-----|
| Diavgeia | All public spending decisions | diavgeia.gov.gr |
| ΚΗΜΔΗΣ | Contract registry (ADAM numbers) | cerpp.eprocurement.gov.gr |
| ΓΕΜΗ | Company registry (ΚΑΔ, status, owners) | opendata.businessportal.gr |

---

## Lamia pilot — summary stats (2018–2026)

| Metric | Value |
|--------|-------|
| Total Diavgeia decisions | 67,878 |
| Unique contracts (deduplicated) | 28,867 |
| Clean spend | **€103.17M** |
| Unique suppliers | 243 |
| Suppliers with no ΓΕΜΗ record | 116 (48%) |
| Inactive / struck-off suppliers | 11 |
| Peak spend year | **2021 — €31.41M** (EU Recovery funds) |

Full methodology: [`reports/STUDY_RECAP.md`](reports/STUDY_RECAP.md)
