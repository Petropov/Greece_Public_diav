# Δήμος Λαμιέων — Procurement Intelligence Study Recap

> A data-driven analysis of public spending by the Municipality of Lamia, Greece (2018–2026),
> derived from the Diavgeia mandatory transparency platform and the ΓΕΜΗ commercial registry.

---

## What is Diavgeia?

[Diavgeia](https://diavgeia.gov.gr) (Διαύγεια, "Clarity") is Greece's mandatory government transparency platform. By law, every decision that commits or spends public funds — contracts, awards, payment orders, committee minutes — must be published on Diavgeia with a unique identifier (ADA) before it takes legal effect. Lamia Municipality (Δήμος Λαμιέων, Diavgeia org ID `6166`) has published tens of thousands of such decisions since 2010.

---

## What We Did

We built a data pipeline to:

1. **Fetch** all procurement-related decisions for Lamia Municipality from 2018 to 2026 via the Diavgeia search/export API.
2. **Recover missing data** — the API returns at most 500 decisions per query. Busy months (many had 900–1,650 decisions) appeared capped at 500. We solved this by re-fetching each capped month in weekly sub-windows (7-day date ranges), then merging and deduplicating results. This recovered roughly 35% more data from high-volume months.
3. **Normalize** the raw JSON into flat tables (decisions, suppliers, procurements).
4. **Deduplicate across lifecycle stages** — a single Greek public contract generates several Diavgeia decisions: a committee minutes (ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ), a formal award (ΚΑΤΑΚΥΡΩΣΗ), a contract signing (ΣΥΜΒΑΣΗ), and payment orders (ΕΝΤΟΛΗ ΠΛΗΡΩΜΗΣ). Naively summing amounts across all stages would nearly double-count every contract. We linked stages by matching amounts within ±2% tolerance within a 180-day window, and produced a single canonical contract row per procurement.
5. **Enrich with ΓΕΜΗ** — we looked up each supplier's tax ID in the Greek General Commercial Registry (ΓΕΜΗ / businessportal.gr) to obtain legal names, company status, share capital, registration dates, and activity codes. We then flagged anomalies that may warrant further scrutiny.

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Total Diavgeia decisions fetched | **67,878** |
| Period covered | **2018 – May 2026** |
| Unique contracts (after lifecycle dedup) | **28,867** |
| Clean spend (deduplicated) | **€103.17M** |
| Raw spend (naive sum, with multi-stage double-counting) | **~€189.83M** |
| Unique suppliers identified | **243** |
| Suppliers found in ΓΕΜΗ | **126 (52%)** |
| Suppliers with no ΓΕΜΗ record | **116 (48%)** |

---

## Spend by Year

| Year | Contracts | Clean Spend |
|------|-----------|-------------|
| 2018 | 800 | €4.54M |
| 2019 | 513 | €13.94M |
| 2020 | 3,910 | €9.59M |
| 2021 | **4,863** | **€31.41M** |
| 2022 | 4,478 | €8.55M |
| 2023 | 4,852 | €9.22M |
| 2024 | 3,064 | €7.21M |
| 2025 | 3,659 | €11.98M |
| 2026 (partial) | 361 | €1.70M |
| **Total** | **28,867** | **€103.17M** |

**2021 stands out.** Spend jumped to €31.41M — more than three times the surrounding years. This spike aligns with the EU COVID-19 Recovery and Resilience Facility (RRF) and ΕΣΠΑ 2021-2027 infrastructure tranches, which directed large capital allocations to municipalities in that period.

---

## Largest Individual Contracts

| Date | ADA | Amount | Subject |
|------|-----|--------|---------|
| 2019-12-03 | Ω2ΖΘΩΛΚ-Δ3Β | **€9.50M** | Fuel and lubricants framework agreement (multi-year) |
| 2021-05-10 | ΩΝΣΔΩΛΚ-ΦΒΜ | €6.03M | Construction committee — public works |
| 2021-05-10 | — | €5.37M | Complete infrastructure rehabilitation project |
| 2021-05-10 | — | €5.23M | Public works procurement committee |
| 2025-04-10 | — | €4.00M | Public works committee |
| 2024-03-12 | 9ΠΓΚΩΛΚ-ΨΩΣ | €2.80M | Fuel and lubricants procurement |
| 2020-04-07 | 6ΝΝΜΩΛΚ-ΚΥΝ | €2.39M | Solid waste collection services |
| 2021-05-10 | ΩΝΣΔΩΛΚ-ΦΒΜ | €2.24M | Solid waste and recycling services |

The largest single contract on record is a **€9.50M fuel and services framework** (Κατακύρωση, December 2019), awarded via an open electronic tender (ΕΣΗΔΗΣ). Multi-year fuel contracts of this scale are common for Greek municipalities that operate vehicle fleets for waste collection, street-cleaning, and civil engineering.

---

## Supplier Landscape

### Concentration

Of the 243 identified suppliers, only a small number account for the largest share of spend. The high concentration in categories like fuel, solid waste collection, and infrastructure works is typical for Greek municipalities of Lamia's size (~70,000 inhabitants in the municipality proper).

### Supplier Transparency Flags

We cross-referenced all suppliers with ΓΕΜΗ and computed four transparency flags:

#### 1. No ΓΕΜΗ Record (116 suppliers — 48%)
Nearly half of all supplier tax IDs returned no result in the Greek commercial registry. This is the most significant data quality gap. Possible explanations:
- Individual contractors (natural persons) who are registered with tax authorities but not as commercial entities in ΓΕΜΗ
- Foreign entities (EU or non-EU vendors) not registered in the Greek registry
- Data entry errors in tax IDs as published on Diavgeia
- Dissolved entities that were purged from the registry

This does not necessarily indicate wrongdoing, but it means **verification of contractor identity is impossible** for nearly half of all spending.

#### 2. Inactive Companies (11 suppliers)
Eleven suppliers were found in ΓΕΜΗ with an inactive or struck-off status at the time of our enrichment (May 2026):

| Tax ID | Name | Status |
|--------|------|--------|
| 029430370 | ΚΑΡΑΝΤΖΟΥΝΗΣ ΔΗΜΗΤΡΙΟΣ ΤΟΥ ΓΕΩΡΓΙΟΥ | ΔΙΑΓΡΑΦΗ |
| 040678990 | ΜΠΟΤΣΗΣ ΚΩΝ/ΝΟΣ ΤΟΥ ΗΛΙΑ | ΔΙΑΓΡΑΦΗ |
| 054292490 | ΚΩΝΣΤΑΝΤΟΠΟΥΛΟΣ ΙΩΑΝΝΗΣ ΤΟΥ ΚΩΝΣΤΑΝΤΙΝΟΥ | ΔΙΑΓΡΑΦΗ |
| 082422820 | ΚΩΝ/ΝΟΣ ΜΑΥΡΟΣ - ΧΡΗΣΤΟΣ ΠΑΡΤΣΑΛΑΚΗΣ | ΔΙΑΓΡΑΦΗΚΕ |
| 136873531 | ΣΤΑΥΡΟΥ ΠΑΝΑΓΙΩΤΗΣ ΤΟΥ ΗΛΙΑ | ΔΙΑΓΡΑΦΗ |
| 016926061 | ΡΩΜΑΙΟΣ ΗΛΙΑΣ ΤΟΥ ΘΕΟΔΩΡΟΥ | Inactive |
| 037941483 | ΤΣΙΑΒΟΣ ΗΛΙΑΣ ΤΟΥ ΓΕΩΡΓΙΟΥ | Inactive |
| 044395629 | ΑΛΕΞΑΝΔΡΗΣ ΝΙΚΟΛΑΟΣ ΤΟΥ ΓΕΩΡΓΙΟΥ | Inactive |
| 044395918 | ΣΤΕΡΓΙΟΠΟΥΛΟΣ ΣΠΥΡΙΔΩΝ ΤΟΥ ΓΕΩΡΓΙΟΥ | Inactive |
| 101930370 | ΠΡΕΖΑΣ ΠΑΝΑΓΙΩΤΗΣ ΤΟΥ ΙΩΑΝΝΗ | Inactive |
| 117677540 | ΠΑΠΑΒΑΣΙΛΕΙΟΥ ΠΕΡΙΚΛΗΣ ΤΟΥ ΕΥΣΤΑΘΙΟΥ | Inactive |

Four suppliers bear the "ΔΙΑΓΡΑΦΗ" (struck-off) prefix in their registered name, meaning they were formally deregistered. Payments to struck-off entities warrant review — either the contracts pre-date the deregistration, the entity re-registered under a different number, or there is an administrative irregularity.

#### 3. Low Capital vs. Large Contract (2 suppliers)
Two suppliers received contracts worth more than €100,000 despite having registered share capital below €10,000, a disparity that can indicate insufficient financial capacity for the contracted scope:

| Tax ID | Name | Registered |
|--------|------|------------|
| 011278629 | ΜΑΛΑΜΟΣ ΜΙΧΑΗΛ ΤΟΥ ΝΙΚΟΛΑΟΥ | 1996-01-17 |
| 082785679 | ΚΩΝ. ΜΑΝΤΕΣ - ΔΗΜ ΤΣΑΜΗΣ Ο.Ε. | 1993-02-12 |

Note: Share capital is not a hard legal barrier for contract award in Greece for most procurement categories. However, very low capital coupled with high-value contracts is a standard audit red flag.

#### 4. Recently Registered (0 suppliers)
No supplier was found to have registered in the commercial registry within 12 months of their first appearance in Lamia's procurement decisions. This is a positive finding — there is no evidence of shell-company creation shortly before contract award.

---

## Methodological Notes

### The 500-ADA Cap Problem
The Diavgeia search/export API enforces a hard limit of 500 unique decisions per query. For months with high decision volume (December months in particular had 900–1,650 actual decisions), the initial cache held exactly 500 ADAs — masking the rest. The windowed re-fetch strategy splits each such month into 7-day windows, fetches each independently, then merges by ADA. This is a purely additive operation: the existing cache is seeded first, and window fetches only add new rows.

**Result:** +67% more decisions recovered vs. the initial cap-limited fetch (40,632 → 67,878).

### Lifecycle Deduplication
Without deduplication, the raw sum of all procurement amounts is approximately €189.83M. After linking multi-stage records into single contracts, the figure drops to **€103.17M** — a 46% reduction. This is the more accurate measure of actual spending.

The matching algorithm:
- Same supplier (by tax ID or canonical name cluster)
- Amount within ±2% tolerance
- Both decisions within a 180-day window
- Priority ranking: ΚΑΤΑΚΥΡΩΣΗ > ΣΥΜΒΑΣΗ > ΑΝΑΘΕΣΗ > ΕΝΤΟΛΗ ΠΛΗΡΩΜΗΣ > others

### Data Limitations
- **Amount coverage is partial**: The bulk fetch (`--search-only`) does not retrieve detailed decision metadata. Only hydrated decisions (where the full per-ADA JSON was fetched) carry reliable structured amounts and supplier tax IDs. Search-export rows often have amounts but in unstructured subject-line text.
- **Supplier identification**: Suppliers are matched first by tax ID, then by canonical name normalization. Name variations (spacing, punctuation, abbreviations) across decisions from different years can still result in duplicate supplier entities.
- **ΓΕΜΗ enrichment is a point-in-time snapshot**: Company status as of the enrichment date (May 2026) may differ from status at the time contracts were awarded.

---

## Reproducibility

This analysis was performed using the pipeline in this repository. To reproduce it for Lamia Municipality:

```bash
# 1. Fetch all decisions (2020–2026)
python digest_monthly.py --org 6166 --from 2020-01 --to 2026-05 --search-only

# 2. Recover capped months
python scripts/fetch_windowed.py --org 6166

# 3. Normalize
python scripts/build_normalized_tables.py --org 6166 --format csv

# 4. Cluster suppliers
python scripts/cluster_suppliers.py --org 6166

# 5. Deduplicate procurement lifecycle
python scripts/link_procurement_lifecycle.py --org 6166

# 6. ΓΕΜΗ enrichment (free API key required)
python scripts/enrich_gemi.py --org 6166 --api-key YOUR_KEY

# 7. Generate report
python scripts/build_markdown_report.py --org 6166
```

Or run the entire pipeline in one command:

```bash
python scripts/pipeline.py --org 6166 --gemi-key YOUR_KEY
```

To analyze a different municipality, replace `6166` with the Diavgeia `organizationUid` for your target entity.

---

## Data Sources

| Source | URL | Access |
|--------|-----|--------|
| Diavgeia search/export API | `https://diavgeia.gov.gr/luminapi/api/search/export` | Public, no key required |
| Diavgeia decision detail | `https://diavgeia.gov.gr/opendata/decisions/{ADA}` | Public, no key required |
| ΓΕΜΗ OpenData API | `https://opendata-api.businessportal.gr/api/opendata/v1/companies` | Free registration required |

---

*Study conducted May 2026. All data sourced from public government APIs. No personal data was collected beyond what is already published on Diavgeia.*
