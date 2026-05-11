# Greece Public Diavgeia Intelligence Project

## Repository purpose

This project is evolving from a Diavgeia scraper into a normalized intelligence layer for Greek public administration data.

The goal is not just document retrieval.

The goal is:
- normalization
- procurement intelligence
- supplier intelligence
- lifecycle reconstruction
- public-sector graph analytics

---

# Current project state

## Repository location

Current local path:

/Users/petropov/Greece_Public_diav

---

# What currently works

## 1. Raw Diavgeia ingestion

We successfully:
- download monthly decision exports
- cache search exports
- enrich with detail endpoints
- normalize records into structured tables

Main script:

scripts/build_normalized_tables.py

Outputs:

data/normalized/org=6166/

Including:
- decisions.csv
- procurements.csv
- suppliers.csv
- monthly_summary.csv

---

## 2. Procurement normalization

Current normalization includes:
- amount extraction
- supplier extraction
- procurement filtering
- duplicate removal
- trusted budget phrase extraction
- cancellation filtering

Examples:
- removes “Ακύρωση”
- removes “Ανάκληση”
- removes committee-only decisions

---

## 3. Procurement stages

Current procurement stage tagging:

- approval
- committee
- award
- cancellation
- other

This enables future lifecycle reconstruction.

---

## 4. Data quality improvements

Implemented:
- duplicate ADA removal
- invalid amount filtering
- max amount sanity checks
- trusted amount extraction
- procurement noise filtering

Tests passing:
56 unit tests

---

## 5. Intelligence direction

Project is shifting toward:

### Supplier intelligence
Understanding:
- top suppliers
- repeat winners
- concentration
- procurement frequency
- temporal patterns

### Procurement lifecycle intelligence
Reconstructing:
- approval
- committee creation
- award
- cancellation
- payment chain

### Public-sector graph analytics
Future graph:
- municipality
- suppliers
- committees
- signers
- units
- procurements
- linked decisions

---

# Current problems

## 1. Historical amount coverage is weak

Most older years still have:
- low supplier extraction
- low amount extraction

Reason:
older Diavgeia exports are inconsistent.

Need:
- OCR fallback
- PDF parsing
- semantic extraction

---

## 2. Lifecycle linkage not implemented

We classify stages but do not yet connect:
- approval -> award
- award -> payment
- cancellation -> prior procurement

This is a major future milestone.

---

## 3. HTML intelligence reporting incomplete

Need:
- proper HTML dashboards
- charts
- supplier drilldowns
- procurement timelines
- anomaly detection

Planned script:
scripts/supplier_intelligence_report.py

---

# Strategic opportunity

This project can evolve into:

- Greek public procurement observability layer
- municipality intelligence engine
- supplier concentration monitor
- anti-corruption analytics
- EU-wide procurement normalization system

The real value is not scraping.

The real value is:
- normalization
- linkage
- graph reconstruction
- intelligence

---

# Immediate next steps

## High priority

1. Finish supplier intelligence HTML report
2. Add procurement lifecycle linkage
3. Add supplier concentration metrics
4. Add anomaly detection
5. Add charts and timelines

## Medium priority

1. PDF parsing
2. OCR extraction
3. Semantic procurement clustering
4. Cross-municipality supplier analysis

## Long-term

1. Graph database
2. Entity resolution
3. Public API
4. EU procurement normalization
5. AI-assisted audit tooling

