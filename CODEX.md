# Codex Setup & Working Instructions

## Project

Greece Public AI Infrastructure — Lamia Municipality MVP
Diavgeia procurement data ingestion and intelligence layer.

## Environment Requirements

Python 3.10+

Dependencies:

```bash
pip install requests pandas python-dotenv
```

No database required at this stage.
No API keys required — Diavgeia is a public open API.

## Repo Structure

```text
data/raw/         # raw JSON from API — gitignored, never commit
scripts/          # ingestion and parsing scripts
notebooks/        # exploration notebooks
CODEX.md          # this file
```

## The Working Endpoint

The only confirmed working Diavgeia endpoint for Lamia:

```text
https://opendata.diavgeia.gov.gr/luminapi/opendata/search/advanced
  ?q=organizationUid:"6166"
  &wt=json
  &page=0
  &size=50
```

Organization: Δήμος Λαμιέων (Lamia Municipality)
Organization UID: 6166
Slug: dhmos_lamieon

Do NOT use:

- `/luminapi/api/search` with `fq=` parameter → returns `InvalidQuertSyntaxException`
- `/luminapi/api/organizations/dhmos_lamieon/decisions` → 404

## Key Data Fields Returned

Each decision object contains:

- `ada` — unique decision ID (e.g. `9ΧΤΒΩΛΚ-6Γ9`)
- `subject` — full Greek text description
- `issueDate` — unix timestamp in milliseconds
- `organizationId` — always `6166` for Lamia
- `decisionTypeId` — e.g. `Β.1.3` (expense approval)
- `amountWithVAT.amount` — contract value in EUR
- `documentUrl` — direct PDF link
- `unitIds` — department identifiers
- `signerIds` — signing authority identifiers

Total available decisions (last 6 months): ~5,485

## Current Scripts

### `scripts/fetch_lamia.py`

Fetches paginated decisions from the working endpoint.
Saves raw JSON to `data/raw/lamia_decisions_raw.json`.

Run with:

```bash
python scripts/fetch_lamia.py
```

## Current Known Issues

1. Diavgeia search API intermittently injects invalid `DT()` date syntax causing `InvalidQuertSyntaxException` on the `fq=` endpoint.
   Workaround: use the opendata advanced search endpoint with `q=` parameter.
2. The API auto-applies a 6-month date window.
   To fetch older data, date range parameters will need to be tested separately.
3. GitHub Actions / sandboxed environments block outbound HTTPS to `diavgeia.gov.gr`.
   Always run ingestion scripts locally or from a VPS with open outbound access.

## What We Are Building

An intelligence layer on top of Lamia's public procurement data:

- searchable decisions
- supplier normalization
- spend by category and department
- anomaly detection (repeated awards, concentration patterns)

Start narrow. One municipality. One working endpoint. Build from real data.
