# Greece_Public_diav

Tiny CLI to pull decisions from the Greek "Διαύγεια" Open Data API and optionally download the signed PDFs.

## Quick start

```bash
python fetch_diavgeia.py --org 99220018 --keyword "σύμβαση" --from 2025-01-01 --to 2025-12-31 --limit 100 --download-pdf

