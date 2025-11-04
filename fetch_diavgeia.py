#!/usr/bin/env python3
#!/usr/bin/env python3
import argparse, csv, json, os, sys, time
from datetime import datetime
from typing import Dict, List, Optional
import requests
from dateutil import tz
from tqdm import tqdm

BASE = "https://diavgeia.gov.gr"
SEARCH_URL = f"{BASE}/opendata/search"
DECISION_URL = f"{BASE}/opendata/decisions"
DOC_URL = f"{BASE}/doc"

def build_query(org: Optional[str], dtype: Optional[str], keyword: Optional[str],
                date_from: Optional[str], date_to: Optional[str]) -> str:
    parts = []
    if org: parts.append(f'organizationUid:"{org}"')
    if dtype: parts.append(f'type:"{dtype}"')
    if keyword: parts.append(f'"{keyword}"')
    if date_from or date_to:
        tzinfo = tz.gettz("Europe/Athens")
        def stamp(d, end=False):
            if not d: return None
            dt = datetime.strptime(d, "%Y-%m-%d").replace(
                hour=23 if end else 0, minute=59 if end else 0, second=59 if end else 0
            )
            off = dt.replace(tzinfo=tzinfo).strftime("%z"); off = f"{off[:-2]}:{off[-2:]}"
            return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}{off}"
        left = stamp(date_from, end=False) or "1970-01-01T00:00:00+02:00"
        right = stamp(date_to, end=True) or "2099-12-31T23:59:59+02:00"
        parts.append(f"submissionTimestamp:[DT({left}) TO DT({right})]")
    if not parts: parts.append("*:*")
    return " AND ".join(parts)

def _get(url: str, params: Dict = None, retries: int = 3, timeout: int = 30):
    params = params or {}
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException:
            if attempt == retries: raise
            time.sleep(0.7 * attempt)

def fetch_decisions(q: str, sort: str = "recent") -> Dict:
    return _get(SEARCH_URL, {"q": q, "sort": sort}).json()

def fetch_metadata(ada: str) -> Dict:
    return _get(f"{DECISION_URL}/{ada}").json()

def download_pdf(ada: str, out_dir: str):
    r = _get(f"{DOC_URL}/{ada}")
    if "pdf" not in (r.headers.get("content-type","").lower()): return None
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{ada}.pdf")
    with open(path, "wb") as f: f.write(r.content)
    return path

def to_csv(rows: List[Dict], path: str):
    if not rows: return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    keys = sorted({k for row in rows for k in row.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
        for row in rows: w.writerow({k: row.get(k, "") for k in keys})

def flatten(hit: Dict) -> Dict:
    out = {
        "ada": hit.get("ada"),
        "subject": hit.get("subject"),
        "organizationUid": hit.get("organizationUid"),
        "organizationName": hit.get("organizationName"),
        "decisionTypeId": hit.get("decisionTypeId") or hit.get("type"),
        "protocolNumber": hit.get("protocolNumber"),
        "submissionTimestamp": hit.get("submissionTimestamp"),
        "publishTimestamp": hit.get("publishTimestamp"),
        "url": hit.get("url"),
        "documentUrl": hit.get("documentUrl"),
    }
    for k in ("amount","currency","cpv","afm","contractingAuthority","kpee","vatIncluded"):
        if k in hit: out[k] = hit[k]
    return out

def main():
    ap = argparse.ArgumentParser(description="Pull Διαύγεια decisions via public API")
    ap.add_argument("--org", help="organizationUid (e.g. 99220018)")
    ap.add_argument("--type", dest="dtype", help="decision type code (e.g. Γ.3.3)")
    ap.add_argument("--keyword", help="phrase to search")
    ap.add_argument("--from", dest="date_from", help="YYYY-MM-DD")
    ap.add_argument("--to", dest="date_to", help="YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--download-pdf", action="store_true")
    ap.add_argument("--no-csv", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    query = build_query(args.org, args.dtype, args.keyword, args.date_from, args.date_to)
    if args.verbose: print("Query:", query, file=sys.stderr)

    data = fetch_decisions(query, sort="recent")
    hits = data.get("decisions") or data.get("diavgeia_decisions") or []
    if not isinstance(hits, list):
        print("Unexpected API response; no 'decisions' list found.", file=sys.stderr); sys.exit(2)

    hits = hits[: args.limit] if args.limit and args.limit > 0 else hits
    rows = []
    os.makedirs("output", exist_ok=True)
    with open("output/decisions.jsonl", "w", encoding="utf-8") as jf:
        for hit in tqdm(hits, desc="Processing", unit="decision"):
            ada = hit.get("ada")
            try: merged = {**hit, **fetch_metadata(ada)}
            except Exception: merged = hit
            flat = flatten(merged)
            jf.write(json.dumps(flat, ensure_ascii=False) + "\n")
            rows.append(flat)
            if args.download_pdf and ada:
                try: download_pdf(ada, "diavgeia_docs")
                except Exception as e:
                    if args.verbose: print(f"PDF download failed for {ada}: {e}", file=sys.stderr)
            time.sleep(0.15)

    if not args.no_csv: to_csv(rows, "output/decisions.csv")
    print(f"Done. {len(rows)} decisions written to output/decisions.jsonl")
    if args.download_pdf: print("PDFs saved under diavgeia_docs/")

if __name__ == "__main__":
    main()

