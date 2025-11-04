#!/usr/bin/env python3
import argparse, csv, json, os, sys
import requests

BASE = "https://diavgeia.gov.gr"
EXPORT_URL   = f"{BASE}/luminapi/api/search/export"
DECISION_URL = f"{BASE}/opendata/decisions"

def build_query(org, dtype, keyword, date_from, date_to, date_field="issueDate"):
    parts = []
    if org:     parts.append(f'organizationUid:"{org}"')
    if dtype:   parts.append(f'type:"{dtype}"')
    if keyword: parts.append(f'"{keyword}"')
    if date_from or date_to:
        left  = (date_from or "1970-01-01") + "T00:00:00"
        right = (date_to   or "2099-12-31") + "T23:59:59"
        field = (
            "publishTimestamp" if date_field == "publish"
            else "submissionTimestamp" if date_field == "submission"
            else "issueDate"
        )
        parts.append(f"{field}:[DT({left}) TO DT({right})]")
    return " AND ".join(parts) if parts else "*:*"

def fetch_export(q, sort="recent", page=0, size=200):
    params = {"q": q, "sort": sort, "wt": "json", "page": page, "size": size}
    r = requests.get(EXPORT_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    # Accept the common JSON shapes
    if isinstance(data, dict):
        if isinstance(data.get("decisionResultList"), list):
            return data["decisionResultList"]
        if isinstance(data.get("decisions"), list):
            return data["decisions"]
        if isinstance(data.get("diavgeia_decisions"), list):
            return data["diavgeia_decisions"]
        dr = data.get("decisionresults") or data.get("decisionResults")
        if isinstance(dr, dict):
            dec = dr.get("decision") or dr.get("decisions") or []
            if isinstance(dec, dict): dec = [dec]
            if isinstance(dec, list): return dec
    return []

def fetch_meta(ada):
    r = requests.get(f"{DECISION_URL}/{ada}", timeout=30)
    r.raise_for_status()
    return r.json()

def normalize(hit):
    # Map alternative field names from export -> unified keys
    org_name = hit.get("organizationName") or hit.get("organizationLabel")
    dctype   = hit.get("decisionTypeId") or hit.get("decisionTypeUid") or hit.get("type")
    return {
        "ada": hit.get("ada"),
        "subject": hit.get("subject"),
        "organizationUid": hit.get("organizationUid"),
        "organizationName": org_name,
        "decisionTypeId": dctype,
        "decisionTypeLabel": hit.get("decisionTypeLabel"),
        "protocolNumber": hit.get("protocolNumber"),
        "issueDate": hit.get("issueDate"),
        "submissionTimestamp": hit.get("submissionTimestamp"),
        "publishTimestamp": hit.get("publishTimestamp"),
        "url": hit.get("url"),
        "documentUrl": hit.get("documentUrl"),
    }

def main():
    ap = argparse.ArgumentParser(description="Diavgeia export fetch (luminapi, wt=json)")
    ap.add_argument("--org")
    ap.add_argument("--type", dest="dtype")
    ap.add_argument("--keyword")
    ap.add_argument("--from", dest="date_from")
    ap.add_argument("--to", dest="date_to")
    ap.add_argument("--date-field", choices=["issueDate","publish","submission"], default="issueDate")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--no-csv", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    q = build_query(args.org, args.dtype, args.keyword, args.date_from, args.date_to, args.date_field)
    if args.verbose:
        print("Query:", q, file=sys.stderr)

    remaining = args.limit
    page = 0
    rows = []
    os.makedirs("output", exist_ok=True)
    with open("output/decisions_export.jsonl", "w", encoding="utf-8") as jf:
        while remaining > 0:
            batch = fetch_export(q, sort="recent", page=page, size=min(remaining, 500))
            if not batch: break
            for h in batch:
                flat = normalize(h)
                jf.write(json.dumps(flat, ensure_ascii=False) + "\n")
                rows.append(flat)
                remaining -= 1
                if remaining <= 0: break
            if len(batch) < 1: break
            page += 1

    if not args.no_csv and rows:
        keys = sorted({k for r in rows for k in r.keys()})
        with open("output/decisions_export.csv","w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in rows: w.writerow({k:r.get(k,"") for k in keys})

    print(f"Done. {len(rows)} decisions -> output/decisions_export.jsonl")

if __name__ == "__main__":
    main()
