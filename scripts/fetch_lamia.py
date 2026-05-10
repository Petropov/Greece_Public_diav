import requests
import json
import time
from pathlib import Path

BASE_URL = "https://opendata.diavgeia.gov.gr/luminapi/opendata/search/advanced"
ORG_UID = "6166"
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_page(page=0, size=50):
    params = {
        "q": f'organizationUid:"{ORG_UID}"',
        "wt": "json",
        "page": page,
        "size": size,
    }
    r = requests.get(BASE_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_all(max_pages=20):
    all_decisions = []
    page = 0

    while page < max_pages:
        print(f"Fetching page {page}...")
        data = fetch_page(page=page, size=50)
        decisions = data.get("decisions", [])
        total = data.get("info", {}).get("total", 0)

        if not decisions:
            break

        all_decisions.extend(decisions)
        print(f"  Got {len(decisions)} decisions | total so far: {len(all_decisions)} / {total}")

        if len(all_decisions) >= total:
            break

        page += 1
        time.sleep(0.5)  # be polite to the API

    return all_decisions

if __name__ == "__main__":
    decisions = fetch_all(max_pages=20)  # ~1000 decisions to start

    out_path = OUTPUT_DIR / "lamia_decisions_raw.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(decisions, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(decisions)} decisions to {out_path}")
