Here’s a clean recap you can drop straight into your repo’s **README.md** (or a `NOTES.md`) — written for future you, so you don’t have to re-debug this whole mess again.

---

## 🧭 Diavgeia API Situation – 2025-11 Recap

### Summary

As of **November 2025**, the Diavgeia open-data API is partially broken.
Any query—no matter how simple—triggers a server-side syntax error caused by the system injecting an invalid date range filter (`DT()` syntax) into every request.

This is **not a client bug**. The API itself currently rejects all fielded and match-all queries, so no new data can be fetched programmatically.

---

### 💡 Root Cause

* The legacy endpoint

  ```
  https://diavgeia.gov.gr/luminapi/api/search/advanced
  ```

  now returns **HTTP 404** — permanently retired.

* The public endpoints that still respond:

  * **JSON “advanced” search:**
    `https://opendata.diavgeia.gov.gr/luminapi/opendata/search/advanced`
  * **XML export:**
    `https://opendata.diavgeia.gov.gr/luminapi/api/search/export`

* Both currently fail with:

  ```json
  {
    "exception": "InvalidQuertSyntaxException",
    "errorMessage": "Error in query syntax. Query:*:* AND issueDate:[DT(...) TO DT(...)] Error:"
  }
  ```

  meaning the backend automatically adds an invalid `issueDate:[DT(...)]` clause and rejects the request.

* The official [Diavgeia help page](https://diavgeia.gov.gr/api/help) shows a banner stating that *search is temporarily limited to ADA-only and statistics are suspended* — confirming system-wide maintenance.

---

### ✅ What We’ve Fixed / Improved

* **Cleaned repository code:**

  * Fixed indentation, `pct()` shadowing, and datetime parsing.
  * Added `decision_labels.json` mapping and readable labels in reports.
  * Added artifact output: `artifacts/digest.html`, `raw_month.csv`, `outliers.csv`, `unmapped_codes.csv`.
  * Added safe email send via `send_email.py` using UTF-8 subject and SMTP env vars.

* **Modernized API handling:**

  * Removed use of dead endpoint (`/api/search/advanced`).
  * Added fallback to `/opendata/search/advanced` (JSON) and `/api/search/export` (XML).
  * Implemented robust extractor that handles multiple JSON shapes and raises loudly on unknown schemas.
  * Added retry/backoff logic for 5xx responses and chunked date ranges for large queries.

* **Discovered**: API 500s for long date ranges → solved via month-by-month chunking when service works.

---

### 🚧 Current Limitation (Unsolved)

* **All search queries fail** due to the server injecting `DT()` range filters.
* No combination of `q`, `fq`, date format, or endpoint bypasses this.
* Only single-ADA lookups (by known ADA code) might still work.

---

### 🧰 Temporary Workaround

To keep CI/CD and monthly jobs green:

1. **Add a maintenance guard**:

   ```python
   import requests

   def diavgeia_online():
       try:
           r = requests.get(
               "https://opendata.diavgeia.gov.gr/luminapi/opendata/search/advanced",
               params={"q": "ada:*", "wt": "json", "page": 0, "size": 1},
               timeout=15
           )
           j = r.json()
           return "InvalidQuertSyntaxException" not in str(j)
       except Exception:
           return False
   ```

2. **Wrap data fetch:**

   ```python
   if not diavgeia_online():
       print("⚠️  Diavgeia search API in maintenance — skipping data fetch.")
       # Reuse cached artifacts (last successful CSVs)
       raise SystemExit(0)
   ```

3. **Artifacts / email:**
   Keep producing `digest.html` using previous month’s data and append a banner:

   > *Data unavailable — Diavgeia API under maintenance (InvalidQuertSyntaxException).*

---

### 🕓 What To Do When It’s Back

1. Re-run:

   ```bash
   curl -s "https://opendata.diavgeia.gov.gr/luminapi/opendata/search/advanced" \
     --get --data-urlencode 'q=ada:*' --data-urlencode 'wt=json' \
     --data-urlencode 'page=0' --data-urlencode 'size=1'
   ```

   ✅ If it returns JSON **without** the `InvalidQuertSyntaxException`, search is healthy again.

2. Restore normal query patterns (plain ISO dates, **no DT()**):

   ```
   q=organizationUid:"<UUID>" AND issueDate:["2025-05-01T00:00:00" TO "2025-05-31T23:59:59"]
   wt=json&page=0&size=0
   ```

3. Remove the maintenance guard and re-enable monthly data pulls.

---

### 🧾 TL;DR

| Status                 | What Works                                        | What Fails        | Action                        |
| :--------------------- | :------------------------------------------------ | :---------------- | :---------------------------- |
| ✅ Legacy fixes         | Data parsing, digest generation, email sending    | –                 | All solid                     |
| ✅ Endpoints discovered | `/opendata/search/advanced`, `/api/search/export` | –                 | Use these going forward       |
| 🚧 Current API         | Injects invalid `DT()` filter                     | All range queries | Guard + reuse cache           |
| 🕓 Next steps          | Wait for Diavgeia to restore search               | –                 | Test & re-enable date queries |

---

> **Bottom line:**
> The project code is healthy. The Diavgeia API isn’t.
> Once the platform stops injecting that `DT()` clause, everything here should run normally again — no code change needed.
