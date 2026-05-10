#!/usr/bin/env python3
import argparse
import json
import math
import os
import re
import time
from datetime import date, datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests

# Decision type labels (short)
decision_map = {
    "Α.1": "Regulatory act",
    "Α.2": "Internal regulation",
    "Β.1.1": "Budget commitment",
    "Β.1.2": "Budget amendment",
    "Β.1.3": "Payment warrant",
    "Β.2.1": "Expenditure approval",
    "Β.2.2": "Payment finalization",
    "Γ.2": "Personnel change",
    "Δ.1": "Procurement assignment",
    "Δ.2.2": "Contract award",
    "2.4.7.1": "Other administrative act",
}

LABELS_FILE = "decision_labels.json"
try:
    with open(LABELS_FILE, "r", encoding="utf-8") as f:
        decision_map.update(json.load(f))
except FileNotFoundError:
    pass

BASE = "https://diavgeia.gov.gr/luminapi/api/search/export"
DETAIL_BASE = "https://diavgeia.gov.gr/opendata/decisions"
OUT = "artifacts"
DEFAULT_CACHE_DIR = "data/raw/diavgeia"


def dtfmt(d):
    return d.strftime("%Y-%m-%d")


def month_bounds(y, m):
    start = date(y, m, 1)
    end = date(y + (m // 12), (m % 12) + 1, 1) - timedelta(days=1)
    return start, end


def month_key(y, m):
    return f"{y:04d}-{m:02d}"


def parse_month(value):
    try:
        parsed = date.fromisoformat(f"{value}-01")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM") from exc
    return parsed.year, parsed.month


def iter_months(start_year, start_month, end_year, end_month):
    current_year, current_month = start_year, start_month
    while (current_year, current_month) <= (end_year, end_month):
        yield current_year, current_month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1


def cache_month_dir(cache_dir, org, year, month):
    org_uid = str(org) if org else "all"
    return (
        Path(cache_dir)
        / f"organization_uid={org_uid}"
        / f"year={year:04d}"
        / f"month={month:02d}"
    )


def search_cache_path(cache_dir, org, year, month):
    return cache_month_dir(cache_dir, org, year, month) / "search_export.json"


def safe_ada_filename(ada):
    return re.sub(r"[^0-9A-Za-zΑ-Ωα-ωΆ-ώ._-]+", "_", str(ada)).strip("._") or "unknown"


def decision_cache_path(cache_dir, org, year, month, ada):
    return cache_month_dir(cache_dir, org, year, month) / "decisions" / f"{safe_ada_filename(ada)}.json"


def metadata_path(cache_dir, org, year, month):
    return cache_month_dir(cache_dir, org, year, month) / "fetch_metadata.json"


def incomplete_marker_path(cache_dir, org, year, month):
    return cache_month_dir(cache_dir, org, year, month) / "INCOMPLETE"


def detail_failures_path(cache_dir, org, year, month):
    return cache_month_dir(cache_dir, org, year, month) / "detail_fetch_failures.json"


class DiavgeiaFetchError(Exception):
    def __init__(self, message, *, rate_limited=False, status_codes=None, api_calls_attempted=0):
        super().__init__(message)
        self.rate_limited = rate_limited
        self.status_codes = status_codes or []
        self.api_calls_attempted = api_calls_attempted


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_retry_after(value):
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, AttributeError, IndexError, OverflowError):
        return None


def response_text(response):
    return str(getattr(response, "text", "") or "")


def response_headers(response):
    return getattr(response, "headers", {}) or {}


def is_rate_limit_response(response):
    status_code = getattr(response, "status_code", 200)
    if status_code == 429:
        return True
    content = response_text(response).lower()
    try:
        payload = response.json()
    except Exception:
        payload = None
    error_keys = {"error", "errors", "message", "messages", "status", "reason"}
    if isinstance(payload, dict):
        content = " ".join([content] + [str(payload.get(key, "")).lower() for key in error_keys])
    rate_limit_markers = (
        "rate limit",
        "rate-limit",
        "too many requests",
        "threshold",
        "quota",
        "throttl",
        "429",
        "όριο",
        "πολλά αιτήματα",
    )
    has_marker = any(marker in content for marker in rate_limit_markers)
    if status_code in {403, 408, 425, 500, 502, 503, 504} and has_marker:
        return True
    return status_code == 200 and isinstance(payload, dict) and bool(error_keys & payload.keys()) and has_marker


def get_json_with_retries(url, *, params=None, timeout=60, max_retries=3, retry_sleep_seconds=5):
    status_codes = []
    attempts = 0
    last_error = None
    saw_rate_limit = False
    for retry_index in range(max_retries + 1):
        attempts += 1
        response = requests.get(url, params=params, timeout=timeout)
        status_code = getattr(response, "status_code", 200)
        status_codes.append(status_code)
        if is_rate_limit_response(response):
            saw_rate_limit = True
            last_error = DiavgeiaFetchError(
                f"Diavgeia API rate limited request with HTTP {status_code}",
                rate_limited=True,
                status_codes=status_codes,
                api_calls_attempted=attempts,
            )
            if retry_index < max_retries:
                retry_after = parse_retry_after(response_headers(response).get("Retry-After"))
                sleep_seconds = retry_after if retry_after is not None else retry_sleep_seconds * (2 ** retry_index)
                time.sleep(sleep_seconds)
                continue
            raise last_error
        try:
            response.raise_for_status()
            payload = response.json()
            return payload, {
                "api_calls_attempted": attempts,
                "http_status_codes": status_codes,
                "api_rate_limited": saw_rate_limit,
            }
        except requests.RequestException as exc:
            last_error = exc
            if retry_index < max_retries and status_code in {408, 425, 500, 502, 503, 504}:
                time.sleep(retry_sleep_seconds * (2 ** retry_index))
                continue
            raise DiavgeiaFetchError(
                f"Diavgeia API request failed with HTTP {status_code}",
                rate_limited=False,
                status_codes=status_codes,
                api_calls_attempted=attempts,
            ) from exc
        except ValueError as exc:
            raise DiavgeiaFetchError(
                "Diavgeia API returned invalid JSON",
                rate_limited=False,
                status_codes=status_codes,
                api_calls_attempted=attempts,
            ) from exc
    raise last_error


def write_fetch_metadata(cache_dir, org, year, month, metadata):
    payload = {
        "org": str(org) if org else None,
        "year": year,
        "month": month,
        "cache_hit": False,
        "force_refresh": False,
        "api_calls_attempted": 0,
        "api_rate_limited": False,
        "http_status_codes": [],
        "fetch_status": "failed",
        "fetched_at": utc_now_iso(),
    }
    payload.update(metadata)
    write_json(metadata_path(cache_dir, org, year, month), payload)


def write_incomplete_marker(cache_dir, org, year, month, reason):
    path = incomplete_marker_path(cache_dir, org, year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{utc_now_iso()} {reason}\n", encoding="utf-8")


def clear_incomplete_marker(cache_dir, org, year, month):
    path = incomplete_marker_path(cache_dir, org, year, month)
    if path.exists():
        path.unlink()


def append_detail_failure(cache_dir, org, year, month, failure):
    path = detail_failures_path(cache_dir, org, year, month)
    failures = read_json(path) if path.exists() else []
    failures.append(failure)
    write_json(path, failures)


def extract_export_rows(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("decisionResultList", "decisions", "diavgeia_decisions"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    decision_results = payload.get("decisionResults")
    if isinstance(decision_results, dict):
        rows = decision_results.get("decision") or decision_results.get("decisions") or []
        if isinstance(rows, list):
            return rows
    return []


def fetch_export_payload(
    issue_from,
    issue_to,
    org=None,
    limit=5000,
    *,
    max_retries=3,
    retry_sleep_seconds=5,
    return_stats=False,
):
    q = []
    if org:
        q.append(f'organizationUid:"{org}"')
    q.append(f"issueDate:[DT({issue_from}T00:00:00) TO DT({issue_to}T23:59:59)]")
    query = " AND ".join(q)
    rows, page, remaining = [], 0, limit
    stats = {"api_calls_attempted": 0, "api_rate_limited": False, "http_status_codes": []}
    while remaining > 0:
        params = {
            "q": query,
            "sort": "recent",
            "wt": "json",
            "page": page,
            "size": min(remaining, 500),
        }
        try:
            data, request_stats = get_json_with_retries(
                BASE,
                params=params,
                timeout=60,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            )
        except DiavgeiaFetchError as exc:
            stats["api_calls_attempted"] += exc.api_calls_attempted
            stats["http_status_codes"].extend(exc.status_codes)
            stats["api_rate_limited"] = stats["api_rate_limited"] or exc.rate_limited
            exc.api_calls_attempted = stats["api_calls_attempted"]
            exc.status_codes = stats["http_status_codes"]
            exc.rate_limited = stats["api_rate_limited"]
            raise
        stats["api_calls_attempted"] += request_stats["api_calls_attempted"]
        stats["http_status_codes"].extend(request_stats["http_status_codes"])
        stats["api_rate_limited"] = stats["api_rate_limited"] or request_stats["api_rate_limited"]
        batch = extract_export_rows(data)
        if not batch:
            break
        rows.extend(batch)
        remaining -= len(batch)
        if len(batch) < params["size"]:
            break
        page += 1
    payload = {"decisionResultList": rows}
    if return_stats:
        return payload, stats
    return payload


def fetch_export(issue_from, issue_to, org=None, limit=5000, max_retries=3, retry_sleep_seconds=5):
    return pd.DataFrame(
        extract_export_rows(
            fetch_export_payload(
                issue_from,
                issue_to,
                org,
                limit,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            )
        )
    )


def fetch_month_export(
    cache_dir,
    org,
    year,
    month,
    force_refresh=False,
    limit=5000,
    max_retries=3,
    retry_sleep_seconds=5,
    detail_enrichment=None,
):
    path = search_cache_path(cache_dir, org, year, month)
    if path.exists() and not force_refresh:
        payload = read_json(path)
        metadata = {
            "cache_hit": True,
            "force_refresh": force_refresh,
            "fetch_status": "cache_hit",
        }
        if detail_enrichment is not None:
            metadata["detail_enrichment"] = detail_enrichment
        write_fetch_metadata(cache_dir, org, year, month, metadata)
        return pd.DataFrame(extract_export_rows(payload))

    start, end = month_bounds(year, month)
    try:
        payload, stats = fetch_export_payload(
            dtfmt(start),
            dtfmt(end),
            org,
            limit=limit,
            max_retries=max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
            return_stats=True,
        )
    except DiavgeiaFetchError as exc:
        status = "rate_limited" if exc.rate_limited else "failed"
        metadata = {
            "cache_hit": path.exists(),
            "force_refresh": force_refresh,
            "api_calls_attempted": exc.api_calls_attempted,
            "api_rate_limited": exc.rate_limited,
            "http_status_codes": exc.status_codes,
            "fetch_status": status,
        }
        if detail_enrichment is not None:
            metadata["detail_enrichment"] = detail_enrichment
        write_fetch_metadata(cache_dir, org, year, month, metadata)
        write_incomplete_marker(cache_dir, org, year, month, status)
        if path.exists():
            return pd.DataFrame(extract_export_rows(read_json(path)))
        return pd.DataFrame()

    write_json(path, payload)
    clear_incomplete_marker(cache_dir, org, year, month)
    metadata = {
        "cache_hit": False,
        "force_refresh": force_refresh,
        "api_calls_attempted": stats["api_calls_attempted"],
        "api_rate_limited": stats["api_rate_limited"],
        "http_status_codes": stats["http_status_codes"],
        "fetch_status": "success",
    }
    if detail_enrichment is not None:
        metadata["detail_enrichment"] = detail_enrichment
    write_fetch_metadata(cache_dir, org, year, month, metadata)
    return pd.DataFrame(extract_export_rows(payload))


def fetch_period_months(
    cache_dir,
    org,
    start,
    end,
    force_refresh=False,
    limit=5000,
    max_retries=3,
    retry_sleep_seconds=5,
):
    frames = [
        fetch_month_export(
            cache_dir,
            org,
            year,
            month,
            force_refresh=force_refresh,
            limit=limit,
            max_retries=max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )
        for year, month in iter_months(start.year, start.month, end.year, end.month)
    ]
    non_empty = [df for df in frames if not df.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()


def fetch_decision_detail(ada, max_retries=3, retry_sleep_seconds=5):
    payload, _stats = get_json_with_retries(
        f"{DETAIL_BASE}/{quote(str(ada), safe='')}",
        timeout=30,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    return payload


def fetch_cached_decision_detail(
    cache_dir,
    org,
    year,
    month,
    ada,
    force_refresh=False,
    max_retries=3,
    retry_sleep_seconds=5,
):
    path = decision_cache_path(cache_dir, org, year, month, ada)
    if path.exists() and not force_refresh:
        return read_json(path)
    payload = fetch_decision_detail(ada, max_retries=max_retries, retry_sleep_seconds=retry_sleep_seconds)
    write_json(path, payload)
    return payload


def enrich_current_month_details(
    df,
    cache_dir,
    org,
    year,
    month,
    force_refresh=False,
    max_retries=3,
    retry_sleep_seconds=5,
):
    """Cache raw decision detail responses for each ADA and lightly enrich rows.

    The digest keeps using the export rows as its primary source, but this step
    turns the monthly run into a reusable ingestion pass by persisting the raw
    detail payload under decisions/<ADA>.json and filling a few missing fields
    when Diavgeia returns them in the detail response.
    """
    if df.empty or "ada" not in df.columns:
        return df

    detail_fields = ("subject", "protocolNumber", "decisionTypeUid", "documentUrl")
    rows = []
    for _, row in df.iterrows():
        record = row.to_dict()
        ada = record.get("ada")
        if ada:
            try:
                detail = fetch_cached_decision_detail(
                    cache_dir,
                    org,
                    year,
                    month,
                    ada,
                    force_refresh,
                    max_retries=max_retries,
                    retry_sleep_seconds=retry_sleep_seconds,
                )
            except DiavgeiaFetchError as exc:
                append_detail_failure(
                    cache_dir,
                    org,
                    year,
                    month,
                    {
                        "ada": str(ada),
                        "fetch_status": "rate_limited" if exc.rate_limited else "failed",
                        "api_rate_limited": exc.rate_limited,
                        "api_calls_attempted": exc.api_calls_attempted,
                        "http_status_codes": exc.status_codes,
                        "fetched_at": utc_now_iso(),
                        "error": str(exc),
                    },
                )
                if exc.rate_limited:
                    write_incomplete_marker(cache_dir, org, year, month, "detail_rate_limited")
                detail = None
            if isinstance(detail, dict):
                for field in detail_fields:
                    if not record.get(field) and detail.get(field):
                        record[field] = detail[field]
        rows.append(record)
    return pd.DataFrame(rows)


def parse_dates(df):
    if "issueDate" not in df.columns:
        df["issueDate"] = pd.NA
    if "submissionTimestamp" not in df.columns:
        df["submissionTimestamp"] = pd.NA
    to_dt = lambda s, fmt: pd.to_datetime(s, format=fmt, errors="coerce")
    df["issue_dt"] = df["issueDate"].apply(lambda s: to_dt(s, "%d/%m/%Y %H:%M:%S"))
    df["subm_dt"] = df["submissionTimestamp"].apply(lambda s: to_dt(s, "%d/%m/%Y %H:%M:%S"))
    df["delay_days"] = (df["subm_dt"] - df["issue_dt"]).dt.total_seconds() / 86400
    return df


def kpis(df):
    if df.empty:
        return {"count": 0, "median": math.nan, "p90": math.nan, "miss_pub": math.nan, "miss_org": math.nan}
    return {
        "count": int(len(df)),
        "median": float(df["delay_days"].median(skipna=True)),
        "p90": float(df["delay_days"].quantile(0.9)),
        "miss_pub": float(df["publishTimestamp"].isna().mean() * 100 if "publishTimestamp" in df else math.nan),
        "miss_org": float(df["organizationLabel"].isna().mean() * 100 if "organizationLabel" in df else math.nan),
    }


def safe_kpis(df):
    if df is None or getattr(df, "empty", True):
        return kpis(pd.DataFrame())
    return kpis(df)


def build_scopes(cur=None, prv=None, ytd=None, ypr=None, ymo=None):
    scopes = {"cur": cur, "prv": prv, "ytd": ytd, "ypr": ypr, "ymo": ymo}
    for key, df in scopes.items():
        if df is None or not isinstance(df, pd.DataFrame):
            scopes[key] = pd.DataFrame()
    return scopes


def pct(cur, prev):
    if prev in (None, 0) or pd.isna(prev):
        return math.nan
    return (cur - prev) / prev * 100.0


def render_html(ctx):
    def fmt(x):
        if x is None or (isinstance(x, float) and (pd.isna(x) or math.isinf(x))):
            return "—"
        return f"{x:.2f}" if isinstance(x, float) else str(x)

    m, pm, ytd, ytd_prev, yoy = ctx["labels"]
    mk, pk, yk, ypk, ymk = ctx["kpi"]
    mix = ctx.get("mix", [])
    outliers = ctx.get("outliers", pd.DataFrame())
    recent = ctx.get("recent", [])
    trend = ctx.get("trend", {"count": {}, "median": {}})

    html = []
    html.append("<!doctype html>")
    html.append("<html><head><meta charset='utf-8'><title>Diavgeia Digest</title></head>")
    html.append("<body style='font:14px -apple-system,Segoe UI,Roboto,Helvetica,Arial;color:#222;margin:24px'>")
    html.append(f"<h2 style='margin:0 0 16px'>Diavgeia Digest — {m}</h2>")

    html.append("<h3 style='margin:24px 0 8px'>Overview</h3>")
    html.append("<table style='border-collapse:collapse'>")

    def row(k, v):
        html.append(
            f"<tr><td style='padding:4px 12px 4px 0;color:#555'>{k}</td>"
            f"<td style='padding:4px 0;font-weight:600;text-align:right'>{v}</td></tr>"
        )

    row("Decisions (Month)", mk["count"])
    row("Median delay (days)", fmt(mk["median"]))
    row("P90 delay (days)", fmt(mk["p90"]))
    row(f"MoM change vs {pm} (count)", fmt(mk["count"] - pk["count"]))
    row("MoM change (median delay, %)", fmt(pct(mk["median"], pk["median"])))
    row(f"YTD decisions ({ytd})", yk["count"])
    row("YoY (YTD) change (%)", fmt(pct(yk["count"], ypk["count"])))
    row("YoY (month) change (count)", fmt(mk["count"] - ymk["count"]))
    row("Missing publishTimestamp (month, %)", fmt(mk["miss_pub"]))
    row("Missing organization (month, %)", fmt(mk["miss_org"]))
    row("Trend (count) — M-1 / M-2 / M-3", f"{trend['count'].get('m1', 0)} / {trend['count'].get('m2', 0)} / {trend['count'].get('m3', 0)}")
    row("Trend (count avg) — Av6M / Av12M", f"{trend['count'].get('avg6', 0)} / {trend['count'].get('avg12', 0)}")
    row("Trend (median days) — M-1 / M-2 / M-3", f"{trend['median'].get('m1', 0.0)} / {trend['median'].get('m2', 0.0)} / {trend['median'].get('m3', 0.0)}")
    row("Trend (median days avg) — Av6M / Av12M", f"{trend['median'].get('avg6', 0.0)} / {trend['median'].get('avg12', 0.0)}")
    html.append("</table>")

    if mix:
        html.append("<h3 style='margin:24px 0 8px'>Decision type mix (month)</h3><ul>")
        for code, label, p in mix:
            label_txt = f" — {label}" if label else ""
            html.append(f"<li><b>{code}</b>{label_txt}: {p:.1f}%</li>")
        html.append("</ul>")

    if recent:
        html.append("<h3 style='margin:24px 0 8px'>Recent months</h3>")
        html.append("<table style='border-collapse:collapse;width:100%;font-size:14px'>")
        html.append("<tr><th style='text-align:left;padding:6px;border-bottom:1px solid #eee'>Month</th>"
                    "<th style='text-align:right;padding:6px;border-bottom:1px solid #eee'>Decisions</th>"
                    "<th style='text-align:right;padding:6px;border-bottom:1px solid #eee'>Median days</th></tr>")
        for r in recent:
            html.append(f"<tr><td style='padding:6px'>{r['month']}</td>"
                        f"<td style='padding:6px;text-align:right'>{int(r['count'])}</td>"
                        f"<td style='padding:6px;text-align:right'>{float(r['median']):.2f}</td></tr>")
        html.append("</table>")

    if isinstance(outliers, pd.DataFrame) and not outliers.empty:
        html.append("<h3 style='margin:24px 0 8px'>Slowest decisions (top 10)</h3><ol>")
        for _, r in outliers.iterrows():
            link = r.get("documentUrl") or "#"
            code = r.get("decisionTypeUid", "")
            delay = r.get("delay_days", 0.0)
            subj = str(r.get("subject", ""))[:120]
            ada = r.get("ada", "")
            html.append(f"<li><a href='{link}'>{ada}</a> — {code} — {delay:.2f}d — {subj}</li>")
        html.append("</ol>")

    html.append("<p style='color:#777;margin-top:16px'>Source: diavgeia.gov.gr export API • issueDate window</p>")
    html.append("</body></html>")
    return "".join(html)


def run_search_only_month(args, year, month):
    fetch_month_export(
        args.cache_dir,
        args.org,
        year,
        month,
        args.force_refresh,
        max_retries=args.max_retries,
        retry_sleep_seconds=args.retry_sleep_seconds,
        detail_enrichment="skipped",
    )
    print(f"Cached search export for {month_key(year, month)}")


def run_monthly_digest(args, year, month):
    if getattr(args, "search_only", False):
        run_search_only_month(args, year, month)
        return

    mo_start, mo_end = month_bounds(year, month)
    prev_start, prev_end = month_bounds(year if month > 1 else year - 1, (month - 1 or 12))
    ytd_start = date(year, 1, 1)

    last_year = year - 1
    ytd_prev_start = date(last_year, 1, 1)
    ytd_prev_end = date(last_year, month, month_bounds(last_year, month)[1].day)
    yoy_mo_start, yoy_mo_end = month_bounds(last_year, month)

    cur = prv = ytd = ypr = ymo = pd.DataFrame()
    debug = os.getenv("DEBUG")

    fetches = [
        ("current month", "cur", mo_start, mo_end),
        ("previous month", "prv", prev_start, prev_end),
        ("YTD", "ytd", ytd_start, mo_end),
        ("YTD previous year", "ypr", ytd_prev_start, ytd_prev_end),
        ("YoY month", "ymo", yoy_mo_start, yoy_mo_end),
    ]
    fetched = {}
    for label, key, start, end in fetches:
        try:
            fetched[key] = fetch_period_months(
                args.cache_dir,
                args.org,
                start,
                end,
                args.force_refresh,
                max_retries=args.max_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
        except Exception as e:
            if debug:
                print(f"[DEBUG] Failed to fetch {label}: {e}")
            fetched[key] = pd.DataFrame()

    cur = fetched["cur"]
    if not cur.empty:
        try:
            cur = enrich_current_month_details(
                cur,
                args.cache_dir,
                args.org,
                year,
                month,
                args.force_refresh,
                max_retries=args.max_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
        except Exception as e:
            if debug:
                print(f"[DEBUG] Failed to cache/enrich decision details: {e}")

    scopes = build_scopes(cur, fetched["prv"], fetched["ytd"], fetched["ypr"], fetched["ymo"])

    if debug:
        for name, df in scopes.items():
            rows = len(df) if isinstance(df, pd.DataFrame) else 0
            print(f"[DEBUG] {name} populated: {isinstance(df, pd.DataFrame)} rows={rows}")

    for df in scopes.values():
        if not df.empty:
            if "organizationLabel" in df.columns and "organizationName" not in df.columns:
                df.rename(columns={"organizationLabel": "organizationName"}, inplace=True)
            parse_dates(df)

    mk = safe_kpis(scopes["cur"])
    pk = safe_kpis(scopes["prv"])
    yk = safe_kpis(scopes["ytd"])
    ypk = safe_kpis(scopes["ypr"])
    ymk = safe_kpis(scopes["ymo"])

    cur_scope = scopes["cur"]
    if not cur_scope.empty and "decisionTypeUid" in cur_scope.columns:
        mix_series = cur_scope["decisionTypeUid"].value_counts(normalize=True).head(5).mul(100).round(1)
        mix = [(code, decision_map.get(code, ""), float(p)) for code, p in mix_series.items()]
    else:
        mix = []

    monthly_out = Path(OUT) / (str(args.org) if args.org else "all") / month_key(year, month)
    output_dirs = [Path(OUT), monthly_out]
    unknown_codes = [code for (code, label, _) in mix if not label]
    if unknown_codes:
        for output_dir in output_dirs:
            output_dir.mkdir(parents=True, exist_ok=True)
            (
                pd.Series(unknown_codes, name="unmapped_code")
                .value_counts()
                .rename_axis("code")
                .reset_index(name="mentions")
                .to_csv(output_dir / "unmapped_codes.csv", index=False)
            )

    recent = []
    trend = {"count": {}, "median": {}}
    ytd_scope = scopes["ytd"]
    if not ytd_scope.empty and "issue_dt" in ytd_scope.columns:
        df = ytd_scope.copy()
        df["month_key"] = df["issue_dt"].dt.strftime("%Y-%m")
        monthly = df.groupby("month_key").agg(count=("ada", "count"), median=("delay_days", "median")).sort_index()

        last6, last12 = monthly.tail(6), monthly.tail(12)
        trend["count"]["avg6"] = int(last6["count"].mean()) if len(last6) else 0
        trend["count"]["avg12"] = int(last12["count"].mean()) if len(last12) else 0
        trend["median"]["avg6"] = round(float(last6["median"].mean()), 2) if len(last6) else 0.0
        trend["median"]["avg12"] = round(float(last12["median"].mean()), 2) if len(last12) else 0.0

        tail = monthly.tail(4).to_dict("records")

        def rel(idx, key):
            return tail[-(idx + 2)][key] if len(tail) >= (idx + 2) else 0

        trend["count"].update({"m1": int(rel(0, "count")), "m2": int(rel(1, "count")), "m3": int(rel(2, "count"))})
        trend["median"].update({"m1": round(float(rel(0, "median")), 2), "m2": round(float(rel(1, "median")), 2), "m3": round(float(rel(2, "median")), 2)})

        recent_rows = monthly.tail(6).reset_index()
        recent = [{"month": r["month_key"], "count": int(r["count"]), "median": float(r["median"])} for _, r in recent_rows.iterrows()]

    cols = ["ada", "organizationUid", "organizationName", "decisionTypeUid", "issueDate", "submissionTimestamp", "documentUrl", "delay_days", "subject"]
    if not cur_scope.empty:
        for col in cols:
            if col not in cur_scope.columns:
                cur_scope[col] = pd.NA
        outliers = cur_scope.sort_values("delay_days", ascending=False).drop_duplicates(subset=["ada"])[cols].head(10)
    else:
        outliers = pd.DataFrame()

    html = render_html({
        "labels": (
            mo_start.strftime("%B %Y"),
            prev_start.strftime("%B %Y"),
            f"{ytd_start} → {mo_end}",
            f"{ytd_prev_start} → {ytd_prev_end}",
            yoy_mo_start.strftime("%B %Y"),
        ),
        "kpi": (mk, pk, yk, ypk, ymk),
        "mix": mix,
        "outliers": outliers,
        "recent": recent,
        "trend": trend,
    })

    for output_dir in output_dirs:
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_dir / "digest.html", "w", encoding="utf-8") as f:
            f.write(html)
        cur_scope.to_csv(output_dir / "raw_month.csv", index=False)
        outliers.to_csv(output_dir / "outliers.csv", index=False)

    print(f"Wrote {Path(OUT) / 'digest.html'}")
    print(f"Wrote {monthly_out / 'digest.html'}")


def build_parser():
    ap = argparse.ArgumentParser(description="Build Monthly Diavgeia Digest")
    ap.add_argument("--org", help="organizationUid (optional)")
    ap.add_argument("--year", type=int)
    ap.add_argument("--month", type=int)
    ap.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR, help="Raw Diavgeia cache directory")
    ap.add_argument("--force-refresh", action="store_true", help="Ignore cached API responses and refetch")
    ap.add_argument(
        "--search-only",
        action="store_true",
        help="Only fetch/cache monthly search_export.json files; skip per-ADA detail enrichment and digest rendering",
    )
    ap.add_argument("--max-retries", type=int, default=3, help="Maximum retries after Diavgeia rate-limit responses")
    ap.add_argument("--retry-sleep-seconds", type=float, default=5, help="Initial sleep between Diavgeia retries")
    ap.add_argument("--from", dest="from_month", type=parse_month, help="Start month for historical backfill (YYYY-MM)")
    ap.add_argument("--to", dest="to_month", type=parse_month, help="End month for historical backfill (YYYY-MM)")
    return ap


def main():
    ap = build_parser()
    args = ap.parse_args()

    if bool(args.from_month) != bool(args.to_month):
        ap.error("--from and --to must be provided together")

    if args.from_month and args.to_month:
        if args.from_month > args.to_month:
            ap.error("--from must be earlier than or equal to --to")
        for year, month in iter_months(*args.from_month, *args.to_month):
            run_monthly_digest(args, year, month)
        return

    today = date.today()
    year = args.year or (today.year if today.month > 1 else today.year - 1)
    month = args.month or (today.month - 1 or 12)
    if not 1 <= month <= 12:
        ap.error("--month must be between 1 and 12")
    run_monthly_digest(args, year, month)


if __name__ == "__main__":
    main()
