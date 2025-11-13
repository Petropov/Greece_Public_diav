#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

BASE_URL = "https://diavgeia.gov.gr/luminapi/api/search/export"
ARTIFACT_DIR = "artifacts"
LABELS_FILE = "decision_labels.json"
REGION_MAP_FILE = "org_region_map.csv"
PAGE_SIZE = 500


def load_decision_labels() -> Dict[str, str]:
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
    if os.path.exists(LABELS_FILE):
        try:
            with open(LABELS_FILE, "r", encoding="utf-8") as handle:
                decision_map.update(json.load(handle))
        except json.JSONDecodeError:
            pass
    return decision_map


def month_bounds(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    next_month = date(year + (month // 12), (month % 12) + 1, 1)
    end = next_month - timedelta(days=1)
    return start, end


def fetch_range(issue_from: date, issue_to: date, org: Optional[str]) -> pd.DataFrame:
    query_parts = [
        f'issueDate:[DT({issue_from.strftime("%Y-%m-%d")}T00:00:00) TO DT({issue_to.strftime("%Y-%m-%d")}T23:59:59)]'
    ]
    if org:
        query_parts.append(f'organizationUid:"{org}"')
    query = " AND ".join(query_parts)

    rows: List[dict] = []
    page = 0
    while True:
        params = {
            "q": query,
            "wt": "json",
            "sort": "recent",
            "page": page,
            "size": PAGE_SIZE,
        }
        response = requests.get(BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        batch = data.get("decisionResultList") or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return pd.DataFrame(rows)


def fetch_decisions(start: date, end: date, org: Optional[str], chunk_by_day: bool) -> pd.DataFrame:
    if chunk_by_day:
        ranges = [
            (start + timedelta(days=i), start + timedelta(days=i))
            for i in range((end - start).days + 1)
        ]
    else:
        ranges = [(start, end)]
    frames = []
    for rng_start, rng_end in ranges:
        df = fetch_range(rng_start, rng_end, org)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    to_dt = lambda value, fmt: pd.to_datetime(value, format=fmt, errors="coerce")
    df["issue_dt"] = df["issueDate"].apply(lambda value: to_dt(value, "%d/%m/%Y %H:%M:%S"))
    df["subm_dt"] = df["submissionTimestamp"].apply(lambda value: to_dt(value, "%d/%m/%Y %H:%M:%S"))
    df["delay_days"] = (df["subm_dt"] - df["issue_dt"]).dt.total_seconds() / 86400
    return df


def compute_kpis(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "count": 0,
            "median": math.nan,
            "p90": math.nan,
            "miss_pub": math.nan,
            "miss_org": math.nan,
        }
    return {
        "count": int(len(df)),
        "median": float(df["delay_days"].median(skipna=True)),
        "p90": float(df["delay_days"].quantile(0.9)),
        "miss_pub": float(df["publishTimestamp"].isna().mean() * 100 if "publishTimestamp" in df else math.nan),
        "miss_org": float(df["organizationLabel"].isna().mean() * 100 if "organizationLabel" in df else math.nan),
    }


def pct_change(cur: float, prev: float) -> float:
    if prev in (None, 0) or (isinstance(prev, float) and math.isnan(prev)):
        return math.nan
    return (cur - prev) / prev * 100.0


REGION_ALIASES: Dict[str, str] = {
    "ΑΤΤΙΚΗ": "Αττική",
    "ΚΕΝΤΡΙΚΗ ΜΑΚΕΔΟΝΙΑ": "Κεντρική Μακεδονία",
    "ΔΥΤΙΚΗ ΜΑΚΕΔΟΝΙΑ": "Δυτική Μακεδονία",
    "ΔΥΤΙΚΗ ΕΛΛΑΔΑ": "Δυτική Ελλάδα",
    "ΑΝΑΤΟΛΙΚΗ ΜΑΚΕΔΟΝΙΑ ΚΑΙ ΘΡΑΚΗ": "Ανατολική Μακεδονία και Θράκη",
    "ΘΕΣΣΑΛΙΑ": "Θεσσαλία",
    "ΠΕΛΟΠΟΝΝΗΣΟΣ": "Πελοπόννησος",
    "ΗΠΕΙΡΟΣ": "Ήπειρος",
    "ΙΟΝΙΑ ΝΗΣΙΑ": "Ιόνια Νησιά",
    "ΝΟΤΙΟ ΑΙΓΑΙΟ": "Νότιο Αιγαίο",
    "ΒΟΡΕΙΟ ΑΙΓΑΙΟ": "Βόρειο Αιγαίο",
    "ΣΤΕΡΕΑ ΕΛΛΑΔΑ": "Στερεά Ελλάδα",
    "ΚΡΗΤΗ": "Κρήτη",
}

REGION_KEYWORDS: List[Tuple[str, str]] = [
    ("ΑΘΗΝ", "Αττική"),
    ("ΠΕΙΡ", "Αττική"),
    ("ΜΑΡΟΥΣΙ", "Αττική"),
    ("ΘΕΣΣΑΛΟΝΙΚ", "Κεντρική Μακεδονία"),
    ("ΣΕΡΡ", "Κεντρική Μακεδονία"),
    ("ΚΑΒΑΛ", "Ανατολική Μακεδονία και Θράκη"),
    ("ΚΟΜΟΤΗΝ", "Ανατολική Μακεδονία και Θράκη"),
    ("ΞΑΝΘ", "Ανατολική Μακεδονία και Θράκη"),
    ("ΠΑΤΡ", "Δυτική Ελλάδα"),
    ("ΑΓΡΙΝ", "Δυτική Ελλάδα"),
    ("ΙΩΑΝΝ", "Ήπειρος"),
    ("ΛΑΡΙΣ", "Θεσσαλία"),
    ("ΒΟΛ", "Θεσσαλία"),
    ("ΤΡΙΚ", "Θεσσαλία"),
    ("ΚΕΡΚΥΡ", "Ιόνια Νησιά"),
    ("ΖΑΚΥΝ", "Ιόνια Νησιά"),
    ("ΛΕΥΚΑΔ", "Ιόνια Νησιά"),
    ("ΡΟΔ", "Νότιο Αιγαίο"),
    ("ΣΥΡ", "Νότιο Αιγαίο"),
    ("ΚΩ", "Νότιο Αιγαίο"),
    ("ΜΥΤΙΛ", "Βόρειο Αιγαίο"),
    ("ΧΙ", "Βόρειο Αιγαίο"),
    ("ΗΡΑΚΛΕΙ", "Κρήτη"),
    ("ΧΑΝΙ", "Κρήτη"),
    ("ΡΕΘΥΜ", "Κρήτη"),
    ("ΛΑΣΙΘ", "Κρήτη"),
    ("ΤΡΙΠΟΛ", "Πελοπόννησος"),
    ("ΚΑΛΑΜΑΤ", "Πελοπόννησος"),
    ("ΚΟΡΙΝΘ", "Πελοπόννησος"),
    ("ΚΑΡΔΙΤ", "Θεσσαλία"),
    ("ΚΟΖΑΝ", "Δυτική Μακεδονία"),
    ("ΦΛΩΡΙΝ", "Δυτική Μακεδονία"),
]


def load_region_mapping() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if os.path.exists(REGION_MAP_FILE):
        with open(REGION_MAP_FILE, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                org_uid = (row.get("organizationUid") or "").strip()
                region = (row.get("region") or "").strip()
                if org_uid and region:
                    mapping[org_uid] = normalize_region(region)
    return mapping


def normalize_region(value: str) -> str:
    upper = value.strip().upper()
    if not upper:
        return "Άγνωστη"
    for alias, canonical in REGION_ALIASES.items():
        if alias in upper:
            return canonical
    return value.strip() or "Άγνωστη"


def infer_region(row: pd.Series, mapping: Dict[str, str]) -> str:
    org_uid = row.get("organizationUid")
    if isinstance(org_uid, str) and org_uid in mapping:
        return mapping[org_uid]

    for key in row.keys():
        if "region" in key.lower():
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return normalize_region(value)

    for field in ("organizationName", "organizationLabel", "subject"):
        value = row.get(field)
        if isinstance(value, str):
            upper = value.upper()
            for needle, region in REGION_KEYWORDS:
                if needle in upper:
                    return region

    return "Άγνωστη"


def enrich_regions(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        df["region"] = pd.Series(dtype=str)
        return df
    df["region"] = df.apply(lambda row: infer_region(row, mapping), axis=1)
    return df


def compute_recent_months(df: pd.DataFrame) -> List[Dict[str, float]]:
    if df.empty:
        return []
    df = df.copy()
    df["month_key"] = df["issue_dt"].dt.strftime("%Y-%m")
    monthly = (
        df.groupby("month_key")
        .agg(count=("ada", "count"), median=("delay_days", "median"))
        .sort_index()
    )
    recent_rows = monthly.tail(6).reset_index()
    return [
        {
            "month": row["month_key"],
            "count": int(row["count"]),
            "median": float(row["median"]),
        }
        for _, row in recent_rows.iterrows()
    ]


def compute_trend_stats(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    trend = {"count": {}, "median": {}}
    if df.empty:
        return trend
    df = df.copy()
    df["month_key"] = df["issue_dt"].dt.strftime("%Y-%m")
    monthly = (
        df.groupby("month_key")
        .agg(count=("ada", "count"), median=("delay_days", "median"))
        .sort_index()
    )
    last6, last12 = monthly.tail(6), monthly.tail(12)
    trend["count"]["avg6"] = int(last6["count"].mean()) if len(last6) else 0
    trend["count"]["avg12"] = int(last12["count"].mean()) if len(last12) else 0
    trend["median"]["avg6"] = round(float(last6["median"].mean()), 2) if len(last6) else 0.0
    trend["median"]["avg12"] = round(float(last12["median"].mean()), 2) if len(last12) else 0.0

    tail = monthly.tail(4).to_dict("records")
    def rel(index: int, key: str) -> float:
        if len(tail) >= index + 2:
            return tail[-(index + 2)][key]
        return 0

    trend["count"].update({
        "m1": int(rel(0, "count")),
        "m2": int(rel(1, "count")),
        "m3": int(rel(2, "count")),
    })
    trend["median"].update({
        "m1": round(float(rel(0, "median")), 2),
        "m2": round(float(rel(1, "median")), 2),
        "m3": round(float(rel(2, "median")), 2),
    })
    return trend


def compute_mix(df: pd.DataFrame, decision_labels: Dict[str, str]) -> List[Tuple[str, str, float]]:
    if df.empty or "decisionTypeUid" not in df.columns:
        return []
    mix_series = (
        df["decisionTypeUid"].value_counts(normalize=True).head(5).mul(100).round(1)
    )
    mix = [
        (code, decision_labels.get(code, ""), float(percent))
        for code, percent in mix_series.items()
    ]
    unknown_codes = [code for code, label, _ in mix if not label]
    if unknown_codes:
        os.makedirs(ARTIFACT_DIR, exist_ok=True)
        (
            pd.Series(unknown_codes, name="unmapped_code")
            .value_counts()
            .rename_axis("code")
            .reset_index(name="mentions")
            .to_csv(os.path.join(ARTIFACT_DIR, "unmapped_codes.csv"), index=False)
        )
    return mix


def compute_outliers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    cols = [
        "ada",
        "organizationUid",
        "organizationName",
        "decisionTypeUid",
        "issueDate",
        "submissionTimestamp",
        "documentUrl",
        "delay_days",
        "subject",
    ]
    subset = df.sort_values("delay_days", ascending=False).drop_duplicates(subset=["ada"])
    existing_cols = [c for c in cols if c in subset.columns]
    return subset[existing_cols].head(10)


def compute_regional_trends(df: pd.DataFrame, months: int) -> Tuple[List[Dict[str, str]], pd.DataFrame]:
    if df.empty:
        return [], pd.DataFrame(columns=["month", "region", "count", "median_delay"])

    df = df.copy()
    df["month_period"] = df["issue_dt"].dt.to_period("M")
    end_period = df["month_period"].max()
    if pd.isna(end_period):
        return [], pd.DataFrame(columns=["month", "region", "count", "median_delay"])

    cutoff_period = end_period - (months - 1)
    filtered = df[df["month_period"] >= cutoff_period]

    monthly_region = (
        filtered.groupby(["month_period", "region"])
        .agg(count=("ada", "count"), median_delay=("delay_days", "median"))
        .reset_index()
    )
    monthly_region["month"] = monthly_region["month_period"].astype(str)
    monthly_region = monthly_region[["month", "region", "count", "median_delay"]]

    agg_region = (
        monthly_region.groupby("region")
        .agg(total_decisions=("count", "sum"), median_delay=("median_delay", "median"))
        .reset_index()
        .sort_values("total_decisions", ascending=False)
    )

    top_regions = []
    for _, row in agg_region.iterrows():
        median_delay = row["median_delay"]
        if pd.isna(median_delay):
            formatted_median = math.nan
        else:
            formatted_median = round(float(median_delay), 2)
        top_regions.append(
            {
                "region": row["region"],
                "total_decisions": int(row["total_decisions"]),
                "median_delay": formatted_median,
            }
        )
    return top_regions, monthly_region


def render_html(context: Dict[str, object]) -> str:
    def fmt(value: object) -> str:
        if value is None:
            return "—"
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return "—"
        if isinstance(value, float):
            return f"{value:.2f}"
        return str(value)

    html: List[str] = []
    html.append("<!doctype html>")
    html.append("<html><head><meta charset='utf-8'><title>Diavgeia Digest</title></head>")
    html.append("<body style='font:14px -apple-system,Segoe UI,Roboto,Helvetica,Arial;color:#222;margin:24px'>")
    html.append(f"<h2 style='margin:0 0 16px'>Diavgeia Digest — {context['labels'][0]}</h2>")

    mk, pk, yk, ypk, ymk = context["kpi"]

    html.append("<h3 style='margin:24px 0 8px'>Overview</h3>")
    html.append("<table style='border-collapse:collapse'>")

    def row(label: str, value: object) -> None:
        html.append(
            f"<tr><td style='padding:4px 12px 4px 0;color:#555'>{label}</td>"
            f"<td style='padding:4px 0;font-weight:600;text-align:right'>{fmt(value)}</td></tr>"
        )

    row("Decisions (Month)", mk["count"])
    row("Median delay (days)", mk["median"])
    row("P90 delay (days)", mk["p90"])
    row(f"MoM change vs {context['labels'][1]} (count)", mk["count"] - pk["count"])
    row("MoM change (median delay, %)", pct_change(mk["median"], pk["median"]))
    row(f"YTD decisions ({context['labels'][2]})", yk["count"])
    row("YoY (YTD) change (%)", pct_change(yk["count"], ypk["count"]))
    row("YoY (month) change (count)", mk["count"] - ymk["count"])
    row("Missing publishTimestamp (month, %)", mk["miss_pub"])
    row("Missing organization (month, %)", mk["miss_org"])

    trend = context.get("trend", {"count": {}, "median": {}})
    row(
        "Trend (count) — M-1 / M-2 / M-3",
        f"{trend['count'].get('m1', 0)} / {trend['count'].get('m2', 0)} / {trend['count'].get('m3', 0)}",
    )
    row(
        "Trend (count avg) — Av6M / Av12M",
        f"{trend['count'].get('avg6', 0)} / {trend['count'].get('avg12', 0)}",
    )
    row(
        "Trend (median days) — M-1 / M-2 / M-3",
        f"{trend['median'].get('m1', 0.0)} / {trend['median'].get('m2', 0.0)} / {trend['median'].get('m3', 0.0)}",
    )
    row(
        "Trend (median days avg) — Av6M / Av12M",
        f"{trend['median'].get('avg6', 0.0)} / {trend['median'].get('avg12', 0.0)}",
    )
    html.append("</table>")

    mix = context.get("mix", [])
    if mix:
        html.append("<h3 style='margin:24px 0 8px'>Decision type mix (month)</h3><ul>")
        for code, label, percent in mix:
            label_text = f" — {label}" if label else ""
            html.append(f"<li><b>{code}</b>{label_text}: {percent:.1f}%</li>")
        html.append("</ul>")

    recent = context.get("recent", [])
    if recent:
        html.append("<h3 style='margin:24px 0 8px'>Recent months</h3>")
        html.append("<table style='border-collapse:collapse;width:100%;font-size:14px'>")
        html.append(
            "<tr><th style='text-align:left;padding:6px;border-bottom:1px solid #eee'>Month</th>"
            "<th style='text-align:right;padding:6px;border-bottom:1px solid #eee'>Decisions</th>"
            "<th style='text-align:right;padding:6px;border-bottom:1px solid #eee'>Median days</th></tr>"
        )
        for row_data in recent:
            html.append(
                f"<tr><td style='padding:6px'>{row_data['month']}</td>"
                f"<td style='padding:6px;text-align:right'>{int(row_data['count'])}</td>"
                f"<td style='padding:6px;text-align:right'>{row_data['median']:.2f}</td></tr>"
            )
        html.append("</table>")

    regional_summary = context.get("regional_summary", [])
    if regional_summary:
        html.append("<h3 style='margin:24px 0 8px'>Regional trends (last 6 months)</h3>")
        html.append("<table style='border-collapse:collapse;width:100%;font-size:14px'>")
        html.append(
            "<tr><th style='text-align:left;padding:6px;border-bottom:1px solid #eee'>Region</th>"
            "<th style='text-align:right;padding:6px;border-bottom:1px solid #eee'>Decisions</th>"
            "<th style='text-align:right;padding:6px;border-bottom:1px solid #eee'>Median days</th></tr>"
        )
        for row_data in regional_summary:
            html.append(
                f"<tr><td style='padding:6px'>{row_data['region']}</td>"
                f"<td style='padding:6px;text-align:right'>{row_data['total_decisions']}</td>"
                f"<td style='padding:6px;text-align:right'>{fmt(row_data['median_delay'])}</td></tr>"
            )
        html.append("</table>")

    outliers = context.get("outliers")
    if isinstance(outliers, pd.DataFrame) and not outliers.empty:
        html.append("<h3 style='margin:24px 0 8px'>Slowest decisions (top 10)</h3><ol>")
        for _, record in outliers.iterrows():
            link = record.get("documentUrl") or "#"
            code = record.get("decisionTypeUid", "")
            delay = record.get("delay_days", 0.0)
            subject = str(record.get("subject", ""))[:120]
            ada = record.get("ada", "")
            html.append(
                f"<li><a href='{link}'>{ada}</a> — {code} — {delay:.2f}d — {subject}</li>"
            )
        html.append("</ol>")

    html.append(
        "<p style='color:#777;margin-top:16px'>Source: diavgeia.gov.gr export API • issueDate window</p>"
    )
    html.append("</body></html>")
    return "".join(html)


def ensure_artifacts_dir() -> None:
    os.makedirs(ARTIFACT_DIR, exist_ok=True)


def write_csv(df: pd.DataFrame, filename: str) -> None:
    ensure_artifacts_dir()
    df.to_csv(os.path.join(ARTIFACT_DIR, filename), index=False)


def determine_target_month(year: Optional[int], month: Optional[int]) -> Tuple[int, int]:
    today = date.today()
    target_year = year
    target_month = month
    if target_year is None or target_month is None:
        prev_month_date = (today.replace(day=1) - timedelta(days=1))
        if target_year is None:
            target_year = prev_month_date.year
        if target_month is None:
            target_month = prev_month_date.month
    return target_year, target_month


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Monthly Diavgeia Digest")
    parser.add_argument("--org", help="organizationUid (optional)")
    parser.add_argument("--year", type=int, help="Target year")
    parser.add_argument("--month", type=int, help="Target month")
    parser.add_argument(
        "--chunk-by-day",
        action="store_true",
        help="Fetch decisions day-by-day (useful when the API is unstable)",
    )
    args = parser.parse_args()

    target_year, target_month = determine_target_month(args.year, args.month)
    month_start, month_end = month_bounds(target_year, target_month)

    prev_year, prev_month = (target_year, target_month - 1) if target_month > 1 else (target_year - 1, 12)
    prev_start, prev_end = month_bounds(prev_year, prev_month)

    last_year = target_year - 1
    ytd_start = date(target_year, 1, 1)
    ytd_prev_start = date(last_year, 1, 1)
    ytd_prev_end = date(last_year, target_month, month_bounds(target_year, target_month)[1].day)
    yoy_month_start, yoy_month_end = month_bounds(last_year, target_month)

    current = fetch_decisions(month_start, month_end, args.org, args.chunk_by_day)
    previous = fetch_decisions(prev_start, prev_end, args.org, args.chunk_by_day)
    ytd = fetch_decisions(ytd_start, month_end, args.org, args.chunk_by_day)
    ytd_previous = fetch_decisions(ytd_prev_start, ytd_prev_end, args.org, args.chunk_by_day)
    yoy_month = fetch_decisions(yoy_month_start, yoy_month_end, args.org, args.chunk_by_day)

    for frame in (current, previous, ytd, ytd_previous, yoy_month):
        if not frame.empty:
            if "organizationLabel" in frame.columns and "organizationName" not in frame.columns:
                frame.rename(columns={"organizationLabel": "organizationName"}, inplace=True)
            parse_dates(frame)

    decision_labels = load_decision_labels()

    kpi_current = compute_kpis(current)
    kpi_previous = compute_kpis(previous)
    kpi_ytd = compute_kpis(ytd)
    kpi_ytd_prev = compute_kpis(ytd_previous)
    kpi_yoy_month = compute_kpis(yoy_month)

    mix = compute_mix(current, decision_labels)
    recent_months = compute_recent_months(ytd)
    trend = compute_trend_stats(ytd)
    outliers = compute_outliers(current)

    region_mapping = load_region_mapping()
    ytd_with_region = enrich_regions(ytd, region_mapping)
    regional_summary, regional_monthly = compute_regional_trends(ytd_with_region, months=6)

    ensure_artifacts_dir()
    html = render_html(
        {
            "labels": (
                month_start.strftime("%B %Y"),
                prev_start.strftime("%B %Y"),
                f"{ytd_start} → {month_end}",
                f"{ytd_prev_start} → {ytd_prev_end}",
                yoy_month_start.strftime("%B %Y"),
            ),
            "kpi": (
                kpi_current,
                kpi_previous,
                kpi_ytd,
                kpi_ytd_prev,
                kpi_yoy_month,
            ),
            "mix": mix,
            "recent": recent_months,
            "trend": trend,
            "outliers": outliers,
            "regional_summary": regional_summary,
        }
    )

    ensure_artifacts_dir()
    with open(os.path.join(ARTIFACT_DIR, "digest.html"), "w", encoding="utf-8") as handle:
        handle.write(html)

    write_csv(current, "raw_month.csv")
    write_csv(outliers, "outliers.csv")
    if not regional_monthly.empty:
        write_csv(regional_monthly, "regional_trends_last6.csv")
    else:
        write_csv(
            pd.DataFrame(columns=["month", "region", "count", "median_delay"]),
            "regional_trends_last6.csv",
        )

    print(f"Wrote {os.path.join(ARTIFACT_DIR, 'digest.html')}")


if __name__ == "__main__":
    main()
