#!/usr/bin/env python3
"""Build the Diavgeia monthly digest HTML and CSV artifacts."""

from __future__ import annotations

import argparse
import math
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests

BASE_URL = "https://diavgeia.gov.gr/luminapi/api/search/export"
ARTIFACT_DIR = "artifacts"
PAGE_SIZE = 500
MAX_RESULTS = 5000

DECISION_LABELS: Dict[str, str] = {
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


@dataclass(frozen=True)
class DigestWindow:
    label: str
    start: date
    end: date


def month_bounds(year: int, month: int) -> DigestWindow:
    """Return the inclusive bounds for the requested month."""

    start = date(year, month, 1)
    next_month = start.replace(day=28) + timedelta(days=4)
    end = next_month.replace(day=1) - timedelta(days=1)
    return DigestWindow(label=start.strftime("%B %Y"), start=start, end=end)


def _dtfmt(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def _export_query(issue_from: date, issue_to: date, org: Optional[str]) -> str:
    clauses: List[str] = [
        f"issueDate:[DT({_dtfmt(issue_from)}T00:00:00) TO DT({_dtfmt(issue_to)}T23:59:59)]"
    ]
    if org:
        clauses.append(f'organizationUid:"{org}"')
    return " AND ".join(clauses)


def fetch_export(issue_from: date, issue_to: date, org: Optional[str]) -> pd.DataFrame:
    """Download up to MAX_RESULTS decisions from the export API."""

    remaining = MAX_RESULTS
    page = 0
    rows: List[dict] = []
    query = _export_query(issue_from, issue_to, org)

    while remaining > 0:
        response = requests.get(
            BASE_URL,
            params={
                "q": query,
                "page": page,
                "size": min(PAGE_SIZE, remaining),
                "sort": "recent",
                "wt": "json",
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("decisionResultList") or []
        if not batch:
            break

        rows.extend(batch)
        remaining -= len(batch)
        page += 1

        if len(batch) < PAGE_SIZE:
            break

    return pd.DataFrame(rows)


def enrich_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Append parsed datetime columns and compute the publication delay."""

    if df.empty:
        return df

    df = df.copy()
    df["issue_dt"] = pd.to_datetime(
        df["issueDate"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )
    df["subm_dt"] = pd.to_datetime(
        df["submissionTimestamp"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )
    df["delay_days"] = (df["subm_dt"] - df["issue_dt"]).dt.total_seconds() / 86400
    return df


def _pct_missing(series: pd.Series) -> float:
    if series.empty:
        return math.nan
    return float(series.isna().mean() * 100)


def compute_kpis(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "count": 0,
            "median": math.nan,
            "p90": math.nan,
            "miss_pub": math.nan,
            "miss_org": math.nan,
        }

    delay = df["delay_days"]
    return {
        "count": int(len(df)),
        "median": float(delay.median(skipna=True)),
        "p90": float(delay.quantile(0.9)),
        "miss_pub": _pct_missing(df.get("publishTimestamp", pd.Series(dtype=float))),
        "miss_org": _pct_missing(df.get("organizationLabel", pd.Series(dtype=float))),
    }


def pct_change(current: float, previous: float) -> float:
    if previous in (0, None) or pd.isna(previous):
        return math.nan
    return (current - previous) / previous * 100.0


def format_value(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "—"
        return f"{value:.2f}"
    return str(value)


def build_mix(df: pd.DataFrame, limit: int = 5) -> List[Tuple[str, str, float]]:
    if df.empty or "decisionTypeUid" not in df.columns:
        return []
    series = df["decisionTypeUid"].value_counts(normalize=True).head(limit) * 100
    return [(code, DECISION_LABELS.get(code, ""), round(share, 1)) for code, share in series.items()]


def slowest_decisions(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty or "delay_days" not in df.columns:
        return pd.DataFrame()

    keep_columns = [
        "ada",
        "organizationUid",
        "organizationName",
        "organizationLabel",
        "decisionTypeUid",
        "issueDate",
        "submissionTimestamp",
        "documentUrl",
        "delay_days",
        "subject",
    ]

    existing = [col for col in keep_columns if col in df.columns]
    return (
        df.sort_values("delay_days", ascending=False)
        .drop_duplicates(subset=["ada"])
        [existing]
        .head(limit)
    )


def render_html(
    label_bundle: Tuple[str, str, str, str, str],
    kpis_bundle: Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float]],
    decision_mix: Iterable[Tuple[str, str, float]],
    outliers: pd.DataFrame,
) -> str:
    month_label, prev_label, ytd_label, ytd_prev_label, yoy_label = label_bundle
    month_kpi, prev_kpi, ytd_kpi, ytd_prev_kpi, yoy_month_kpi = kpis_bundle

    parts: List[str] = []
    parts.append("<div style='font:14px -apple-system,Segoe UI,Roboto,Helvetica,Arial;color:#222'>")
    parts.append(f"<h2>Diavgeia Digest — {month_label}</h2>")
    parts.append("<h3>Overview</h3><table style='border-collapse:collapse'>")

    def add_row(label: str, value: object) -> None:
        parts.append(
            "<tr><td style='padding:4px 8px;color:#555'>"
            f"{label}</td><td style='padding:4px 8px;font-weight:600'>{format_value(value)}</td></tr>"
        )

    add_row("Decisions (month)", month_kpi["count"])
    add_row("Median delay (days)", month_kpi["median"])
    add_row("P90 delay (days)", month_kpi["p90"])
    add_row(f"MoM change vs {prev_label} (count)", month_kpi["count"] - prev_kpi["count"])
    add_row("MoM change (median delay, %)", pct_change(month_kpi["median"], prev_kpi["median"]))
    add_row(f"YTD decisions ({ytd_label})", ytd_kpi["count"])
    add_row("YoY (YTD) change (%)", pct_change(ytd_kpi["count"], ytd_prev_kpi["count"]))
    add_row(f"YoY (month) change (count)", month_kpi["count"] - yoy_month_kpi["count"])
    add_row("Missing publishTimestamp (month, %)", month_kpi["miss_pub"])
    add_row("Missing organization (month, %)", month_kpi["miss_org"])
    parts.append("</table>")

    mix_list = list(decision_mix)
    if mix_list:
        parts.append("<h3>Decision type mix (month)</h3><ul>")
        for code, label, share in mix_list:
            suffix = f" — {label}" if label else ""
            parts.append(f"<li><b>{code}</b>{suffix}: {share:.1f}%</li>")
        parts.append("</ul>")

    if not outliers.empty:
        parts.append("<h3>Slowest decisions (top 10)</h3><ol>")
        for _, record in outliers.iterrows():
            link = record.get("documentUrl") or "#"
            parts.append(
                "<li><a href='{url}'>{ada}</a> — {dtype} — {delay:.2f}d — {subject}</li>".format(
                    url=link,
                    ada=record.get("ada", ""),
                    dtype=record.get("decisionTypeUid", ""),
                    delay=record.get("delay_days", 0.0),
                    subject=str(record.get("subject", ""))[:120],
                )
            )
        parts.append("</ol>")

    parts.append("<p style='color:#777'>Source: diavgeia.gov.gr export API • issueDate window</p></div>")
    return "".join(parts)


def default_period(today: date) -> Tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Monthly Diavgeia Digest")
    parser.add_argument("--org", help="organizationUid (optional)")
    parser.add_argument("--year", type=int, help="Target year (defaults to previous month)")
    parser.add_argument("--month", type=int, help="Target month (defaults to previous month)")
    return parser.parse_args(argv)


def resolve_period(args: argparse.Namespace, today: date) -> Tuple[int, int]:
    year = args.year
    month = args.month
    if year and month:
        return year, month
    default_year, default_month = default_period(today)
    return year or default_year, month or default_month


def prepare_windows(year: int, month: int) -> Tuple[DigestWindow, DigestWindow, DigestWindow, DigestWindow, DigestWindow]:
    current = month_bounds(year, month)
    prev_year = year if month > 1 else year - 1
    prev_month = month - 1 if month > 1 else 12
    previous = month_bounds(prev_year, prev_month)

    ytd = DigestWindow(
        label=f"{date(year, 1, 1)} → {current.end}",
        start=date(year, 1, 1),
        end=current.end,
    )

    prior_year = year - 1
    ytd_prev = DigestWindow(
        label=f"{date(prior_year, 1, 1)} → {date(prior_year, month, current.end.day)}",
        start=date(prior_year, 1, 1),
        end=date(prior_year, month, current.end.day),
    )

    yoy_month = month_bounds(prior_year, month)
    return current, previous, ytd, ytd_prev, yoy_month


def ensure_artifact_dir() -> None:
    os.makedirs(ARTIFACT_DIR, exist_ok=True)


def write_outputs(html: str, current: pd.DataFrame, outliers: pd.DataFrame) -> None:
    ensure_artifact_dir()
    with open(os.path.join(ARTIFACT_DIR, "digest.html"), "w", encoding="utf-8") as handle:
        handle.write(html)
    current.to_csv(os.path.join(ARTIFACT_DIR, "raw_month.csv"), index=False)
    outliers.to_csv(os.path.join(ARTIFACT_DIR, "outliers.csv"), index=False)
    print(f"Wrote {os.path.join(ARTIFACT_DIR, 'digest.html')}")


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    today = date.today()
    year, month = resolve_period(args, today)

    current, previous, ytd, ytd_prev, yoy_month = prepare_windows(year, month)

    frames = {
        "current": fetch_export(current.start, current.end, args.org),
        "previous": fetch_export(previous.start, previous.end, args.org),
        "ytd": fetch_export(ytd.start, ytd.end, args.org),
        "ytd_prev": fetch_export(ytd_prev.start, ytd_prev.end, args.org),
        "yoy_month": fetch_export(yoy_month.start, yoy_month.end, args.org),
    }

    for key, frame in frames.items():
        if frame.empty:
            continue
        if "organizationLabel" in frame.columns and "organizationName" not in frame.columns:
            frame = frame.rename(columns={"organizationLabel": "organizationName"})
        frames[key] = enrich_dates(frame)

    current_df = frames["current"]
    previous_df = frames["previous"]
    ytd_df = frames["ytd"]
    ytd_prev_df = frames["ytd_prev"]
    yoy_month_df = frames["yoy_month"]

    kpi_bundle = (
        compute_kpis(current_df),
        compute_kpis(previous_df),
        compute_kpis(ytd_df),
        compute_kpis(ytd_prev_df),
        compute_kpis(yoy_month_df),
    )

    mix = build_mix(current_df)
    outliers = slowest_decisions(current_df)

    labels = (
        current.label,
        previous.label,
        ytd.label,
        ytd_prev.label,
        yoy_month.label,
    )

    html = render_html(labels, kpi_bundle, mix, outliers)
    write_outputs(html, current_df, outliers)


if __name__ == "__main__":
    main()
