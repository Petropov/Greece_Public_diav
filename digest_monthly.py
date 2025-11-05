#!/usr/bin/env python3
import os, sys, json, math, requests, pandas as pd
from datetime import date, datetime, timedelta

# Decision type labels (short)
decision_map = {
    "Α.1":   "Regulatory act",
    "Α.2":   "Internal regulation",
    "Β.1.1": "Budget commitment",
    "Β.1.2": "Budget amendment",
    "Β.1.3": "Payment warrant",
    "Β.2.1": "Expenditure approval",
    "Β.2.2": "Payment finalization",
    "Γ.2":   "Personnel change",
    "Δ.1":   "Procurement assignment",
    "Δ.2.2": "Contract award",
    "2.4.7.1": "Other administrative act",
}

BASE = "https://diavgeia.gov.gr/luminapi/api/search/export"
OUT = "artifacts"

def dtfmt(d): return d.strftime("%Y-%m-%d")

def month_bounds(y, m):
    start = date(y, m, 1)
    end = (date(y + (m//12), (m % 12) + 1, 1) - timedelta(days=1))
    return start, end

def fetch_export(issue_from, issue_to, org=None, limit=5000):
    q = []
    if org: q.append(f'organizationUid:"{org}"')
    q.append(f'issueDate:[DT({issue_from}T00:00:00) TO DT({issue_to}T23:59:59)]')
    query = " AND ".join(q)
    rows, page, remaining = [], 0, limit
    while remaining > 0:
        params = {"q": query, "sort": "recent", "wt": "json",
                  "page": page, "size": min(remaining, 500)}
        r = requests.get(BASE, params=params, timeout=60); r.raise_for_status()
        data = r.json()
        batch = data.get("decisionResultList") or []
        if not batch: break
        rows.extend(batch); remaining -= len(batch); page += 1
    return pd.DataFrame(rows)

def parse_dates(df):
    to_dt = lambda s, fmt: pd.to_datetime(s, format=fmt, errors="coerce")
    df["issue_dt"] = df["issueDate"].apply(lambda s: to_dt(s, "%d/%m/%Y %H:%M:%S"))
    df["subm_dt"]  = df["submissionTimestamp"].apply(lambda s: to_dt(s, "%d/%m/%Y %H:%M:%S"))
    df["delay_days"] = (df["subm_dt"] - df["issue_dt"]).dt.total_seconds() / 86400
    return df

def kpis(df):
    if df.empty:
        return {"count":0,"median":math.nan,"p90":math.nan,"miss_pub":math.nan,"miss_org":math.nan}
    return {
        "count": int(len(df)),
        "median": float(df["delay_days"].median(skipna=True)),
        "p90": float(df["delay_days"].quantile(0.9)),
        "miss_pub": float(df["publishTimestamp"].isna().mean()*100 if "publishTimestamp" in df else math.nan),
        "miss_org": float(df["organizationLabel"].isna().mean()*100 if "organizationLabel" in df else math.nan),
    }

def pct(cur, prev):
    if prev in (None, 0) or pd.isna(prev): return math.nan
    return (cur - prev) / prev * 100.0

def render_html(ctx):
    def fmt(x):
        if x is None or (isinstance(x, float) and (pd.isna(x) or math.isinf(x))):
            return "—"
        return f"{x:.2f}" if isinstance(x, float) else str(x)

    m, pm, ytd, ytd_prev, yoy = ctx["labels"]
    mk, pk, yk, ypk, ymk      = ctx["kpi"]
    mix        = ctx.get("mix", [])
    outliers   = ctx.get("outliers", pd.DataFrame())
    recent     = ctx.get("recent", [])
    trend      = ctx.get("trend", {"count":{}, "median":{}})

    html = []
    html.append("<!doctype html>")
    html.append("<html><head><meta charset='utf-8'><title>Diavgeia Digest</title></head>")
    html.append("<body style='font:14px -apple-system,Segoe UI,Roboto,Helvetica,Arial;color:#222;margin:24px'>")
    html.append(f"<h2 style='margin:0 0 16px'>Diavgeia Digest — {m}</h2>")

    # Overview
    html.append("<h3 style='margin:24px 0 8px'>Overview</h3>")
    html.append("<table style='border-collapse:collapse'>")
    row = lambda k,v: html.append(
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

    # Trend summary
    row("Trend (count) — M-1 / M-2 / M-3",
        f"{trend['count'].get('m1', 0)} / {trend['count'].get('m2', 0)} / {trend['count'].get('m3', 0)}")
    row("Trend (count avg) — Av6M / Av12M",
        f"{trend['count'].get('avg6', 0)} / {trend['count'].get('avg12', 0)}")
    row("Trend (median days) — M-1 / M-2 / M-3",
        f"{trend['median'].get('m1', 0.0)} / {trend['median'].get('m2', 0.0)} / {trend['median'].get('m3', 0.0)}")
    row("Trend (median days avg) — Av6M / Av12M",
        f"{trend['median'].get('avg6', 0.0)} / {trend['median'].get('avg12', 0.0)}")
    html.append("</table>")

    # Decision type mix (month)
    if mix:
        html.append("<h3 style='margin:24px 0 8px'>Decision type mix (month)</h3><ul>")
        for code, label, p in mix:
            label_txt = f" — {label}" if label else ""
            html.append(f"<li><b>{code}</b>{label_txt}: {p:.1f}%</li>")
        html.append("</ul>")

    # Recent months (compact)
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

    # Outliers
    if isinstance(outliers, pd.DataFrame) and not outliers.empty:
        html.append("<h3 style='margin:24px 0 8px'>Slowest decisions (top 10)</h3><ol>")
        for _, r in outliers.iterrows():
            link  = r.get("documentUrl") or "#"
            code  = r.get("decisionTypeUid","")
            delay = r.get("delay_days", 0.0)
            subj  = str(r.get("subject",""))[:120]
            ada   = r.get("ada","")
            html.append(f"<li><a href='{link}'>{ada}</a> — {code} — {delay:.2f}d — {subj}</li>")
        html.append("</ol>")

    html.append("<p style='color:#777;margin-top:16px'>Source: diavgeia.gov.gr export API • issueDate window</p>")
    html.append("</body></html>")
    return "".join(html)

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Build Monthly Diavgeia Digest")
    ap.add_argument("--org", help="organizationUid (optional)")
    ap.add_argument("--year", type=int)
    ap.add_argument("--month", type=int)
    args = ap.parse_args()

    today = date.today()
    year = args.year or (today.year if today.month > 1 else today.year - 1)
    month = args.month or (today.month - 1 or 12)

    mo_start, mo_end = month_bounds(year, month)
    prev_start, prev_end = month_bounds(year if month > 1 else year - 1, (month - 1 or 12))
    ytd_start = date(year, 1, 1)

    last_year = year - 1
    ytd_prev_start = date(last_year, 1, 1)
    ytd_prev_end = date(last_year, month, month_bounds(year, month)[1].day)
    yoy_mo_start, yoy_mo_end = month_bounds(last_year, month)

    # Fetch
    cur = fetch_export(dtfmt(mo_start), dtfmt(mo_end), args.org)
    prv = fetch_export(dtfmt(prev_start), dtfmt(prev_end), args.org)
    ytd = fetch_export(dtfmt(ytd_start), dtfmt(mo_end), args.org)
    ypr = fetch_export(dtfmt(ytd_prev_start), dtfmt(ytd_prev_end), args.org)
    ymo = fetch_export(dtfmt(yoy_mo_start), dtfmt(yoy_mo_end), args.org)

    # Normalize & compute delays BEFORE aggregations
    for df in (cur, prv, ytd, ypr, ymo):
        if not df.empty:
            if "organizationLabel" in df.columns and "organizationName" not in df.columns:
                df.rename(columns={"organizationLabel": "organizationName"}, inplace=True)
            parse_dates(df)

    # KPIs
    mk, pk, yk, ypk, ymk = kpis(cur), kpis(prv), kpis(ytd), kpis(ypr), kpis(ymo)

    # Mix (top 5) as (code, label, pct)
    if not cur.empty and "decisionTypeUid" in cur.columns:
        mix_series = (cur["decisionTypeUid"].value_counts(normalize=True)
                      .head(5).mul(100).round(1))
        mix = [(code, decision_map.get(code, ""), float(p))
               for code, p in mix_series.items()]
    else:
        mix = []

    # Trend & recent months from YTD
    recent = []
    trend  = {"count": {}, "median": {}}
    if not ytd.empty:
        df = ytd.copy()
        df["month_key"] = df["issue_dt"].dt.strftime("%Y-%m")
        monthly = (df.groupby("month_key")
                   .agg(count=("ada", "count"),
                        median=("delay_days", "median"))
                   .sort_index())

        last6, last12 = monthly.tail(6), monthly.tail(12)
        trend["count"]["avg6"]   = int(last6["count"].mean())   if len(last6)  else 0
        trend["count"]["avg12"]  = int(last12["count"].mean())  if len(last12) else 0
        trend["median"]["avg6"]  = round(float(last6["median"].mean()), 2) if len(last6)  else 0.0
        trend["median"]["avg12"] = round(float(last12["median"].mean()),2) if len(last12) else 0.0

        tail = monthly.tail(4).to_dict("records")  # current + up to 3 previous
        def rel(idx, key): return tail[-(idx+2)][key] if len(tail) >= (idx+2) else 0
        trend["count"].update({
            "m1": int(rel(0,"count")),
            "m2": int(rel(1,"count")),
            "m3": int(rel(2,"count")),
        })
        trend["median"].update({
            "m1": round(float(rel(0,"median")),2),
            "m2": round(float(rel(1,"median")),2),
            "m3": round(float(rel(2,"median")),2),
        })

        recent_rows = monthly.tail(6).reset_index()
        recent = [{"month": r["month_key"], "count": int(r["count"]), "median": float(r["median"])}
                  for _, r in recent_rows.iterrows()]

    # Outliers (dedup by ADA)
    cols = ["ada","organizationUid","organizationName","decisionTypeUid",
            "issueDate","submissionTimestamp","documentUrl","delay_days","subject"]
    outliers = (cur.sort_values("delay_days", ascending=False)
                  .drop_duplicates(subset=["ada"])
                  [cols].head(10)) if not cur.empty else pd.DataFrame()

    # Render & write
    os.makedirs(OUT, exist_ok=True)
    html = render_html({
        "labels": (mo_start.strftime("%B %Y"),
                   prev_start.strftime("%B %Y"),
                   f"{ytd_start} → {mo_end}",
                   f"{ytd_prev_start} → {ytd_prev_end}",
                   yoy_mo_start.strftime("%B %Y")),
        "kpi": (mk, pk, yk, ypk, ymk),
        "mix": mix,
        "outliers": outliers,
        "recent": recent,
        "trend": trend,
    })
    with open(os.path.join(OUT, "digest.html"), "w", encoding="utf-8") as f:
        f.write(html)
    cur.to_csv(os.path.join(OUT, "raw_month.csv"), index=False)
    outliers.to_csv(os.path.join(OUT, "outliers.csv"), index=False)
    print(f"Wrote {os.path.join(OUT,'digest.html')}")

if __name__ == "__main__":
    main()
