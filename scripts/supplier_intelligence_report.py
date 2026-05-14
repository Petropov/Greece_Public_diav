from pathlib import Path
import argparse
import pandas as pd

def eur(x):
    return "" if pd.isna(x) else f"€{float(x):,.2f}"

def html_table(df, cols, n=30):
    return df[cols].head(n).to_html(index=False, escape=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--org", required=True)
    ap.add_argument("--input-dir", default="data/normalized")
    ap.add_argument("--output")
    args = ap.parse_args()

    base = Path(args.input_dir) / f"org={args.org}"
    p = pd.read_csv(base / "procurements.csv", low_memory=False)
    p["amount"] = pd.to_numeric(p["amount"], errors="coerce")

    by_supplier = (
        p.dropna(subset=["supplier_key"])
        .groupby(["supplier_key", "supplier_name"], dropna=False)
        .agg(
            procurement_count=("ada", "nunique"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            first_seen=("issue_date", "min"),
            last_seen=("issue_date", "max"),
        )
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )

    top_proc = p[p["amount"].notna()].sort_values("amount", ascending=False)

    monthly = (
        p.groupby(["year", "month"], dropna=False)
        .agg(
            procurement_count=("ada", "nunique"),
            supplier_count=("supplier_key", "nunique"),
            total_amount=("amount", "sum"),
        )
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )

    out = Path(args.output or f"reports/supplier_intelligence_org_{args.org}.html")
    out.parent.mkdir(parents=True, exist_ok=True)

    doc = f"""
<html>
<head>
<meta charset="utf-8">
<title>Supplier Intelligence {args.org}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 32px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
th {{ background: #f2f2f2; }}
.card {{ display:inline-block; padding:14px; margin:8px; border:1px solid #ddd; border-radius:8px; }}
.big {{ font-size:24px; font-weight:bold; }}
</style>
</head>
<body>
<h1>Supplier Intelligence Report</h1>

<div class="card">Procurements<br><span class="big">{len(p):,}</span></div>
<div class="card">Known suppliers<br><span class="big">{p['supplier_key'].nunique():,}</span></div>
<div class="card">Known amount<br><span class="big">{eur(p['amount'].sum())}</span></div>
<div class="card">Max amount<br><span class="big">{eur(p['amount'].max())}</span></div>

<h2>Top suppliers by amount</h2>
{html_table(by_supplier, ["supplier_name", "procurement_count", "total_amount", "avg_amount", "first_seen", "last_seen"])}

<h2>Repeat suppliers</h2>
{html_table(by_supplier[by_supplier["procurement_count"] >= 3], ["supplier_name", "procurement_count", "total_amount", "avg_amount", "first_seen", "last_seen"])}

<h2>Top procurements</h2>
{html_table(top_proc, ["issue_date", "ada", "supplier_name", "amount", "subject"], 40)}

<h2>Top months</h2>
{html_table(monthly, ["year", "month", "procurement_count", "supplier_count", "total_amount"], 40)}
</body>
</html>
"""
    out.write_text(doc, encoding="utf-8")
    print(f"Wrote {out}")

if __name__ == "__main__":
    main()
