#!/usr/bin/env python3
"""Search-wide, hydrate-narrow procurement intelligence pipeline.

Orchestrates the full pipeline for one or more organizations:

  Step 1a (fetch)     — digest_monthly.py: fetch search_export.json for each month
  Step 1b (refetch)   — fetch_windowed.py: re-fetch capped months with weekly windows
  Step 2  (hydrate)   — hydrate_narrow.py: fetch detail JSONs for high-value decisions
  Step 3  (normalize) — build_normalized_tables.py: build decisions/suppliers/procurements CSVs
  Step 4  (cluster)   — cluster_suppliers.py: deduplicate supplier entities
  Step 5  (lifecycle) — link_procurement_lifecycle.py: link stages → one row per contract
  Step 6  (gemi)      — enrich_gemi.py: enrich suppliers with ΓΕΜΗ company data (needs API key)
  Step 7  (report)    — supplier_intelligence_report.py: HTML intelligence report
  Step 8  (markdown)  — build_markdown_report.py: Markdown intelligence report
  Step 9  (dossiers)  — build_dossier.py: per-supplier dossiers (optional, --dossiers)

Any step can be skipped with the appropriate --skip-* flag.
Steps 1a, 1b, 2, and 6 require network access.
Step 6 requires a ΓΕΜΗ API key (free: https://opendata.businessportal.gr/register/).

Usage:
    python scripts/pipeline.py --org 6166
    python scripts/pipeline.py --org 6166 --months 2024-01:2024-12
    python scripts/pipeline.py --org 6166 --skip-fetch --skip-refetch --skip-hydrate
    python scripts/pipeline.py --org 6166 --gemi-key YOUR_KEY
    python scripts/pipeline.py --org 6166 --dossiers --dossier-top 30
    python scripts/pipeline.py --org 6166 --dry-run
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"

DEFAULT_RAW_ROOT = REPO_ROOT / "data" / "raw" / "diavgeia"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "normalized"
DEFAULT_REPORTS_DIR = REPO_ROOT / "reports"


def run(
    cmd: Sequence[str | Path],
    *,
    dry_run: bool = False,
    label: str = "",
) -> int:
    printable = " ".join(str(c) for c in cmd)
    if label:
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"{'='*60}")
    print(f"$ {printable}")
    if dry_run:
        print("  [dry-run — skipped]")
        return 0
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"  [exit {result.returncode}]")
    return result.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="End-to-end procurement intelligence pipeline."
    )
    parser.add_argument("--org", required=True, help="Diavgeia organizationUid, e.g. 6166")
    parser.add_argument(
        "--months",
        default=None,
        help="Month range e.g. 2024-01 or 2024-01:2024-12 (passed to fetch + hydrate steps)",
    )
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)

    parser.add_argument("--skip-fetch", action="store_true", help="Skip step 1a (digest_monthly fetch)")
    parser.add_argument("--skip-refetch", action="store_true", help="Skip step 1b (fetch_windowed cap-busting re-fetch)")
    parser.add_argument("--skip-hydrate", action="store_true", help="Skip step 2 (hydrate_narrow)")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip step 3 (build_normalized_tables)")
    parser.add_argument("--skip-cluster", action="store_true", help="Skip step 4 (cluster_suppliers)")
    parser.add_argument("--skip-lifecycle", action="store_true", help="Skip step 5 (link_procurement_lifecycle)")
    parser.add_argument("--skip-gemi", action="store_true", help="Skip step 6 (enrich_gemi)")
    parser.add_argument("--skip-report", action="store_true", help="Skip step 7 (supplier_intelligence_report)")

    parser.add_argument("--gemi-key", default="", help="ΓΕΜΗ API key (or set GEMI_API_KEY env var)")

    parser.add_argument("--dossiers", action="store_true", help="Run step 6: build per-supplier dossiers")
    parser.add_argument("--dossier-top", type=int, default=50, help="Number of top suppliers for dossiers (default: 50)")
    parser.add_argument("--dossier-format", choices=("json", "html", "both"), default="html")

    parser.add_argument("--min-hydrate-score", type=int, default=2, help="Min score for hydrate_narrow (default: 2)")
    parser.add_argument("--hydrate-delay", type=float, default=0.3, help="Seconds between hydration requests")
    parser.add_argument("--normalize-format", choices=("csv", "parquet"), default="csv")
    parser.add_argument("--limit-months", type=int, default=None, help="Limit normalize to first N cached months")

    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them")
    parser.add_argument("--verbose", action="store_true", help="Pass --verbose to hydrate_narrow")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    python = sys.executable
    errors = 0

    # ── Step 1a: Fetch search exports ─────────────────────────────────────────
    if not args.skip_fetch:
        cmd: list[str | Path] = [
            python,
            REPO_ROOT / "digest_monthly.py",
            "--org", args.org,
            "--cache-dir", str(args.raw_root),
        ]
        if args.months:
            start, _, end = args.months.partition(":")
            cmd += ["--month", start]
            if end:
                cmd += ["--end-month", end]
        rc = run(cmd, dry_run=args.dry_run, label="Step 1a: Fetch search exports (digest_monthly)")
        if rc != 0:
            errors += 1
            print("  WARNING: fetch step failed, continuing with cached data")

    # ── Step 1b: Re-fetch capped months with weekly windows ───────────────────
    if not args.skip_refetch:
        cmd = [
            python,
            SCRIPTS_DIR / "fetch_windowed.py",
            "--org", args.org,
            "--raw-root", str(args.raw_root),
        ]
        if args.months:
            cmd += ["--months", args.months]
        if args.verbose:
            cmd.append("--verbose")
        rc = run(cmd, dry_run=args.dry_run, label="Step 1b: Re-fetch capped months (fetch_windowed)")
        if rc != 0:
            errors += 1
            print("  WARNING: windowed re-fetch had errors, continuing with existing cache")

    # ── Step 2: Selective hydration ──────────────────────────────────────────
    if not args.skip_hydrate:
        cmd = [
            python,
            SCRIPTS_DIR / "hydrate_narrow.py",
            "--org", args.org,
            "--raw-root", str(args.raw_root),
            "--min-score", str(args.min_hydrate_score),
            "--delay", str(args.hydrate_delay),
        ]
        if args.months:
            cmd += ["--months", args.months]
        if args.verbose:
            cmd.append("--verbose")
        rc = run(cmd, dry_run=args.dry_run, label="Step 2: Selective hydration (hydrate_narrow)")
        if rc != 0:
            errors += 1
            print("  WARNING: hydration step returned errors, continuing")

    # ── Step 3: Normalize ────────────────────────────────────────────────────
    if not args.skip_normalize:
        cmd = [
            python,
            SCRIPTS_DIR / "build_normalized_tables.py",
            "--org", args.org,
            "--raw-root", str(args.raw_root),
            "--output-root", str(args.output_root),
            "--format", args.normalize_format,
        ]
        if args.limit_months is not None:
            cmd += ["--limit-months", str(args.limit_months)]
        rc = run(cmd, dry_run=args.dry_run, label="Step 3: Normalize (build_normalized_tables)")
        if rc != 0:
            errors += 1
            print("  ERROR: normalize step failed")

    # ── Step 4: Cluster suppliers ────────────────────────────────────────────
    if not args.skip_cluster:
        cmd = [
            python,
            SCRIPTS_DIR / "cluster_suppliers.py",
            "--org", args.org,
            "--input-dir", str(args.output_root),
            "--output-dir", str(args.output_root),
        ]
        rc = run(cmd, dry_run=args.dry_run, label="Step 4: Cluster suppliers (cluster_suppliers)")
        if rc != 0:
            errors += 1
            print("  WARNING: cluster step failed")

    # ── Step 5: Procurement lifecycle deduplication ───────────────────────────
    if not args.skip_lifecycle:
        cmd = [
            python,
            SCRIPTS_DIR / "link_procurement_lifecycle.py",
            "--org", args.org,
            "--input-dir", str(args.output_root),
        ]
        if args.verbose:
            cmd.append("--verbose")
        rc = run(cmd, dry_run=args.dry_run, label="Step 5: Lifecycle linking (link_procurement_lifecycle)")
        if rc != 0:
            errors += 1
            print("  WARNING: lifecycle step failed")

    # ── Step 6: ΓΕΜΗ enrichment (requires API key) ────────────────────────────
    gemi_key = args.gemi_key or __import__("os").environ.get("GEMI_API_KEY", "")
    if not args.skip_gemi and gemi_key:
        cmd = [
            python,
            SCRIPTS_DIR / "enrich_gemi.py",
            "--org", args.org,
            "--input-dir", str(args.output_root),
            "--api-key", gemi_key,
        ]
        if args.verbose:
            cmd.append("--verbose")
        rc = run(cmd, dry_run=args.dry_run, label="Step 6: ΓΕΜΗ enrichment (enrich_gemi)")
        if rc != 0:
            errors += 1
            print("  WARNING: ΓΕΜΗ enrichment had errors")
    elif not args.skip_gemi and not gemi_key:
        print("\n  [Step 6 skipped] No ΓΕΜΗ API key — pass --gemi-key or set GEMI_API_KEY")
        print("  Register free at: https://opendata.businessportal.gr/register/")

    # ── Step 7: HTML intelligence report ────────────────────────────────────
    if not args.skip_report:
        report_path = args.reports_dir / f"supplier_intelligence_org_{args.org}.html"
        cmd = [
            python,
            SCRIPTS_DIR / "supplier_intelligence_report.py",
            "--org", args.org,
            "--input-dir", str(args.output_root),
            "--output", str(report_path),
        ]
        rc = run(cmd, dry_run=args.dry_run, label="Step 7: Intelligence report (supplier_intelligence_report)")
        if rc != 0:
            errors += 1
            print("  WARNING: report step failed")
        elif not args.dry_run:
            print(f"  Report: {report_path}")

    # ── Step 8: Markdown report ───────────────────────────────────────────────
    md_report_path = args.reports_dir / f"intelligence_org_{args.org}.md"
    cmd = [
        python,
        SCRIPTS_DIR / "build_markdown_report.py",
        "--org", args.org,
        "--input-dir", str(args.output_root),
        "--output", str(md_report_path),
    ]
    rc = run(cmd, dry_run=args.dry_run, label="Step 8: Markdown report (build_markdown_report)")
    if rc != 0:
        errors += 1
        print("  WARNING: markdown report step failed")
    elif not args.dry_run:
        print(f"  Report: {md_report_path}")

    # ── Step 9: Dossiers (optional) ───────────────────────────────────────────
    if args.dossiers:
        cmd = [
            python,
            SCRIPTS_DIR / "build_dossier.py",
            "--org", args.org,
            "--input-dir", str(args.output_root),
            "--output-dir", str(args.reports_dir / "dossiers"),
            "--top", str(args.dossier_top),
            "--format", args.dossier_format,
        ]
        rc = run(cmd, dry_run=args.dry_run, label="Step 9: Per-supplier dossiers (build_dossier)")
        if rc != 0:
            errors += 1
            print("  WARNING: dossier step failed")

    print(f"\n{'='*60}")
    if errors == 0:
        print("Pipeline complete — no errors.")
    else:
        print(f"Pipeline complete — {errors} step(s) had errors.")
    print(f"{'='*60}")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
