#!/usr/bin/env python3
"""Search-wide, hydrate-narrow procurement intelligence pipeline.

Orchestrates the full pipeline for one or more organizations:

  Step 1 (fetch)    — digest_monthly.py: fetch search_export.json for each month
  Step 2 (hydrate)  — hydrate_narrow.py: fetch detail JSONs for high-value decisions
  Step 3 (normalize)— build_normalized_tables.py: build decisions/suppliers/procurements CSVs
  Step 4 (cluster)  — cluster_suppliers.py: deduplicate supplier entities
  Step 5 (report)   — supplier_intelligence_report.py: HTML intelligence report
  Step 6 (dossiers) — build_dossier.py: per-supplier dossiers (optional, --dossiers)

Any step can be skipped with --skip-fetch, --skip-hydrate, --skip-normalize,
--skip-cluster, --skip-report.  Steps 1 and 2 require network access.

Usage:
    python scripts/pipeline.py --org 6166
    python scripts/pipeline.py --org 6166 --months 2024-01:2024-12
    python scripts/pipeline.py --org 6166 --skip-fetch --skip-hydrate
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

    parser.add_argument("--skip-fetch", action="store_true", help="Skip step 1 (digest_monthly fetch)")
    parser.add_argument("--skip-hydrate", action="store_true", help="Skip step 2 (hydrate_narrow)")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip step 3 (build_normalized_tables)")
    parser.add_argument("--skip-cluster", action="store_true", help="Skip step 4 (cluster_suppliers)")
    parser.add_argument("--skip-report", action="store_true", help="Skip step 5 (supplier_intelligence_report)")

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

    # ── Step 1: Fetch search exports ──────────────────────────────────────────
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
        rc = run(cmd, dry_run=args.dry_run, label="Step 1: Fetch search exports (digest_monthly)")
        if rc != 0:
            errors += 1
            print("  WARNING: fetch step failed, continuing with cached data")

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

    # ── Step 5: HTML intelligence report ────────────────────────────────────
    if not args.skip_report:
        report_path = args.reports_dir / f"supplier_intelligence_org_{args.org}.html"
        cmd = [
            python,
            SCRIPTS_DIR / "supplier_intelligence_report.py",
            "--org", args.org,
            "--input-dir", str(args.output_root),
            "--output", str(report_path),
        ]
        rc = run(cmd, dry_run=args.dry_run, label="Step 5: Intelligence report (supplier_intelligence_report)")
        if rc != 0:
            errors += 1
            print("  WARNING: report step failed")
        elif not args.dry_run:
            print(f"  Report: {report_path}")

    # ── Step 6: Markdown report ──────────────────────────────────────────────
    md_report_path = args.reports_dir / f"intelligence_org_{args.org}.md"
    cmd = [
        python,
        SCRIPTS_DIR / "build_markdown_report.py",
        "--org", args.org,
        "--input-dir", str(args.output_root),
        "--output", str(md_report_path),
    ]
    rc = run(cmd, dry_run=args.dry_run, label="Step 6: Markdown report (build_markdown_report)")
    if rc != 0:
        errors += 1
        print("  WARNING: markdown report step failed")
    elif not args.dry_run:
        print(f"  Report: {md_report_path}")

    # ── Step 7: Dossiers (optional) ──────────────────────────────────────────
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
        rc = run(cmd, dry_run=args.dry_run, label="Step 6: Per-supplier dossiers (build_dossier)")
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
