#!/usr/bin/env bash
# Run the full pipeline for municipalities comparable to Lamia.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
LOG_DIR="$REPO/logs/comparables_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

GEMI_KEY="${GEMI_API_KEY:-}"
FROM_MONTH="2020-01"
TO_MONTH="2026-05"
DRY_RUN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gemi-key) GEMI_KEY="$2"; shift 2 ;;
    --from) FROM_MONTH="$2"; shift 2 ;;
    --to) TO_MONTH="$2"; shift 2 ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

ENTITIES=(
  "6298|Δήμος Τρικκαίων (Τρίκαλα)"
  "6154|Δήμος Κοζάνης"
  "6272|Δήμος Σερρών"
  # "6135|Δήμος Καρδίτσας"  # skipped — Diavgeia server timeouts; 24/77 months cached, rerun later
)

MONTHS="${FROM_MONTH}:${TO_MONTH}"

echo "========================================================"
echo "  Comparable Municipalities Pipeline"
echo "  Entities: ${#ENTITIES[@]}"
echo "  Period:   $MONTHS"
echo "  GEMI:     $([ -z \"$GEMI_KEY\" ] && echo 'skipped' || echo 'enabled')"
echo "  Logs:     $LOG_DIR"
echo "========================================================"

SUMMARY_FILE="$LOG_DIR/summary.txt"
echo "org,name,status,elapsed_s" > "$SUMMARY_FILE"

for ENTRY in "${ENTITIES[@]}"; do
  ORG="${ENTRY%%|*}"
  NAME="${ENTRY##*|}"
  LOG="$LOG_DIR/org_${ORG}.log"

  echo ""
  echo "────────────────────────────────────────────────────────"
  echo "  org=$ORG  $NAME"
  echo "────────────────────────────────────────────────────────"

  START_TS=$(date +%s)

  CMD=(
    "$PYTHON" "$REPO/scripts/pipeline.py"
    --org "$ORG"
    --months "$MONTHS"
    --skip-hydrate
    --normalize-format csv
  )

  if [[ -z "$GEMI_KEY" ]]; then
    CMD+=(--skip-gemi)
  else
    CMD+=(--gemi-key "$GEMI_KEY")
  fi

  [[ -n "$DRY_RUN" ]] && CMD+=("$DRY_RUN")

  STATUS="ok"
  "${CMD[@]}" 2>&1 | tee "$LOG" || STATUS="error"

  END_TS=$(date +%s)
  ELAPSED=$(( END_TS - START_TS ))
  echo "$ORG,$NAME,$STATUS,$ELAPSED" >> "$SUMMARY_FILE"
  echo "  → $STATUS in ${ELAPSED}s"
done

echo ""
echo "========================================================"
echo "  All done. Summary:"
cat "$SUMMARY_FILE"
echo "========================================================"
