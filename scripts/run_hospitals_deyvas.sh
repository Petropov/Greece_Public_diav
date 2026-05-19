#!/usr/bin/env bash
# Run the full pipeline for hospital and ΔΕΥΑ comparables.
# Compatible with bash 3.2 (macOS default).
#
# Usage:
#   bash scripts/run_hospitals_deyvas.sh [--gemi-key KEY] [--skip-gemi] [--from YYYY-MM] [--to YYYY-MM] [--dry-run]

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
LOG_DIR="$REPO/logs/hospitals_deyvas_run_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

GEMI_KEY="${GEMI_API_KEY:-}"
SKIP_GEMI=0
FROM_MONTH="2020-01"
TO_MONTH="2026-05"
DRY_RUN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gemi-key) GEMI_KEY="$2"; shift 2 ;;
    --skip-gemi) SKIP_GEMI=1; shift ;;
    --from) FROM_MONTH="$2"; shift 2 ;;
    --to) TO_MONTH="$2"; shift 2 ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# Hospitals
ENTITIES=(
  "99221946|ΓΝ Τρικάλων"
  "99221913|ΓΝ Καρδίτσας"
  "99221920|ΓΝ Κοζάνης (Μαμάτσειο)"
  "99221942|ΓΝ Σερρών"
  "51546|ΔΕΥΑ Τρικάλων"
  "50449|ΔΕΥΑ Κοζάνης"
  "50621|ΔΕΥΑ Σερρών"
  "50432|ΔΕΥΑ Καρδίτσας"
)

MONTHS="${FROM_MONTH}:${TO_MONTH}"

echo "========================================================"
echo "  Hospitals & DEYAs Pipeline"
echo "  Entities: ${#ENTITIES[@]}"
echo "  Period:   $MONTHS"
echo "  GEMI:     $([ $SKIP_GEMI -eq 1 ] && echo 'skipped' || echo 'enabled')"
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
  echo "  log: $LOG"
  echo "────────────────────────────────────────────────────────"

  START_TS=$(date +%s)

  CMD=(
    "$PYTHON" "$REPO/scripts/pipeline.py"
    --org "$ORG"
    --months "$MONTHS"
    --skip-hydrate
    --normalize-format csv
  )

  if [[ $SKIP_GEMI -eq 1 ]] || [[ -z "$GEMI_KEY" ]]; then
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
echo "  Full logs: $LOG_DIR"
echo "========================================================"
