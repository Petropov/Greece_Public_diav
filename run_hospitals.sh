#!/bin/bash
# Run procurement pipeline for Greece's 10 biggest hospitals
# Logs per-hospital to logs/hospital_<uid>.log

set -e
cd "$(dirname "$0")"
mkdir -p logs

HOSPITALS=(
  "99221993:Ευαγγελισμός_Αθηνών"
  "99221998:ΑΧΕΠΑ_Θεσσαλονίκης"
  "99221990:Σισμανόγλειο_Αθηνών"
  "99222000:Ιπποκράτειο_Θεσσαλονίκης"
  "99221994:Λαϊκό_Αθηνών"
  "99221999:Παπανικόλαου_Θεσσαλονίκης"
  "99221997:Βενιζέλειο_Ηρακλείου"
  "99221992:Γεννηματάς_Αθηνών"
  "99222007:ΑγΑνδρέας_Πατρών"
  "99222003:Νικαία_Πειραιάς"
)

for entry in "${HOSPITALS[@]}"; do
  ORG="${entry%%:*}"
  NAME="${entry##*:}"
  LOG="logs/hospital_${ORG}_${NAME}.log"
  echo "========================================"
  echo "Starting: $NAME (org $ORG)"
  echo "Log: $LOG"
  echo "========================================"
  python scripts/pipeline.py \
    --org "$ORG" \
    --months 2019-01:2026-05 \
    --gemi-key HiCVYDZOdWNVTLo170NeiifIGCTLcEbQ \
    --normalize-format csv \
    --skip-refetch \
    2>&1 | tee "$LOG"
  echo "Done: $NAME at $(date)"
  echo ""
done

echo "All hospitals complete."
