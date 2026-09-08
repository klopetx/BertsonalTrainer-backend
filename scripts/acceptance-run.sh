#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 8 ]; then
  echo "Usage: $0 --profile <demo|dev|nominal|load> --business-date YYYY-MM-DD --rhyme <text> --rhyme-id <id> [--seed N] [--force]" >&2
  exit 1
fi

PROFILE=""
BUSINESS_DATE=""
RHYME=""
RHYME_ID=""
SEED="42"
FORCE_BATCH="false"

while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --business-date) BUSINESS_DATE="$2"; shift 2 ;;
    --rhyme) RHYME="$2"; shift 2 ;;
    --rhyme-id) RHYME_ID="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --force) FORCE_BATCH="true"; shift 1 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$PROFILE" ] || [ -z "$BUSINESS_DATE" ] || [ -z "$RHYME" ] || [ -z "$RHYME_ID" ]; then
  echo "Missing required args" >&2
  exit 2
fi

case "$PROFILE" in
  demo) USERS=20 ;;
  dev) USERS=500 ;;
  nominal) USERS=1000 ;;
  load) USERS=60000 ;;
  *) echo "Invalid profile: $PROFILE" >&2; exit 2 ;;
esac

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

run_id=$(date -u +%Y%m%dT%H%M%SZ)
raw_dir="$ROOT_DIR/evidence/runs/${run_id}_${PROFILE}_${BUSINESS_DATE}"
summary_dir="$ROOT_DIR/docs/evidence"
mkdir -p "$raw_dir" "$summary_dir"

echo "[acceptance-run] Starting infra + streaming" >&2
./scripts/streaming-up.sh | tee "$raw_dir/streaming-up.log"

start=$(date +%s)
echo "[acceptance-run] Simulating profile=$PROFILE users=$USERS date=$BUSINESS_DATE" >&2
./scripts/simulate.sh \
  --users "$USERS" \
  --business-date "$BUSINESS_DATE" \
  --rhyme "$RHYME" \
  --rhyme-id "$RHYME_ID" \
  --seed "$SEED" | tee "$raw_dir/simulate.log"

./scripts/acceptance-wait.sh "$BUSINESS_DATE" "$USERS" 1800 5 | tee "$raw_dir/wait.log"

ingest_done=$(date +%s)
ingest_seconds=$((ingest_done - start))

echo "[acceptance-run] Running batch" >&2
batch_start=$(date +%s)
if [ "$FORCE_BATCH" = "true" ]; then
  ./scripts/batch-run.sh --business-date "$BUSINESS_DATE" --force | tee "$raw_dir/batch.log"
else
  ./scripts/batch-run.sh --business-date "$BUSINESS_DATE" | tee "$raw_dir/batch.log"
fi
batch_done=$(date +%s)
batch_seconds=$((batch_done - batch_start))

echo "[acceptance-run] Measuring" >&2
./scripts/acceptance-measure.sh "$BUSINESS_DATE" | tee "$raw_dir/measure.out"

throughput="0"
if [ "$ingest_seconds" -gt 0 ]; then
  # Keep a readable float for small demo runs.
  throughput=$(awk -v u="$USERS" -v s="$ingest_seconds" 'BEGIN { printf("%.2f", u / s) }')
fi

sha=$(git rev-parse --short HEAD)

summary_file="$summary_dir/${run_id}_${PROFILE}_${BUSINESS_DATE}.md"
cat > "$summary_file" <<EOF
## Acceptance Run

- run_id: $run_id
- commit: $sha
- profile: $PROFILE
- users: $USERS
- business_date: $BUSINESS_DATE
- ingest_seconds: $ingest_seconds
- approx_throughput_events_per_second: $throughput
- batch_seconds: $batch_seconds

### Measurements

Raw measurements file: evidence/runs/${run_id}_${PROFILE}_${BUSINESS_DATE}/measure.out
EOF

echo "[acceptance-run] Wrote summary: $summary_file" >&2
echo "[acceptance-run] Raw artifacts: $raw_dir" >&2
