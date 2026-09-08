#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <business-date YYYY-MM-DD>" >&2
  exit 1
fi

BUSINESS_DATE="$1"

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

if [ -z "${COMPOSE_BIN:-}" ]; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_BIN="podman compose"
  elif command -v podman.exe >/dev/null 2>&1; then
    COMPOSE_BIN="podman.exe compose"
  else
    echo "[acceptance-measure] podman is not available in PATH" >&2
    exit 1
  fi
fi

echo "[acceptance-measure] Measuring for business_date=${BUSINESS_DATE}" >&2

$COMPOSE_BIN exec -T postgres psql -U bertsonal -d bertsonal -v ON_ERROR_STOP=1 -c "
\echo '--- provisional completeness'
SELECT COUNT(*) AS provisional_rows
FROM serving.provisional_scores
WHERE business_date = DATE '${BUSINESS_DATE}';

\echo '--- provisional latency seconds (requires kafka_timestamp)'
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (calculated_at - kafka_timestamp))) AS p95_seconds,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (calculated_at - kafka_timestamp))) AS p99_seconds
FROM serving.provisional_scores
WHERE business_date = DATE '${BUSINESS_DATE}'
  AND kafka_timestamp IS NOT NULL;

\echo '--- gold daily rows'
SELECT COUNT(*) AS gold_daily_rows
FROM gold.daily_scores
WHERE business_date = DATE '${BUSINESS_DATE}';

\echo '--- gold weekly scope rows (derived)'
WITH ws AS (
  SELECT (DATE '${BUSINESS_DATE}' - (EXTRACT(DOW FROM DATE '${BUSINESS_DATE}')::int + 6) % 7) AS week_start
)
SELECT ws.week_start, COUNT(*) AS gold_weekly_rows
FROM ws
LEFT JOIN gold.weekly_rankings r ON r.week_start_date = ws.week_start
GROUP BY ws.week_start;

\echo '--- gold monthly scope rows (derived)'
WITH ms AS (
  SELECT DATE_TRUNC('month', DATE '${BUSINESS_DATE}')::date AS month_start
)
SELECT ms.month_start, COUNT(*) AS gold_monthly_rows
FROM ms
LEFT JOIN gold.monthly_rankings r ON r.month_start_date = ms.month_start
GROUP BY ms.month_start;
" 
