#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/check-run-metrics.sh <run_id> [--db <sqlite_path>] [--report <report.md>] [--strict]

Description:
  Computes core believability metrics for a persisted run and exits non-zero when gates fail.

Threshold env vars (optional):
  MIN_UNIQUE_ACTIONS           default: 7
  MIN_UNIQUE_COMPOSED_ACTIONS  default: dynamic (>=720 ticks:250, >=240 ticks:80, else:24)
  MAX_DOMINANT_ACTION_SHARE    default: 0.65
  MAX_PAY_RENT_ACTION_SHARE    default: 0.12
  MAX_THEFT_ACTION_SHARE       default: 0.12
  MAX_THEFTS_PER_DAY           default: 2
  MIN_NON_COMMIT_SHARE         default: 0.20
  MAX_NON_COMMIT_SHARE         default: 0.90

Examples:
  scripts/check-run-metrics.sh test3
  scripts/check-run-metrics.sh test3 --db /tmp/runs.sqlite --strict
USAGE
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 1
  fi
}

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

ratio() {
  local numerator="$1"
  local denominator="$2"
  awk -v n="$numerator" -v d="$denominator" 'BEGIN { if (d <= 0) { printf "0" } else { printf "%.6f", n / d } }'
}

gt() {
  local left="$1"
  local right="$2"
  awk -v l="$left" -v r="$right" 'BEGIN { exit(!(l > r)) }'
}

lt() {
  local left="$1"
  local right="$2"
  awk -v l="$left" -v r="$right" 'BEGIN { exit(!(l < r)) }'
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${1:-}"
if [[ -z "$RUN_ID" || "$RUN_ID" == "-h" || "$RUN_ID" == "--help" ]]; then
  usage
  exit 2
fi
shift || true

DB_PATH="${ROOT_DIR}/threads_runs.sqlite"
REPORT_PATH=""
STRICT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)
      DB_PATH="${2:-}"
      shift 2
      ;;
    --report)
      REPORT_PATH="${2:-}"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

require_tool sqlite3
require_tool awk

if [[ ! -f "$DB_PATH" ]]; then
  echo "sqlite db not found: $DB_PATH" >&2
  exit 1
fi

RUN_ID_SQL="$(sql_escape "$RUN_ID")"

sql() {
  sqlite3 "$DB_PATH" "$1"
}

run_exists="$(sql "SELECT COUNT(*) FROM runs WHERE run_id='${RUN_ID_SQL}';")"
if [[ "$run_exists" != "1" ]]; then
  echo "run not found: $RUN_ID" >&2
  exit 1
fi

actions_total="$(sql "SELECT COUNT(*) FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted';")"
unique_actions="$(sql "SELECT COUNT(DISTINCT json_extract(payload_json,'$.details.chosen_action')) FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted';")"
unique_composed_actions="$(sql "SELECT COUNT(DISTINCT json_extract(payload_json,'$.details.composed_action')) FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted';")"
max_ticks="$(sql "SELECT COALESCE(CAST(json_extract(status_json,'$.max_ticks') AS INT),0) FROM runs WHERE run_id='${RUN_ID_SQL}';")"
npc_count="$(sql "SELECT COALESCE(json_array_length(json_extract(payload_json,'$.region_state.npc_profiles')),0) FROM snapshots WHERE run_id='${RUN_ID_SQL}' ORDER BY tick ASC LIMIT 1;")"
if [[ -z "$npc_count" || "$npc_count" == "0" ]]; then
  npc_count="$(sql "SELECT COALESCE(CAST(json_extract(config_json,'$.npc_count_min') AS INT),1) FROM runs WHERE run_id='${RUN_ID_SQL}';")"
fi
max_possible_actions=$(( max_ticks * npc_count ))
if [[ "$max_possible_actions" -le 0 ]]; then
  non_commit_share="0"
else
  non_commit_share="$(awk -v total="$actions_total" -v max_possible="$max_possible_actions" 'BEGIN { value=(max_possible-total)/max_possible; if (value < 0) value=0; printf "%.6f", value }')"
fi

dominant_row="$(sql "SELECT COALESCE(json_extract(payload_json,'$.details.chosen_action'),'none'), COUNT(*) AS c FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted' GROUP BY 1 ORDER BY c DESC LIMIT 1;")"
if [[ -z "$dominant_row" ]]; then
  dominant_action="none"
  dominant_count=0
else
  dominant_action="${dominant_row%|*}"
  dominant_count="${dominant_row##*|}"
fi

pay_rent_actions="$(sql "SELECT COUNT(*) FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted' AND json_extract(payload_json,'$.details.chosen_action')='pay_rent';")"
theft_actions="$(sql "SELECT COUNT(*) FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='TheftCommitted';")"
max_thefts_per_day="$(sql "WITH daily AS (SELECT ((tick - 1) / 24) + 1 AS day, SUM(event_type='TheftCommitted') AS thefts FROM events WHERE run_id='${RUN_ID_SQL}' GROUP BY day) SELECT COALESCE(MAX(thefts),0) FROM daily;")"

trust_range_row="$(sql "SELECT COALESCE(MIN(CAST(json_extract(payload_json,'$.details.trust') AS INT)),0), COALESCE(MAX(CAST(json_extract(payload_json,'$.details.trust') AS INT)),0) FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='TrustChanged';")"
trust_min="${trust_range_row%|*}"
trust_max="${trust_range_row##*|}"

dominant_share="$(ratio "$dominant_count" "$actions_total")"
pay_rent_share="$(ratio "$pay_rent_actions" "$actions_total")"
theft_share="$(ratio "$theft_actions" "$actions_total")"

min_unique="${MIN_UNIQUE_ACTIONS:-7}"
if [[ -n "${MIN_UNIQUE_COMPOSED_ACTIONS:-}" ]]; then
  min_unique_composed="${MIN_UNIQUE_COMPOSED_ACTIONS}"
elif [[ "$max_ticks" -ge 720 ]]; then
  min_unique_composed=250
elif [[ "$max_ticks" -ge 240 ]]; then
  min_unique_composed=80
else
  min_unique_composed=24
fi
max_dominant="${MAX_DOMINANT_ACTION_SHARE:-0.65}"
max_pay_rent="${MAX_PAY_RENT_ACTION_SHARE:-0.12}"
max_theft_share="${MAX_THEFT_ACTION_SHARE:-0.12}"
max_thefts_day="${MAX_THEFTS_PER_DAY:-2}"
min_non_commit_share="${MIN_NON_COMMIT_SHARE:-0.20}"
max_non_commit_share="${MAX_NON_COMMIT_SHARE:-0.90}"

if [[ "$STRICT" -eq 1 ]]; then
  max_dominant="${MAX_DOMINANT_ACTION_SHARE_STRICT:-0.55}"
  max_pay_rent="${MAX_PAY_RENT_ACTION_SHARE_STRICT:-0.10}"
  max_theft_share="${MAX_THEFT_ACTION_SHARE_STRICT:-0.08}"
  max_thefts_day="${MAX_THEFTS_PER_DAY_STRICT:-1}"
  if [[ -n "${MIN_UNIQUE_COMPOSED_ACTIONS_STRICT:-}" ]]; then
    min_unique_composed="${MIN_UNIQUE_COMPOSED_ACTIONS_STRICT}"
  elif [[ "$max_ticks" -ge 720 ]]; then
    min_unique_composed=280
  elif [[ "$max_ticks" -ge 240 ]]; then
    min_unique_composed=110
  else
    min_unique_composed=30
  fi
  min_non_commit_share="${MIN_NON_COMMIT_SHARE_STRICT:-0.25}"
  max_non_commit_share="${MAX_NON_COMMIT_SHARE_STRICT:-0.85}"
fi

failures=()

if lt "$unique_actions" "$min_unique"; then
  failures+=("unique_actions=${unique_actions} < ${min_unique}")
fi
if lt "$unique_composed_actions" "$min_unique_composed"; then
  failures+=("unique_composed_actions=${unique_composed_actions} < ${min_unique_composed}")
fi
if gt "$dominant_share" "$max_dominant"; then
  failures+=("dominant_share=${dominant_share} > ${max_dominant} (action=${dominant_action})")
fi
if gt "$pay_rent_share" "$max_pay_rent"; then
  failures+=("pay_rent_share=${pay_rent_share} > ${max_pay_rent}")
fi
if gt "$theft_share" "$max_theft_share"; then
  failures+=("theft_share=${theft_share} > ${max_theft_share}")
fi
if gt "$max_thefts_per_day" "$max_thefts_day"; then
  failures+=("max_thefts_per_day=${max_thefts_per_day} > ${max_thefts_day}")
fi
if lt "$non_commit_share" "$min_non_commit_share"; then
  failures+=("non_commit_share=${non_commit_share} < ${min_non_commit_share}")
fi
if gt "$non_commit_share" "$max_non_commit_share"; then
  failures+=("non_commit_share=${non_commit_share} > ${max_non_commit_share}")
fi

summary="run_id=${RUN_ID} actions_total=${actions_total} unique_actions=${unique_actions} unique_composed_actions=${unique_composed_actions} dominant_action=${dominant_action} dominant_share=${dominant_share} pay_rent_share=${pay_rent_share} theft_share=${theft_share} max_thefts_per_day=${max_thefts_per_day} non_commit_share=${non_commit_share} max_possible_actions=${max_possible_actions} trust_range=${trust_min}..${trust_max}"
echo "$summary"

if [[ -n "$REPORT_PATH" ]]; then
  {
    echo "# Run Metrics Gate"
    echo
    echo "- run_id: \`${RUN_ID}\`"
    echo "- actions_total: \`${actions_total}\`"
    echo "- unique_actions: \`${unique_actions}\`"
    echo "- unique_composed_actions: \`${unique_composed_actions}\`"
    echo "- dominant_action: \`${dominant_action}\`"
    echo "- dominant_share: \`${dominant_share}\`"
    echo "- pay_rent_share: \`${pay_rent_share}\`"
    echo "- theft_share: \`${theft_share}\`"
    echo "- max_thefts_per_day: \`${max_thefts_per_day}\`"
    echo "- non_commit_share: \`${non_commit_share}\`"
    echo "- max_possible_actions: \`${max_possible_actions}\`"
    echo "- trust_range: \`${trust_min}..${trust_max}\`"
    echo
    echo "## Thresholds"
    echo "- min_unique_actions: \`${min_unique}\`"
    echo "- min_unique_composed_actions: \`${min_unique_composed}\`"
    echo "- max_dominant_action_share: \`${max_dominant}\`"
    echo "- max_pay_rent_action_share: \`${max_pay_rent}\`"
    echo "- max_theft_action_share: \`${max_theft_share}\`"
    echo "- max_thefts_per_day: \`${max_thefts_day}\`"
    echo "- min_non_commit_share: \`${min_non_commit_share}\`"
    echo "- max_non_commit_share: \`${max_non_commit_share}\`"
    echo
    if [[ ${#failures[@]} -eq 0 ]]; then
      echo "## Result"
      echo "- PASS"
    else
      echo "## Result"
      echo "- FAIL"
      echo
      echo "## Failures"
      for failure in "${failures[@]}"; do
        echo "- ${failure}"
      done
    fi
  } > "$REPORT_PATH"
fi

if [[ ${#failures[@]} -gt 0 ]]; then
  printf 'gate failures (%s):\n' "$RUN_ID" >&2
  for failure in "${failures[@]}"; do
    printf '  - %s\n' "$failure" >&2
  done
  exit 1
fi
