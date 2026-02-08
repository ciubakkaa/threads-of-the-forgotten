#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/inspect-run.sh <run_id> [--db <sqlite_path>] [--api-base <url>] [--out <report.md>] [--no-api]

Examples:
  scripts/inspect-run.sh forest
  scripts/inspect-run.sh run_demo --out /tmp/run_demo_report.md
  scripts/inspect-run.sh forest --db /path/to/threads_runs.sqlite --no-api
EOF
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUN_ID="${1:-}"
if [[ -z "$RUN_ID" || "$RUN_ID" == "--help" || "$RUN_ID" == "-h" ]]; then
  usage
  exit 2
fi
shift || true

DB_PATH="${ROOT_DIR}/threads_runs.sqlite"
API_BASE="http://127.0.0.1:8080"
OUT_PATH=""
USE_API=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)
      DB_PATH="${2:-}"
      shift 2
      ;;
    --api-base)
      API_BASE="${2:-}"
      shift 2
      ;;
    --out)
      OUT_PATH="${2:-}"
      shift 2
      ;;
    --no-api)
      USE_API=0
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
require_tool jq

if [[ ! -f "$DB_PATH" ]]; then
  echo "sqlite database not found: $DB_PATH" >&2
  exit 1
fi

RUN_ID_SQL="$(sql_escape "$RUN_ID")"
RUN_ID_URI="$(printf '%s' "$RUN_ID" | jq -sRr @uri)"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/threads_inspect_${RUN_ID}_XXXX")"
ACTION_LOG="${WORK_DIR}/action-log.txt"
SNAPSHOT_JSON="${WORK_DIR}/snapshot_latest.json"

log_action() {
  printf '%s\n' "$1" >> "$ACTION_LOG"
}

sql() {
  local query="$1"
  log_action "sqlite3 \"$DB_PATH\" \"$query\""
  sqlite3 "$DB_PATH" "$query"
}

sql_csv() {
  local query="$1"
  log_action "sqlite3 -header -csv \"$DB_PATH\" \"$query\""
  sqlite3 -header -csv "$DB_PATH" "$query"
}

RUN_EXISTS="$(sql "SELECT COUNT(*) FROM runs WHERE run_id='${RUN_ID_SQL}';")"
if [[ "$RUN_EXISTS" != "1" ]]; then
  echo "run not found: ${RUN_ID}" >&2
  echo "available runs:" >&2
  sql "SELECT run_id FROM runs ORDER BY updated_at DESC;" >&2 || true
  exit 1
fi

API_STATUS_STATE="skipped"
API_STATUS_PAYLOAD=""
if [[ "$USE_API" -eq 1 ]]; then
  if command -v curl >/dev/null 2>&1; then
    API_URL="${API_BASE}/api/v1/runs/${RUN_ID_URI}/status"
    log_action "curl -sS \"$API_URL\""
    if API_STATUS_PAYLOAD="$(curl -sS "$API_URL" 2>"${WORK_DIR}/api-error.txt")"; then
      API_STATUS_STATE="ok"
    else
      API_STATUS_STATE="unreachable"
      API_STATUS_PAYLOAD="$(cat "${WORK_DIR}/api-error.txt" 2>/dev/null || true)"
    fi
  else
    API_STATUS_STATE="curl_missing"
  fi
fi

RUN_CREATED_AT="$(sql "SELECT created_at FROM runs WHERE run_id='${RUN_ID_SQL}' LIMIT 1;")"
RUN_UPDATED_AT="$(sql "SELECT updated_at FROM runs WHERE run_id='${RUN_ID_SQL}' LIMIT 1;")"
RUN_SEED="$(sql "SELECT seed FROM runs WHERE run_id='${RUN_ID_SQL}' LIMIT 1;")"
RUN_TICK="$(sql "SELECT json_extract(status_json,'$.current_tick') FROM runs WHERE run_id='${RUN_ID_SQL}' LIMIT 1;")"
RUN_MODE="$(sql "SELECT json_extract(status_json,'$.mode') FROM runs WHERE run_id='${RUN_ID_SQL}' LIMIT 1;")"

EVENT_COUNT="$(sql "SELECT COUNT(*) FROM events WHERE run_id='${RUN_ID_SQL}';")"
REASON_COUNT="$(sql "SELECT COUNT(*) FROM reason_packets WHERE run_id='${RUN_ID_SQL}';")"
COMMAND_COUNT="$(sql "SELECT COUNT(*) FROM commands WHERE run_id='${RUN_ID_SQL}';")"
EVENT_TICK_SPAN="$(sql "SELECT MIN(tick) || ' .. ' || MAX(tick) || ' (' || COUNT(DISTINCT tick) || ' ticks)' FROM events WHERE run_id='${RUN_ID_SQL}';")"

LATEST_SNAPSHOT_TICK="$(sql "SELECT COALESCE(MAX(tick),0) FROM snapshots WHERE run_id='${RUN_ID_SQL}';")"
if [[ "$LATEST_SNAPSHOT_TICK" -gt 0 ]]; then
  log_action "sqlite3 \"$DB_PATH\" \"SELECT payload_json FROM snapshots WHERE run_id='${RUN_ID_SQL}' AND tick=${LATEST_SNAPSHOT_TICK} LIMIT 1;\" > \"$SNAPSHOT_JSON\""
  sqlite3 "$DB_PATH" "SELECT payload_json FROM snapshots WHERE run_id='${RUN_ID_SQL}' AND tick=${LATEST_SNAPSHOT_TICK} LIMIT 1;" > "$SNAPSHOT_JSON"
  SNAPSHOT_CORE="$(jq '{tick,pressure_index:.region_state.pressure_index,social_cohesion:.region_state.social_cohesion,wanted_npc_count:.region_state.wanted_npc_count,stolen_item_count:.region_state.stolen_item_count,law_case_load_total:.region_state.law_case_load_total,winter_severity:.region_state.winter_severity}' "$SNAPSHOT_JSON")"
  SNAPSHOT_NPC_LEDGERS="$(jq '.npc_state_refs.npc_ledgers' "$SNAPSHOT_JSON")"
  SNAPSHOT_HOUSEHOLDS="$(jq '.region_state.households' "$SNAPSHOT_JSON")"
  SNAPSHOT_LABOR="$(jq '.region_state.labor' "$SNAPSHOT_JSON")"
  SNAPSHOT_PRODUCTION="$(jq '.region_state.production' "$SNAPSHOT_JSON")"
  SNAPSHOT_RELATIONSHIPS="$(jq '{relationships_count:(.region_state.relationships|length),trust_min:(.region_state.relationships|map(.trust)|min),trust_max:(.region_state.relationships|map(.trust)|max),grievance_avg:(.region_state.relationships|map(.grievance)|add/length),trust_levels:(.region_state.relationships|group_by(.trust_level)|map({trust_level:.[0].trust_level,count:length}))}' "$SNAPSHOT_JSON")"
  SNAPSHOT_INSTITUTIONS="$(jq '.region_state.institutions' "$SNAPSHOT_JSON")"
  SNAPSHOT_GROUPS="$(jq '.region_state.groups' "$SNAPSHOT_JSON")"
  SNAPSHOT_MOBILITY="$(jq '.region_state.mobility' "$SNAPSHOT_JSON")"
  SNAPSHOT_BELIEFS="$(jq '{belief_count:(.region_state.beliefs|length),confidence_min:(.region_state.beliefs|map(.confidence)|min),confidence_max:(.region_state.beliefs|map(.confidence)|max),distortion_min:(.region_state.beliefs|map(.distortion_score)|min),distortion_max:(.region_state.beliefs|map(.distortion_score)|max),share_min:(.region_state.beliefs|map(.willingness_to_share)|min),share_max:(.region_state.beliefs|map(.willingness_to_share)|max)}' "$SNAPSHOT_JSON")"
  SNAPSHOT_NARRATIVE="$(jq '.region_state.narrative_summaries | {count:length,sample:(.[0:8])}' "$SNAPSHOT_JSON")"
else
  SNAPSHOT_CORE="null"
  SNAPSHOT_NPC_LEDGERS="[]"
  SNAPSHOT_HOUSEHOLDS="[]"
  SNAPSHOT_LABOR="{}"
  SNAPSHOT_PRODUCTION="{}"
  SNAPSHOT_RELATIONSHIPS="{}"
  SNAPSHOT_INSTITUTIONS="[]"
  SNAPSHOT_GROUPS="[]"
  SNAPSHOT_MOBILITY="[]"
  SNAPSHOT_BELIEFS="{}"
  SNAPSHOT_NARRATIVE="{}"
fi

EVENTS_BY_TYPE="$(sql_csv "SELECT event_type, COUNT(*) AS count FROM events WHERE run_id='${RUN_ID_SQL}' GROUP BY event_type ORDER BY count DESC;")"
ACTION_MIX_EVENTS="$(sql_csv "SELECT json_extract(payload_json,'$.details.chosen_action') AS chosen_action, COUNT(*) AS count FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted' GROUP BY chosen_action ORDER BY count DESC;")"
ACTION_MIX_REASONS="$(sql_csv "SELECT json_extract(payload_json,'$.chosen_action') AS chosen_action, COUNT(*) AS count FROM reason_packets WHERE run_id='${RUN_ID_SQL}' GROUP BY chosen_action ORDER BY count DESC;")"
MOTIVE_MIX="$(sql_csv "SELECT je.value AS motive_family, COUNT(*) AS count FROM reason_packets rp, json_each(rp.payload_json,'$.motive_families') je WHERE rp.run_id='${RUN_ID_SQL}' GROUP BY je.value ORDER BY count DESC;")"
PRESSURE_MIX="$(sql_csv "SELECT je.value AS top_pressure, COUNT(*) AS count FROM reason_packets rp, json_each(rp.payload_json,'$.top_pressures') je WHERE rp.run_id='${RUN_ID_SQL}' GROUP BY je.value ORDER BY count DESC;")"

OUTCOME_EVENTS="$(sql_csv "SELECT event_type, COUNT(*) AS count FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type IN ('TheftCommitted','WagePaid','WageDelayed','RentUnpaid','RentDue','HouseholdBufferExhausted','EvictionRiskChanged','InstitutionCaseResolved','ArrestMade','BeliefUpdated','BeliefDisputed','RelationshipShifted','GroupFormed','GroupSplit','GroupDissolved','NarrativeWhySummary') GROUP BY event_type ORDER BY count DESC;")"

DAILY_ROLLUP="$(sql_csv "WITH daily AS (
  SELECT
    ((tick - 1) / 24) + 1 AS day,
    SUM(event_type='TheftCommitted') AS thefts,
    SUM(event_type='WagePaid') AS wage_paid,
    SUM(event_type='WageDelayed') AS wage_delayed,
    SUM(event_type='HouseholdBufferExhausted') AS household_buffer_exhausted,
    SUM(event_type='RentUnpaid') AS rent_unpaid
  FROM events
  WHERE run_id='${RUN_ID_SQL}'
  GROUP BY day
)
SELECT day, thefts, wage_paid, wage_delayed, household_buffer_exhausted, rent_unpaid
FROM daily
ORDER BY day;")"

NARRATIVE_CONSEQUENCE_MIX="$(sql_csv "SELECT json_extract(payload_json,'$.details.social_consequence') AS social_consequence, COUNT(*) AS count FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NarrativeWhySummary' GROUP BY social_consequence ORDER BY count DESC;")"

REPORT_CONTENT="$(
  cat <<EOF
# Run Inspection Report: ${RUN_ID}

## Run Meta
- run_id: \`${RUN_ID}\`
- created_at: \`${RUN_CREATED_AT}\`
- updated_at: \`${RUN_UPDATED_AT}\`
- seed: \`${RUN_SEED}\`
- current_tick: \`${RUN_TICK}\`
- mode: \`${RUN_MODE}\`
- events: \`${EVENT_COUNT}\`
- reason_packets: \`${REASON_COUNT}\`
- commands: \`${COMMAND_COUNT}\`
- event_tick_span: \`${EVENT_TICK_SPAN}\`
- latest_snapshot_tick: \`${LATEST_SNAPSHOT_TICK}\`

## API Probe
- api_base: \`${API_BASE}\`
- api_status: \`${API_STATUS_STATE}\`
\`\`\`text
${API_STATUS_PAYLOAD}
\`\`\`

## Events By Type
\`\`\`csv
${EVENTS_BY_TYPE}
\`\`\`

## Action Mix (Events)
\`\`\`csv
${ACTION_MIX_EVENTS}
\`\`\`

## Action Mix (Reason Packets)
\`\`\`csv
${ACTION_MIX_REASONS}
\`\`\`

## Motive Families
\`\`\`csv
${MOTIVE_MIX}
\`\`\`

## Top Pressures
\`\`\`csv
${PRESSURE_MIX}
\`\`\`

## Outcome Event Counts
\`\`\`csv
${OUTCOME_EVENTS}
\`\`\`

## Daily Rollup
\`\`\`csv
${DAILY_ROLLUP}
\`\`\`

## Narrative Consequence Mix
\`\`\`csv
${NARRATIVE_CONSEQUENCE_MIX}
\`\`\`

## Snapshot Core
\`\`\`json
${SNAPSHOT_CORE}
\`\`\`

## Snapshot NPC Ledgers
\`\`\`json
${SNAPSHOT_NPC_LEDGERS}
\`\`\`

## Snapshot Households
\`\`\`json
${SNAPSHOT_HOUSEHOLDS}
\`\`\`

## Snapshot Labor
\`\`\`json
${SNAPSHOT_LABOR}
\`\`\`

## Snapshot Production
\`\`\`json
${SNAPSHOT_PRODUCTION}
\`\`\`

## Snapshot Relationships (summary)
\`\`\`json
${SNAPSHOT_RELATIONSHIPS}
\`\`\`

## Snapshot Institutions
\`\`\`json
${SNAPSHOT_INSTITUTIONS}
\`\`\`

## Snapshot Groups
\`\`\`json
${SNAPSHOT_GROUPS}
\`\`\`

## Snapshot Mobility
\`\`\`json
${SNAPSHOT_MOBILITY}
\`\`\`

## Snapshot Belief Stats
\`\`\`json
${SNAPSHOT_BELIEFS}
\`\`\`

## Snapshot Narrative Sample
\`\`\`json
${SNAPSHOT_NARRATIVE}
\`\`\`

## Fetch Action Log
\`\`\`text
$(cat "$ACTION_LOG")
\`\`\`
EOF
)"

if [[ -n "$OUT_PATH" ]]; then
  mkdir -p "$(dirname "$OUT_PATH")"
  printf '%s\n' "$REPORT_CONTENT" > "$OUT_PATH"
  echo "wrote report: $OUT_PATH"
else
  printf '%s\n' "$REPORT_CONTENT"
fi

