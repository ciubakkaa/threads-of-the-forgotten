#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run-fast-feedback.sh <run_id> <seed> [--ticks <n>] [--npc-count <n>] [--db <sqlite_path>] [--out <report.md>] [--strict]

Examples:
  scripts/run-fast-feedback.sh quick_a 1337
  scripts/run-fast-feedback.sh quick_b 2025 --ticks 720 --npc-count 5 --strict
USAGE
}

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 1
  fi
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${1:-}"
SEED="${2:-}"
if [[ -z "$RUN_ID" || -z "$SEED" ]]; then
  usage
  exit 2
fi
shift 2 || true

TICKS=240
NPC_COUNT=5
DB_PATH="${ROOT_DIR}/threads_runs.sqlite"
OUT_PATH="/tmp/${RUN_ID}_feedback_report.md"
STRICT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ticks)
      TICKS="${2:-}"
      shift 2
      ;;
    --npc-count)
      NPC_COUNT="${2:-}"
      shift 2
      ;;
    --db)
      DB_PATH="${2:-}"
      shift 2
      ;;
    --out)
      OUT_PATH="${2:-}"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

require_tool sqlite3

RUN_CMD=("${ROOT_DIR}/scripts/run-and-inspect.sh" "$RUN_ID" "$SEED" --ticks "$TICKS" --db "$DB_PATH" --out "$OUT_PATH" --npc-count "$NPC_COUNT")
if [[ "$STRICT" -eq 1 ]]; then
  RUN_CMD+=(--strict)
fi
"${RUN_CMD[@]}"

RUN_ID_SQL="$(sql_escape "$RUN_ID")"
sql() {
  sqlite3 "$DB_PATH" "$1"
}

printf '\n=== Fast Feedback: %s ===\n' "$RUN_ID"
printf 'ticks=%s npc_count=%s db=%s\n' "$TICKS" "$NPC_COUNT" "$DB_PATH"

printf '\n-- Top Actions --\n'
sql "SELECT COALESCE(json_extract(payload_json,'$.details.chosen_action'),'none') AS action, COUNT(*) AS c FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted' GROUP BY action ORDER BY c DESC LIMIT 20;"

printf '\n-- Top Composed Actions --\n'
sql "SELECT COALESCE(json_extract(payload_json,'$.details.composed_action'),'none') AS action, COUNT(*) AS c FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted' GROUP BY action ORDER BY c DESC LIMIT 20;"

printf '\n-- Reaction Keys --\n'
sql "SELECT json_extract(payload_json,'$.details.reaction_key') AS reaction_key, COUNT(*) AS c FROM events WHERE run_id='${RUN_ID_SQL}' AND json_extract(payload_json,'$.details.reaction_key') IS NOT NULL GROUP BY reaction_key ORDER BY c DESC;"

printf '\n-- Per-NPC Action Volume --\n'
sql "SELECT json_extract(payload_json,'$.actors[0].actor_id') AS npc_id, COUNT(*) AS c FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type='NpcActionCommitted' GROUP BY npc_id ORDER BY c DESC;"

printf '\n-- Snapshot Occupancy Mix --\n'
sql "WITH occ AS (SELECT json_extract(o.value,'$.occupancy') AS occupancy FROM snapshots s, json_each(s.payload_json,'$.region_state.occupancy') o WHERE s.run_id='${RUN_ID_SQL}') SELECT COALESCE(occupancy,'unknown'), COUNT(*) FROM occ GROUP BY 1 ORDER BY 2 DESC;"

printf '\n-- Conflict / Authority Signals --\n'
sql "SELECT event_type, COUNT(*) AS c FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type IN ('InsultExchanged','PunchThrown','BrawlStarted','BrawlStopped','GuardsDispatched','ArrestMade','TheftCommitted','IllnessContracted','IllnessRecovered','RomanceAdvanced') GROUP BY event_type ORDER BY c DESC;"

printf '\n-- Recent Notable Events (Last 40) --\n'
sql "SELECT tick, event_type, COALESCE(json_extract(payload_json,'$.actors[0].actor_id'),'none') AS actor, COALESCE(json_extract(payload_json,'$.targets[0].actor_id'),'none') AS target, COALESCE(json_extract(payload_json,'$.details.reaction_key'),''), COALESCE(json_extract(payload_json,'$.details.topic'),''), COALESCE(json_extract(payload_json,'$.details.chosen_action'),'') FROM events WHERE run_id='${RUN_ID_SQL}' AND event_type IN ('NpcActionCommitted','ObservationLogged','ConversationHeld','InsultExchanged','PunchThrown','BrawlStarted','BrawlStopped','GuardsDispatched','TheftCommitted','ArrestMade','RomanceAdvanced') ORDER BY tick DESC, sequence_in_tick DESC LIMIT 40;"

printf '\nreport=%s\n' "$OUT_PATH"
