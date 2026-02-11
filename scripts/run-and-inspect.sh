#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run-and-inspect.sh <run_id> <seed> [--ticks <n>] [--db <sqlite_path>] [--out <report.md>] [--strict] [--npc-count <n>] [--npc-min <n> --npc-max <n>]

Examples:
  scripts/run-and-inspect.sh test_auto 1337
  scripts/run-and-inspect.sh test_auto 1337 --ticks 720 --db threads_runs.sqlite --out /tmp/test_auto_report.md
  scripts/run-and-inspect.sh test_small 1337 --ticks 240 --npc-count 5 --strict

What it does:
  1) Runs simulation directly via kernel-cli (no server needed)
  2) Persists run to SQLite
  3) Generates full run inspection report
  4) Evaluates metrics gates
USAGE
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

TICKS=720
DB_PATH="${ROOT_DIR}/threads_runs.sqlite"
OUT_PATH=""
STRICT=0
NPC_COUNT=""
NPC_MIN=""
NPC_MAX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ticks)
      TICKS="${2:-}"
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
    --npc-count)
      NPC_COUNT="${2:-}"
      shift 2
      ;;
    --npc-min)
      NPC_MIN="${2:-}"
      shift 2
      ;;
    --npc-max)
      NPC_MAX="${2:-}"
      shift 2
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

if [[ -z "$OUT_PATH" ]]; then
  OUT_PATH="/tmp/${RUN_ID}_inspection.md"
fi

if ! command -v cargo >/dev/null 2>&1; then
  if [[ -f "$HOME/.zshrc" ]]; then
    # shellcheck source=/dev/null
    source "$HOME/.zshrc"
  fi
fi

require_tool cargo
require_tool sqlite3
require_tool jq

(
  cd "$ROOT_DIR"
  SIM_ARGS=(simulate "$RUN_ID" "$SEED" "$TICKS" "$DB_PATH")
  if [[ -n "$NPC_COUNT" ]]; then
    SIM_ARGS+=(--npc-count "$NPC_COUNT")
  else
    if [[ -n "$NPC_MIN" ]]; then
      SIM_ARGS+=(--npc-min "$NPC_MIN")
    fi
    if [[ -n "$NPC_MAX" ]]; then
      SIM_ARGS+=(--npc-max "$NPC_MAX")
    fi
  fi
  cargo run -p kernel-cli -- "${SIM_ARGS[@]}"
)

"${ROOT_DIR}/scripts/inspect-run.sh" "$RUN_ID" --db "$DB_PATH" --no-api --out "$OUT_PATH"

METRICS_REPORT="${OUT_PATH%.md}_metrics.md"
METRICS_CMD=("${ROOT_DIR}/scripts/check-run-metrics.sh" "$RUN_ID" --db "$DB_PATH" --report "$METRICS_REPORT")
if [[ "$STRICT" -eq 1 ]]; then
  METRICS_CMD+=(--strict)
fi

METRICS_EXIT=0
if "${METRICS_CMD[@]}"; then
  METRICS_RESULT="PASS"
else
  METRICS_RESULT="FAIL"
  METRICS_EXIT=1
fi

echo "run_id: $RUN_ID"
echo "seed: $SEED"
echo "ticks: $TICKS"
echo "db: $DB_PATH"
echo "report: $OUT_PATH"
echo "metrics_report: $METRICS_REPORT"
echo "metrics: $METRICS_RESULT"
if [[ -n "$NPC_COUNT" ]]; then
  echo "npc_count: $NPC_COUNT"
elif [[ -n "$NPC_MIN" || -n "$NPC_MAX" ]]; then
  echo "npc_count_range: ${NPC_MIN:-default}..${NPC_MAX:-default}"
fi
exit "$METRICS_EXIT"
