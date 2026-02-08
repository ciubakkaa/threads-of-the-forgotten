#!/usr/bin/env bash
set -euo pipefail

CARGO_BIN="$(command -v cargo || true)"

if [ -z "${CARGO_BIN}" ] && command -v rustup >/dev/null 2>&1; then
  CARGO_BIN="$(rustup which cargo 2>/dev/null || true)"
  if [ -n "${CARGO_BIN}" ]; then
    export PATH="$(dirname "${CARGO_BIN}"):${PATH}"
  fi
fi

if [ -n "${CARGO_BIN}" ] && [ -x "${CARGO_BIN}" ]; then
  "${CARGO_BIN}" test --workspace
else
  echo "cargo not found. Skipping Rust tests." >&2
fi

if [ -f "observatory-web/package.json" ]; then
  yarn --cwd observatory-web test
fi
