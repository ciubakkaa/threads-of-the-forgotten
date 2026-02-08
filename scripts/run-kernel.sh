#!/usr/bin/env bash
set -euo pipefail

CARGO_BIN="$(command -v cargo || true)"

if [ -z "${CARGO_BIN}" ] && command -v rustup >/dev/null 2>&1; then
  CARGO_BIN="$(rustup which cargo 2>/dev/null || true)"
  if [ -n "${CARGO_BIN}" ]; then
    export PATH="$(dirname "${CARGO_BIN}"):${PATH}"
  fi
fi

if [ -z "${CARGO_BIN}" ] || [ ! -x "${CARGO_BIN}" ]; then
  echo "cargo not found. Install Rust toolchain first." >&2
  exit 1
fi

"${CARGO_BIN}" run -p kernel-cli -- step 1
