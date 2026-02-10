# Threads of the Forgotten

Milestone 0 scaffold for the vertical-slice kernel + observatory stack.

## Workspace Layout

- `crates/contracts`: shared Rust contract structs (minimal v1 shell)
- `crates/kernel-core`: deterministic no-op tick loop
- `crates/kernel-api`: in-process control/query facade for the kernel
- `crates/kernel-cli`: CLI for start/pause/step/run-to commands
- `observatory-web`: React + TypeScript web app scaffold
- `scripts`: shared run/test helpers

## Prerequisites

- Rust toolchain (`cargo`, `rustc`)
- Node.js and Yarn

## Quick Commands

- Run one tick from CLI:
  - `scripts/run-kernel.sh`
- Run API server:
  - `cargo run -p kernel-cli -- serve`
- Run tests:
  - `scripts/test.sh`
- Run a full 720-tick simulation and inspect all outcomes:
  - `scripts/run-and-inspect.sh <run_id> <seed>`
  - Example: `scripts/run-and-inspect.sh test_auto 1337`
  - Produces a report in `/tmp/<run_id>_inspection.md` plus metrics report.
- Start frontend dev server:
  - `yarn --cwd observatory-web install`
  - `yarn --cwd observatory-web dev`

## API Baseline (Milestone 4)

Start server:

- `cargo run -p kernel-cli -- serve 127.0.0.1:8080`

Persistence default:

- New runs persist to SQLite by default at `threads_runs.sqlite` (relative to the server process working directory).
- Override path with env var `THREADS_SQLITE_PATH=/absolute/or/relative/path.sqlite`.

Create run:

- `curl -X POST http://127.0.0.1:8080/api/v1/runs -H 'content-type: application/json' -d '{\"schema_version\":\"1.0\",\"run_id\":\"run_demo\",\"seed\":\"1337\",\"duration_days\":30,\"region_id\":\"crownvale\",\"snapshot_every_ticks\":24}'`

Step run:

- `curl -X POST http://127.0.0.1:8080/api/v1/runs/run_demo/step -H 'content-type: application/json' -d '{\"steps\":1}'`

Read timeline:

- `curl \"http://127.0.0.1:8080/api/v1/runs/run_demo/timeline?from_tick=1&to_tick=5\"`

## Observatory MVP (Milestone 5 baseline)

Run backend + frontend:

1. `cargo run -p kernel-cli -- serve 127.0.0.1:8080`
2. `yarn --cwd observatory-web dev`

Then open the observatory, create a run, and use the panels for run control, timeline, map, inspector, trace, and stream.

Milestone 6 baseline adds:
- scenario controls for all v1 injectors
- seed comparison lab (run the same scenario across multiple seeds and compare outcome summaries)

## Milestone 8 Sign-off Docs

- Observatory runbook: `plans/observatoryRunbook.md`
- Example scenario pack: `plans/exampleScenarioPack.md`
- Known limitations (baseline): `plans/knownLimitations.md`

## Post-Signoff Expansion (Iteration 1)

- Baseline law/justice loop: theft, investigation, and arrest events
- Baseline item continuity loop: persistent item registry with deterministic transfers
- Baseline discovery/leverage + cooperation/betrayal relationship loop events
- Timeline filtering support for new event types in API and observatory

Believability program roadmap:

- `plans/believabilityImplementationPlan.md`
