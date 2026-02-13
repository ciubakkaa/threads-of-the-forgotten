import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";

const DEFAULT_API_BASE =
  (import.meta as ImportMeta & { env?: { VITE_API_BASE?: string } }).env?.VITE_API_BASE ??
  "http://127.0.0.1:8080";
const SCHEMA_VERSION = "1.0";

const REGION_OPTIONS = [
  "crownvale",
  "ironreach_march",
  "saltmere_coast",
  "sunsteppe",
  "fenreach",
  "ashen_wilds",
  "skylark_range"
] as const;

const EVENT_FILTERS = [
  "all",
  "npc_action_committed",
  "plan_created",
  "plan_step_started",
  "plan_step_completed",
  "plan_interrupted",
  "conflict_detected",
  "belief_updated",
  "aspiration_changed",
  "accounting_transfer_recorded"
] as const;

type RegionId = (typeof REGION_OPTIONS)[number];
type EventFilter = (typeof EVENT_FILTERS)[number];

interface ApiErrorPayload {
  schema_version: string;
  error_code: string;
  message: string;
  details?: string;
}

interface RunStatus {
  schema_version: string;
  run_id: string;
  current_tick: number;
  max_ticks: number;
  mode: "running" | "paused";
  queue_depth: number;
}

interface RunConfig {
  schema_version: string;
  run_id: string;
  seed: number;
  duration_days: number;
  region_id: RegionId;
  snapshot_every_ticks: number;
  npc_count_min: number;
  npc_count_max: number;
}

interface CreateRunResponse {
  schema_version: string;
  run_id: string;
  status: RunStatus;
  replaced_existing_run: boolean;
  started: boolean;
}

interface RunControlResponse {
  schema_version: string;
  run_id: string;
  status: RunStatus;
  committed?: number;
  advanced_ticks?: number;
  processed_batch_tick?: number;
  processed_agents?: number;
}

interface RunHistoryEntry {
  run_id: string;
  seed: string;
  region_id: string;
  duration_days: number;
  snapshot_every_ticks: number;
  updated_at: string;
  status: RunStatus;
}

interface RunHistoryResponse {
  schema_version: string;
  active_run_id: string | null;
  runs: RunHistoryEntry[];
}

interface ActorRef {
  actor_id: string;
  actor_kind: string;
}

interface EventRecord {
  schema_version: string;
  run_id: string;
  tick: number;
  created_at: string;
  event_id: string;
  sequence_in_tick: number;
  event_type: string;
  location_id: string;
  actors: ActorRef[];
  reason_packet_id: string | null;
  caused_by: string[];
  tags?: string[];
  details?: Record<string, unknown>;
}

interface QueryResponse<TData> {
  schema_version: string;
  query_type: string;
  run_id: string;
  generated_at_tick: number;
  data: TData;
}

interface TimelineData {
  cursor: number;
  next_cursor: number | null;
  from_tick: number;
  to_tick: number;
  total: number;
  events: EventRecord[];
}

interface ReasonPacket {
  reason_packet_id: string;
  actor_id?: string;
  selection_rationale?: string;
  top_intents?: string[];
  top_beliefs?: string[];
  top_pressures?: string[];
  alternatives_considered?: string[];
  goal_id?: string | null;
  plan_id?: string | null;
  operator_chain_ids?: string[];
  why_chain?: string[];
}

interface EventDetailData {
  event: EventRecord;
  reason_packet: ReasonPacket | null;
}

interface TraceNode {
  depth: number;
  event: EventRecord;
  reason_packet: ReasonPacket | null;
}

interface TraceData {
  root_event_id: string;
  depth: number;
  nodes: TraceNode[];
}

interface DriveEntry {
  current?: number;
  satisfaction?: number;
  urgency_threshold?: number;
  urgent?: boolean;
}

interface ReasonEnvelope {
  reason_envelope_id?: string;
  goal?: string;
  goal_id?: string;
  selected_plan_id?: string;
  plan_id?: string;
  operator_chain?: string[];
  operator_chain_ids?: string[];
  planning_mode?: string;
  context_constraints?: string[];
  rejected_plans?: Array<Record<string, unknown>>;
  drive_pressures?: Record<string, number>;
  selection_rationale?: string;
  why_chain?: string[];
}

interface NpcInspectorData {
  npc_id: string;
  current_location: string;
  drives: Record<string, DriveEntry>;
  active_plan: Record<string, unknown> | null;
  occupancy: Record<string, unknown> | null;
  beliefs: Array<Record<string, unknown>>;
  relationships: Array<Record<string, unknown>>;
  goal_tracks?: Array<Record<string, unknown>>;
  arc_tracks?: Array<Record<string, unknown>>;
  shared_arcs?: Array<Record<string, unknown>>;
  arc_timeline?: EventRecord[];
  arc_summary?: Record<string, number>;
  fatigue?: number;
  recreation_need?: number;
  time_budget?: Record<string, number>;
  recent_reason_envelope: ReasonEnvelope | null;
  recent_actions: EventRecord[];
}

interface NpcArcSummaryData {
  npc_id: string;
  tick: number;
  arc_summary: Record<string, number>;
  arc_tracks: Array<Record<string, unknown>>;
  shared_arcs: Array<Record<string, unknown>>;
  joint_contracts?: Array<Record<string, unknown>>;
  open_blockers: string[];
  timeline: EventRecord[];
  storyline?: Record<string, unknown>;
}

interface NpcStorylineData {
  headline: string;
  chapters: string[];
  summary_text: string;
}

interface SettlementInspectorData {
  settlement_id: string;
  current_tick: number;
  local_npc_count: number;
  recent_events: EventRecord[];
  institution_queues: Array<Record<string, unknown>>;
  market_state: Record<string, unknown>;
  economic_state: Record<string, unknown>;
  local_activity_summary: Record<string, number>;
  local_activity_family_summary?: Record<string, number>;
  local_arc_summary?: Record<string, number>;
  shared_arc_summary?: Record<string, number>;
  joint_contract_summary?: Record<string, number>;
  shared_arcs?: Array<Record<string, unknown>>;
  joint_contracts?: Array<Record<string, unknown>>;
}

async function requestJson<T>(apiBase: string, path: string, init?: RequestInit): Promise<T> {
  const normalizedBase = apiBase.replace(/\/$/, "");
  const response = await fetch(`${normalizedBase}${path}`, init);

  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const apiError = (await response.json()) as ApiErrorPayload;
      if (apiError.error_code && apiError.message) {
        message = `${apiError.error_code}: ${apiError.message}`;
        if (apiError.details) {
          message = `${message} (${apiError.details})`;
        }
      }
    } catch {
      // keep status fallback
    }
    throw new Error(message);
  }

  return (await response.json()) as T;
}

function postJson<T>(apiBase: string, path: string, body: unknown): Promise<T> {
  return requestJson<T>(apiBase, path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
}

function formatMode(mode: RunStatus["mode"]): string {
  return mode === "running" ? "Running" : "Paused";
}

function formatEventType(eventType: string): string {
  return eventType
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function compactValue(value: unknown): string {
  if (value == null) {
    return "(none)";
  }
  if (typeof value === "string") {
    return value.length > 0 ? value : "(none)";
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "(none)";
    }
    return value.slice(0, 4).map((entry) => compactValue(entry)).join(", ");
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 4);
    if (entries.length === 0) {
      return "(none)";
    }
    return entries.map(([key, entry]) => `${key}=${compactValue(entry)}`).join(", ");
  }
  return String(value);
}

function operatorFromEvent(event: EventRecord): string {
  const raw = event.details?.["operator_id"];
  return typeof raw === "string" && raw.length > 0 ? raw : "(unknown operator)";
}

function parametersFromEvent(event: EventRecord): Record<string, unknown> {
  const raw = event.details?.["parameters"];
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return {};
  }
  return raw as Record<string, unknown>;
}

function eventActor(event: EventRecord): string {
  const actor = event.actors[0];
  if (!actor) {
    return "system";
  }
  return `${actor.actor_kind}:${actor.actor_id}`;
}

export function App() {
  const [apiBase, setApiBase] = useState(DEFAULT_API_BASE);
  const [runId, setRunId] = useState("test1");
  const [seed, setSeed] = useState(1337);
  const [durationDays, setDurationDays] = useState(30);
  const [regionId, setRegionId] = useState<RegionId>("crownvale");
  const [snapshotEveryTicks, setSnapshotEveryTicks] = useState(24);
  const [npcCountMin, setNpcCountMin] = useState(40);
  const [npcCountMax, setNpcCountMax] = useState(60);

  const [stepWindows, setStepWindows] = useState(720);
  const [runToTickTarget, setRunToTickTarget] = useState(720);
  const [advanceTicksBy, setAdvanceTicksBy] = useState(24);

  const [status, setStatus] = useState<RunStatus | null>(null);
  const [lastCommitted, setLastCommitted] = useState<number | null>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);

  const [timelineFromTick, setTimelineFromTick] = useState(1);
  const [timelineToTick, setTimelineToTick] = useState(72);
  const [timelineType, setTimelineType] = useState<EventFilter>("all");
  const [timelineActorId, setTimelineActorId] = useState("");
  const [timelineLocationId, setTimelineLocationId] = useState("");
  const [timelineEvents, setTimelineEvents] = useState<EventRecord[]>([]);

  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [eventDetail, setEventDetail] = useState<EventDetailData | null>(null);
  const [traceNodes, setTraceNodes] = useState<TraceNode[]>([]);

  const [npcId, setNpcId] = useState("npc_001");
  const [npcInspector, setNpcInspector] = useState<NpcInspectorData | null>(null);
  const [npcArcSummary, setNpcArcSummary] = useState<NpcArcSummaryData | null>(null);
  const [npcStoryline, setNpcStoryline] = useState<NpcStorylineData | null>(null);

  const [settlementId, setSettlementId] = useState("settlement:greywall");
  const [settlementInspector, setSettlementInspector] = useState<SettlementInspectorData | null>(
    null
  );

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);

  const activeRunId = status?.run_id ?? runId;

  const withAction = useCallback(async (action: () => Promise<void>) => {
    setLoading(true);
    setError(null);
    setInfo(null);
    try {
      await action();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  const refreshRunHistory = useCallback(async () => {
    const response = await requestJson<RunHistoryResponse>(apiBase, "/api/v1/runs?page_size=100");
    setRunHistory(response.runs);
  }, [apiBase]);

  const refreshStatus = useCallback(async () => {
    const response = await requestJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/status`
    );
    setStatus(response.status);
  }, [activeRunId, apiBase]);

  const refreshTimeline = useCallback(async () => {
    const params = new URLSearchParams();
    params.set("from_tick", String(Math.max(1, timelineFromTick)));
    params.set("to_tick", String(Math.max(timelineFromTick, timelineToTick)));
    params.set("page_size", "5000");

    if (timelineType !== "all") {
      params.append("event_types", timelineType);
    }

    const actorFilter = timelineActorId.trim();
    if (actorFilter.length > 0) {
      params.set("actor_id", actorFilter);
    }

    const locationFilter = timelineLocationId.trim();
    if (locationFilter.length > 0) {
      params.set("location_id", locationFilter);
    }

    const response = await requestJson<QueryResponse<TimelineData>>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/timeline?${params.toString()}`
    );
    setTimelineEvents(response.data.events);
  }, [
    activeRunId,
    apiBase,
    timelineActorId,
    timelineFromTick,
    timelineLocationId,
    timelineToTick,
    timelineType
  ]);

  const loadEventDetail = useCallback(
    async (eventId: string) => {
      const [detailResponse, traceResponse] = await Promise.all([
        requestJson<QueryResponse<EventDetailData>>(
          apiBase,
          `/api/v1/runs/${encodeURIComponent(activeRunId)}/events/${encodeURIComponent(eventId)}`
        ),
        requestJson<QueryResponse<TraceData>>(
          apiBase,
          `/api/v1/runs/${encodeURIComponent(activeRunId)}/trace/${encodeURIComponent(
            eventId
          )}?depth=20`
        )
      ]);

      setSelectedEventId(eventId);
      setEventDetail(detailResponse.data);
      setTraceNodes(traceResponse.data.nodes);
    },
    [activeRunId, apiBase]
  );

  const loadNpcInspector = useCallback(async () => {
    const normalized = npcId.trim();
    if (!normalized) {
      return;
    }
    const [response, arcSummaryResponse, storylineResponse] = await Promise.all([
      requestJson<QueryResponse<NpcInspectorData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/npc/${encodeURIComponent(normalized)}`
      ),
      requestJson<QueryResponse<NpcArcSummaryData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/npc/${encodeURIComponent(
          normalized
        )}/arc_summary`
      ),
      requestJson<QueryResponse<NpcStorylineData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/npc/${encodeURIComponent(
          normalized
        )}/storyline`
      )
    ]);
    setNpcInspector(response.data);
    setNpcArcSummary(arcSummaryResponse.data);
    setNpcStoryline(storylineResponse.data);
  }, [activeRunId, apiBase, npcId]);

  const loadSettlementInspector = useCallback(async () => {
    const normalized = settlementId.trim();
    if (!normalized) {
      return;
    }
    const response = await requestJson<QueryResponse<SettlementInspectorData>>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/settlement/${encodeURIComponent(normalized)}`
    );
    setSettlementInspector(response.data);
  }, [activeRunId, apiBase, settlementId]);

  const refreshAll = useCallback(async () => {
    await Promise.all([refreshStatus(), refreshTimeline(), refreshRunHistory()]);
  }, [refreshRunHistory, refreshStatus, refreshTimeline]);

  const createRun = useCallback(async () => {
    const normalizedNpcMin = Math.max(1, Math.floor(npcCountMin));
    const normalizedNpcMax = Math.max(normalizedNpcMin, Math.floor(npcCountMax));

    const config: RunConfig = {
      schema_version: SCHEMA_VERSION,
      run_id: runId.trim(),
      seed: Math.max(0, Math.floor(seed)),
      duration_days: Math.max(1, Math.floor(durationDays)),
      region_id: regionId,
      snapshot_every_ticks: Math.max(1, Math.floor(snapshotEveryTicks)),
      npc_count_min: normalizedNpcMin,
      npc_count_max: normalizedNpcMax
    };

    const response = await postJson<CreateRunResponse>(apiBase, "/api/v1/runs", {
      config,
      auto_start: false,
      replace_existing: true
    });

    setStatus(response.status);
    setLastCommitted(null);
    setTimelineFromTick(1);
    setTimelineToTick(Math.max(72, response.status.current_tick + 72));
    setTimelineEvents([]);
    setSelectedEventId(null);
    setEventDetail(null);
    setTraceNodes([]);
    setNpcInspector(null);
    setNpcArcSummary(null);
    setNpcStoryline(null);
    setSettlementInspector(null);

    await refreshRunHistory();
    setInfo(`Run created: ${response.run_id}`);
  }, [
    apiBase,
    durationDays,
    npcCountMax,
    npcCountMin,
    refreshRunHistory,
    regionId,
    runId,
    seed,
    snapshotEveryTicks
  ]);

  const startRun = useCallback(async () => {
    const response = await postJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/start`,
      {}
    );
    setStatus(response.status);
    setLastCommitted(response.committed ?? null);
    setInfo("Run started.");
  }, [activeRunId, apiBase]);

  const pauseRun = useCallback(async () => {
    const response = await postJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/pause`,
      {}
    );
    setStatus(response.status);
    setLastCommitted(response.committed ?? null);
    setInfo("Run paused.");
  }, [activeRunId, apiBase]);

  const stepRun = useCallback(async () => {
    const response = await postJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/step`,
      { steps: Math.max(1, Math.floor(stepWindows)) }
    );

    setStatus(response.status);
    setLastCommitted(response.committed ?? null);
    setTimelineToTick((current) => Math.max(current, response.status.current_tick));

    await Promise.all([refreshTimeline(), refreshRunHistory()]);
    setInfo(
      `Stepped ${response.committed ?? 0} windows (advanced ${response.advanced_ticks ?? 0} ticks, processed agents ${response.processed_agents ?? 0}). Tick ${response.status.current_tick}.`
    );
  }, [activeRunId, apiBase, refreshRunHistory, refreshTimeline, stepWindows]);

  const runToTick = useCallback(async () => {
    const targetTick = Math.max(0, Math.floor(runToTickTarget));
    const response = await postJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/run_to_tick`,
      { target_tick: targetTick }
    );

    setStatus(response.status);
    setLastCommitted(response.committed ?? null);
    setTimelineToTick((current) => Math.max(current, response.status.current_tick));

    await Promise.all([refreshTimeline(), refreshRunHistory()]);
    setInfo(
      `Advanced to tick ${response.status.current_tick} (windows ${response.committed ?? 0}, advanced ${response.advanced_ticks ?? 0} ticks).`
    );
  }, [activeRunId, apiBase, refreshRunHistory, refreshTimeline, runToTickTarget]);

  const runForwardByTicks = useCallback(async () => {
    const currentTick = status?.current_tick ?? 0;
    setRunToTickTarget(currentTick + Math.max(1, Math.floor(advanceTicksBy)));

    const response = await postJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/run_to_tick`,
      { target_tick: currentTick + Math.max(1, Math.floor(advanceTicksBy)) }
    );

    setStatus(response.status);
    setLastCommitted(response.committed ?? null);
    setTimelineToTick((current) => Math.max(current, response.status.current_tick));

    await Promise.all([refreshTimeline(), refreshRunHistory()]);
    setInfo(
      `Advanced ${Math.max(1, Math.floor(advanceTicksBy))} ticks to ${response.status.current_tick}.`
    );
  }, [activeRunId, advanceTicksBy, apiBase, refreshRunHistory, refreshTimeline, status?.current_tick]);

  const onLoadHistoryRun = useCallback(
    (entry: RunHistoryEntry) => {
      setRunId(entry.run_id);
      setSeed(Number(entry.seed) || 0);
      setDurationDays(entry.duration_days);
      if (REGION_OPTIONS.includes(entry.region_id as RegionId)) {
        setRegionId(entry.region_id as RegionId);
      }
      setSnapshotEveryTicks(entry.snapshot_every_ticks);
      setStatus(entry.status);
      setRunToTickTarget(Math.max(0, entry.status.current_tick + 24));
      setTimelineToTick(Math.max(72, entry.status.current_tick));
      setNpcInspector(null);
      setNpcArcSummary(null);
      setNpcStoryline(null);
      setSettlementInspector(null);
      setInfo(`Loaded run ${entry.run_id}`);
    },
    []
  );

  useEffect(() => {
    void refreshRunHistory();
  }, [refreshRunHistory]);

  useEffect(() => {
    if (!status) {
      return;
    }
    if (timelineToTick < status.current_tick) {
      setTimelineToTick(status.current_tick);
    }
  }, [status, timelineToTick]);

  const actionEvents = useMemo(
    () => timelineEvents.filter((event) => event.event_type === "npc_action_committed"),
    [timelineEvents]
  );

  const actionByNpc = useMemo(() => {
    const counts = new Map<string, number>();
    for (const event of actionEvents) {
      const actor = event.actors.find((candidate) => candidate.actor_kind === "actor");
      if (!actor) {
        continue;
      }
      counts.set(actor.actor_id, (counts.get(actor.actor_id) ?? 0) + 1);
    }

    return Array.from(counts.entries())
      .map(([npc, count]) => ({ npc, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 12);
  }, [actionEvents]);

  const reasonEnvelope = npcInspector?.recent_reason_envelope;

  return (
    <main className="app-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Threads of the Forgotten</p>
          <h1>Agent Observatory</h1>
          <p className="subtitle">
            Scheduler-window controls and inspection for autonomous NPC planning. `step` advances
            windows, while `run_to_tick` targets world time.
          </p>
        </div>
        <label>
          API base
          <input value={apiBase} onChange={(event) => setApiBase(event.target.value)} />
        </label>
      </header>

      <section className="status-strip" aria-live="polite">
        {error ? <p className="error">{error}</p> : null}
        {info ? <p className="info">{info}</p> : null}
      </section>

      <section className="panel">
        <h2>Run Setup</h2>
        <form
          className="grid-form"
          onSubmit={(event: FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            void withAction(createRun);
          }}
        >
          <label>
            Run ID
            <input value={runId} onChange={(event) => setRunId(event.target.value)} />
          </label>
          <label>
            Seed
            <input
              type="number"
              value={seed}
              onChange={(event) => setSeed(Number(event.target.value) || 0)}
            />
          </label>
          <label>
            Duration (days)
            <input
              type="number"
              min={1}
              value={durationDays}
              onChange={(event) => setDurationDays(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <label>
            Region
            <select value={regionId} onChange={(event) => setRegionId(event.target.value as RegionId)}>
              {REGION_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <label>
            Snapshot cadence
            <input
              type="number"
              min={1}
              value={snapshotEveryTicks}
              onChange={(event) =>
                setSnapshotEveryTicks(Math.max(1, Number(event.target.value) || 1))
              }
            />
          </label>
          <label>
            NPC min
            <input
              type="number"
              min={1}
              value={npcCountMin}
              onChange={(event) => setNpcCountMin(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <label>
            NPC max
            <input
              type="number"
              min={npcCountMin}
              value={npcCountMax}
              onChange={(event) =>
                setNpcCountMax(Math.max(npcCountMin, Number(event.target.value) || npcCountMin))
              }
            />
          </label>
          <div className="button-row">
            <button type="submit" disabled={loading}>
              Create / Replace Run
            </button>
            <button type="button" disabled={loading} onClick={() => void withAction(refreshAll)}>
              Refresh All
            </button>
          </div>
        </form>
      </section>

      <section className="panel controls-panel">
        <h2>Runtime Controls</h2>
        <div className="button-row">
          <button type="button" disabled={loading} onClick={() => void withAction(startRun)}>
            Start
          </button>
          <button type="button" disabled={loading} onClick={() => void withAction(pauseRun)}>
            Pause
          </button>
          <button type="button" disabled={loading} onClick={() => void withAction(refreshStatus)}>
            Status
          </button>
        </div>

        <div className="grid-form compact">
          <label>
            Step windows
            <input
              type="number"
              min={1}
              value={stepWindows}
              onChange={(event) => setStepWindows(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <button type="button" disabled={loading} onClick={() => void withAction(stepRun)}>
            Step
          </button>

          <label>
            Target tick
            <input
              type="number"
              min={0}
              value={runToTickTarget}
              onChange={(event) => setRunToTickTarget(Math.max(0, Number(event.target.value) || 0))}
            />
          </label>
          <button type="button" disabled={loading} onClick={() => void withAction(runToTick)}>
            Run To Tick
          </button>

          <label>
            Advance by ticks
            <input
              type="number"
              min={1}
              value={advanceTicksBy}
              onChange={(event) => setAdvanceTicksBy(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <button
            type="button"
            disabled={loading || !status}
            onClick={() => void withAction(runForwardByTicks)}
          >
            Forward
          </button>
        </div>

        <dl className="status-grid">
          <div>
            <dt>Run</dt>
            <dd>{status?.run_id ?? "(none)"}</dd>
          </div>
          <div>
            <dt>Tick</dt>
            <dd>{status ? `${status.current_tick}/${status.max_ticks}` : "-"}</dd>
          </div>
          <div>
            <dt>Mode</dt>
            <dd>{status ? formatMode(status.mode) : "-"}</dd>
          </div>
          <div>
            <dt>Queue depth</dt>
            <dd>{status?.queue_depth ?? "-"}</dd>
          </div>
          <div>
            <dt>Last committed windows</dt>
            <dd>{lastCommitted ?? "-"}</dd>
          </div>
          <div>
            <dt>Timeline events loaded</dt>
            <dd>{timelineEvents.length}</dd>
          </div>
          <div>
            <dt>Action events loaded</dt>
            <dd>{actionEvents.length}</dd>
          </div>
        </dl>
      </section>

      <section className="panel">
        <h2>Run History</h2>
        <div className="run-history-list">
          {runHistory.map((entry) => (
            <button
              key={entry.run_id}
              type="button"
              className="history-row"
              onClick={() => onLoadHistoryRun(entry)}
            >
              <strong>{entry.run_id}</strong>
              <span>
                seed {entry.seed} · tick {entry.status.current_tick}/{entry.status.max_ticks} ·{" "}
                {formatMode(entry.status.mode)}
              </span>
            </button>
          ))}
          {runHistory.length === 0 ? <p className="empty-state">No persisted runs found.</p> : null}
        </div>
      </section>

      <section className="panel">
        <h2>Timeline</h2>
        <form
          className="grid-form compact"
          onSubmit={(event: FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            void withAction(refreshTimeline);
          }}
        >
          <label>
            From tick
            <input
              type="number"
              min={1}
              value={timelineFromTick}
              onChange={(event) => setTimelineFromTick(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <label>
            To tick
            <input
              type="number"
              min={timelineFromTick}
              value={timelineToTick}
              onChange={(event) =>
                setTimelineToTick(Math.max(timelineFromTick, Number(event.target.value) || timelineFromTick))
              }
            />
          </label>
          <label>
            Event type
            <select value={timelineType} onChange={(event) => setTimelineType(event.target.value as EventFilter)}>
              {EVENT_FILTERS.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            Actor filter
            <input
              placeholder="npc_001"
              value={timelineActorId}
              onChange={(event) => setTimelineActorId(event.target.value)}
            />
          </label>
          <label>
            Location filter
            <input
              placeholder="settlement:greywall"
              value={timelineLocationId}
              onChange={(event) => setTimelineLocationId(event.target.value)}
            />
          </label>
          <div className="button-row">
            <button type="submit" disabled={loading}>
              Refresh Timeline
            </button>
          </div>
        </form>

        <div className="timeline-list">
          {timelineEvents.map((event) => (
            <button
              key={event.event_id}
              type="button"
              className={`timeline-item ${selectedEventId === event.event_id ? "selected" : ""}`}
              onClick={() => void withAction(() => loadEventDetail(event.event_id))}
            >
              <strong>
                t{event.tick} · {formatEventType(event.event_type)}
              </strong>
              <span>
                {eventActor(event)} @ {event.location_id}
              </span>
              {event.event_type === "npc_action_committed" ? (
                <span>
                  {operatorFromEvent(event)} · {compactValue(parametersFromEvent(event))}
                </span>
              ) : null}
            </button>
          ))}
          {timelineEvents.length === 0 ? (
            <p className="empty-state">No events in the selected window.</p>
          ) : null}
        </div>
      </section>

      <section className="panel two-col">
        <div>
          <h2>Selected Event</h2>
          {eventDetail ? (
            <>
              <p>
                <strong>{formatEventType(eventDetail.event.event_type)}</strong> at tick {eventDetail.event.tick}
              </p>
              <p>Location: {eventDetail.event.location_id}</p>
              <p>Actors: {eventDetail.event.actors.map((actor) => actor.actor_id).join(", ") || "(none)"}</p>
              {eventDetail.event.event_type === "npc_action_committed" ? (
                <>
                  <p>Operator: {operatorFromEvent(eventDetail.event)}</p>
                  <p>Parameters: {compactValue(parametersFromEvent(eventDetail.event))}</p>
                </>
              ) : null}

              {eventDetail.reason_packet ? (
                <div className="sub-block">
                  <h3>Reason Packet</h3>
                  <p>{eventDetail.reason_packet.selection_rationale ?? "(no rationale)"}</p>
                  <p>
                    Goal/Plan: {compactValue(eventDetail.reason_packet.goal_id)} /{" "}
                    {compactValue(eventDetail.reason_packet.plan_id)}
                  </p>
                  <p>
                    Operator chain: {compactValue(eventDetail.reason_packet.operator_chain_ids ?? [])}
                  </p>
                  <p>Why chain: {compactValue(eventDetail.reason_packet.why_chain ?? [])}</p>
                </div>
              ) : (
                <p className="empty-state">No reason packet linked to this event.</p>
              )}
            </>
          ) : (
            <p className="empty-state">Select an event from timeline.</p>
          )}
        </div>

        <div>
          <h2>Causal Trace</h2>
          <div className="trace-list">
            {traceNodes.map((node) => (
              <div key={node.event.event_id} className="trace-item">
                <strong>
                  depth {node.depth}: t{node.event.tick} {formatEventType(node.event.event_type)}
                </strong>
                <span>{node.event.event_id}</span>
              </div>
            ))}
            {traceNodes.length === 0 ? (
              <p className="empty-state">No trace loaded yet.</p>
            ) : null}
          </div>
        </div>
      </section>

      <section className="panel two-col">
        <div>
          <h2>NPC Inspector</h2>
          <form
            className="inline-form"
            onSubmit={(event: FormEvent<HTMLFormElement>) => {
              event.preventDefault();
              void withAction(loadNpcInspector);
            }}
          >
            <input value={npcId} onChange={(event) => setNpcId(event.target.value)} placeholder="npc_001" />
            <button type="submit" disabled={loading}>
              Inspect NPC
            </button>
          </form>

          {npcInspector ? (
            <>
              <p>
                <strong>{npcInspector.npc_id}</strong> @ {npcInspector.current_location}
              </p>
              <p>
                Beliefs: {npcInspector.beliefs.length} · Relationships: {npcInspector.relationships.length} · Recent
                actions: {npcInspector.recent_actions.length}
              </p>
              <p>
                Fatigue: {compactValue(npcInspector.fatigue)} · Recreation need:{" "}
                {compactValue(npcInspector.recreation_need)}
              </p>

              <h3>Drives</h3>
              <div className="kv-grid">
                {Object.entries(npcInspector.drives).map(([key, value]) => (
                  <div key={key} className="kv-row">
                    <span>{key}</span>
                    <span>{compactValue(value.current ?? value.satisfaction ?? value)}</span>
                  </div>
                ))}
              </div>

              <h3>Time Budget</h3>
              <div className="kv-grid">
                {Object.entries(npcInspector.time_budget ?? {}).map(([key, value]) => (
                  <div key={key} className="kv-row">
                    <span>{key}</span>
                    <span>{compactValue(value)}%</span>
                  </div>
                ))}
                {!npcInspector.time_budget ? <p className="empty-state">No time budget.</p> : null}
              </div>

              <h3>Arc Tracks</h3>
              <pre>{JSON.stringify(npcInspector.arc_tracks ?? [], null, 2)}</pre>

              <h3>Shared Arcs</h3>
              <pre>{JSON.stringify(npcInspector.shared_arcs ?? [], null, 2)}</pre>

              <h3>Arc Timeline</h3>
              <div className="trace-list">
                {(npcInspector.arc_timeline ?? []).map((event) => (
                  <div key={event.event_id} className="trace-item">
                    <strong>
                      t{event.tick} {formatEventType(event.event_type)}
                    </strong>
                    <span>{compactValue(event.details ?? {})}</span>
                  </div>
                ))}
                {(npcInspector.arc_timeline ?? []).length === 0 ? (
                  <p className="empty-state">No arc timeline yet.</p>
                ) : null}
              </div>

              <h3>Arc Summary (Dedicated)</h3>
              <pre>{JSON.stringify(npcArcSummary ?? {}, null, 2)}</pre>

              <h3>Storyline</h3>
              {npcStoryline ? (
                <div className="sub-block">
                  <p>
                    <strong>{npcStoryline.headline}</strong>
                  </p>
                  <p>{npcStoryline.summary_text}</p>
                  <ol>
                    {npcStoryline.chapters.map((chapter) => (
                      <li key={chapter}>{chapter}</li>
                    ))}
                  </ol>
                </div>
              ) : (
                <p className="empty-state">No storyline generated.</p>
              )}

              <h3>Plan / Occupancy</h3>
              <pre>{JSON.stringify({ active_plan: npcInspector.active_plan, occupancy: npcInspector.occupancy }, null, 2)}</pre>

              <h3>Reason Envelope</h3>
              {reasonEnvelope ? (
                <>
                  <p>{reasonEnvelope.selection_rationale ?? "(no rationale)"}</p>
                  <p>
                    Goal/Plan: {compactValue(reasonEnvelope.goal ?? reasonEnvelope.goal_id)} /{" "}
                    {compactValue(reasonEnvelope.selected_plan_id ?? reasonEnvelope.plan_id)}
                  </p>
                  <p>
                    Operators: {compactValue(reasonEnvelope.operator_chain ?? reasonEnvelope.operator_chain_ids ?? [])}
                  </p>
                  <p>Mode: {compactValue(reasonEnvelope.planning_mode)}</p>
                </>
              ) : (
                <p className="empty-state">No recent reason envelope.</p>
              )}
            </>
          ) : (
            <p className="empty-state">No NPC loaded.</p>
          )}
        </div>

        <div>
          <h2>Settlement Inspector</h2>
          <form
            className="inline-form"
            onSubmit={(event: FormEvent<HTMLFormElement>) => {
              event.preventDefault();
              void withAction(loadSettlementInspector);
            }}
          >
            <input
              value={settlementId}
              onChange={(event) => setSettlementId(event.target.value)}
              placeholder="settlement:greywall"
            />
            <button type="submit" disabled={loading}>
              Inspect Settlement
            </button>
          </form>

          {settlementInspector ? (
            <>
              <p>
                <strong>{settlementInspector.settlement_id}</strong> at tick {settlementInspector.current_tick}
              </p>
              <p>Local NPC count: {settlementInspector.local_npc_count}</p>

              <h3>Local Activity Summary</h3>
              <div className="kv-grid">
                {Object.entries(settlementInspector.local_activity_summary).map(([operator, count]) => (
                  <div key={operator} className="kv-row">
                    <span>{operator}</span>
                    <span>{count}</span>
                  </div>
                ))}
                {Object.keys(settlementInspector.local_activity_summary).length === 0 ? (
                  <p className="empty-state">No local action summary yet.</p>
                ) : null}
              </div>

              <h3>Activity Families</h3>
              <div className="kv-grid">
                {Object.entries(settlementInspector.local_activity_family_summary ?? {}).map(
                  ([family, count]) => (
                    <div key={family} className="kv-row">
                      <span>{family}</span>
                      <span>{count}</span>
                    </div>
                  )
                )}
              </div>

              <h3>Arc Summaries</h3>
              <pre>
                {JSON.stringify(
                  {
                    local_arc_summary: settlementInspector.local_arc_summary ?? {},
                    shared_arc_summary: settlementInspector.shared_arc_summary ?? {},
                    joint_contract_summary: settlementInspector.joint_contract_summary ?? {}
                  },
                  null,
                  2
                )}
              </pre>

              <h3>Shared Arcs</h3>
              <pre>{JSON.stringify(settlementInspector.shared_arcs ?? [], null, 2)}</pre>

              <h3>Joint Contracts</h3>
              <pre>{JSON.stringify(settlementInspector.joint_contracts ?? [], null, 2)}</pre>

              <h3>Market</h3>
              <pre>{JSON.stringify(settlementInspector.market_state, null, 2)}</pre>

              <h3>Institutions</h3>
              <pre>{JSON.stringify(settlementInspector.institution_queues, null, 2)}</pre>
            </>
          ) : (
            <p className="empty-state">No settlement loaded.</p>
          )}
        </div>
      </section>

      <section className="panel">
        <h2>Top Active NPCs (Loaded Window)</h2>
        <div className="kv-grid">
          {actionByNpc.map((entry) => (
            <div key={entry.npc} className="kv-row">
              <span>{entry.npc}</span>
              <span>{entry.count} actions</span>
            </div>
          ))}
          {actionByNpc.length === 0 ? (
            <p className="empty-state">No action events in selected timeline window.</p>
          ) : null}
        </div>
      </section>
    </main>
  );
}
