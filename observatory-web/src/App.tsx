import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

const DEFAULT_API_BASE =
  (import.meta as ImportMeta & { env?: { VITE_API_BASE?: string } }).env?.
    VITE_API_BASE ?? "http://127.0.0.1:8080";

const REGION_OPTIONS = [
  "crownvale",
  "ironreach_march",
  "saltmere_coast",
  "sunsteppe",
  "fenreach",
  "ashen_wilds",
  "skylark_range"
] as const;

const TIMELINE_EVENT_TYPES = [
  "all",
  "system_tick",
  "npc_action_committed",
  "command_applied",
  "rumor_injected",
  "caravan_spawned",
  "npc_removed",
  "bad_harvest_forced",
  "winter_severity_set",
  "theft_committed",
  "item_transferred",
  "investigation_progressed",
  "arrest_made",
  "site_discovered",
  "leverage_gained",
  "relationship_shifted",
  "pressure_economy_updated",
  "household_consumption_applied",
  "rent_due",
  "rent_unpaid",
  "eviction_risk_changed",
  "household_buffer_exhausted",
  "job_sought",
  "contract_signed",
  "wage_paid",
  "wage_delayed",
  "contract_breached",
  "employment_terminated",
  "production_started",
  "production_completed",
  "spoilage_occurred",
  "stock_shortage",
  "stock_recovered",
  "trust_changed",
  "obligation_created",
  "obligation_called",
  "grievance_recorded",
  "relationship_status_changed",
  "belief_formed",
  "belief_updated",
  "belief_disputed",
  "rumor_mutated",
  "belief_forgotten",
  "institution_profile_updated",
  "institution_case_resolved",
  "institutional_error_recorded",
  "group_formed",
  "group_membership_changed",
  "group_split",
  "group_dissolved",
  "apprenticeship_progressed",
  "succession_transferred",
  "route_risk_updated",
  "travel_window_shifted",
  "narrative_why_summary",
  "opportunity_opened",
  "opportunity_expired",
  "opportunity_accepted",
  "opportunity_rejected",
  "commitment_started",
  "commitment_continued",
  "commitment_completed",
  "commitment_broken",
  "market_cleared",
  "market_failed",
  "accounting_transfer_recorded",
  "institution_queue_updated"
] as const;

type RegionId = (typeof REGION_OPTIONS)[number];
type TimelineEventTypeFilter = (typeof TIMELINE_EVENT_TYPES)[number];

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
  seed: string;
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

interface ReasonPacket {
  schema_version: string;
  run_id: string;
  tick: number;
  created_at: string;
  reason_packet_id: string;
  actor_id: string;
  chosen_action: string;
  top_intents: string[];
  top_beliefs: string[];
  top_pressures: string[];
  alternatives_considered: string[];
  motive_families?: string[];
  feasibility_checks?: string[];
  chosen_verb?: string | null;
  context_constraints?: string[];
  why_chain?: string[];
  expected_consequences?: string[];
  selection_rationale: string;
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

interface NpcInspectorData {
  npc_id: string;
  current_location: string;
  top_intents: string[];
  last_action: EventRecord | null;
  reason_packet: ReasonPacket | null;
  recent_belief_updates: string[];
  recent_actions: EventRecord[];
  key_relationships: unknown[];
  household_status?: Record<string, unknown> | null;
  npc_ledger?: Record<string, unknown> | null;
  contract_status?: Array<Record<string, unknown>>;
  relationship_edges?: Array<Record<string, unknown>>;
  active_beliefs?: Array<Record<string, unknown>>;
  opportunities?: Array<Record<string, unknown>>;
  commitments?: Array<Record<string, unknown>>;
  time_budget?: Record<string, unknown> | null;
  motive_chain?: string[];
  why_summaries?: Array<Record<string, unknown>>;
}

interface SettlementInspectorData {
  settlement_id: string;
  food_status: "low" | "stable" | "surplus";
  security_status: "calm" | "tense" | "unrest";
  institutional_health: "clean" | "corrupt" | "fragile";
  pressure_readouts: Record<string, number>;
  labor_market?: Record<string, unknown>;
  stock_ledger?: Record<string, unknown>;
  institution_profile?: Record<string, unknown>;
  production_nodes?: Array<Record<string, unknown>>;
  groups?: Array<Record<string, unknown>>;
  routes?: Array<Record<string, unknown>>;
  market_clearing?: Record<string, unknown>;
  institution_queue?: Record<string, unknown>;
  accounting_transfers?: Array<Record<string, unknown>>;
  notable_events: EventRecord[];
}

interface Snapshot {
  schema_version: string;
  run_id: string;
  tick: number;
  created_at: string;
  snapshot_id: string;
  world_state_hash: string;
}

interface SnapshotsData {
  count: number;
  snapshots: Snapshot[];
}

interface CommandResult {
  schema_version: string;
  command_id: string;
  run_id: string;
  accepted: boolean;
  error: ApiErrorPayload | null;
}

interface CommandRecord {
  command: {
    schema_version: string;
    command_id: string;
    run_id: string;
    issued_at_tick: number;
    command_type: string;
    payload: Record<string, unknown>;
  };
  result: CommandResult;
  effective_tick: number;
}

interface CommandAuditData {
  schema_version: string;
  run_id: string;
  cursor: number;
  next_cursor: number | null;
  entries: CommandRecord[];
}

interface StreamMessage {
  schema_version: string;
  type: "run.status" | "event.appended" | "snapshot.created" | "command.result" | "warning";
  run_id: string;
  tick: number;
  sequence_in_tick: number | null;
  reconnect_token: string;
  payload: unknown;
}

type ComparisonScenario =
  | "none"
  | "inject_rumor"
  | "inject_spawn_caravan"
  | "inject_remove_npc"
  | "inject_force_bad_harvest"
  | "inject_set_winter_severity";

interface SeedComparisonRow {
  seed: number;
  run_id: string;
  final_tick: number;
  event_count: number;
  npc_action_count: number;
  rumor_events: number;
  caravan_events: number;
  bad_harvest_events: number;
  winter_events: number;
  pressure_events: number;
  terminal_pressure_index: number;
}

interface SettlementMapNode {
  id: string;
  label: string;
  x: number;
  y: number;
}

const MAP_NODES: SettlementMapNode[] = [
  { id: "settlement:greywall", label: "Greywall", x: 120, y: 70 },
  { id: "settlement:millford", label: "Millford", x: 330, y: 150 },
  { id: "settlement:oakham", label: "Oakham", x: 140, y: 250 }
];

const MAP_EDGES: Array<[string, string]> = [
  ["settlement:greywall", "settlement:millford"],
  ["settlement:greywall", "settlement:oakham"],
  ["settlement:oakham", "settlement:millford"]
];

const SCHEMA_VERSION = "1.0";

function toWsBase(apiBase: string): string {
  try {
    const parsed = new URL(apiBase);
    parsed.protocol = parsed.protocol === "https:" ? "wss:" : "ws:";
    parsed.pathname = parsed.pathname.replace(/\/$/, "");
    return parsed.toString().replace(/\/$/, "");
  } catch {
    return apiBase.replace(/^http/i, "ws").replace(/\/$/, "");
  }
}

function commandId(prefix: string): string {
  const random = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${Date.now()}_${random}`;
}

async function requestJson<T>(apiBase: string, path: string, init?: RequestInit): Promise<T> {
  const normalizedBase = apiBase.replace(/\/$/, "");
  const response = await fetch(`${normalizedBase}${path}`, init);

  if (!response.ok) {
    let message = `HTTP ${response.status}`;

    try {
      const apiError = (await response.json()) as ApiErrorPayload;
      if (apiError?.error_code && apiError?.message) {
        message = `${apiError.error_code}: ${apiError.message}`;
        if (apiError.details) {
          message = `${message} (${apiError.details})`;
        }
      }
    } catch {
      // keep default message
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

function formatEventType(eventType: string): string {
  return eventType
    .replace(/_/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatMode(mode: RunStatus["mode"]): string {
  return mode === "running" ? "Running" : "Paused";
}

function parseSeedsCsv(raw: string): number[] {
  return Array.from(
    new Set(
      raw
        .split(",")
        .map((value) => Number(value.trim()))
        .filter((value) => Number.isInteger(value) && value >= 0)
    )
  );
}

function extractPressureIndex(event: EventRecord): number {
  const details = event.details;
  if (!details) {
    return 0;
  }

  const rawValue = details["pressure_index"];
  if (typeof rawValue === "number" && Number.isFinite(rawValue)) {
    return rawValue;
  }

  return 0;
}

function mergeEventsById(current: EventRecord[], incoming: EventRecord[]): EventRecord[] {
  if (incoming.length === 0) {
    return current;
  }

  const byId = new Map<string, EventRecord>();
  for (const event of current) {
    byId.set(event.event_id, event);
  }
  for (const event of incoming) {
    byId.set(event.event_id, event);
  }

  return Array.from(byId.values()).sort((left, right) => {
    if (left.tick !== right.tick) {
      return left.tick - right.tick;
    }
    return left.sequence_in_tick - right.sequence_in_tick;
  });
}

function eventActorLabel(event: EventRecord): string {
  const actor = event.actors?.[0];
  if (!actor) {
    return "system";
  }
  return `${actor.actor_kind}:${actor.actor_id}`;
}

function eventPrimaryTopic(event: EventRecord): string {
  const details = event.details ?? {};
  const topic = details["topic"];
  if (typeof topic === "string" && topic.length > 0) {
    return topic;
  }
  const chosenAction = details["chosen_action"];
  if (typeof chosenAction === "string" && chosenAction.length > 0) {
    return chosenAction;
  }
  return formatEventType(event.event_type);
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry): entry is string => typeof entry === "string");
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
    const preview = value.slice(0, 4).map((entry) => compactValue(entry)).join(", ");
    return value.length > 4 ? `${preview}, ...` : preview;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 4);
    if (entries.length === 0) {
      return "(none)";
    }
    const preview = entries.map(([key, entry]) => `${key}=${compactValue(entry)}`).join(", ");
    return Object.keys(value as Record<string, unknown>).length > 4
      ? `${preview}, ...`
      : preview;
  }
  return String(value);
}

function summarizeReasonFromEvent(event: EventRecord): string {
  const details = event.details ?? {};
  const rationale = details["selection_rationale"];
  if (typeof rationale === "string" && rationale.length > 0) {
    return rationale;
  }

  const whyChain = asStringList(details["why_chain"]);
  if (whyChain.length > 0) {
    return whyChain.slice(0, 3).join(" -> ");
  }

  const chosenAction = details["chosen_action"];
  if (typeof chosenAction === "string" && chosenAction.length > 0) {
    return chosenAction;
  }

  return "No structured rationale on this event.";
}

export function App() {
  const [apiBase, setApiBase] = useState(DEFAULT_API_BASE);
  const [runId, setRunId] = useState("run_demo");
  const [seed, setSeed] = useState("1337");
  const [durationDays, setDurationDays] = useState(30);
  const [snapshotEveryTicks, setSnapshotEveryTicks] = useState(24);
  const [regionId, setRegionId] = useState<RegionId>("crownvale");

  const [status, setStatus] = useState<RunStatus | null>(null);
  const [timelineEvents, setTimelineEvents] = useState<EventRecord[]>([]);
  const [liveEvents, setLiveEvents] = useState<EventRecord[]>([]);
  const [eventDetail, setEventDetail] = useState<EventDetailData | null>(null);
  const [traceNodes, setTraceNodes] = useState<TraceNode[]>([]);
  const [commandAudit, setCommandAudit] = useState<CommandRecord[]>([]);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);

  const [npcInspectorId, setNpcInspectorId] = useState("npc_001");
  const [npcInspector, setNpcInspector] = useState<NpcInspectorData | null>(null);
  const [settlementInspectorId, setSettlementInspectorId] = useState("settlement:greywall");
  const [settlementInspector, setSettlementInspector] =
    useState<SettlementInspectorData | null>(null);

  const [timelineFromTick, setTimelineFromTick] = useState(1);
  const [timelineToTick, setTimelineToTick] = useState(72);
  const [timelineEventType, setTimelineEventType] =
    useState<TimelineEventTypeFilter>("all");
  const [timelineActorId, setTimelineActorId] = useState("");
  const [timelineLocationId, setTimelineLocationId] = useState("");

  const [stepCount, setStepCount] = useState(1);
  const [targetTick, setTargetTick] = useState(24);

  const [rumorLocation, setRumorLocation] = useState("settlement:greywall");
  const [rumorText, setRumorText] = useState("A pale fire was seen by the pass.");
  const [caravanOrigin, setCaravanOrigin] = useState("settlement:oakham");
  const [caravanDestination, setCaravanDestination] = useState("settlement:millford");
  const [removeNpcId, setRemoveNpcId] = useState("npc_002");
  const [badHarvestSettlement, setBadHarvestSettlement] = useState("settlement:oakham");
  const [winterSeverity, setWinterSeverity] = useState(55);
  const [comparisonSeeds, setComparisonSeeds] = useState("1337, 2026, 9001");
  const [comparisonTargetTick, setComparisonTargetTick] = useState(96);
  const [comparisonScenario, setComparisonScenario] =
    useState<ComparisonScenario>("inject_rumor");
  const [comparisonRows, setComparisonRows] = useState<SeedComparisonRow[]>([]);

  const [streamEnabled, setStreamEnabled] = useState(true);
  const [streamState, setStreamState] = useState<"idle" | "connecting" | "open" | "closed">(
    "idle"
  );
  const [streamMessages, setStreamMessages] = useState<StreamMessage[]>([]);
  const [autoFollowLive, setAutoFollowLive] = useState(true);
  const [autoRefreshInspector, setAutoRefreshInspector] = useState(true);
  const [liveEventFilter, setLiveEventFilter] = useState("");
  const [liveActorFilter, setLiveActorFilter] = useState("");
  const pollingInFlightRef = useRef(false);

  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);

  const activeRunId = status?.run_id ?? runId;
  const streamRunId = status?.run_id ?? null;

  const focusedLocation =
    eventDetail?.event.location_id ?? settlementInspector?.settlement_id ?? "";

  const settlementCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const node of MAP_NODES) {
      counts.set(node.id, 0);
    }

    for (const event of liveEvents.length > 0 ? liveEvents : timelineEvents) {
      if (counts.has(event.location_id)) {
        counts.set(event.location_id, (counts.get(event.location_id) ?? 0) + 1);
      }
    }

    return counts;
  }, [liveEvents, timelineEvents]);

  const eventDataset = useMemo(
    () => (liveEvents.length > 0 ? liveEvents : timelineEvents),
    [liveEvents, timelineEvents]
  );

  const liveTick = status?.current_tick ?? eventDataset[eventDataset.length - 1]?.tick ?? 0;

  const liveWindowEvents = useMemo(() => {
    const fromTick = Math.max(1, liveTick - 24);
    const eventFilter = liveEventFilter.trim().toLowerCase();
    const actorFilter = liveActorFilter.trim().toLowerCase();

    return eventDataset
      .filter((event) => event.tick >= fromTick)
      .filter((event) => {
        if (eventFilter.length > 0 && !event.event_type.includes(eventFilter)) {
          return false;
        }
        if (actorFilter.length === 0) {
          return true;
        }
        return event.actors.some((actor) => actor.actor_id.toLowerCase().includes(actorFilter));
      })
      .slice(-400);
  }, [eventDataset, liveActorFilter, liveEventFilter, liveTick]);

  const livePulse = useMemo(() => {
    let npcActions = 0;
    let conflicts = 0;
    let thefts = 0;
    let arrests = 0;
    let conversations = 0;
    let romance = 0;
    let observations = 0;

    for (const event of liveWindowEvents) {
      if (event.event_type === "npc_action_committed") {
        npcActions += 1;
      }
      if (
        event.event_type === "insult_exchanged" ||
        event.event_type === "punch_thrown" ||
        event.event_type === "brawl_started"
      ) {
        conflicts += 1;
      }
      if (event.event_type === "theft_committed") {
        thefts += 1;
      }
      if (event.event_type === "arrest_made") {
        arrests += 1;
      }
      if (event.event_type === "conversation_held") {
        conversations += 1;
      }
      if (event.event_type === "romance_advanced") {
        romance += 1;
      }
      if (event.event_type === "observation_logged") {
        observations += 1;
      }
    }

    return {
      windowFromTick: Math.max(1, liveTick - 24),
      npcActions,
      conflicts,
      thefts,
      arrests,
      conversations,
      romance,
      observations
    };
  }, [liveTick, liveWindowEvents]);

  const npcActivityRows = useMemo(() => {
    const rows = new Map<
      string,
      {
        npcId: string;
        locationId: string;
        lastTick: number;
        lastEventType: string;
        lastAction: string;
        topIntents: string[];
        topPressures: string[];
        eventCount: number;
      }
    >();

    for (const event of [...eventDataset].reverse()) {
      const actor = event.actors.find(
        (entry) => entry.actor_kind === "npc" && entry.actor_id.startsWith("npc_")
      );
      if (!actor) {
        continue;
      }

      const details = event.details ?? {};
      const existing = rows.get(actor.actor_id);
      const chosenAction =
        typeof details["chosen_action"] === "string" ? String(details["chosen_action"]) : "";
      const topIntents = Array.isArray(details["top_intents"])
        ? details["top_intents"].filter((value): value is string => typeof value === "string")
        : [];
      const topPressures = Array.isArray(details["top_pressures"])
        ? details["top_pressures"].filter((value): value is string => typeof value === "string")
        : [];

      if (!existing) {
        rows.set(actor.actor_id, {
          npcId: actor.actor_id,
          locationId: event.location_id,
          lastTick: event.tick,
          lastEventType: event.event_type,
          lastAction: chosenAction,
          topIntents,
          topPressures,
          eventCount: 1
        });
      } else {
        existing.eventCount += 1;
        if (event.tick > existing.lastTick) {
          existing.lastTick = event.tick;
          existing.lastEventType = event.event_type;
          existing.locationId = event.location_id;
          if (chosenAction.length > 0) {
            existing.lastAction = chosenAction;
          }
          if (topIntents.length > 0) {
            existing.topIntents = topIntents;
          }
          if (topPressures.length > 0) {
            existing.topPressures = topPressures;
          }
        }
      }
    }

    return Array.from(rows.values())
      .sort((left, right) => right.lastTick - left.lastTick)
      .slice(0, 60);
  }, [eventDataset]);

  const npcInspectorOverview = useMemo(() => {
    if (!npcInspector) {
      return [];
    }

    const household = asRecord(npcInspector.household_status);
    const ledger = asRecord(npcInspector.npc_ledger);
    const budget = asRecord(npcInspector.time_budget);
    const contractCount = npcInspector.contract_status?.length ?? 0;

    return [
      {
        label: "Profession",
        value: compactValue(
          ledger?.["profession"] ?? ledger?.["job"] ?? household?.["profession"] ?? "unknown"
        )
      },
      {
        label: "Wallet",
        value: compactValue(ledger?.["wallet"] ?? ledger?.["coin"] ?? ledger?.["money"])
      },
      {
        label: "Debt",
        value: compactValue(ledger?.["debt"] ?? ledger?.["debt_total"] ?? household?.["debt"])
      },
      {
        label: "Food Buffer",
        value: compactValue(
          household?.["food_days"] ?? household?.["food_buffer_days"] ?? ledger?.["food_days"]
        )
      },
      {
        label: "Shelter",
        value: compactValue(household?.["shelter_status"] ?? household?.["housing"])
      },
      {
        label: "Dependents",
        value: compactValue(household?.["dependents"] ?? household?.["household_size"])
      },
      {
        label: "Work Hours",
        value: compactValue(
          budget?.["work_hours"] ?? budget?.["work_allocated_hours"] ?? budget?.["hours_work"]
        )
      },
      {
        label: "Contracts",
        value: contractCount > 0 ? `${contractCount} active/known` : "(none)"
      }
    ];
  }, [npcInspector]);

  const npcInspectorActionTimeline = useMemo(() => {
    if (!npcInspector) {
      return [];
    }

    return npcInspector.recent_actions.slice(0, 25).map((event) => {
      const details = event.details ?? {};
      const whyChain = asStringList(details["why_chain"]);
      const topIntents = asStringList(details["top_intents"]);

      return {
        event,
        action:
          typeof details["chosen_action"] === "string"
            ? (details["chosen_action"] as string)
            : eventPrimaryTopic(event),
        rationale: summarizeReasonFromEvent(event),
        whyChain,
        topIntents
      };
    });
  }, [npcInspector]);

  const settlementOverview = useMemo(() => {
    if (!settlementInspector) {
      return [];
    }

    const labor = asRecord(settlementInspector.labor_market);
    const stock = asRecord(settlementInspector.stock_ledger);
    const institution = asRecord(settlementInspector.institution_profile);
    const market = asRecord(settlementInspector.market_clearing);
    const queue = asRecord(settlementInspector.institution_queue);

    return [
      {
        label: "Unemployment",
        value: compactValue(
          labor?.["unemployment_rate"] ?? labor?.["unemployment"] ?? labor?.["jobless_share"]
        )
      },
      {
        label: "Vacancies",
        value: compactValue(labor?.["vacancies"] ?? labor?.["open_positions"])
      },
      {
        label: "Food Stock",
        value: compactValue(stock?.["food"] ?? stock?.["food_stock"] ?? stock?.["grain"])
      },
      {
        label: "Spoilage",
        value: compactValue(stock?.["spoilage"] ?? stock?.["spoilage_rate"])
      },
      {
        label: "Case Backlog",
        value: compactValue(
          queue?.["backlog"] ?? queue?.["pending_cases"] ?? institution?.["pending_cases"]
        )
      },
      {
        label: "Response Delay",
        value: compactValue(
          institution?.["response_latency"] ??
            institution?.["response_delay_ticks"] ??
            queue?.["avg_latency_ticks"]
        )
      },
      {
        label: "Corruption",
        value: compactValue(
          institution?.["corruption"] ?? institution?.["corruption_level"] ?? "(unknown)"
        )
      },
      {
        label: "Market Status",
        value: compactValue(
          market?.["status"] ?? market?.["clearing_state"] ?? market?.["price_pressure"]
        )
      }
    ];
  }, [settlementInspector]);

  const traceChain = useMemo(() => {
    const sorted = [...traceNodes].sort((left, right) => {
      if (left.depth !== right.depth) {
        return right.depth - left.depth;
      }
      if (left.event.tick !== right.event.tick) {
        return left.event.tick - right.event.tick;
      }
      return left.event.sequence_in_tick - right.event.sequence_in_tick;
    });

    if (sorted.length === 0 && eventDetail) {
      return [
        {
          depth: 0,
          event: eventDetail.event,
          reason_packet: eventDetail.reason_packet
        }
      ];
    }

    return sorted;
  }, [eventDetail, traceNodes]);

  const withAction = useCallback(async (action: () => Promise<void>) => {
    setBusy(true);
    setError(null);
    setInfo(null);

    try {
      await action();
    } catch (actionError) {
      setError(actionError instanceof Error ? actionError.message : String(actionError));
    } finally {
      setBusy(false);
    }
  }, []);

  const refreshStatus = useCallback(async () => {
    const response = await requestJson<RunControlResponse>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/status`
    );
    setStatus(response.status);
    return response.status;
  }, [activeRunId, apiBase]);

  const refreshTimeline = useCallback(async () => {
    const params = new URLSearchParams();
    params.set("from_tick", String(Math.max(1, timelineFromTick)));
    params.set("to_tick", String(Math.max(timelineFromTick, timelineToTick)));

    if (timelineEventType !== "all") {
      params.append("event_types", timelineEventType);
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
    setLiveEvents((current) => mergeEventsById(current, response.data.events));
  }, [
    activeRunId,
    apiBase,
    timelineActorId,
    timelineEventType,
    timelineFromTick,
    timelineLocationId,
    timelineToTick
  ]);

  const refreshCommands = useCallback(async () => {
    const response = await requestJson<CommandAuditData>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/commands?page_size=250`
    );
    setCommandAudit(response.entries);
  }, [activeRunId, apiBase]);

  const refreshSnapshots = useCallback(async () => {
    const params = new URLSearchParams();
    params.set("from_tick", "1");
    params.set("to_tick", String(Math.max(1, status?.current_tick ?? timelineToTick)));

    const response = await requestJson<QueryResponse<SnapshotsData>>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/snapshots?${params.toString()}`
    );

    setSnapshots(response.data.snapshots);
  }, [activeRunId, apiBase, status?.current_tick, timelineToTick]);

  const fetchEventContext = useCallback(
    async (eventId: string) => {
      const detailResponse = await requestJson<QueryResponse<EventDetailData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/events/${encodeURIComponent(eventId)}`
      );

      const traceResponse = await requestJson<QueryResponse<TraceData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/trace/${encodeURIComponent(eventId)}?depth=16`
      );

      setEventDetail(detailResponse.data);
      setTraceNodes(traceResponse.data.nodes);
    },
    [activeRunId, apiBase]
  );

  const fetchNpcInspectorById = useCallback(
    async (npcId: string) => {
      const response = await requestJson<QueryResponse<NpcInspectorData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/npc/${encodeURIComponent(
          npcId.trim()
        )}`
      );

      setNpcInspector(response.data);
    },
    [activeRunId, apiBase]
  );

  const fetchNpcInspector = useCallback(async () => {
    await fetchNpcInspectorById(npcInspectorId);
  }, [fetchNpcInspectorById, npcInspectorId]);

  const fetchSettlementInspector = useCallback(async () => {
    const response = await requestJson<QueryResponse<SettlementInspectorData>>(
      apiBase,
      `/api/v1/runs/${encodeURIComponent(activeRunId)}/settlement/${encodeURIComponent(
        settlementInspectorId.trim()
      )}`
    );

    setSettlementInspector(response.data);
  }, [activeRunId, apiBase, settlementInspectorId]);

  const submitScenarioCommand = useCallback(
    async (commandType: string, payload: Record<string, unknown>) => {
      const issuedAtTick = (status?.current_tick ?? 0) + 1;

      const command = {
        schema_version: SCHEMA_VERSION,
        command_id: commandId(commandType),
        run_id: activeRunId,
        issued_at_tick: issuedAtTick,
        command_type: commandType,
        payload
      };

      const result = await postJson<CommandResult>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/commands`,
        {
          command,
          effective_tick: issuedAtTick
        }
      );

      if (!result.accepted) {
        throw new Error(result.error?.message ?? "command rejected");
      }

      setInfo(`Command accepted: ${commandType}`);
      await Promise.all([refreshCommands(), refreshStatus()]);
    },
    [activeRunId, apiBase, refreshCommands, refreshStatus, status?.current_tick]
  );

  const buildComparisonScenarioCommand = useCallback(
    (runIdValue: string) => {
      const issuedAtTick = 1;

      switch (comparisonScenario) {
        case "inject_rumor":
          return {
            schema_version: SCHEMA_VERSION,
            command_id: commandId("inject_rumor_cmp"),
            run_id: runIdValue,
            issued_at_tick: issuedAtTick,
            command_type: "inject_rumor",
            payload: {
              type: "inject_rumor",
              location_id: rumorLocation,
              rumor_text: rumorText
            }
          };
        case "inject_spawn_caravan":
          return {
            schema_version: SCHEMA_VERSION,
            command_id: commandId("inject_spawn_caravan_cmp"),
            run_id: runIdValue,
            issued_at_tick: issuedAtTick,
            command_type: "inject_spawn_caravan",
            payload: {
              type: "inject_spawn_caravan",
              origin_settlement_id: caravanOrigin,
              destination_settlement_id: caravanDestination
            }
          };
        case "inject_remove_npc":
          return {
            schema_version: SCHEMA_VERSION,
            command_id: commandId("inject_remove_npc_cmp"),
            run_id: runIdValue,
            issued_at_tick: issuedAtTick,
            command_type: "inject_remove_npc",
            payload: {
              type: "inject_remove_npc",
              npc_id: removeNpcId
            }
          };
        case "inject_force_bad_harvest":
          return {
            schema_version: SCHEMA_VERSION,
            command_id: commandId("inject_force_bad_harvest_cmp"),
            run_id: runIdValue,
            issued_at_tick: issuedAtTick,
            command_type: "inject_force_bad_harvest",
            payload: {
              type: "inject_force_bad_harvest",
              settlement_id: badHarvestSettlement
            }
          };
        case "inject_set_winter_severity":
          return {
            schema_version: SCHEMA_VERSION,
            command_id: commandId("inject_set_winter_severity_cmp"),
            run_id: runIdValue,
            issued_at_tick: issuedAtTick,
            command_type: "inject_set_winter_severity",
            payload: {
              type: "inject_set_winter_severity",
              severity: Math.max(0, Math.min(100, winterSeverity))
            }
          };
        case "none":
        default:
          return null;
      }
    },
    [
      badHarvestSettlement,
      caravanDestination,
      caravanOrigin,
      comparisonScenario,
      removeNpcId,
      rumorLocation,
      rumorText,
      winterSeverity
    ]
  );

  const createRun = useCallback(async () => {
    const config: RunConfig = {
      schema_version: SCHEMA_VERSION,
      run_id: runId.trim(),
      seed,
      duration_days: durationDays,
      region_id: regionId,
      snapshot_every_ticks: snapshotEveryTicks,
      npc_count_min: 60,
      npc_count_max: 90
    };

    const response = await postJson<CreateRunResponse>(apiBase, "/api/v1/runs", {
      config,
      auto_start: false,
      replace_existing: true
    });

    setStatus(response.status);
    setTimelineEvents([]);
    setLiveEvents([]);
    setEventDetail(null);
    setTraceNodes([]);
    setNpcInspector(null);
    setSettlementInspector(null);
    setCommandAudit([]);
    setSnapshots([]);
    setStreamMessages([]);
    setRunId(response.run_id);
    setTimelineToTick(Math.max(24, response.status.current_tick + 24));

    setInfo(`Run created: ${response.run_id}`);
  }, [apiBase, durationDays, regionId, runId, seed, snapshotEveryTicks]);

  const onSubmitCreateRun = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      await withAction(createRun);
    },
    [createRun, withAction]
  );

  const onStart = useCallback(async () => {
    await withAction(async () => {
      const response = await postJson<RunControlResponse>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/start`,
        {}
      );
      setStatus(response.status);
      setInfo("Run started.");
    });
  }, [activeRunId, apiBase, withAction]);

  const onPause = useCallback(async () => {
    await withAction(async () => {
      const response = await postJson<RunControlResponse>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/pause`,
        {}
      );
      setStatus(response.status);
      setInfo("Run paused.");
    });
  }, [activeRunId, apiBase, withAction]);

  const onStep = useCallback(async () => {
    await withAction(async () => {
      const response = await postJson<RunControlResponse>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/step`,
        { steps: Math.max(1, stepCount) }
      );
      setStatus(response.status);
      await Promise.all([refreshTimeline(), refreshSnapshots(), refreshCommands()]);
      setInfo(`Committed ${response.committed ?? 0} tick(s).`);
    });
  }, [activeRunId, apiBase, refreshSnapshots, refreshTimeline, stepCount, withAction]);

  const onRunToTick = useCallback(async () => {
    await withAction(async () => {
      const response = await postJson<RunControlResponse>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(activeRunId)}/run_to_tick`,
        { target_tick: Math.max(1, targetTick) }
      );
      setStatus(response.status);
      setTimelineToTick(Math.max(timelineToTick, response.status.current_tick));
      await Promise.all([refreshTimeline(), refreshSnapshots()]);
      await refreshCommands();
      setInfo(`Advanced by ${response.committed ?? 0} tick(s).`);
    });
  }, [
    activeRunId,
    apiBase,
    refreshSnapshots,
    refreshTimeline,
    targetTick,
    timelineToTick,
    withAction
  ]);

  const onRefreshAll = useCallback(async () => {
    await withAction(async () => {
      await Promise.all([refreshStatus(), refreshTimeline(), refreshCommands(), refreshSnapshots()]);
      setInfo("Refreshed status, timeline, commands, and snapshots.");
    });
  }, [refreshCommands, refreshSnapshots, refreshStatus, refreshTimeline, withAction]);

  const onSelectEvent = useCallback(
    async (eventId: string) => {
      await withAction(async () => {
        await fetchEventContext(eventId);
      });
    },
    [fetchEventContext, withAction]
  );

  const onFetchNpcInspector = useCallback(async () => {
    await withAction(fetchNpcInspector);
  }, [fetchNpcInspector, withAction]);

  const onFetchSettlementInspector = useCallback(async () => {
    await withAction(fetchSettlementInspector);
  }, [fetchSettlementInspector, withAction]);

  const onRefreshTimeline = useCallback(async () => {
    await withAction(refreshTimeline);
  }, [refreshTimeline, withAction]);

  const onInjectRumor = useCallback(async () => {
    await withAction(async () => {
      await submitScenarioCommand("inject_rumor", {
        type: "inject_rumor",
        location_id: rumorLocation,
        rumor_text: rumorText
      });
    });
  }, [rumorLocation, rumorText, submitScenarioCommand, withAction]);

  const onSpawnCaravan = useCallback(async () => {
    await withAction(async () => {
      await submitScenarioCommand("inject_spawn_caravan", {
        type: "inject_spawn_caravan",
        origin_settlement_id: caravanOrigin,
        destination_settlement_id: caravanDestination
      });
    });
  }, [caravanDestination, caravanOrigin, submitScenarioCommand, withAction]);

  const onRemoveNpc = useCallback(async () => {
    await withAction(async () => {
      await submitScenarioCommand("inject_remove_npc", {
        type: "inject_remove_npc",
        npc_id: removeNpcId
      });
    });
  }, [removeNpcId, submitScenarioCommand, withAction]);

  const onForceBadHarvest = useCallback(async () => {
    await withAction(async () => {
      await submitScenarioCommand("inject_force_bad_harvest", {
        type: "inject_force_bad_harvest",
        settlement_id: badHarvestSettlement
      });
    });
  }, [badHarvestSettlement, submitScenarioCommand, withAction]);

  const onSetWinterSeverity = useCallback(async () => {
    await withAction(async () => {
      await submitScenarioCommand("inject_set_winter_severity", {
        type: "inject_set_winter_severity",
        severity: Math.max(0, Math.min(100, winterSeverity))
      });
    });
  }, [submitScenarioCommand, winterSeverity, withAction]);

  const runSeedComparison = useCallback(async () => {
    const seeds = parseSeedsCsv(comparisonSeeds);
    if (seeds.length === 0) {
      throw new Error("Comparison seeds are invalid. Use comma-separated non-negative integers.");
    }

    setComparisonRows([]);
    const summaryRows: SeedComparisonRow[] = [];
    const tickTarget = Math.max(1, comparisonTargetTick);

    for (const [index, seedValue] of seeds.entries()) {
      const comparisonRunId = `cmp_${seedValue}_${Date.now()}_${index}`;
      const config: RunConfig = {
        schema_version: SCHEMA_VERSION,
        run_id: comparisonRunId,
        seed: String(seedValue),
        duration_days: durationDays,
        region_id: regionId,
        snapshot_every_ticks: snapshotEveryTicks,
        npc_count_min: 60,
        npc_count_max: 90
      };

      await postJson<CreateRunResponse>(apiBase, "/api/v1/runs", {
        config,
        auto_start: false,
        replace_existing: true
      });

      const comparisonCommand = buildComparisonScenarioCommand(comparisonRunId);
      if (comparisonCommand) {
        const commandResult = await postJson<CommandResult>(
          apiBase,
          `/api/v1/runs/${encodeURIComponent(comparisonRunId)}/commands`,
          {
            command: comparisonCommand,
            effective_tick: 1
          }
        );

        if (!commandResult.accepted) {
          throw new Error(
            `Comparison command rejected for seed ${seedValue}: ${
              commandResult.error?.message ?? "unknown error"
            }`
          );
        }
      }

      const runControl = await postJson<RunControlResponse>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(comparisonRunId)}/run_to_tick`,
        { target_tick: tickTarget }
      );

      const timelineResponse = await requestJson<QueryResponse<TimelineData>>(
        apiBase,
        `/api/v1/runs/${encodeURIComponent(
          comparisonRunId
        )}/timeline?from_tick=1&to_tick=${tickTarget}&page_size=5000`
      );

      const events = timelineResponse.data.events;
      const pressureEvents = events.filter(
        (event) => event.event_type === "pressure_economy_updated"
      );
      const terminalPressureIndex = pressureEvents.length
        ? extractPressureIndex(pressureEvents[pressureEvents.length - 1])
        : 0;

      summaryRows.push({
        seed: seedValue,
        run_id: comparisonRunId,
        final_tick: runControl.status.current_tick,
        event_count: events.length,
        npc_action_count: events.filter((event) => event.event_type === "npc_action_committed")
          .length,
        rumor_events: events.filter((event) => event.event_type === "rumor_injected").length,
        caravan_events: events.filter((event) => event.event_type === "caravan_spawned").length,
        bad_harvest_events: events.filter(
          (event) => event.event_type === "bad_harvest_forced"
        ).length,
        winter_events: events.filter((event) => event.event_type === "winter_severity_set").length,
        pressure_events: pressureEvents.length,
        terminal_pressure_index: terminalPressureIndex
      });
    }

    setComparisonRows(summaryRows);
    setInfo(
      `Seed comparison completed for ${summaryRows.length} run(s). Note: comparison creates replacement runs on the API server.`
    );
  }, [
    apiBase,
    buildComparisonScenarioCommand,
    comparisonSeeds,
    comparisonTargetTick,
    durationDays,
    regionId,
    snapshotEveryTicks
  ]);

  const onRunSeedComparison = useCallback(async () => {
    await withAction(runSeedComparison);
  }, [runSeedComparison, withAction]);

  useEffect(() => {
    if (!streamEnabled || !streamRunId) {
      setStreamState("idle");
      return;
    }

    const wsBase = toWsBase(apiBase);
    const socket = new WebSocket(
      `${wsBase}/api/v1/runs/${encodeURIComponent(streamRunId)}/stream`
    );

    setStreamState("connecting");

    socket.onopen = () => {
      setStreamState("open");
    };

    socket.onmessage = (messageEvent) => {
      try {
        const message = JSON.parse(messageEvent.data) as StreamMessage;

        setStreamMessages((current) => [message, ...current].slice(0, 250));

        if (message.type === "run.status") {
          setStatus(message.payload as RunStatus);
        } else if (message.type === "event.appended") {
          const event = message.payload as EventRecord;
          setLiveEvents((current) => mergeEventsById(current, [event]));
          if (
            autoFollowLive &&
            timelineEventType === "all" &&
            timelineActorId.trim().length === 0 &&
            timelineLocationId.trim().length === 0
          ) {
            setTimelineEvents((current) => mergeEventsById(current, [event]).slice(-1200));
          }
        } else if (message.type === "snapshot.created") {
          const snapshot = message.payload as Snapshot;
          setSnapshots((current) =>
            [...current.filter((entry) => entry.snapshot_id !== snapshot.snapshot_id), snapshot]
              .sort((left, right) => left.tick - right.tick)
              .slice(-400)
          );
        } else if (message.type === "command.result") {
          const commandEntry = message.payload as CommandRecord;
          setCommandAudit((current) => {
            const without = current.filter(
              (entry) => entry.command.command_id !== commandEntry.command.command_id
            );
            return [commandEntry, ...without].slice(0, 250);
          });
        }
      } catch {
        // ignore malformed stream frames
      }
    };

    socket.onerror = () => {
      setStreamState("closed");
    };

    socket.onclose = () => {
      setStreamState("closed");
    };

    return () => {
      socket.close();
    };
  }, [
    apiBase,
    autoFollowLive,
    streamEnabled,
    streamRunId,
    timelineActorId,
    timelineEventType,
    timelineLocationId
  ]);

  useEffect(() => {
    if (!streamEnabled || !streamRunId || streamState === "open") {
      return;
    }

    const intervalId = window.setInterval(() => {
      if (pollingInFlightRef.current) {
        return;
      }

      pollingInFlightRef.current = true;
      const normalizedApiBase = apiBase.replace(/\/$/, "");
      const runPath = encodeURIComponent(streamRunId);

      requestJson<RunControlResponse>(apiBase, `/api/v1/runs/${runPath}/status`)
        .then((response) => {
          setStatus(response.status);
          const toTick = response.status.current_tick;
          const fromTick = Math.max(1, toTick - 96);
          return requestJson<QueryResponse<TimelineData>>(
            apiBase,
            `/api/v1/runs/${runPath}/timeline?from_tick=${fromTick}&to_tick=${toTick}&page_size=3000`
          );
        })
        .then((timelineResponse) => {
          setLiveEvents((current) => mergeEventsById(current, timelineResponse.data.events));
          if (autoFollowLive) {
            setTimelineEvents(timelineResponse.data.events);
            setTimelineFromTick(timelineResponse.data.from_tick);
            setTimelineToTick(timelineResponse.data.to_tick);
          }
          setStreamMessages((current) =>
            [
              {
                schema_version: SCHEMA_VERSION,
                type: "warning",
                run_id: streamRunId,
                tick: timelineResponse.generated_at_tick,
                sequence_in_tick: null,
                reconnect_token: `poll:${timelineResponse.generated_at_tick}`,
                payload: {
                  message: `stream fallback polling from ${normalizedApiBase}`
                }
              } as StreamMessage,
              ...current
            ].slice(0, 250)
          );
        })
        .catch(() => {
          // silent retry loop while stream is disconnected
        })
        .finally(() => {
          pollingInFlightRef.current = false;
        });
    }, 2500);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [apiBase, autoFollowLive, streamEnabled, streamRunId, streamState]);

  useEffect(() => {
    if (!autoRefreshInspector || !streamRunId || !npcInspectorId.trim()) {
      return;
    }
    if (status?.mode !== "running") {
      return;
    }

    const intervalId = window.setInterval(() => {
      void fetchNpcInspector();
      void fetchSettlementInspector();
    }, 3500);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [
    autoRefreshInspector,
    fetchNpcInspector,
    fetchSettlementInspector,
    npcInspectorId,
    status?.mode,
    streamRunId
  ]);

  return (
    <main className="app-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Milestone 6 Baseline</p>
          <h1>Threads Observatory</h1>
          <p className="subtitle">
            Run control, scenario ripples, seed comparison, timeline, map, inspector, trace
            explorer, and live stream are wired against the simulation API.
          </p>
        </div>

        <div className="hero-meta">
          <label>
            API Base
            <input
              type="text"
              value={apiBase}
              onChange={(event) => setApiBase(event.target.value)}
              placeholder="http://127.0.0.1:8080"
            />
          </label>

          <label className="stream-toggle">
            <input
              type="checkbox"
              checked={streamEnabled}
              onChange={(event) => setStreamEnabled(event.target.checked)}
            />
            Live stream
          </label>

          <p className={`pill ${streamState}`}>Stream: {streamState}</p>
        </div>
      </header>

      {(error || info) && (
        <section className="status-strip" aria-live="polite">
          {error ? <p className="error">{error}</p> : null}
          {info ? <p className="info">{info}</p> : null}
        </section>
      )}

      <section className="panel live-panel">
        <div className="live-header">
          <div>
            <h2>Live World Monitor</h2>
            <p className="panel-meta">
              Real-time feed of world activity, NPC behavior, and causal context.
            </p>
          </div>
          <div className="live-flags">
            <label className="stream-toggle">
              <input
                type="checkbox"
                checked={autoFollowLive}
                onChange={(event) => setAutoFollowLive(event.target.checked)}
              />
              Auto-follow feed
            </label>
            <label className="stream-toggle">
              <input
                type="checkbox"
                checked={autoRefreshInspector}
                onChange={(event) => setAutoRefreshInspector(event.target.checked)}
              />
              Auto-refresh inspector
            </label>
          </div>
        </div>

        <div className="pulse-grid">
          <article>
            <h3>Tick window</h3>
            <p>
              {livePulse.windowFromTick} - {liveTick}
            </p>
          </article>
          <article>
            <h3>NPC actions</h3>
            <p>{livePulse.npcActions}</p>
          </article>
          <article>
            <h3>Conflicts</h3>
            <p>{livePulse.conflicts}</p>
          </article>
          <article>
            <h3>Thefts / Arrests</h3>
            <p>
              {livePulse.thefts} / {livePulse.arrests}
            </p>
          </article>
          <article>
            <h3>Conversations</h3>
            <p>{livePulse.conversations}</p>
          </article>
          <article>
            <h3>Romance / Observations</h3>
            <p>
              {livePulse.romance} / {livePulse.observations}
            </p>
          </article>
        </div>

        <form
          className="live-filters"
          onSubmit={(event) => {
            event.preventDefault();
          }}
        >
          <label>
            Event type contains
            <input
              placeholder="theft, brawl, romance..."
              value={liveEventFilter}
              onChange={(event) => setLiveEventFilter(event.target.value)}
            />
          </label>
          <label>
            Actor contains
            <input
              placeholder="npc_001"
              value={liveActorFilter}
              onChange={(event) => setLiveActorFilter(event.target.value)}
            />
          </label>
          <button
            type="button"
            disabled={busy}
            onClick={() => {
              void onRefreshAll();
            }}
          >
            Sync now
          </button>
        </form>

        <div className="live-grid">
          <article>
            <h3>Event feed</h3>
            <p className="panel-meta">{liveWindowEvents.length} event(s) in current live window</p>
            <div className="live-feed-list">
              {[...liveWindowEvents]
                .reverse()
                .map((event) => (
                  <button
                    key={event.event_id}
                    type="button"
                    className={`timeline-item ${
                      eventDetail?.event.event_id === event.event_id ? "selected" : ""
                    }`}
                    onClick={() => {
                      void onSelectEvent(event.event_id);
                    }}
                  >
                    <span>
                      T{event.tick}:{event.sequence_in_tick}
                    </span>
                    <strong>{formatEventType(event.event_type)}</strong>
                    <em>
                      {event.location_id} · {eventActorLabel(event)} · {eventPrimaryTopic(event)}
                    </em>
                    <small>{summarizeReasonFromEvent(event)}</small>
                    <span className="meta-inline">
                      caused-by: {event.caused_by.length} · tags: {event.tags?.length ?? 0}
                    </span>
                  </button>
                ))}
              {liveWindowEvents.length === 0 ? (
                <p className="empty-state">No live events in the current window.</p>
              ) : null}
            </div>
          </article>

          <article>
            <h3>NPC activity board</h3>
            <p className="panel-meta">
              Click a row to inspect the NPC and trace why actions happened.
            </p>
            <div className="npc-activity-list">
              {npcActivityRows.map((row) => (
                <button
                  key={row.npcId}
                  type="button"
                  className={`npc-activity-row ${npcInspectorId === row.npcId ? "selected" : ""}`}
                  onClick={() => {
                    setNpcInspectorId(row.npcId);
                    void withAction(() => fetchNpcInspectorById(row.npcId));
                  }}
                >
                  <strong>{row.npcId}</strong>
                  <span>T{row.lastTick}</span>
                  <span>{formatEventType(row.lastEventType)}</span>
                  <span>{row.lastAction || "(no action)"}</span>
                  <span>{row.locationId}</span>
                  <span>{row.topIntents.slice(0, 2).join(", ") || "(no intents)"}</span>
                  <span>{row.topPressures.slice(0, 2).join(", ") || "(no pressures)"}</span>
                  <span>{row.eventCount} events</span>
                </button>
              ))}
              {npcActivityRows.length === 0 ? (
                <p className="empty-state">No NPC activity yet.</p>
              ) : null}
            </div>
          </article>
        </div>
      </section>

      <section className="panel run-panel">
        <h2>Run Control</h2>

        <form className="run-config" onSubmit={onSubmitCreateRun}>
          <label>
            Run ID
            <input
              required
              value={runId}
              onChange={(event) => setRunId(event.target.value)}
            />
          </label>

          <label>
            Seed
            <input
              required
              value={seed}
              onChange={(event) => setSeed(event.target.value)}
            />
          </label>

          <label>
            Duration (days)
            <input
              type="number"
              min={1}
              value={durationDays}
              onChange={(event) => setDurationDays(Number(event.target.value) || 1)}
            />
          </label>

          <label>
            Snapshot cadence (ticks)
            <input
              type="number"
              min={1}
              value={snapshotEveryTicks}
              onChange={(event) =>
                setSnapshotEveryTicks(Number(event.target.value) || 1)
              }
            />
          </label>

          <label>
            Region
            <select
              value={regionId}
              onChange={(event) => setRegionId(event.target.value as RegionId)}
            >
              {REGION_OPTIONS.map((region) => (
                <option key={region} value={region}>
                  {region}
                </option>
              ))}
            </select>
          </label>

          <button type="submit" disabled={busy}>
            Create Run
          </button>
        </form>

        <div className="control-row">
          <button type="button" onClick={onStart} disabled={busy}>
            Start
          </button>
          <button type="button" onClick={onPause} disabled={busy}>
            Pause
          </button>
          <button type="button" onClick={onRefreshAll} disabled={busy}>
            Refresh All
          </button>
        </div>

        <div className="control-row">
          <label>
            Step count
            <input
              type="number"
              min={1}
              value={stepCount}
              onChange={(event) => setStepCount(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <button type="button" onClick={onStep} disabled={busy}>
            Step
          </button>

          <label>
            Target tick
            <input
              type="number"
              min={1}
              value={targetTick}
              onChange={(event) =>
                setTargetTick(Math.max(1, Number(event.target.value) || 1))
              }
            />
          </label>
          <button type="button" onClick={onRunToTick} disabled={busy}>
            Run to Tick
          </button>
        </div>

        <dl className="status-grid">
          <div>
            <dt>Active run</dt>
            <dd>{status?.run_id ?? "(none)"}</dd>
          </div>
          <div>
            <dt>Tick</dt>
            <dd>
              {status ? `${status.current_tick}/${status.max_ticks}` : "-"}
            </dd>
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
            <dt>Snapshots loaded</dt>
            <dd>{snapshots.length}</dd>
          </div>
        </dl>
      </section>

      <section className="panel scenario-panel">
        <h2>Scenario Controls</h2>

        <div className="scenario-grid">
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void onInjectRumor();
            }}
          >
            <h3>Inject rumor</h3>
            <label>
              Location
              <input
                value={rumorLocation}
                onChange={(event) => setRumorLocation(event.target.value)}
              />
            </label>
            <label>
              Text
              <input
                value={rumorText}
                onChange={(event) => setRumorText(event.target.value)}
              />
            </label>
            <button type="submit" disabled={busy}>
              Submit
            </button>
          </form>

          <form
            onSubmit={(event) => {
              event.preventDefault();
              void onSpawnCaravan();
            }}
          >
            <h3>Spawn caravan</h3>
            <label>
              Origin
              <input
                value={caravanOrigin}
                onChange={(event) => setCaravanOrigin(event.target.value)}
              />
            </label>
            <label>
              Destination
              <input
                value={caravanDestination}
                onChange={(event) => setCaravanDestination(event.target.value)}
              />
            </label>
            <button type="submit" disabled={busy}>
              Submit
            </button>
          </form>

          <form
            onSubmit={(event) => {
              event.preventDefault();
              void onRemoveNpc();
            }}
          >
            <h3>Remove NPC</h3>
            <label>
              NPC ID
              <input
                value={removeNpcId}
                onChange={(event) => setRemoveNpcId(event.target.value)}
              />
            </label>
            <button type="submit" disabled={busy}>
              Submit
            </button>
          </form>

          <form
            onSubmit={(event) => {
              event.preventDefault();
              void onForceBadHarvest();
            }}
          >
            <h3>Force bad harvest</h3>
            <label>
              Settlement ID
              <input
                value={badHarvestSettlement}
                onChange={(event) => setBadHarvestSettlement(event.target.value)}
              />
            </label>
            <button type="submit" disabled={busy}>
              Submit
            </button>
          </form>

          <form
            onSubmit={(event) => {
              event.preventDefault();
              void onSetWinterSeverity();
            }}
          >
            <h3>Set winter severity</h3>
            <label>
              Severity (0-100)
              <input
                type="number"
                min={0}
                max={100}
                value={winterSeverity}
                onChange={(event) =>
                  setWinterSeverity(Math.max(0, Math.min(100, Number(event.target.value) || 0)))
                }
              />
            </label>
            <button type="submit" disabled={busy}>
              Submit
            </button>
          </form>
        </div>
      </section>

      <section className="panel comparison-panel">
        <h2>Seed Comparison Lab</h2>
        <p className="panel-meta">
          Runs the same scenario against multiple seeds and summarizes outcome deltas.
        </p>

        <form
          className="comparison-form"
          onSubmit={(event) => {
            event.preventDefault();
            void onRunSeedComparison();
          }}
        >
          <label>
            Seeds CSV
            <input
              value={comparisonSeeds}
              onChange={(event) => setComparisonSeeds(event.target.value)}
              placeholder="1337, 2026, 9001"
            />
          </label>

          <label>
            Target tick
            <input
              type="number"
              min={1}
              value={comparisonTargetTick}
              onChange={(event) =>
                setComparisonTargetTick(Math.max(1, Number(event.target.value) || 1))
              }
            />
          </label>

          <label>
            Scenario
            <select
              value={comparisonScenario}
              onChange={(event) =>
                setComparisonScenario(event.target.value as ComparisonScenario)
              }
            >
              <option value="none">none</option>
              <option value="inject_rumor">inject_rumor</option>
              <option value="inject_spawn_caravan">inject_spawn_caravan</option>
              <option value="inject_remove_npc">inject_remove_npc</option>
              <option value="inject_force_bad_harvest">inject_force_bad_harvest</option>
              <option value="inject_set_winter_severity">inject_set_winter_severity</option>
            </select>
          </label>

          <button type="submit" disabled={busy}>
            Run Comparison
          </button>
        </form>

        <div className="comparison-table-wrap">
          <table className="comparison-table">
            <thead>
              <tr>
                <th>Seed</th>
                <th>Final Tick</th>
                <th>Events</th>
                <th>NPC Actions</th>
                <th>Rumor</th>
                <th>Caravan</th>
                <th>Harvest</th>
                <th>Winter</th>
                <th>Pressure Events</th>
                <th>Terminal Pressure</th>
              </tr>
            </thead>
            <tbody>
              {comparisonRows.map((row) => (
                <tr key={row.run_id}>
                  <td>{row.seed}</td>
                  <td>{row.final_tick}</td>
                  <td>{row.event_count}</td>
                  <td>{row.npc_action_count}</td>
                  <td>{row.rumor_events}</td>
                  <td>{row.caravan_events}</td>
                  <td>{row.bad_harvest_events}</td>
                  <td>{row.winter_events}</td>
                  <td>{row.pressure_events}</td>
                  <td>{row.terminal_pressure_index}</td>
                </tr>
              ))}
              {comparisonRows.length === 0 ? (
                <tr>
                  <td colSpan={10} className="empty-state">
                    No comparison rows yet.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <div className="content-grid">
        <section className="panel timeline-panel">
          <h2>Timeline</h2>

          <form
            className="timeline-filters"
            onSubmit={(event) => {
              event.preventDefault();
              void onRefreshTimeline();
            }}
          >
            <label>
              From
              <input
                type="number"
                min={1}
                value={timelineFromTick}
                onChange={(event) =>
                  setTimelineFromTick(Math.max(1, Number(event.target.value) || 1))
                }
              />
            </label>

            <label>
              To
              <input
                type="number"
                min={1}
                value={timelineToTick}
                onChange={(event) =>
                  setTimelineToTick(Math.max(1, Number(event.target.value) || 1))
                }
              />
            </label>

            <label>
              Event type
              <select
                value={timelineEventType}
                onChange={(event) =>
                  setTimelineEventType(event.target.value as TimelineEventTypeFilter)
                }
              >
                {TIMELINE_EVENT_TYPES.map((eventType) => (
                  <option key={eventType} value={eventType}>
                    {eventType}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Actor
              <input
                placeholder="npc_001"
                value={timelineActorId}
                onChange={(event) => setTimelineActorId(event.target.value)}
              />
            </label>

            <label>
              Location
              <input
                placeholder="settlement:greywall"
                value={timelineLocationId}
                onChange={(event) => setTimelineLocationId(event.target.value)}
              />
            </label>

            <button type="submit" disabled={busy}>
              Query
            </button>
          </form>

          <p className="panel-meta">{timelineEvents.length} event(s) loaded</p>

          <div className="timeline-list">
            {timelineEvents.map((event) => (
              <button
                key={event.event_id}
                type="button"
                className={`timeline-item ${
                  eventDetail?.event.event_id === event.event_id ? "selected" : ""
                }`}
                onClick={() => {
                  void onSelectEvent(event.event_id);
                }}
              >
                <span>
                  T{event.tick}:{event.sequence_in_tick}
                </span>
                <strong>{formatEventType(event.event_type)}</strong>
                <em>{event.location_id}</em>
              </button>
            ))}

            {timelineEvents.length === 0 && (
              <p className="empty-state">No events loaded yet. Run and query a window.</p>
            )}
          </div>
        </section>

        <section className="panel map-panel">
          <h2>Map Baseline</h2>

          <svg viewBox="0 0 460 320" role="img" aria-label="Crownvale slice map">
            {MAP_EDGES.map(([fromId, toId]) => {
              const fromNode = MAP_NODES.find((node) => node.id === fromId);
              const toNode = MAP_NODES.find((node) => node.id === toId);

              if (!fromNode || !toNode) {
                return null;
              }

              return (
                <line
                  key={`${fromId}_${toId}`}
                  x1={fromNode.x}
                  y1={fromNode.y}
                  x2={toNode.x}
                  y2={toNode.y}
                  className="map-edge"
                />
              );
            })}

            {MAP_NODES.map((node) => {
              const eventCount = settlementCounts.get(node.id) ?? 0;
              const isFocused = focusedLocation === node.id;

              return (
                <g
                  key={node.id}
                  transform={`translate(${node.x} ${node.y})`}
                  className={isFocused ? "focused" : ""}
                >
                  <circle r={22} className="map-node" />
                  <text y={4} textAnchor="middle" className="map-node-count">
                    {eventCount}
                  </text>
                  <text y={42} textAnchor="middle" className="map-node-label">
                    {node.label}
                  </text>
                </g>
              );
            })}
          </svg>

          <p className="panel-meta">
            Node labels show timeline event counts. Selected event/settlement is highlighted.
          </p>
        </section>

        <section className="panel inspector-panel">
          <h2>Inspector</h2>

          <div className="inspector-forms">
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void onFetchNpcInspector();
              }}
            >
              <label>
                NPC ID
                <input
                  value={npcInspectorId}
                  onChange={(event) => setNpcInspectorId(event.target.value)}
                />
              </label>
              <button type="submit" disabled={busy}>
                Inspect NPC
              </button>
            </form>

            <form
              onSubmit={(event) => {
                event.preventDefault();
                void onFetchSettlementInspector();
              }}
            >
              <label>
                Settlement ID
                <input
                  value={settlementInspectorId}
                  onChange={(event) => setSettlementInspectorId(event.target.value)}
                />
              </label>
              <button type="submit" disabled={busy}>
                Inspect Settlement
              </button>
            </form>
          </div>

          <div className="inspector-grid">
            <article>
              <h3>NPC</h3>
              {npcInspector ? (
                <>
                  <p>
                    <strong>{npcInspector.npc_id}</strong> at {npcInspector.current_location}
                  </p>
                  <div className="inspector-kv-grid">
                    {npcInspectorOverview.map((entry) => (
                      <div key={entry.label}>
                        <dt>{entry.label}</dt>
                        <dd>{entry.value}</dd>
                      </div>
                    ))}
                  </div>
                  <div className="inspector-block">
                    <h4>Intent and motive context</h4>
                    <p>Top intents: {npcInspector.top_intents.join(", ") || "(none)"}</p>
                    <p>Motive chain: {npcInspector.motive_chain?.join(" -> ") || "(none)"}</p>
                    <p>
                      Belief updates: {npcInspector.recent_belief_updates.join(", ") || "(none)"}
                    </p>
                    <p>
                      Relationships: {npcInspector.relationship_edges?.length ?? 0} · Beliefs:{" "}
                      {npcInspector.active_beliefs?.length ?? 0} · Opportunities:{" "}
                      {npcInspector.opportunities?.length ?? 0} · Commitments:{" "}
                      {npcInspector.commitments?.length ?? 0}
                    </p>
                  </div>
                  {npcInspector.reason_packet ? (
                    <div className="inspector-block">
                      <h4>Latest reason packet</h4>
                      <p>{npcInspector.reason_packet.selection_rationale}</p>
                      <p>
                        Why chain: {npcInspector.reason_packet.why_chain?.join(" -> ") || "(none)"}
                      </p>
                      <p>
                        Constraints:{" "}
                        {npcInspector.reason_packet.context_constraints?.join(", ") || "(none)"}
                      </p>
                      <p>
                        Alternatives:{" "}
                        {npcInspector.reason_packet.alternatives_considered?.join(", ") || "(none)"}
                      </p>
                    </div>
                  ) : (
                    <p className="empty-state">No reason packet on latest inspected state.</p>
                  )}
                  <div className="inspector-block">
                    <h4>Recent action chain</h4>
                    <div className="inspector-action-list">
                      {npcInspectorActionTimeline.map((entry) => (
                        <button
                          key={entry.event.event_id}
                          type="button"
                          className={`timeline-item ${
                            eventDetail?.event.event_id === entry.event.event_id ? "selected" : ""
                          }`}
                          onClick={() => {
                            void onSelectEvent(entry.event.event_id);
                          }}
                        >
                          <span>
                            T{entry.event.tick}:{entry.event.sequence_in_tick}
                          </span>
                          <strong>{formatEventType(entry.event.event_type)}</strong>
                          <em>{entry.action}</em>
                          <small>{entry.rationale}</small>
                          <span className="meta-inline">
                            intents: {entry.topIntents.slice(0, 3).join(", ") || "(none)"} ·
                            caused-by: {entry.event.caused_by.length}
                          </span>
                        </button>
                      ))}
                      {npcInspectorActionTimeline.length === 0 ? (
                        <span className="empty-state">(none)</span>
                      ) : null}
                    </div>
                  </div>
                  <div className="inspector-json-grid">
                    <div>
                      <h4>Ledger</h4>
                      <pre className="json-block">
                        {JSON.stringify(npcInspector.npc_ledger ?? {}, null, 2)}
                      </pre>
                    </div>
                    <div>
                      <h4>Time budget</h4>
                      <pre className="json-block">
                        {JSON.stringify(npcInspector.time_budget ?? {}, null, 2)}
                      </pre>
                    </div>
                  </div>
                  <p>Why summaries: {npcInspector.why_summaries?.length ?? 0}</p>
                </>
              ) : (
                <p className="empty-state">No NPC loaded.</p>
              )}
            </article>

            <article>
              <h3>Settlement</h3>
              {settlementInspector ? (
                <>
                  <p>
                    <strong>{settlementInspector.settlement_id}</strong>
                  </p>
                  <p>Food: {settlementInspector.food_status}</p>
                  <p>Security: {settlementInspector.security_status}</p>
                  <p>Institution: {settlementInspector.institutional_health}</p>
                  <div className="inspector-kv-grid">
                    {settlementOverview.map((entry) => (
                      <div key={entry.label}>
                        <dt>{entry.label}</dt>
                        <dd>{entry.value}</dd>
                      </div>
                    ))}
                  </div>
                  <p>Production nodes: {settlementInspector.production_nodes?.length ?? 0}</p>
                  <p>Groups: {settlementInspector.groups?.length ?? 0}</p>
                  <p>Routes: {settlementInspector.routes?.length ?? 0}</p>
                  <div className="inspector-block">
                    <h4>Pressure readouts</h4>
                    <div className="inspector-chip-list">
                      {Object.entries(settlementInspector.pressure_readouts).map(([key, value]) => (
                        <span key={key} className="chip-link">
                          {key}: {value.toFixed(2)}
                        </span>
                      ))}
                      {Object.keys(settlementInspector.pressure_readouts).length === 0 ? (
                        <span className="empty-state">(none)</span>
                      ) : null}
                    </div>
                  </div>
                  <div className="inspector-block">
                    <h4>Notable events</h4>
                    <div className="inspector-action-list">
                      {settlementInspector.notable_events.slice(0, 20).map((event) => (
                        <button
                          key={event.event_id}
                          type="button"
                          className={`timeline-item ${
                            eventDetail?.event.event_id === event.event_id ? "selected" : ""
                          }`}
                          onClick={() => {
                            void onSelectEvent(event.event_id);
                          }}
                        >
                          <span>
                            T{event.tick}:{event.sequence_in_tick}
                          </span>
                          <strong>{formatEventType(event.event_type)}</strong>
                          <em>{event.location_id}</em>
                          <small>{summarizeReasonFromEvent(event)}</small>
                        </button>
                      ))}
                      {settlementInspector.notable_events.length === 0 ? (
                        <span className="empty-state">(none)</span>
                      ) : null}
                    </div>
                  </div>
                  <div className="inspector-json-grid">
                    <div>
                      <h4>Labor market</h4>
                      <pre className="json-block">
                        {JSON.stringify(settlementInspector.labor_market ?? {}, null, 2)}
                      </pre>
                    </div>
                    <div>
                      <h4>Stock + market</h4>
                      <pre className="json-block">
                        {JSON.stringify(
                          {
                            stock_ledger: settlementInspector.stock_ledger ?? {},
                            market_clearing: settlementInspector.market_clearing ?? {}
                          },
                          null,
                          2
                        )}
                      </pre>
                    </div>
                  </div>
                  <div className="inspector-json-grid">
                    <div>
                      <h4>Institution profile</h4>
                      <pre className="json-block">
                        {JSON.stringify(settlementInspector.institution_profile ?? {}, null, 2)}
                      </pre>
                    </div>
                    <div>
                      <h4>Institution queue + accounting</h4>
                      <pre className="json-block">
                        {JSON.stringify(
                          {
                            institution_queue: settlementInspector.institution_queue ?? {},
                            accounting_transfers:
                              settlementInspector.accounting_transfers?.slice(0, 20) ?? []
                          },
                          null,
                          2
                        )}
                      </pre>
                    </div>
                  </div>
                  <p>
                    Accounting transfers: {settlementInspector.accounting_transfers?.length ?? 0}
                  </p>
                </>
              ) : (
                <p className="empty-state">No settlement loaded.</p>
              )}
            </article>
          </div>
        </section>

        <section className="panel trace-panel">
          <h2>Trace Explorer</h2>

          {eventDetail ? (
            <div className="event-summary">
              <p>
                <strong>{eventDetail.event.event_id}</strong>
              </p>
              <p>
                Tick {eventDetail.event.tick} · {formatEventType(eventDetail.event.event_type)}
              </p>
              <p>Location: {eventDetail.event.location_id}</p>
              <p>Actor: {eventActorLabel(eventDetail.event)}</p>
              <p>
                Triggered by:{" "}
                {eventDetail.event.caused_by.length > 0
                  ? eventDetail.event.caused_by.join(", ")
                  : "(root event)"}
              </p>
              {eventDetail.reason_packet ? (
                <>
                  <p>Why: {eventDetail.reason_packet.selection_rationale}</p>
                  <p>
                    Why chain: {eventDetail.reason_packet.why_chain?.join(" -> ") || "(none)"}
                  </p>
                  <p>
                    Constraints:{" "}
                    {eventDetail.reason_packet.context_constraints?.join(", ") || "(none)"}
                  </p>
                  <p>
                    Expected consequences:{" "}
                    {eventDetail.reason_packet.expected_consequences?.join(", ") || "(none)"}
                  </p>
                </>
              ) : (
                <p>Why: {summarizeReasonFromEvent(eventDetail.event)}</p>
              )}
              <p>Details: {compactValue(eventDetail.event.details)}</p>
            </div>
          ) : (
            <p className="empty-state">Select a timeline event to open trace details.</p>
          )}

          <div className="trace-list">
            {traceChain.map((node) => (
              <article
                key={`${node.event.event_id}_${node.depth}`}
                className="trace-node"
                style={{ marginLeft: `${node.depth * 0.7}rem` }}
              >
                <p>
                  <strong>{node.event.event_id}</strong> · T{node.event.tick}:{" "}
                  {node.event.sequence_in_tick}
                </p>
                <p>{formatEventType(node.event.event_type)}</p>
                <p>{node.event.location_id}</p>
                <p>
                  Actor: {eventActorLabel(node.event)} · caused-by:{" "}
                  {node.event.caused_by.length > 0 ? node.event.caused_by.join(", ") : "(none)"}
                </p>
                {node.reason_packet ? (
                  <>
                    <p>{node.reason_packet.selection_rationale}</p>
                    <p>Why chain: {node.reason_packet.why_chain?.join(" -> ") || "(none)"}</p>
                    <p>
                      Constraints:{" "}
                      {node.reason_packet.context_constraints?.join(", ") || "(none)"}
                    </p>
                    <p>
                      Alternatives:{" "}
                      {node.reason_packet.alternatives_considered?.slice(0, 4).join(", ") ||
                        "(none)"}
                    </p>
                  </>
                ) : (
                  <p>Why: {summarizeReasonFromEvent(node.event)}</p>
                )}
              </article>
            ))}

            {traceChain.length === 0 && eventDetail && (
              <p className="empty-state">No linked parents found for this event.</p>
            )}
          </div>
        </section>

        <section className="panel stream-panel">
          <h2>Live Stream + Command Audit</h2>

          <div className="stream-audit-grid">
            <article>
              <h3>Stream</h3>
              <div className="stream-list">
                {streamMessages.map((message, index) => (
                  <p key={`${message.reconnect_token}_${index}`}>
                    [{message.type}] tick {message.tick}
                  </p>
                ))}
                {streamMessages.length === 0 ? (
                  <p className="empty-state">No stream messages yet.</p>
                ) : null}
              </div>
            </article>

            <article>
              <h3>Commands</h3>
              <button
                type="button"
                disabled={busy}
                onClick={() => {
                  void withAction(refreshCommands);
                }}
              >
                Refresh Commands
              </button>
              <div className="command-list">
                {commandAudit.map((entry) => (
                  <p key={entry.command.command_id}>
                    {entry.command.command_type} @ tick {entry.effective_tick} ·{" "}
                    {entry.result.accepted ? "accepted" : "rejected"}
                  </p>
                ))}
                {commandAudit.length === 0 ? (
                  <p className="empty-state">No commands loaded.</p>
                ) : null}
              </div>
            </article>
          </div>
        </section>
      </div>
    </main>
  );
}
