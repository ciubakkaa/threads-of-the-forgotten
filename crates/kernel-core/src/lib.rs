//! Deterministic phase executor and command queue with minimal NPC intent/action commits.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use contracts::{
    ActorRef, AffordanceVerb, BeliefClaimState, BeliefSource, Command, CommandPayload,
    ContractCadence, ContractCompensationType, EmploymentContractRecord, Event, EventType,
    FeasibilityCheck, GroupEntityState, HouseholdLedgerSnapshot, InstitutionProfileState,
    MobilityRouteState, MotiveFamily, NarrativeWhyChainSummary, NpcHouseholdLedgerSnapshot,
    ProductionNodeState, ReasonPacket, RelationshipEdgeState, RunConfig, RunMode, RunStatus,
    SettlementLaborMarketSnapshot, SettlementStockLedger, ShelterStatus, Snapshot, TrustLevel,
    SCHEMA_VERSION_V1,
};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    PreTick,
    Perception,
    MemoryBelief,
    IntentUpdate,
    ActionProposal,
    ActionResolution,
    PressureEconomy,
    Commit,
    PostTick,
}

const PHASE_ORDER: [Phase; 9] = [
    Phase::PreTick,
    Phase::Perception,
    Phase::MemoryBelief,
    Phase::IntentUpdate,
    Phase::ActionProposal,
    Phase::ActionResolution,
    Phase::PressureEconomy,
    Phase::Commit,
    Phase::PostTick,
];

#[derive(Debug, Clone)]
pub struct QueuedCommand {
    pub effective_tick: u64,
    pub insertion_sequence: u64,
    pub command: Command,
}

#[derive(Debug, Clone)]
struct NpcAgent {
    npc_id: String,
    location_id: String,
}

#[derive(Debug, Clone)]
struct ScenarioState {
    rumor_heat_by_location: BTreeMap<String, i64>,
    caravan_flow_by_settlement: BTreeMap<String, i64>,
    harvest_shock_by_settlement: BTreeMap<String, i64>,
    removed_npcs: BTreeSet<String>,
    winter_severity: u8,
}

impl Default for ScenarioState {
    fn default() -> Self {
        Self {
            rumor_heat_by_location: BTreeMap::new(),
            caravan_flow_by_settlement: BTreeMap::new(),
            harvest_shock_by_settlement: BTreeMap::new(),
            removed_npcs: BTreeSet::new(),
            winter_severity: 40,
        }
    }
}

#[derive(Debug, Clone)]
struct ItemRecord {
    owner_id: String,
    location_id: String,
    stolen: bool,
    last_moved_tick: u64,
}

#[derive(Debug, Clone, Default)]
struct ActionMemory {
    last_action: Option<String>,
    repeat_streak: u32,
    last_action_tick: u64,
    last_theft_tick: Option<u64>,
}

#[derive(Debug, Clone)]
struct NpcEconomyState {
    household_id: String,
    wallet: i64,
    debt_balance: i64,
    food_reserve_days: i64,
    shelter_status: ShelterStatus,
    dependents_count: u8,
    apprenticeship_progress: i64,
    employer_contract_id: Option<String>,
}

#[derive(Debug, Clone)]
struct NpcTraitProfile {
    risk_tolerance: i64,
    sociability: i64,
    dutifulness: i64,
    ambition: i64,
    empathy: i64,
    resilience: i64,
}

#[derive(Debug, Clone)]
struct HouseholdState {
    household_id: String,
    member_npc_ids: Vec<String>,
    shared_pantry_stock: i64,
    fuel_stock: i64,
    rent_due_tick: u64,
    rent_cadence_ticks: u64,
    rent_amount: i64,
    rent_reserve_coin: i64,
    landlord_balance: i64,
    eviction_risk_score: i64,
}

#[derive(Debug, Clone)]
struct ContractState {
    contract: EmploymentContractRecord,
}

#[derive(Debug, Clone)]
struct LaborMarketState {
    open_roles: u32,
    wage_band_low: i64,
    wage_band_high: i64,
    underemployment_index: i64,
}

#[derive(Debug, Clone)]
struct StockLedgerState {
    staples: i64,
    fuel: i64,
    medicine: i64,
    craft_inputs: i64,
    local_price_pressure: i64,
    coin_reserve: i64,
}

#[derive(Debug, Clone)]
struct ProductionNodeRuntimeState {
    node_id: String,
    settlement_id: String,
    node_kind: String,
    input_backlog: i64,
    output_backlog: i64,
    spoilage_timer: i64,
}

#[derive(Debug, Clone)]
struct RelationshipEdgeRuntime {
    source_npc_id: String,
    target_npc_id: String,
    trust: i64,
    attachment: i64,
    obligation: i64,
    grievance: i64,
    fear: i64,
    respect: i64,
    jealousy: i64,
    recent_interaction_tick: u64,
    compatibility_score: i64,
    shared_context_tags: Vec<String>,
    relation_tags: Vec<String>,
}

#[derive(Debug, Clone)]
struct BeliefClaimRuntime {
    claim_id: String,
    npc_id: String,
    settlement_id: String,
    confidence: f32,
    source: BeliefSource,
    distortion_score: f32,
    willingness_to_share: f32,
    truth_link: Option<String>,
    claim_text: String,
}

#[derive(Debug, Clone)]
struct InstitutionRuntimeState {
    enforcement_capacity: i64,
    corruption_level: i64,
    bias_level: i64,
    response_latency_ticks: i64,
}

#[derive(Debug, Clone)]
struct GroupRuntimeState {
    group_id: String,
    settlement_id: String,
    leader_npc_id: String,
    member_npc_ids: Vec<String>,
    norm_tags: Vec<String>,
    cohesion_score: i64,
    formed_tick: u64,
    inertia_score: i64,
    shared_cause: String,
    infiltration_pressure: i64,
}

#[derive(Debug, Clone)]
struct MobilityRouteRuntimeState {
    route_id: String,
    origin_settlement_id: String,
    destination_settlement_id: String,
    travel_time_ticks: u64,
    hazard_score: i64,
    weather_window_open: bool,
}

#[derive(Debug, Clone)]
struct NarrativeSummaryRuntime {
    summary_id: String,
    tick: u64,
    location_id: String,
    actor_id: String,
    motive_chain: Vec<String>,
    failed_alternatives: Vec<String>,
    social_consequence: String,
}

#[derive(Debug)]
pub struct Kernel {
    config: RunConfig,
    status: RunStatus,
    queued_commands: Vec<QueuedCommand>,
    event_log: Vec<Event>,
    reason_packet_log: Vec<ReasonPacket>,
    phase_trace: Vec<(u64, Phase)>,
    npcs: Vec<NpcAgent>,
    next_insertion_sequence: u64,
    pressure_index: i64,
    scenario_state: ScenarioState,
    law_case_load_by_settlement: BTreeMap<String, i64>,
    wanted_npcs: BTreeSet<String>,
    item_registry: BTreeMap<String, ItemRecord>,
    action_memory_by_npc: BTreeMap<String, ActionMemory>,
    npc_economy_by_id: BTreeMap<String, NpcEconomyState>,
    npc_traits_by_id: BTreeMap<String, NpcTraitProfile>,
    households_by_id: BTreeMap<String, HouseholdState>,
    contracts_by_id: BTreeMap<String, ContractState>,
    labor_market_by_settlement: BTreeMap<String, LaborMarketState>,
    stock_by_settlement: BTreeMap<String, StockLedgerState>,
    production_nodes_by_id: BTreeMap<String, ProductionNodeRuntimeState>,
    relationship_edges: BTreeMap<(String, String), RelationshipEdgeRuntime>,
    beliefs_by_npc: BTreeMap<String, Vec<BeliefClaimRuntime>>,
    institutions_by_settlement: BTreeMap<String, InstitutionRuntimeState>,
    groups_by_id: BTreeMap<String, GroupRuntimeState>,
    routes_by_id: BTreeMap<String, MobilityRouteRuntimeState>,
    narrative_summaries: Vec<NarrativeSummaryRuntime>,
    social_cohesion: i64,
    state_hash: u64,
    last_tick_terminal_event_id: Option<String>,
}

#[derive(Debug, Default)]
struct TickExecution {
    tick: u64,
    due_commands: Vec<QueuedCommand>,
    planned_events: Vec<PlannedEvent>,
    npc_decisions: Vec<NpcDecision>,
    perception_noise: u64,
    belief_noise: u64,
    intent_noise: u64,
    action_noise: u64,
    rumor_pressure: i64,
    caravan_relief: i64,
    harvest_pressure: i64,
    winter_pressure: i64,
    scenario_removed_npcs: usize,
    law_pressure: i64,
    contraband_pressure: i64,
    active_law_cases: i64,
    active_wanted_npcs: usize,
    stolen_item_count: usize,
    household_pressure: i64,
    labor_pressure: i64,
    supply_pressure: i64,
    institution_pressure: i64,
    social_graph_pressure: i64,
    mobility_pressure: i64,
    pressure_delta: i64,
}

#[derive(Debug)]
struct PlannedEvent {
    event_type: EventType,
    location_id: String,
    actors: Vec<ActorRef>,
    targets: Vec<ActorRef>,
    caused_by: Vec<String>,
    tags: Vec<String>,
    details: Option<Value>,
    reason_draft: Option<ReasonDraft>,
    state_effects: PlannedStateEffects,
}

#[derive(Debug, Clone, Default)]
struct PlannedStateEffects {
    law_case_deltas: Vec<LawCaseDelta>,
    wanted_add: Vec<String>,
    wanted_remove: Vec<String>,
    item_transfers: Vec<ItemTransfer>,
}

#[derive(Debug, Clone)]
struct LawCaseDelta {
    settlement_id: String,
    delta: i64,
}

#[derive(Debug, Clone)]
struct ItemTransfer {
    item_id: String,
    owner_id: String,
    location_id: String,
    stolen: bool,
}

#[derive(Debug, Clone)]
struct NpcDecision {
    npc_id: String,
    location_id: String,
    top_intents: Vec<String>,
    top_beliefs: Vec<String>,
    top_pressures: Vec<String>,
    alternatives: Vec<ActionCandidate>,
    chosen_action: Option<ActionCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionCandidate {
    verb: AffordanceVerb,
    action: String,
    priority: i32,
    score: u64,
    pressure_effect: i64,
}

#[derive(Debug, Clone)]
struct ReasonDraft {
    actor_id: String,
    chosen_verb: AffordanceVerb,
    chosen_action: String,
    top_intents: Vec<String>,
    top_beliefs: Vec<String>,
    top_pressures: Vec<String>,
    alternatives_considered: Vec<String>,
    motive_families: Vec<MotiveFamily>,
    feasibility_checks: Vec<FeasibilityCheck>,
    context_constraints: Vec<String>,
    why_chain: Vec<String>,
    expected_consequences: Vec<String>,
    selection_rationale: String,
}

impl Kernel {
    pub fn new(config: RunConfig) -> Self {
        let status = RunStatus {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: config.run_id.clone(),
            current_tick: 0,
            max_ticks: config.max_ticks(),
            mode: RunMode::Paused,
            queue_depth: 0,
        };

        let npcs = default_npcs();
        let mut npc_economy_by_id = default_npc_economy(&npcs);
        let npc_traits_by_id = default_npc_traits(&npcs);
        let households_by_id = default_households(&npcs);
        let contracts_by_id = default_contracts(&npcs);
        for (contract_id, state) in &contracts_by_id {
            if let Some(economy) = npc_economy_by_id.get_mut(&state.contract.worker_id) {
                economy.employer_contract_id = Some(contract_id.clone());
            }
        }
        let labor_market_by_settlement = default_labor_markets();
        let stock_by_settlement = default_stock_ledgers();
        let production_nodes_by_id = default_production_nodes();
        let relationship_edges = default_relationship_edges(&npcs);
        let beliefs_by_npc = default_beliefs(&npcs);
        let institutions_by_settlement = default_institutions();
        let groups_by_id = default_groups(&npcs);
        let routes_by_id = default_routes();

        Self {
            config,
            status,
            queued_commands: Vec::new(),
            event_log: Vec::new(),
            reason_packet_log: Vec::new(),
            phase_trace: Vec::new(),
            npcs,
            next_insertion_sequence: 0,
            pressure_index: 0,
            scenario_state: ScenarioState::default(),
            law_case_load_by_settlement: BTreeMap::new(),
            wanted_npcs: BTreeSet::new(),
            item_registry: default_items(),
            action_memory_by_npc: BTreeMap::new(),
            npc_economy_by_id,
            npc_traits_by_id,
            households_by_id,
            contracts_by_id,
            labor_market_by_settlement,
            stock_by_settlement,
            production_nodes_by_id,
            relationship_edges,
            beliefs_by_npc,
            institutions_by_settlement,
            groups_by_id,
            routes_by_id,
            narrative_summaries: Vec::new(),
            social_cohesion: 0,
            state_hash: 0,
            last_tick_terminal_event_id: None,
        }
    }

    pub fn run_id(&self) -> &str {
        &self.status.run_id
    }

    pub fn config(&self) -> &RunConfig {
        &self.config
    }

    pub fn start(&mut self) {
        if !self.status.is_complete() {
            self.status.mode = RunMode::Running;
        }
    }

    pub fn pause(&mut self) {
        self.status.mode = RunMode::Paused;
    }

    pub fn enqueue_command(&mut self, command: Command, effective_tick: u64) {
        let queued = QueuedCommand {
            effective_tick,
            insertion_sequence: self.next_insertion_sequence,
            command,
        };
        self.next_insertion_sequence += 1;
        self.queued_commands.push(queued);
        self.sync_queue_depth();
    }

    pub fn queue_depth(&self) -> usize {
        self.queued_commands.len()
    }

    pub fn events(&self) -> &[Event] {
        &self.event_log
    }

    pub fn reason_packets(&self) -> &[ReasonPacket] {
        &self.reason_packet_log
    }

    pub fn pressure_index(&self) -> i64 {
        self.pressure_index
    }

    pub fn phase_trace(&self) -> &[(u64, Phase)] {
        &self.phase_trace
    }

    pub fn state_hash(&self) -> u64 {
        self.state_hash
    }

    pub fn snapshot_for_current_tick(&self) -> Snapshot {
        let npc_ledgers = self
            .npc_economy_by_id
            .iter()
            .map(|(npc_id, economy)| NpcHouseholdLedgerSnapshot {
                npc_id: npc_id.clone(),
                household_id: economy.household_id.clone(),
                wallet: economy.wallet,
                debt_balance: economy.debt_balance,
                food_reserve_days: economy.food_reserve_days,
                shelter_status: economy.shelter_status,
                dependents_count: economy.dependents_count,
            })
            .collect::<Vec<_>>();
        let household_ledgers = self
            .households_by_id
            .values()
            .map(|household| HouseholdLedgerSnapshot {
                household_id: household.household_id.clone(),
                member_npc_ids: household.member_npc_ids.clone(),
                shared_pantry_stock: household.shared_pantry_stock,
                fuel_stock: household.fuel_stock,
                rent_due_tick: household.rent_due_tick,
                rent_cadence_ticks: household.rent_cadence_ticks,
                rent_amount: household.rent_amount,
                rent_reserve_coin: household.rent_reserve_coin,
                landlord_balance: household.landlord_balance,
                eviction_risk_score: household.eviction_risk_score,
            })
            .collect::<Vec<_>>();
        let labor_contracts = self
            .contracts_by_id
            .values()
            .map(|state| state.contract.clone())
            .collect::<Vec<_>>();
        let labor_markets = self
            .labor_market_by_settlement
            .iter()
            .map(|(settlement_id, market)| SettlementLaborMarketSnapshot {
                settlement_id: settlement_id.clone(),
                open_roles: market.open_roles,
                wage_band_low: market.wage_band_low,
                wage_band_high: market.wage_band_high,
                underemployment_index: market.underemployment_index,
            })
            .collect::<Vec<_>>();
        let stock_ledgers = self
            .stock_by_settlement
            .iter()
            .map(|(settlement_id, stock)| SettlementStockLedger {
                settlement_id: settlement_id.clone(),
                staples: stock.staples,
                fuel: stock.fuel,
                medicine: stock.medicine,
                craft_inputs: stock.craft_inputs,
                local_price_pressure: stock.local_price_pressure,
                coin_reserve: stock.coin_reserve,
            })
            .collect::<Vec<_>>();
        let production_nodes = self
            .production_nodes_by_id
            .values()
            .map(|node| ProductionNodeState {
                node_id: node.node_id.clone(),
                settlement_id: node.settlement_id.clone(),
                node_kind: node.node_kind.clone(),
                input_backlog: node.input_backlog,
                output_backlog: node.output_backlog,
                spoilage_timer: node.spoilage_timer,
            })
            .collect::<Vec<_>>();
        let relationship_edges = self
            .relationship_edges
            .values()
            .map(|edge| RelationshipEdgeState {
                source_npc_id: edge.source_npc_id.clone(),
                target_npc_id: edge.target_npc_id.clone(),
                trust: edge.trust,
                attachment: edge.attachment,
                obligation: edge.obligation,
                grievance: edge.grievance,
                fear: edge.fear,
                respect: edge.respect,
                jealousy: edge.jealousy,
                trust_level: trust_level_from_score(edge.trust),
                recent_interaction_tick: edge.recent_interaction_tick,
                compatibility_score: edge.compatibility_score,
                shared_context_tags: edge.shared_context_tags.clone(),
                relation_tags: edge.relation_tags.clone(),
            })
            .collect::<Vec<_>>();
        let belief_claims = self
            .beliefs_by_npc
            .values()
            .flat_map(|claims| claims.iter())
            .map(|claim| BeliefClaimState {
                claim_id: claim.claim_id.clone(),
                npc_id: claim.npc_id.clone(),
                settlement_id: claim.settlement_id.clone(),
                confidence: claim.confidence,
                source: claim.source,
                distortion_score: claim.distortion_score,
                willingness_to_share: claim.willingness_to_share,
                truth_link: claim.truth_link.clone(),
                claim_text: claim.claim_text.clone(),
            })
            .collect::<Vec<_>>();
        let institutions = self
            .institutions_by_settlement
            .iter()
            .map(|(settlement_id, institution)| InstitutionProfileState {
                settlement_id: settlement_id.clone(),
                enforcement_capacity: institution.enforcement_capacity,
                corruption_level: institution.corruption_level,
                bias_level: institution.bias_level,
                response_latency_ticks: institution.response_latency_ticks,
            })
            .collect::<Vec<_>>();
        let groups = self
            .groups_by_id
            .values()
            .map(|group| GroupEntityState {
                group_id: group.group_id.clone(),
                settlement_id: group.settlement_id.clone(),
                leader_npc_id: group.leader_npc_id.clone(),
                member_npc_ids: group.member_npc_ids.clone(),
                norm_tags: group.norm_tags.clone(),
                cohesion_score: group.cohesion_score,
            })
            .collect::<Vec<_>>();
        let mobility_routes = self
            .routes_by_id
            .values()
            .map(|route| MobilityRouteState {
                route_id: route.route_id.clone(),
                origin_settlement_id: route.origin_settlement_id.clone(),
                destination_settlement_id: route.destination_settlement_id.clone(),
                travel_time_ticks: route.travel_time_ticks,
                hazard_score: route.hazard_score,
                weather_window_open: route.weather_window_open,
            })
            .collect::<Vec<_>>();
        let narrative_summaries = self
            .narrative_summaries
            .iter()
            .rev()
            .take(64)
            .cloned()
            .map(|entry| NarrativeWhyChainSummary {
                summary_id: entry.summary_id,
                tick: entry.tick,
                location_id: entry.location_id,
                actor_id: entry.actor_id,
                motive_chain: entry.motive_chain,
                failed_alternatives: entry.failed_alternatives,
                social_consequence: entry.social_consequence,
            })
            .collect::<Vec<_>>();

        Snapshot {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick: self.status.current_tick,
            created_at: synthetic_timestamp(self.status.current_tick, 0),
            snapshot_id: format!("snap_{:06}", self.status.current_tick),
            world_state_hash: format!("{:016x}", self.state_hash),
            region_state: json!({
                "pressure_index": self.pressure_index,
                "winter_severity": self.scenario_state.winter_severity,
                "removed_npc_count": self.scenario_state.removed_npcs.len(),
                "law_case_load_total": sum_positive(self.law_case_load_by_settlement.values()),
                "wanted_npc_count": self.wanted_npcs.len(),
                "stolen_item_count": self.item_registry.values().filter(|item| item.stolen).count(),
                "social_cohesion": self.social_cohesion,
                "npc_traits": self
                    .npc_traits_by_id
                    .iter()
                    .map(|(npc_id, profile)| json!({
                        "npc_id": npc_id,
                        "risk_tolerance": profile.risk_tolerance,
                        "sociability": profile.sociability,
                        "dutifulness": profile.dutifulness,
                        "ambition": profile.ambition,
                        "empathy": profile.empathy,
                        "resilience": profile.resilience,
                    }))
                    .collect::<Vec<_>>(),
                "households": household_ledgers.clone(),
                "labor": {
                    "contracts": labor_contracts,
                    "markets": labor_markets,
                },
                "production": {
                    "stocks": stock_ledgers,
                    "nodes": production_nodes,
                },
                "relationships": relationship_edges,
                "beliefs": belief_claims,
                "institutions": institutions,
                "groups": groups,
                "mobility": mobility_routes,
                "narrative_summaries": narrative_summaries,
            }),
            settlement_states: json!({
                "rumor_heat_by_location": self.scenario_state.rumor_heat_by_location.clone(),
                "caravan_flow_by_settlement": self.scenario_state.caravan_flow_by_settlement.clone(),
                "harvest_shock_by_settlement": self.scenario_state.harvest_shock_by_settlement.clone(),
                "law_case_load_by_settlement": self.law_case_load_by_settlement.clone(),
                "stock_by_settlement": self
                    .stock_by_settlement
                    .iter()
                    .map(|(settlement_id, stock)| json!({
                        "settlement_id": settlement_id,
                        "staples": stock.staples,
                        "fuel": stock.fuel,
                        "medicine": stock.medicine,
                        "craft_inputs": stock.craft_inputs,
                        "local_price_pressure": stock.local_price_pressure,
                        "coin_reserve": stock.coin_reserve,
                    }))
                    .collect::<Vec<_>>(),
                "labor_market_by_settlement": self
                    .labor_market_by_settlement
                    .iter()
                    .map(|(settlement_id, market)| json!({
                        "settlement_id": settlement_id,
                        "open_roles": market.open_roles,
                        "wage_band_low": market.wage_band_low,
                        "wage_band_high": market.wage_band_high,
                        "underemployment_index": market.underemployment_index,
                    }))
                    .collect::<Vec<_>>(),
            }),
            npc_state_refs: json!({
                "npc_ids": self.npcs.iter().map(|npc| npc.npc_id.as_str()).collect::<Vec<_>>(),
                "wanted_npc_ids": self.wanted_npcs.iter().cloned().collect::<Vec<_>>(),
                "npc_ledgers": npc_ledgers,
                "npc_traits": self
                    .npc_traits_by_id
                    .iter()
                    .map(|(npc_id, profile)| json!({
                        "npc_id": npc_id,
                        "risk_tolerance": profile.risk_tolerance,
                        "sociability": profile.sociability,
                        "dutifulness": profile.dutifulness,
                        "ambition": profile.ambition,
                        "empathy": profile.empathy,
                        "resilience": profile.resilience,
                    }))
                    .collect::<Vec<_>>(),
                "households": household_ledgers,
                "item_registry": self
                    .item_registry
                    .iter()
                    .map(|(item_id, item)| json!({
                        "item_id": item_id,
                        "owner_id": item.owner_id,
                        "location_id": item.location_id,
                        "stolen": item.stolen,
                        "last_moved_tick": item.last_moved_tick,
                    }))
                    .collect::<Vec<_>>(),
            }),
            diff_from_prev_snapshot: None,
            perf_stats: Some(json!({
                "event_count": self.event_log.len(),
                "reason_packet_count": self.reason_packet_log.len(),
                "wanted_npc_count": self.wanted_npcs.len(),
                "social_cohesion": self.social_cohesion,
            })),
        }
    }

    pub fn step_tick(&mut self) -> bool {
        if self.status.is_complete() {
            self.status.mode = RunMode::Paused;
            return false;
        }

        let tick = self.status.current_tick + 1;
        let mut execution = TickExecution {
            tick,
            ..TickExecution::default()
        };

        for phase in PHASE_ORDER {
            self.phase_trace.push((tick, phase));
            self.execute_phase(phase, &mut execution);
        }

        if self.status.is_complete() {
            self.status.mode = RunMode::Paused;
        }

        true
    }

    pub fn step_ticks(&mut self, steps: u64) -> u64 {
        let mut committed = 0;
        for _ in 0..steps {
            if !self.step_tick() {
                break;
            }
            committed += 1;
        }
        committed
    }

    pub fn run_to_tick(&mut self, target_tick: u64) -> u64 {
        if target_tick <= self.status.current_tick {
            return 0;
        }

        let remaining = target_tick - self.status.current_tick;
        self.step_ticks(remaining)
    }

    pub fn status(&self) -> &RunStatus {
        &self.status
    }

    fn execute_phase(&mut self, phase: Phase, execution: &mut TickExecution) {
        match phase {
            Phase::PreTick => self.phase_pre_tick(execution),
            Phase::Perception => self.phase_perception(execution),
            Phase::MemoryBelief => self.phase_memory_belief(execution),
            Phase::IntentUpdate => self.phase_intent_update(execution),
            Phase::ActionProposal => self.phase_action_proposal(execution),
            Phase::ActionResolution => self.phase_action_resolution(execution),
            Phase::PressureEconomy => self.phase_pressure_economy(execution),
            Phase::Commit => self.phase_commit(execution),
            Phase::PostTick => self.phase_post_tick(execution),
        }
    }

    fn phase_pre_tick(&mut self, execution: &mut TickExecution) {
        let mut due = Vec::new();
        self.queued_commands.retain(|queued| {
            if queued.effective_tick <= execution.tick {
                due.push(queued.clone());
                false
            } else {
                true
            }
        });

        due.sort_by_key(|queued| (queued.effective_tick, queued.insertion_sequence));

        for queued in &due {
            self.apply_scenario_command(&queued.command);
            execution
                .planned_events
                .push(self.command_to_planned_event(queued, execution.tick));
        }

        execution.scenario_removed_npcs = self.scenario_state.removed_npcs.len();
        execution.due_commands = due;
        self.sync_queue_depth();
    }

    fn phase_perception(&self, execution: &mut TickExecution) {
        execution.perception_noise = self.deterministic_stream(
            execution.tick,
            Phase::Perception,
            "region",
            "discovery_noise",
        );
    }

    fn phase_memory_belief(&self, execution: &mut TickExecution) {
        execution.belief_noise = self.deterministic_stream(
            execution.tick,
            Phase::MemoryBelief,
            "region",
            "rumor_mutation",
        );
    }

    fn phase_intent_update(&self, execution: &mut TickExecution) {
        execution.intent_noise =
            self.deterministic_stream(execution.tick, Phase::IntentUpdate, "npc_set", "intent");

        let mut decisions = Vec::new();

        for npc in &self.npcs {
            let intent_seed = self.deterministic_stream(
                execution.tick,
                Phase::IntentUpdate,
                &npc.npc_id,
                "intent",
            );
            let rumor_heat = self
                .scenario_state
                .rumor_heat_by_location
                .get(&npc.location_id)
                .copied()
                .unwrap_or(0);
            let harvest_shock = self
                .scenario_state
                .harvest_shock_by_settlement
                .get(&npc.location_id)
                .copied()
                .unwrap_or(0);
            let law_case_load = self
                .law_case_load_by_settlement
                .get(&npc.location_id)
                .copied()
                .unwrap_or(0);
            let is_wanted = self.wanted_npcs.contains(&npc.npc_id);
            let has_stolen_item = self.npc_holds_stolen_item(&npc.npc_id);

            let mut top_intents = build_top_intents(
                intent_seed,
                self.pressure_index,
                rumor_heat,
                harvest_shock,
                self.scenario_state.winter_severity,
                execution.scenario_removed_npcs,
                law_case_load,
                is_wanted,
                has_stolen_item,
            );
            let mut top_beliefs = build_top_beliefs(
                execution.tick,
                intent_seed,
                self.pressure_index,
                rumor_heat,
                harvest_shock,
                self.scenario_state.winter_severity,
                law_case_load,
                is_wanted,
                has_stolen_item,
            );
            let mut top_pressures = top_pressures(
                self.pressure_index,
                rumor_heat,
                harvest_shock,
                self.scenario_state.winter_severity,
                law_case_load,
                self.item_registry.values().filter(|item| item.stolen).count(),
            );

            let economy = self.npc_economy_by_id.get(&npc.npc_id);
            if let Some(economy) = economy {
                if economy.food_reserve_days <= 2 {
                    push_unique(&mut top_intents, "secure_food");
                    push_unique(&mut top_pressures, "household_hunger");
                }
                if economy.wallet <= 1 || economy.debt_balance >= 4 {
                    push_unique(&mut top_intents, "seek_income");
                    push_unique(&mut top_pressures, "debt_strain");
                }
                if matches!(economy.shelter_status, ShelterStatus::Unsheltered) {
                    push_unique(&mut top_intents, "seek_shelter");
                    push_unique(&mut top_pressures, "shelter_instability");
                } else if matches!(economy.shelter_status, ShelterStatus::Precarious) {
                    let (eviction_risk, rent_due_in_ticks) = self
                        .households_by_id
                        .get(&economy.household_id)
                        .map(|household| {
                            (
                                household.eviction_risk_score,
                                household.rent_due_tick.saturating_sub(execution.tick) as i64,
                            )
                        })
                        .unwrap_or((0, 240));
                    if eviction_risk >= 80
                        || (eviction_risk >= 65
                            && economy.wallet <= 0
                            && economy.food_reserve_days <= 1)
                    {
                        push_unique(&mut top_intents, "seek_shelter");
                        push_unique(&mut top_pressures, "shelter_instability");
                    } else {
                        push_unique(&mut top_beliefs, "shelter_is_fragile_but_recoverable");
                    }
                    if rent_due_in_ticks <= 12 && economy.wallet < 2 {
                        push_unique(&mut top_intents, "seek_income");
                        push_unique(&mut top_pressures, "rent_deadline");
                    }
                }
                if economy.apprenticeship_progress >= 120 {
                    push_unique(&mut top_intents, "seek_opportunity");
                    push_unique(&mut top_beliefs, "skill_maturity_rising");
                } else if economy.apprenticeship_progress <= 20 {
                    push_unique(&mut top_beliefs, "skills_still_fragile");
                }
            }

            if let Some(contract_id) = economy.and_then(|value| value.employer_contract_id.as_ref()) {
                if let Some(contract) = self.contracts_by_id.get(contract_id) {
                    if contract.contract.reliability_score < 45 {
                        push_unique(&mut top_beliefs, "employer_payment_unreliable");
                    } else {
                        push_unique(&mut top_beliefs, "employer_contract_is_reliable");
                    }
                }
            } else {
                push_unique(&mut top_beliefs, "no_stable_contract");
            }

            let trust_support = self
                .relationship_edges
                .values()
                .filter(|edge| edge.source_npc_id == npc.npc_id && edge.trust >= 20)
                .count();
            if trust_support > 0 {
                push_unique(&mut top_beliefs, "trusted_contacts_available");
            } else {
                push_unique(&mut top_beliefs, "social_support_thin");
            }

            let institution = self.institutions_by_settlement.get(&npc.location_id);
            if let Some(institution) = institution {
                if institution.corruption_level >= 60 {
                    push_unique(&mut top_beliefs, "institutions_can_be_manipulated");
                    push_unique(&mut top_pressures, "institutional_corruption");
                }
                if institution.response_latency_ticks >= 8 {
                    push_unique(&mut top_beliefs, "institution_response_is_slow");
                }
            }

            if let Some(profile) = self.npc_traits_by_id.get(&npc.npc_id) {
                if profile.empathy >= 60 || profile.sociability >= 60 {
                    push_unique(&mut top_intents, "build_trust");
                }
                if profile.dutifulness >= 60 {
                    push_unique(&mut top_intents, "stabilize_security");
                }
                if profile.ambition >= 65 {
                    push_unique(&mut top_intents, "seek_opportunity");
                }
                if profile.risk_tolerance <= 40 {
                    push_unique(&mut top_beliefs, "risk_aversion_bias");
                } else if profile.risk_tolerance >= 65 {
                    push_unique(&mut top_beliefs, "risk_acceptance_bias");
                }
                if profile.resilience <= 40 {
                    push_unique(&mut top_pressures, "stress_sensitivity_high");
                }
            }

            decisions.push(NpcDecision {
                npc_id: npc.npc_id.clone(),
                location_id: npc.location_id.clone(),
                top_intents,
                top_beliefs,
                top_pressures,
                alternatives: Vec::new(),
                chosen_action: None,
            });
        }

        decisions.sort_by(|a, b| a.npc_id.cmp(&b.npc_id));
        execution.npc_decisions = decisions;
    }

    fn phase_action_proposal(&self, execution: &mut TickExecution) {
        for decision in &mut execution.npc_decisions {
            let rumor_heat = self
                .scenario_state
                .rumor_heat_by_location
                .get(&decision.location_id)
                .copied()
                .unwrap_or(0);
            let harvest_shock = self
                .scenario_state
                .harvest_shock_by_settlement
                .get(&decision.location_id)
                .copied()
                .unwrap_or(0);
            let law_case_load = self
                .law_case_load_by_settlement
                .get(&decision.location_id)
                .copied()
                .unwrap_or(0);
            let is_wanted = self.wanted_npcs.contains(&decision.npc_id);
            let has_stolen_item = self.npc_holds_stolen_item(&decision.npc_id);
            let economy = self.npc_economy_by_id.get(&decision.npc_id);
            let wallet = economy.map(|entry| entry.wallet).unwrap_or_default();
            let debt_balance = economy.map(|entry| entry.debt_balance).unwrap_or_default();
            let food_reserve_days = economy.map(|entry| entry.food_reserve_days).unwrap_or(3);
            let dependents_count = economy
                .map(|entry| i64::from(entry.dependents_count))
                .unwrap_or_default();
            let shelter_status = economy
                .map(|entry| entry.shelter_status)
                .unwrap_or(ShelterStatus::Stable);
            let contract_reliability = economy
                .and_then(|entry| entry.employer_contract_id.as_ref())
                .and_then(|contract_id| self.contracts_by_id.get(contract_id))
                .map(|contract| i64::from(contract.contract.reliability_score))
                .unwrap_or(0);
            let (eviction_risk, rent_due_in_ticks, rent_amount, rent_reserve_coin, rent_cadence_ticks) = economy
                .and_then(|entry| self.households_by_id.get(&entry.household_id))
                .map(|household| {
                    (
                        household.eviction_risk_score,
                        household
                            .rent_due_tick
                            .saturating_sub(execution.tick)
                            .min(240) as i64,
                        household.rent_amount,
                        household.rent_reserve_coin,
                        household.rent_cadence_ticks as i64,
                    )
                })
                .unwrap_or((0, 240, 0, 0, 720));
            let rent_shortfall = (rent_amount - rent_reserve_coin).max(0);
            let local_theft_pressure = self
                .event_log
                .iter()
                .rev()
                .take(96)
                .filter(|event| {
                    event.location_id == decision.location_id
                        && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let trust_support = self
                .relationship_edges
                .values()
                .filter(|edge| edge.source_npc_id == decision.npc_id && edge.trust >= 20)
                .count() as i64;
            let local_support_need = self
                .households_by_id
                .values()
                .filter(|household| {
                    household.member_npc_ids.iter().any(|member_npc_id| {
                        self.npc_location_for(member_npc_id)
                            .map(|location| location == decision.location_id)
                            .unwrap_or(false)
                    }) && (household.eviction_risk_score >= 45
                        || household.shared_pantry_stock <= 0
                        || household.fuel_stock <= 0)
                })
                .count() as i64;
            let low_pressure_economic_opportunity = self
                .labor_market_by_settlement
                .get(&decision.location_id)
                .map(|market| market.open_roles as i64 + (4 - market.underemployment_index / 10).max(0))
                .unwrap_or(0)
                + self
                    .stock_by_settlement
                    .get(&decision.location_id)
                    .map(|stock| ((stock.staples / 6).max(0) + (stock.craft_inputs / 8).max(0)).min(8))
                    .unwrap_or(0);
            let default_profile = NpcTraitProfile {
                risk_tolerance: 50,
                sociability: 50,
                dutifulness: 50,
                ambition: 50,
                empathy: 50,
                resilience: 50,
            };
            let profile = self
                .npc_traits_by_id
                .get(&decision.npc_id)
                .unwrap_or(&default_profile);

            decision.alternatives = build_action_candidates(
                execution.tick,
                &decision.npc_id,
                &decision.top_intents,
                self.pressure_index,
                rumor_heat,
                harvest_shock,
                self.scenario_state.winter_severity,
                law_case_load,
                is_wanted,
                has_stolen_item,
                wallet,
                debt_balance,
                food_reserve_days,
                dependents_count,
                shelter_status,
                contract_reliability,
                eviction_risk,
                rent_due_in_ticks,
                rent_shortfall,
                rent_cadence_ticks,
                trust_support,
                local_theft_pressure,
                local_support_need,
                low_pressure_economic_opportunity,
                profile,
                |tick, npc_id, action| {
                    self.deterministic_stream(tick, Phase::ActionProposal, npc_id, action)
                },
            );

            let action_memory = self.action_memory_by_npc.get(&decision.npc_id);
            apply_action_memory_bias(
                &mut decision.alternatives,
                action_memory,
                is_wanted,
                law_case_load,
                execution.tick,
                shelter_status,
                food_reserve_days,
                eviction_risk,
                local_theft_pressure,
                local_support_need,
                profile,
            );
        }
    }

    fn phase_action_resolution(&self, execution: &mut TickExecution) {
        execution.action_noise = self.deterministic_stream(
            execution.tick,
            Phase::ActionResolution,
            "npc_set",
            "action_resolution",
        );

        for decision in &mut execution.npc_decisions {
            let Some(mut chosen) = choose_candidate(&decision.alternatives) else {
                continue;
            };
            let repetitive_streak = self
                .action_memory_by_npc
                .get(&decision.npc_id)
                .map(|memory| memory.repeat_streak)
                .unwrap_or_default();
            let scarcity_mode = decision
                .top_intents
                .iter()
                .any(|intent| intent == "secure_food")
                || decision
                    .top_pressures
                    .iter()
                    .any(|pressure| pressure == "household_hunger" || pressure == "debt_strain");
            let rumor_mode = decision
                .top_intents
                .iter()
                .any(|intent| intent == "investigate_rumor")
                || decision.top_pressures.iter().any(|pressure| {
                    pressure == "information_rising" || pressure == "information_volatile"
                });
            if repetitive_streak >= 3 && scarcity_mode && execution.tick % 12 == 0 {
                let mut ranked = decision.alternatives.clone();
                ranked.sort_by(|left, right| {
                    (right.priority, right.score, &right.action)
                        .cmp(&(left.priority, left.score, &left.action))
                });
                if let Some(alternative) = ranked
                    .into_iter()
                    .find(|candidate| candidate.action != chosen.action)
                {
                    chosen = alternative;
                }
            } else if repetitive_streak >= 2 && rumor_mode && execution.tick % 8 == 0 {
                let mut ranked = decision.alternatives.clone();
                ranked.sort_by(|left, right| {
                    (right.priority, right.score, &right.action)
                        .cmp(&(left.priority, left.score, &left.action))
                });
                if let Some(alternative) = ranked.into_iter().find(|candidate| {
                    candidate.action != chosen.action
                        && matches!(
                            candidate.action.as_str(),
                            "share_rumor"
                                | "question_travelers"
                                | "investigate_rumor"
                                | "mediate_dispute"
                                | "trade_visit"
                        )
                }) {
                    chosen = alternative;
                }
            } else if repetitive_streak >= 3 && execution.tick % 10 == 0 {
                let mut ranked = decision.alternatives.clone();
                ranked.sort_by(|left, right| {
                    (right.priority, right.score, &right.action)
                        .cmp(&(left.priority, left.score, &left.action))
                });
                if let Some(alternative) = ranked
                    .into_iter()
                    .find(|candidate| candidate.action != chosen.action)
                {
                    chosen = alternative;
                }
            } else if execution.tick % 72 == 0 {
                let mut ranked = decision.alternatives.clone();
                ranked.sort_by(|left, right| {
                    (right.priority, right.score, &right.action)
                        .cmp(&(left.priority, left.score, &left.action))
                });
                let exploration_targets = [
                    "craft_goods",
                    "tend_fields",
                    "work_for_food",
                    "forage",
                    "question_travelers",
                    "mediate_dispute",
                    "trade_visit",
                ];
                let target_idx = (self.deterministic_stream(
                    execution.tick,
                    Phase::ActionResolution,
                    &decision.npc_id,
                    "exploration_target",
                ) % exploration_targets.len() as u64) as usize;
                let target_action = exploration_targets[target_idx];
                if let Some(alternative) = ranked
                    .iter()
                    .find(|candidate| {
                        candidate.action != chosen.action
                            && candidate.action.as_str() == target_action
                    })
                    .cloned()
                    .or_else(|| {
                        ranked.into_iter().find(|candidate| {
                            candidate.action != chosen.action
                                && matches!(
                                    candidate.action.as_str(),
                                    "craft_goods"
                                        | "tend_fields"
                                        | "work_for_food"
                                        | "forage"
                                        | "question_travelers"
                                        | "mediate_dispute"
                                        | "trade_visit"
                                )
                        })
                    })
                {
                    chosen = alternative;
                }
            }

            decision.chosen_action = Some(chosen.clone());

            let mut ranked_alternatives = decision
                .alternatives
                .iter()
                .filter(|candidate| candidate.action != chosen.action)
                .cloned()
                .collect::<Vec<_>>();
            ranked_alternatives.sort_by(|left, right| {
                (right.priority, right.score, &right.action)
                    .cmp(&(left.priority, left.score, &left.action))
            });
            let alternatives_considered = ranked_alternatives
                .into_iter()
                .take(8)
                .map(|candidate| candidate.action)
                .collect::<Vec<_>>();

            let rumor_heat = self
                .scenario_state
                .rumor_heat_by_location
                .get(&decision.location_id)
                .copied()
                .unwrap_or(0);
            let harvest_shock = self
                .scenario_state
                .harvest_shock_by_settlement
                .get(&decision.location_id)
                .copied()
                .unwrap_or(0);
            let law_case_load = self
                .law_case_load_by_settlement
                .get(&decision.location_id)
                .copied()
                .unwrap_or(0);
            let is_wanted = self.wanted_npcs.contains(&decision.npc_id);
            let has_stolen_item = self.npc_holds_stolen_item(&decision.npc_id);
            let context_constraints = self.build_context_constraints(
                &decision.npc_id,
                &decision.location_id,
                law_case_load,
                is_wanted,
            );
            let why_chain = self.build_why_chain(
                &decision.top_intents,
                &decision.top_beliefs,
                &decision.top_pressures,
                chosen.verb,
            );
            let expected_consequences =
                self.build_expected_consequences(&decision.location_id, chosen.verb);

            let reason_draft = ReasonDraft {
                actor_id: decision.npc_id.clone(),
                chosen_verb: chosen.verb,
                chosen_action: chosen.action.clone(),
                top_intents: decision.top_intents.clone(),
                top_beliefs: decision.top_beliefs.clone(),
                top_pressures: decision.top_pressures.clone(),
                alternatives_considered,
                motive_families: infer_motive_families(
                    &decision.top_intents,
                    chosen.verb,
                    is_wanted,
                    has_stolen_item,
                    law_case_load,
                    harvest_shock,
                    rumor_heat,
                ),
                feasibility_checks: infer_feasibility_checks(
                    self.pressure_index,
                    rumor_heat,
                    harvest_shock,
                    self.scenario_state.winter_severity,
                    law_case_load,
                    is_wanted,
                    has_stolen_item,
                ),
                context_constraints,
                why_chain,
                expected_consequences,
                selection_rationale: format!(
                    "selected action '{}' by deterministic priority/score ordering",
                    chosen.action
                ),
            };

            execution.planned_events.push(PlannedEvent {
                event_type: EventType::NpcActionCommitted,
                location_id: decision.location_id.clone(),
                actors: vec![ActorRef {
                    actor_id: decision.npc_id.clone(),
                    actor_kind: "npc".to_string(),
                }],
                targets: Vec::new(),
                caused_by: Vec::new(),
                tags: vec!["npc_action".to_string(), chosen.action.clone()],
                details: Some(json!({
                    "chosen_action": chosen.action,
                    "top_intents": decision.top_intents,
                    "top_beliefs": decision.top_beliefs,
                    "top_pressures": decision.top_pressures,
                })),
                reason_draft: Some(reason_draft),
                state_effects: PlannedStateEffects::default(),
            });

            let consequence_events =
                self.action_consequence_events(execution.tick, decision, &chosen);
            execution.planned_events.extend(consequence_events);
        }
    }

    fn phase_pressure_economy(&self, execution: &mut TickExecution) {
        let mut delta = (execution.action_noise % 3) as i64 - 1;

        for queued in &execution.due_commands {
            delta += pressure_delta_for_command(&queued.command);
        }

        for decision in &execution.npc_decisions {
            if let Some(chosen) = &decision.chosen_action {
                delta += chosen.pressure_effect;
            }
        }

        let phase_noise = self.deterministic_stream(
            execution.tick,
            Phase::PressureEconomy,
            "settlements",
            "aggregate_drift",
        );
        delta += (phase_noise % 3) as i64 - 1;

        execution.rumor_pressure =
            sum_positive(self.scenario_state.rumor_heat_by_location.values()) / 3;
        execution.caravan_relief =
            sum_positive(self.scenario_state.caravan_flow_by_settlement.values()) / 4;
        execution.harvest_pressure =
            sum_positive(self.scenario_state.harvest_shock_by_settlement.values()) / 2;
        execution.winter_pressure = winter_pressure_effect(self.scenario_state.winter_severity);

        let projected_law_cases = self.projected_law_case_load(execution.planned_events.as_slice());
        let projected_wanted = self.projected_wanted_count(execution.planned_events.as_slice());
        let projected_stolen = self.projected_stolen_item_count(execution.planned_events.as_slice());
        execution.active_law_cases = projected_law_cases;
        execution.active_wanted_npcs = projected_wanted;
        execution.stolen_item_count = projected_stolen;
        execution.law_pressure = projected_law_cases / 2 + projected_wanted as i64;
        execution.contraband_pressure = (projected_stolen as i64) / 2;
        execution.household_pressure = self.household_pressure_signal();
        execution.labor_pressure = self.labor_pressure_signal();
        execution.supply_pressure = self.supply_pressure_signal();
        execution.institution_pressure = self.institution_pressure_signal();
        execution.social_graph_pressure = self.social_graph_pressure_signal();
        execution.mobility_pressure = self.mobility_pressure_signal();

        delta += execution.rumor_pressure;
        delta -= execution.caravan_relief;
        delta += execution.harvest_pressure;
        delta += execution.winter_pressure;
        delta += execution.law_pressure;
        delta += execution.contraband_pressure;
        delta += execution.household_pressure;
        delta += execution.labor_pressure;
        delta += execution.supply_pressure;
        delta += execution.institution_pressure;
        delta += execution.social_graph_pressure;
        delta += execution.mobility_pressure;
        if execution.scenario_removed_npcs > 0 {
            delta += 1;
        }

        // Adaptive reversion keeps pressure in a scenario-conditioned envelope.
        let equilibrium_target = 80
            + execution.harvest_pressure * 4
            + execution.winter_pressure * 5
            + execution.household_pressure * 4
            + execution.mobility_pressure * 2;
        let gap = self.pressure_index - equilibrium_target;
        delta -= gap / 30;

        // Social cohesion shifts pressure balance gradually over multi-day windows.
        if self.social_cohesion > 0 {
            delta -= 1 + (self.social_cohesion / 40).min(2);
        } else if self.social_cohesion < 0 {
            delta += 1 + ((-self.social_cohesion) / 40).min(2);
        }

        if execution.active_law_cases == 0 && execution.active_wanted_npcs == 0 {
            delta -= 1;
        }

        execution.pressure_delta = delta.clamp(-14, 14);
    }

    fn phase_commit(&mut self, execution: &mut TickExecution) {
        let mut sequence_in_tick = 0_u64;
        let mut causal_chain = Vec::new();
        let mut pending_reason_packets = Vec::new();
        let mut reason_sequence = 0_u64;

        self.pressure_index += execution.pressure_delta;

        let system_tick_event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick: execution.tick,
            created_at: synthetic_timestamp(execution.tick, sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", execution.tick, sequence_in_tick),
            sequence_in_tick,
            event_type: EventType::SystemTick,
            location_id: "region:crownvale".to_string(),
            actors: Vec::new(),
            reason_packet_id: None,
            caused_by: self
                .last_tick_terminal_event_id
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            targets: Vec::new(),
            tags: vec!["system".to_string()],
            visibility: None,
            details: Some(json!({
                "perception_noise": execution.perception_noise,
                "belief_noise": execution.belief_noise,
                "intent_noise": execution.intent_noise,
                "action_noise": execution.action_noise,
                "rumor_pressure": execution.rumor_pressure,
                "caravan_relief": execution.caravan_relief,
                "harvest_pressure": execution.harvest_pressure,
                "winter_pressure": execution.winter_pressure,
                "law_pressure": execution.law_pressure,
                "contraband_pressure": execution.contraband_pressure,
                "household_pressure": execution.household_pressure,
                "labor_pressure": execution.labor_pressure,
                "supply_pressure": execution.supply_pressure,
                "institution_pressure": execution.institution_pressure,
                "social_graph_pressure": execution.social_graph_pressure,
                "mobility_pressure": execution.mobility_pressure,
                "active_law_cases": execution.active_law_cases,
                "active_wanted_npcs": execution.active_wanted_npcs,
                "stolen_item_count": execution.stolen_item_count,
                "pressure_delta": execution.pressure_delta,
                "pressure_index": self.pressure_index,
                "social_cohesion": self.social_cohesion,
                "winter_severity": self.scenario_state.winter_severity,
                "removed_npc_count": self.scenario_state.removed_npcs.len(),
            })),
        };

        let system_event_id = system_tick_event.event_id.clone();
        causal_chain.push(system_event_id.clone());
        self.event_log.push(system_tick_event);
        sequence_in_tick += 1;

        for planned in &execution.planned_events {
            let mut planned_caused_by = planned.caused_by.clone();
            if planned_caused_by.is_empty() {
                planned_caused_by = vec![system_event_id.clone()];
            }

            let reason_packet_id = planned.reason_draft.as_ref().map(|draft| {
                let reason_id = format!("rp_{:06}_{:03}", execution.tick, reason_sequence);
                reason_sequence += 1;

                pending_reason_packets.push(ReasonPacket {
                    schema_version: SCHEMA_VERSION_V1.to_string(),
                    run_id: self.status.run_id.clone(),
                    tick: execution.tick,
                    created_at: synthetic_timestamp(execution.tick, reason_sequence),
                    reason_packet_id: reason_id.clone(),
                    actor_id: draft.actor_id.clone(),
                    chosen_action: draft.chosen_action.clone(),
                    top_intents: draft.top_intents.clone(),
                    top_beliefs: draft.top_beliefs.clone(),
                    top_pressures: draft.top_pressures.clone(),
                    alternatives_considered: draft.alternatives_considered.clone(),
                    motive_families: draft.motive_families.clone(),
                    feasibility_checks: draft.feasibility_checks.clone(),
                    chosen_verb: Some(draft.chosen_verb),
                    context_constraints: draft.context_constraints.clone(),
                    why_chain: draft.why_chain.clone(),
                    expected_consequences: draft.expected_consequences.clone(),
                    selection_rationale: draft.selection_rationale.clone(),
                });

                reason_id
            });

            let event = Event {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: self.status.run_id.clone(),
                tick: execution.tick,
                created_at: synthetic_timestamp(execution.tick, sequence_in_tick),
                event_id: format!("evt_{:06}_{:03}", execution.tick, sequence_in_tick),
                sequence_in_tick,
                event_type: planned.event_type,
                location_id: planned.location_id.clone(),
                actors: planned.actors.clone(),
                reason_packet_id,
                caused_by: planned_caused_by,
                targets: planned.targets.clone(),
                tags: planned.tags.clone(),
                visibility: None,
                details: planned.details.clone(),
            };

            self.record_action_memory_from_event(&event);
            causal_chain.push(event.event_id.clone());
            self.event_log.push(event);
            self.apply_planned_state_effects(&planned.state_effects, execution.tick);
            sequence_in_tick += 1;
        }

        self.progress_story_loops(
            execution.tick,
            &system_event_id,
            &mut sequence_in_tick,
            &mut causal_chain,
        );

        self.progress_justice_loop(
            execution.tick,
            &system_event_id,
            &mut sequence_in_tick,
            &mut causal_chain,
        );
        self.progress_living_world_loops(
            execution.tick,
            &system_event_id,
            &mut sequence_in_tick,
            &mut causal_chain,
        );

        if execution.pressure_delta != 0 {
            let pressure_event = Event {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: self.status.run_id.clone(),
                tick: execution.tick,
                created_at: synthetic_timestamp(execution.tick, sequence_in_tick),
                event_id: format!("evt_{:06}_{:03}", execution.tick, sequence_in_tick),
                sequence_in_tick,
                event_type: EventType::PressureEconomyUpdated,
                location_id: "region:crownvale".to_string(),
                actors: Vec::new(),
                reason_packet_id: None,
                caused_by: causal_chain.clone(),
                targets: Vec::new(),
                tags: vec!["aggregate".to_string()],
                visibility: None,
                details: Some(json!({
                    "delta": execution.pressure_delta,
                    "pressure_index": self.pressure_index,
                    "rumor_pressure": execution.rumor_pressure,
                    "caravan_relief": execution.caravan_relief,
                    "harvest_pressure": execution.harvest_pressure,
                    "winter_pressure": execution.winter_pressure,
                    "law_pressure": execution.law_pressure,
                    "contraband_pressure": execution.contraband_pressure,
                    "household_pressure": execution.household_pressure,
                    "labor_pressure": execution.labor_pressure,
                    "supply_pressure": execution.supply_pressure,
                    "institution_pressure": execution.institution_pressure,
                    "social_graph_pressure": execution.social_graph_pressure,
                    "mobility_pressure": execution.mobility_pressure,
                    "active_law_cases": execution.active_law_cases,
                    "active_wanted_npcs": execution.active_wanted_npcs,
                    "stolen_item_count": execution.stolen_item_count,
                    "social_cohesion": self.social_cohesion,
                    "winter_severity": self.scenario_state.winter_severity,
                    "removed_npc_count": self.scenario_state.removed_npcs.len(),
                })),
            };

            causal_chain.push(pressure_event.event_id.clone());
            self.event_log.push(pressure_event);
        }

        self.reason_packet_log.extend(pending_reason_packets);

        self.last_tick_terminal_event_id = causal_chain.last().cloned();
        self.status.current_tick = execution.tick;
        self.sync_queue_depth();
    }

    fn phase_post_tick(&mut self, execution: &TickExecution) {
        self.decay_scenario_state(execution.tick);
        self.decay_living_world_state(execution.tick);

        self.state_hash = mix64(self.state_hash ^ execution.tick);
        self.state_hash = mix64(self.state_hash ^ self.pressure_index as u64);
        self.state_hash = mix64(self.state_hash ^ self.event_log.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.reason_packet_log.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.scenario_state.winter_severity as u64);
        self.state_hash = mix64(self.state_hash ^ self.scenario_state.removed_npcs.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.social_cohesion as u64);
        self.state_hash = mix64(
            self.state_hash ^ sum_positive(self.law_case_load_by_settlement.values()) as u64,
        );
        self.state_hash = mix64(self.state_hash ^ self.wanted_npcs.len() as u64);
        self.state_hash = mix64(
            self.state_hash
                ^ self
                    .item_registry
                    .values()
                    .filter(|item| item.stolen)
                    .count() as u64,
        );
        self.state_hash = mix64(self.state_hash ^ self.households_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.contracts_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.relationship_edges.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.groups_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.routes_by_id.len() as u64);
        self.state_hash = mix64(
            self.state_hash
                ^ self
                    .npc_economy_by_id
                    .values()
                    .map(|state| {
                        (state.wallet
                            + state.debt_balance
                            + state.food_reserve_days
                            + state.apprenticeship_progress)
                            .max(0) as u64
                    })
                    .sum::<u64>(),
        );
        self.state_hash = mix64(
            self.state_hash
                ^ self
                    .stock_by_settlement
                    .values()
                    .map(|stock| {
                        (stock.staples
                            + stock.fuel
                            + stock.medicine
                            + stock.craft_inputs
                            + stock.coin_reserve)
                            .max(0) as u64
                    })
                    .sum::<u64>(),
        );
        self.state_hash = mix64(
            self.state_hash
                ^ self
                    .households_by_id
                    .values()
                    .map(|household| {
                        (household.shared_pantry_stock
                            + household.fuel_stock
                            + household.rent_amount
                            + household.rent_reserve_coin
                            + household.landlord_balance)
                            .max(0) as u64
                    })
                    .sum::<u64>(),
        );
        for (item_id, item) in &self.item_registry {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(item_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(item.owner_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(item.location_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ item.last_moved_tick);
            self.state_hash = mix64(self.state_hash ^ item.stolen as u64);
        }
        for (npc_id, profile) in &self.npc_traits_by_id {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ profile.risk_tolerance.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.sociability.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.dutifulness.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.ambition.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.empathy.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.resilience.max(0) as u64);
        }

        if let Some(last_event_id) = &self.last_tick_terminal_event_id {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(last_event_id.as_bytes()));
        }

        if execution.tick % 24 == 0 {
            self.social_cohesion = self.social_cohesion.signum() * (self.social_cohesion.abs() - 1);
        }
        if self.pressure_index > 240 {
            self.pressure_index -= ((self.pressure_index - 240) / 35).clamp(1, 8);
        } else if self.pressure_index < -80 {
            self.pressure_index += ((-80 - self.pressure_index) / 35).clamp(1, 5);
        }

        if self.status.current_tick >= self.status.max_ticks {
            self.status.mode = RunMode::Paused;
        }
    }

    fn apply_scenario_command(&mut self, command: &Command) {
        match &command.payload {
            CommandPayload::InjectRumor { location_id, .. } => {
                increase_signal(
                    &mut self.scenario_state.rumor_heat_by_location,
                    location_id,
                    4,
                );
            }
            CommandPayload::InjectSpawnCaravan {
                origin_settlement_id,
                destination_settlement_id,
            } => {
                increase_signal(
                    &mut self.scenario_state.caravan_flow_by_settlement,
                    origin_settlement_id,
                    3,
                );
                increase_signal(
                    &mut self.scenario_state.caravan_flow_by_settlement,
                    destination_settlement_id,
                    3,
                );

                decrease_signal(
                    &mut self.scenario_state.harvest_shock_by_settlement,
                    origin_settlement_id,
                    1,
                );
                decrease_signal(
                    &mut self.scenario_state.harvest_shock_by_settlement,
                    destination_settlement_id,
                    1,
                );
            }
            CommandPayload::InjectRemoveNpc { npc_id } => {
                self.scenario_state.removed_npcs.insert(npc_id.clone());
                self.wanted_npcs.remove(npc_id);
                self.npcs.retain(|npc| npc.npc_id != *npc_id);
                self.npc_economy_by_id.remove(npc_id);
                self.beliefs_by_npc.remove(npc_id);
                self.relationship_edges
                    .retain(|(left, right), _| left != npc_id && right != npc_id);
                for household in self.households_by_id.values_mut() {
                    household.member_npc_ids.retain(|member| member != npc_id);
                }
                for group in self.groups_by_id.values_mut() {
                    group.member_npc_ids.retain(|member| member != npc_id);
                    if group.leader_npc_id == *npc_id {
                        if let Some(next_leader) = group.member_npc_ids.first() {
                            group.leader_npc_id = next_leader.clone();
                        }
                    }
                }
            }
            CommandPayload::InjectForceBadHarvest { settlement_id } => {
                increase_signal(
                    &mut self.scenario_state.harvest_shock_by_settlement,
                    settlement_id,
                    5,
                );
            }
            CommandPayload::InjectSetWinterSeverity { severity } => {
                self.scenario_state.winter_severity = *severity;
            }
            _ => {}
        }
    }

    fn decay_scenario_state(&mut self, tick: u64) {
        if tick % 3 == 0 {
            decay_signal_map(&mut self.scenario_state.rumor_heat_by_location, 1);
        }
        if tick % 2 == 0 {
            decay_signal_map(&mut self.scenario_state.caravan_flow_by_settlement, 1);
        }
        if tick % 6 == 0 {
            decay_signal_map(&mut self.scenario_state.harvest_shock_by_settlement, 1);
        }

        if tick % 8 == 0 {
            if self.scenario_state.winter_severity > 40 {
                self.scenario_state.winter_severity -= 1;
            } else if self.scenario_state.winter_severity < 40 {
                self.scenario_state.winter_severity += 1;
            }
        }

        if tick % 4 == 0 {
            decay_signal_map(&mut self.law_case_load_by_settlement, 1);
        }

        self.wanted_npcs
            .retain(|npc_id| self.npcs.iter().any(|npc| npc.npc_id == *npc_id));
    }

    fn decay_living_world_state(&mut self, tick: u64) {
        if tick % 24 == 0 {
            for economy in self.npc_economy_by_id.values_mut() {
                if tick % 168 == 0 && economy.wallet > 24 {
                    economy.wallet -= 1;
                }
                if economy.debt_balance > 0 {
                    economy.debt_balance -= 1;
                }
            }
            for market in self.labor_market_by_settlement.values_mut() {
                market.underemployment_index = (market.underemployment_index - 1).max(0);
            }
            for stock in self.stock_by_settlement.values_mut() {
                stock.local_price_pressure = (stock.local_price_pressure - 1).max(-10);
            }
            for edge in self.relationship_edges.values_mut() {
                if edge.trust > 0 {
                    edge.trust -= 1;
                }
                if edge.grievance > 0 {
                    edge.grievance -= 1;
                }
            }
            for institution in self.institutions_by_settlement.values_mut() {
                if institution.corruption_level > 0 {
                    institution.corruption_level -= 1;
                }
                if institution.bias_level > 0 {
                    institution.bias_level -= 1;
                }
            }
        }

        if tick % 12 == 0 {
            for route in self.routes_by_id.values_mut() {
                route.hazard_score = (route.hazard_score - 1).max(0);
            }
        }
    }

    fn command_to_planned_event(&self, queued: &QueuedCommand, tick: u64) -> PlannedEvent {
        let command = &queued.command;
        let mut base = PlannedEvent {
            event_type: EventType::CommandApplied,
            location_id: "region:crownvale".to_string(),
            actors: Vec::new(),
            targets: Vec::new(),
            caused_by: Vec::new(),
            tags: vec!["command".to_string()],
            details: Some(json!({
                "command_id": command.command_id,
                "command_type": format!("{:?}", command.command_type),
                "issued_at_tick": command.issued_at_tick,
                "effective_tick": queued.effective_tick,
                "applied_at_tick": tick,
            })),
            reason_draft: None,
            state_effects: PlannedStateEffects::default(),
        };

        match &command.payload {
            CommandPayload::InjectRumor {
                location_id,
                rumor_text,
            } => {
                base.event_type = EventType::RumorInjected;
                base.location_id = location_id.clone();
                base.details = Some(json!({
                    "command_id": command.command_id,
                    "rumor_text": rumor_text,
                    "rumor_heat": self
                        .scenario_state
                        .rumor_heat_by_location
                        .get(location_id)
                        .copied()
                        .unwrap_or(0),
                }));
            }
            CommandPayload::InjectSpawnCaravan {
                origin_settlement_id,
                destination_settlement_id,
            } => {
                base.event_type = EventType::CaravanSpawned;
                base.location_id = origin_settlement_id.clone();

                let transfer = self.select_item_for_route(origin_settlement_id).map(|item_id| {
                    ItemTransfer {
                        item_id: item_id.clone(),
                        owner_id: "caravan:merchant".to_string(),
                        location_id: destination_settlement_id.clone(),
                        stolen: false,
                    }
                });
                if let Some(effect) = transfer.clone() {
                    base.state_effects.item_transfers.push(effect);
                }

                base.details = Some(json!({
                    "command_id": command.command_id,
                    "origin_settlement_id": origin_settlement_id,
                    "destination_settlement_id": destination_settlement_id,
                    "origin_flow": self
                        .scenario_state
                        .caravan_flow_by_settlement
                        .get(origin_settlement_id)
                        .copied()
                        .unwrap_or(0),
                    "destination_flow": self
                        .scenario_state
                        .caravan_flow_by_settlement
                        .get(destination_settlement_id)
                        .copied()
                        .unwrap_or(0),
                    "moved_item_id": transfer.as_ref().map(|entry| entry.item_id.as_str()),
                }));
            }
            CommandPayload::InjectRemoveNpc { npc_id } => {
                base.event_type = EventType::NpcRemoved;
                let rehomed_items = self
                    .item_registry
                    .iter()
                    .filter(|(_, item)| item.owner_id == *npc_id)
                    .map(|(item_id, item)| ItemTransfer {
                        item_id: item_id.clone(),
                        owner_id: format!("estate:{}", item.location_id),
                        location_id: item.location_id.clone(),
                        stolen: false,
                    })
                    .collect::<Vec<_>>();
                base.state_effects.item_transfers.extend(rehomed_items.clone());
                base.details = Some(json!({
                    "command_id": command.command_id,
                    "npc_id": npc_id,
                    "removed": self.scenario_state.removed_npcs.contains(npc_id),
                    "remaining_npc_count": self.npcs.len(),
                    "rehomed_item_count": rehomed_items.len(),
                }));
            }
            CommandPayload::InjectForceBadHarvest { settlement_id } => {
                base.event_type = EventType::BadHarvestForced;
                base.location_id = settlement_id.clone();
                base.details = Some(json!({
                    "command_id": command.command_id,
                    "settlement_id": settlement_id,
                    "harvest_shock": self
                        .scenario_state
                        .harvest_shock_by_settlement
                        .get(settlement_id)
                        .copied()
                        .unwrap_or(0),
                }));
            }
            CommandPayload::InjectSetWinterSeverity { severity } => {
                base.event_type = EventType::WinterSeveritySet;
                base.details = Some(json!({
                    "command_id": command.command_id,
                    "severity": severity,
                    "baseline_target": 40,
                }));
            }
            _ => {}
        }

        base
    }

    fn action_consequence_events(
        &self,
        tick: u64,
        decision: &NpcDecision,
        chosen: &ActionCandidate,
    ) -> Vec<PlannedEvent> {
        let mut planned = Vec::new();

        match chosen.verb {
            AffordanceVerb::StealSupplies => {
                let (item_id, previous_owner) =
                    self.select_item_for_theft(&decision.location_id, &decision.npc_id, tick);

                planned.push(PlannedEvent {
                    event_type: EventType::TheftCommitted,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: previous_owner.clone(),
                        actor_kind: "owner".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["crime".to_string(), "theft".to_string()],
                    details: Some(json!({
                        "item_id": item_id,
                        "from_owner_id": previous_owner,
                        "to_owner_id": decision.npc_id,
                        "wanted": true,
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects {
                        law_case_deltas: vec![LawCaseDelta {
                            settlement_id: decision.location_id.clone(),
                            delta: 2,
                        }],
                        wanted_add: vec![decision.npc_id.clone()],
                        wanted_remove: Vec::new(),
                        item_transfers: vec![ItemTransfer {
                            item_id,
                            owner_id: decision.npc_id.clone(),
                            location_id: decision.location_id.clone(),
                            stolen: true,
                        }],
                    },
                });
            }
            AffordanceVerb::FenceGoods => {
                if let Some(item_id) = self.stolen_item_owned_by_npc(&decision.npc_id) {
                    planned.push(PlannedEvent {
                        event_type: EventType::ItemTransferred,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        targets: vec![ActorRef {
                            actor_id: format!("fence:{}", decision.location_id),
                            actor_kind: "broker".to_string(),
                        }],
                        caused_by: Vec::new(),
                        tags: vec!["item".to_string(), "fencing".to_string()],
                        details: Some(json!({
                            "item_id": item_id,
                            "from_owner_id": decision.npc_id,
                            "to_owner_id": format!("fence:{}", decision.location_id),
                            "transfer_kind": "fencing",
                        })),
                        reason_draft: None,
                        state_effects: PlannedStateEffects {
                            law_case_deltas: vec![LawCaseDelta {
                                settlement_id: decision.location_id.clone(),
                                delta: -1,
                            }],
                            wanted_add: Vec::new(),
                            wanted_remove: Vec::new(),
                            item_transfers: vec![ItemTransfer {
                                item_id,
                                owner_id: format!("fence:{}", decision.location_id),
                                location_id: decision.location_id.clone(),
                                stolen: false,
                            }],
                        },
                    });
                }
            }
            AffordanceVerb::OrganizeWatch
            | AffordanceVerb::PatrolRoad
            | AffordanceVerb::CollectTestimony
            | AffordanceVerb::QuestionTravelers => {
                let law_cases = self
                    .law_case_load_by_settlement
                    .get(&decision.location_id)
                    .copied()
                    .unwrap_or(0);
                if law_cases > 0 {
                    planned.push(PlannedEvent {
                        event_type: EventType::InvestigationProgressed,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        targets: Vec::new(),
                        caused_by: Vec::new(),
                        tags: vec!["justice".to_string(), "investigation".to_string()],
                        details: Some(json!({
                            "investigator_id": decision.npc_id,
                            "law_case_load_before": law_cases,
                            "law_case_delta": -1,
                        })),
                        reason_draft: None,
                        state_effects: PlannedStateEffects {
                            law_case_deltas: vec![LawCaseDelta {
                                settlement_id: decision.location_id.clone(),
                                delta: -1,
                            }],
                            wanted_add: Vec::new(),
                            wanted_remove: Vec::new(),
                            item_transfers: Vec::new(),
                        },
                    });

                    if tick % 6 == 0 {
                        if let Some(suspect_id) = self.first_wanted_npc_at_location(&decision.location_id) {
                            planned.push(PlannedEvent {
                                event_type: EventType::ArrestMade,
                                location_id: decision.location_id.clone(),
                                actors: vec![ActorRef {
                                    actor_id: decision.npc_id.clone(),
                                    actor_kind: "npc".to_string(),
                                }],
                                targets: vec![ActorRef {
                                    actor_id: suspect_id.clone(),
                                    actor_kind: "npc".to_string(),
                                }],
                                caused_by: Vec::new(),
                                tags: vec!["justice".to_string(), "arrest".to_string()],
                                details: Some(json!({
                                    "officer_id": decision.npc_id,
                                    "suspect_id": suspect_id,
                                })),
                                reason_draft: None,
                                state_effects: PlannedStateEffects {
                                    law_case_deltas: vec![LawCaseDelta {
                                        settlement_id: decision.location_id.clone(),
                                        delta: -1,
                                    }],
                                    wanted_add: Vec::new(),
                                    wanted_remove: vec![suspect_id],
                                    item_transfers: Vec::new(),
                                },
                            });
                        }
                    }
                }
            }
            AffordanceVerb::WorkForCoin => {
                let contract_info = self
                    .npc_economy_by_id
                    .get(&decision.npc_id)
                    .and_then(|economy| economy.employer_contract_id.as_ref())
                    .and_then(|contract_id| self.contracts_by_id.get(contract_id))
                    .map(|state| {
                        (
                            state.contract.contract_id.clone(),
                            state.contract.wage_amount,
                            state.contract.compensation_type,
                        )
                    });
                if let Some((contract_id, wage_amount, compensation_type)) = contract_info {
                    planned.push(PlannedEvent {
                        event_type: EventType::JobSought,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        targets: vec![ActorRef {
                            actor_id: contract_id,
                            actor_kind: "contract".to_string(),
                        }],
                        caused_by: Vec::new(),
                        tags: vec!["labor".to_string(), "shift".to_string()],
                        details: Some(json!({
                            "worker_id": decision.npc_id,
                            "expected_wage_amount": wage_amount,
                            "compensation_type": compensation_type,
                            "source": "npc_action",
                        })),
                        reason_draft: None,
                        state_effects: PlannedStateEffects::default(),
                    });
                }
            }
            AffordanceVerb::PayRent => {
                planned.push(PlannedEvent {
                    event_type: EventType::EvictionRiskChanged,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["household".to_string(), "rent".to_string()],
                    details: Some(json!({
                        "actor_id": decision.npc_id,
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::SeekShelter => {
                planned.push(PlannedEvent {
                    event_type: EventType::RelationshipStatusChanged,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("patron:{}", decision.location_id),
                        actor_kind: "patron".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["household".to_string(), "shelter".to_string()],
                    details: Some(json!({
                        "status_change": "shelter_negotiation",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::TendFields | AffordanceVerb::CraftGoods => {
                planned.push(PlannedEvent {
                    event_type: EventType::ProductionStarted,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["production".to_string()],
                    details: Some(json!({
                        "worker_id": decision.npc_id,
                        "verb": chosen.action,
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::ShareMeal => {
                planned.push(PlannedEvent {
                    event_type: EventType::ObligationCreated,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "aid".to_string()],
                    details: Some(json!({
                        "kind": "meal_share",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::FormMutualAidGroup => {
                planned.push(PlannedEvent {
                    event_type: EventType::GroupFormed,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["group".to_string(), "mutual_aid".to_string()],
                    details: Some(json!({
                        "seed_group_id": format!("group:mutual_aid:{}", decision.location_id),
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::DefendPatron => {
                planned.push(PlannedEvent {
                    event_type: EventType::ObligationCalled,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("patron:{}", decision.location_id),
                        actor_kind: "patron".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "defense".to_string()],
                    details: None,
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::SpreadAccusation => {
                planned.push(PlannedEvent {
                    event_type: EventType::BeliefDisputed,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["belief".to_string(), "accusation".to_string()],
                    details: Some(json!({
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::MediateDispute => {
                planned.push(PlannedEvent {
                    event_type: EventType::InstitutionCaseResolved,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["institution".to_string(), "mediation".to_string()],
                    details: None,
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::TrainApprentice => {
                planned.push(PlannedEvent {
                    event_type: EventType::ApprenticeshipProgressed,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["lifecycle".to_string(), "training".to_string()],
                    details: None,
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            _ => {}
        }

        planned
    }

    fn projected_law_case_load(&self, planned_events: &[PlannedEvent]) -> i64 {
        let mut projected = self.law_case_load_by_settlement.clone();
        for planned in planned_events {
            for delta in &planned.state_effects.law_case_deltas {
                apply_case_delta(&mut projected, &delta.settlement_id, delta.delta);
            }
        }

        sum_positive(projected.values())
    }

    fn projected_wanted_count(&self, planned_events: &[PlannedEvent]) -> usize {
        let mut projected = self.wanted_npcs.clone();
        for planned in planned_events {
            for added in &planned.state_effects.wanted_add {
                if self.npcs.iter().any(|npc| npc.npc_id == *added) {
                    projected.insert(added.clone());
                }
            }
            for removed in &planned.state_effects.wanted_remove {
                projected.remove(removed);
            }
        }

        projected.len()
    }

    fn projected_stolen_item_count(&self, planned_events: &[PlannedEvent]) -> usize {
        let mut projected = self
            .item_registry
            .iter()
            .map(|(item_id, item)| (item_id.clone(), item.stolen))
            .collect::<BTreeMap<_, _>>();

        for planned in planned_events {
            for transfer in &planned.state_effects.item_transfers {
                projected.insert(transfer.item_id.clone(), transfer.stolen);
            }
        }

        projected.values().filter(|value| **value).count()
    }

    fn apply_planned_state_effects(&mut self, effects: &PlannedStateEffects, tick: u64) {
        for delta in &effects.law_case_deltas {
            apply_case_delta(
                &mut self.law_case_load_by_settlement,
                &delta.settlement_id,
                delta.delta,
            );
        }

        for npc_id in &effects.wanted_add {
            if self.npcs.iter().any(|npc| npc.npc_id == *npc_id) {
                self.wanted_npcs.insert(npc_id.clone());
            }
        }

        for npc_id in &effects.wanted_remove {
            self.wanted_npcs.remove(npc_id);
        }

        for transfer in &effects.item_transfers {
            self.item_registry.insert(
                transfer.item_id.clone(),
                ItemRecord {
                    owner_id: transfer.owner_id.clone(),
                    location_id: transfer.location_id.clone(),
                    stolen: transfer.stolen,
                    last_moved_tick: tick,
                },
            );
        }
    }

    fn record_action_memory_from_event(&mut self, event: &Event) {
        if event.event_type != EventType::NpcActionCommitted {
            return;
        }

        let Some(actor_id) = event.actors.first().map(|actor| actor.actor_id.as_str()) else {
            return;
        };
        let Some(chosen_action) = event
            .details
            .as_ref()
            .and_then(|details| details.get("chosen_action"))
            .and_then(Value::as_str)
        else {
            return;
        };

        let memory = self
            .action_memory_by_npc
            .entry(actor_id.to_string())
            .or_default();
        if memory.last_action.as_deref() == Some(chosen_action) {
            memory.repeat_streak = memory.repeat_streak.saturating_add(1);
        } else {
            memory.repeat_streak = 0;
        }
        memory.last_action = Some(chosen_action.to_string());
        memory.last_action_tick = event.tick;
        if chosen_action == "steal_supplies" {
            memory.last_theft_tick = Some(event.tick);
        }
    }

    fn progress_story_loops(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        self.progress_discovery_loop(tick, system_event_id, sequence_in_tick, causal_chain);
        self.progress_relationship_loop(tick, system_event_id, sequence_in_tick, causal_chain);
    }

    fn progress_discovery_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        if !self.deterministic_window_fire(tick, 24, "discovery") || self.npcs.is_empty() {
            return;
        }

        let Some(location_id) = self.discovery_hotspot_location(tick) else {
            return;
        };

        let rumor_heat = self
            .scenario_state
            .rumor_heat_by_location
            .get(location_id.as_str())
            .copied()
            .unwrap_or(0);
        if rumor_heat <= 0 {
            return;
        }

        let explorer = self.npcs[tick as usize % self.npcs.len()].clone();
        let site_id = self.hidden_site_for_location(location_id.as_str(), tick);
        let discovery_event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type: EventType::SiteDiscovered,
            location_id: location_id.clone(),
            actors: vec![ActorRef {
                actor_id: explorer.npc_id.clone(),
                actor_kind: "npc".to_string(),
            }],
            reason_packet_id: None,
            caused_by: vec![system_event_id.to_string()],
            targets: Vec::new(),
            tags: vec!["discovery".to_string(), "site".to_string()],
            visibility: None,
            details: Some(json!({
                "site_id": site_id,
                "rumor_heat": rumor_heat,
                "discoverer_id": explorer.npc_id,
            })),
        };

        let discovery_event_id = discovery_event.event_id.clone();
        causal_chain.push(discovery_event_id.clone());
        self.event_log.push(discovery_event);
        *sequence_in_tick += 1;

        let leverage_event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type: EventType::LeverageGained,
            location_id,
            actors: vec![ActorRef {
                actor_id: explorer.npc_id,
                actor_kind: "npc".to_string(),
            }],
            reason_packet_id: None,
            caused_by: vec![discovery_event_id],
            targets: vec![ActorRef {
                actor_id: "institution:settlement_council".to_string(),
                actor_kind: "institution".to_string(),
            }],
            tags: vec!["discovery".to_string(), "leverage".to_string()],
            visibility: None,
            details: Some(json!({
                "leverage_type": "proof",
                "source": "site_discovery",
            })),
        };

        causal_chain.push(leverage_event.event_id.clone());
        self.event_log.push(leverage_event);
        *sequence_in_tick += 1;
    }

    fn progress_relationship_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        if !self.deterministic_window_fire(tick, 18, "relationship") || self.npcs.len() < 2 {
            return;
        }

        let actor = &self.npcs[0];
        let target = &self.npcs[1];
        let betrayal_pressure = self.pressure_index / 40
            + self.wanted_npcs.len() as i64
            + (sum_positive(self.law_case_load_by_settlement.values()) / 5);
        let betrayal_threshold = (35_i64 + betrayal_pressure).clamp(20, 85) as u64;
        let roll = self.deterministic_stream(
            tick,
            Phase::ActionResolution,
            "relationship",
            "shift_kind",
        ) % 100;
        let (shift_kind, trust_delta) = if roll < betrayal_threshold {
            ("betrayal", -2_i64)
        } else {
            ("cooperation", 2_i64)
        };
        self.social_cohesion = (self.social_cohesion + trust_delta).clamp(-200, 200);

        let relationship_event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type: EventType::RelationshipShifted,
            location_id: actor.location_id.clone(),
            actors: vec![ActorRef {
                actor_id: actor.npc_id.clone(),
                actor_kind: "npc".to_string(),
            }],
            reason_packet_id: None,
            caused_by: vec![system_event_id.to_string()],
            targets: vec![ActorRef {
                actor_id: target.npc_id.clone(),
                actor_kind: "npc".to_string(),
            }],
            tags: vec!["social".to_string(), shift_kind.to_string()],
            visibility: None,
            details: Some(json!({
                "shift_kind": shift_kind,
                "trust_delta": trust_delta,
                "pressure_index": self.pressure_index,
                "social_cohesion": self.social_cohesion,
            })),
        };

        causal_chain.push(relationship_event.event_id.clone());
        self.event_log.push(relationship_event);
        *sequence_in_tick += 1;
    }

    fn discovery_hotspot_location(&self, tick: u64) -> Option<String> {
        let mut settlements = self
            .npcs
            .iter()
            .map(|npc| npc.location_id.clone())
            .collect::<Vec<_>>();
        settlements.sort();
        settlements.dedup();

        settlements.into_iter().max_by_key(|location_id| {
            let rumor_heat = self
                .scenario_state
                .rumor_heat_by_location
                .get(location_id)
                .copied()
                .unwrap_or(0)
                .max(0) as u64;
            let jitter = self.deterministic_stream(
                tick,
                Phase::Perception,
                location_id,
                "discovery_hotspot",
            ) % 16;
            rumor_heat * 10 + jitter
        })
    }

    fn deterministic_window_fire(&self, tick: u64, window: u64, channel: &str) -> bool {
        if tick == 0 || window == 0 {
            return false;
        }

        let cycle = (tick - 1) / window;
        let cycle_start_tick = cycle * window + 1;
        let offset = self.deterministic_stream(
            cycle + 1,
            Phase::PreTick,
            "window_scheduler",
            channel,
        ) % window;
        tick == cycle_start_tick + offset
    }

    fn hidden_site_for_location(&self, location_id: &str, tick: u64) -> String {
        let node_site = "site:forgotten_waystone";
        let ruin_site = "site:ashen_vault";
        let watch_post = "site:old_watch_post";
        let roll = self.deterministic_stream(tick, Phase::Perception, location_id, "site_choice") % 100;

        if self.pressure_index > 220 || roll > 70 {
            ruin_site.to_string()
        } else if roll > 35 {
            node_site.to_string()
        } else {
            watch_post.to_string()
        }
    }

    fn progress_justice_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        if !self.deterministic_window_fire(tick, 6, "justice") {
            return;
        }

        let Some((settlement_id, before_case_load)) = self
            .law_case_load_by_settlement
            .iter()
            .find(|(_, value)| **value > 0)
            .map(|(key, value)| (key.clone(), *value))
        else {
            return;
        };

        apply_case_delta(&mut self.law_case_load_by_settlement, &settlement_id, -1);
        let after_case_load = self
            .law_case_load_by_settlement
            .get(&settlement_id)
            .copied()
            .unwrap_or(0);

        let investigation_event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type: EventType::InvestigationProgressed,
            location_id: settlement_id.clone(),
            actors: vec![ActorRef {
                actor_id: "watch:marshal".to_string(),
                actor_kind: "institution".to_string(),
            }],
            reason_packet_id: None,
            caused_by: vec![system_event_id.to_string()],
            targets: Vec::new(),
            tags: vec!["justice".to_string(), "institution".to_string()],
            visibility: None,
            details: Some(json!({
                "law_case_load_before": before_case_load,
                "law_case_load_after": after_case_load,
                "source": "justice_loop",
            })),
        };

        causal_chain.push(investigation_event.event_id.clone());
        self.event_log.push(investigation_event);
        *sequence_in_tick += 1;

        if let Some((suspect_id, suspect_location)) = self.first_wanted_npc_with_case() {
            self.wanted_npcs.remove(&suspect_id);
            apply_case_delta(&mut self.law_case_load_by_settlement, &suspect_location, -1);

            let arrest_event = Event {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: self.status.run_id.clone(),
                tick,
                created_at: synthetic_timestamp(tick, *sequence_in_tick),
                event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                sequence_in_tick: *sequence_in_tick,
                event_type: EventType::ArrestMade,
                location_id: suspect_location,
                actors: vec![ActorRef {
                    actor_id: "watch:marshal".to_string(),
                    actor_kind: "institution".to_string(),
                }],
                reason_packet_id: None,
                caused_by: causal_chain.clone(),
                targets: vec![ActorRef {
                    actor_id: suspect_id,
                    actor_kind: "npc".to_string(),
                }],
                tags: vec!["justice".to_string(), "arrest".to_string()],
                visibility: None,
                details: Some(json!({
                    "source": "justice_loop",
                })),
            };

            causal_chain.push(arrest_event.event_id.clone());
            self.event_log.push(arrest_event);
            *sequence_in_tick += 1;
        }
    }

    fn progress_living_world_loops(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        self.progress_household_and_labor_loop(
            tick,
            system_event_id,
            sequence_in_tick,
            causal_chain,
        );
        self.progress_production_and_supply_loop(
            tick,
            system_event_id,
            sequence_in_tick,
            causal_chain,
        );
        self.progress_relationship_and_belief_loop(
            tick,
            system_event_id,
            sequence_in_tick,
            causal_chain,
        );
        self.progress_institution_group_and_route_loop(
            tick,
            system_event_id,
            sequence_in_tick,
            causal_chain,
        );
        self.emit_narrative_why_summary(tick, system_event_id, sequence_in_tick, causal_chain);
    }

    fn progress_household_and_labor_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let action_by_npc = self
            .event_log
            .iter()
            .rev()
            .take_while(|event| event.tick == tick)
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .filter_map(|event| {
                let actor_id = event.actors.first()?.actor_id.clone();
                let action = event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("chosen_action"))
                    .and_then(Value::as_str)?
                    .to_string();
                Some((actor_id, action))
            })
            .collect::<BTreeMap<_, _>>();
        let shelter_seekers = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| (action.as_str() == "seek_shelter").then_some(npc_id.clone()))
            .collect::<BTreeSet<_>>();
        let livelihood_workers = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| {
                matches!(action.as_str(), "work_for_food" | "work_for_coin" | "tend_fields")
                    .then_some(npc_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let rent_contributors = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| (action.as_str() == "pay_rent").then_some(npc_id.clone()))
            .collect::<Vec<_>>();
        for npc_id in rent_contributors {
            let household_id = self
                .npc_economy_by_id
                .get(&npc_id)
                .map(|economy| economy.household_id.clone());
            let mut contributed = 0_i64;
            if let Some(economy) = self.npc_economy_by_id.get_mut(&npc_id) {
                if economy.wallet > 0 {
                    economy.wallet -= 1;
                    contributed = 1;
                }
            }
            if contributed > 0 {
                if let Some(household_id) = household_id {
                    if let Some(household) = self.households_by_id.get_mut(&household_id) {
                        let cap = (household.rent_amount * 2).max(4);
                        household.rent_reserve_coin =
                            (household.rent_reserve_coin + contributed).min(cap);
                    }
                }
            }
        }

        if tick % 24 == 0 {
            let household_ids = self.households_by_id.keys().cloned().collect::<Vec<_>>();
            for household_id in household_ids {
                let Some(mut household) = self.households_by_id.remove(&household_id) else {
                    continue;
                };
                let members = household.member_npc_ids.clone();
                let member_count = members.len().max(1) as i64;
                let settlement_id = members
                    .first()
                    .and_then(|npc_id| self.npc_location_for(npc_id))
                    .unwrap_or_else(|| "settlement:greywall".to_string());
                let pantry_need = member_count;
                let fuel_need = if tick % 48 == 0 {
                    (member_count / 2).max(1)
                } else {
                    0
                };
                let mut purchased_staples = 0_i64;
                let mut purchased_fuel = 0_i64;

                if household.shared_pantry_stock < pantry_need {
                    let wallet_pool = members
                        .iter()
                        .filter_map(|npc_id| self.npc_economy_by_id.get(npc_id))
                        .map(|economy| economy.wallet.max(0))
                        .sum::<i64>();
                    let shortage = pantry_need - household.shared_pantry_stock.max(0);
                    if wallet_pool > 0 {
                        if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                            let market_offer = (shortage + member_count).min(stock.staples.max(0));
                            purchased_staples = market_offer.min(wallet_pool);
                            stock.staples -= purchased_staples;
                            stock.coin_reserve += purchased_staples;
                        }
                        let mut remaining_cost = purchased_staples;
                        for npc_id in &members {
                            if remaining_cost <= 0 {
                                break;
                            }
                            if let Some(economy) = self.npc_economy_by_id.get_mut(npc_id) {
                                let payment = economy.wallet.max(0).min(remaining_cost);
                                economy.wallet -= payment;
                                remaining_cost -= payment;
                            }
                        }
                        household.shared_pantry_stock += purchased_staples;
                    }
                }

                if household.fuel_stock < fuel_need {
                    let wallet_pool = members
                        .iter()
                        .filter_map(|npc_id| self.npc_economy_by_id.get(npc_id))
                        .map(|economy| economy.wallet.max(0))
                        .sum::<i64>();
                    let shortage = fuel_need - household.fuel_stock.max(0);
                    if wallet_pool > 0 {
                        if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                            let market_offer = (shortage + 1).min(stock.fuel.max(0));
                            purchased_fuel = market_offer.min(wallet_pool);
                            stock.fuel -= purchased_fuel;
                            stock.coin_reserve += purchased_fuel;
                        }
                        let mut remaining_cost = purchased_fuel;
                        for npc_id in &members {
                            if remaining_cost <= 0 {
                                break;
                            }
                            if let Some(economy) = self.npc_economy_by_id.get_mut(npc_id) {
                                let payment = economy.wallet.max(0).min(remaining_cost);
                                economy.wallet -= payment;
                                remaining_cost -= payment;
                            }
                        }
                        household.fuel_stock += purchased_fuel;
                    }
                }

                let pantry_consumed = pantry_need.min(household.shared_pantry_stock.max(0));
                let fuel_consumed = fuel_need.min(household.fuel_stock.max(0));
                household.shared_pantry_stock =
                    (household.shared_pantry_stock - pantry_consumed).max(0);
                household.fuel_stock = (household.fuel_stock - fuel_consumed).max(0);
                let pantry_shortfall = pantry_need - pantry_consumed;
                let fuel_shortfall = fuel_need - fuel_consumed;

                for npc_id in &members {
                    let resilience = self
                        .npc_traits_by_id
                        .get(npc_id)
                        .map(|profile| profile.resilience)
                        .unwrap_or(50);
                    let recovery_roll = (self.deterministic_stream(
                        tick,
                        Phase::Commit,
                        npc_id,
                        "shelter_recovery",
                    ) % 100) as i64;
                    let chosen_action = action_by_npc.get(npc_id).map(String::as_str);
                    let contractless_work_for_coin = self
                        .npc_economy_by_id
                        .get(npc_id)
                        .map(|economy| {
                            chosen_action == Some("work_for_coin")
                                && economy.employer_contract_id.is_none()
                        })
                        .unwrap_or(false);
                    let mut spot_coin_payout = 0_i64;
                    if contractless_work_for_coin {
                        if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                            spot_coin_payout = stock.coin_reserve.max(0).min(1);
                            stock.coin_reserve -= spot_coin_payout;
                        }
                    }
                    if let Some(economy) = self.npc_economy_by_id.get_mut(npc_id) {
                        if spot_coin_payout > 0 {
                            economy.wallet += spot_coin_payout;
                        }
                        if matches!(
                            chosen_action,
                            Some("tend_fields" | "craft_goods")
                        ) {
                            household.shared_pantry_stock =
                                (household.shared_pantry_stock + 1).clamp(0, 12);
                        }
                        if matches!(chosen_action, Some("work_for_food")) {
                            household.shared_pantry_stock =
                                (household.shared_pantry_stock + 1).clamp(0, 12);
                            economy.food_reserve_days = (economy.food_reserve_days + 2).clamp(0, 14);
                        }
                        if livelihood_workers.contains(npc_id) {
                            economy.food_reserve_days = (economy.food_reserve_days + 1).clamp(0, 14);
                        }
                        if pantry_shortfall > 0 {
                            economy.food_reserve_days = (economy.food_reserve_days - 1).max(0);
                        } else {
                            economy.food_reserve_days = (economy.food_reserve_days + 1).clamp(0, 14);
                        }
                        if fuel_shortfall > 0
                            && matches!(economy.shelter_status, ShelterStatus::Stable)
                        {
                            economy.shelter_status = ShelterStatus::Precarious;
                        }
                        if pantry_shortfall == 0 && fuel_shortfall == 0 {
                            if matches!(economy.shelter_status, ShelterStatus::Precarious)
                                && (shelter_seekers.contains(npc_id)
                                    || economy.wallet >= 2
                                    || tick % 24 == 0)
                            {
                                if recovery_roll + resilience >= 70 {
                                    economy.shelter_status = ShelterStatus::Stable;
                                }
                            }
                        }
                    }
                }
                if pantry_shortfall > 0 || fuel_shortfall > 0 {
                    household.eviction_risk_score = (household.eviction_risk_score + 1).clamp(0, 100);
                } else {
                    household.eviction_risk_score = (household.eviction_risk_score - 1).max(0);
                }
                let pantry_after = household.shared_pantry_stock;
                let fuel_after = household.fuel_stock;
                self.households_by_id.insert(household_id.clone(), household);

                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::HouseholdConsumptionApplied,
                    household_id.clone(),
                    vec![ActorRef {
                        actor_id: household_id.clone(),
                        actor_kind: "household".to_string(),
                    }],
                    Vec::new(),
                    vec!["household".to_string(), "consumption".to_string()],
                    Some(json!({
                        "household_id": household_id,
                        "settlement_id": settlement_id,
                        "pantry_consumed": pantry_consumed,
                        "fuel_consumed": fuel_consumed,
                        "pantry_shortfall": pantry_shortfall,
                        "fuel_shortfall": fuel_shortfall,
                        "purchased_staples": purchased_staples,
                        "purchased_fuel": purchased_fuel,
                        "market_spend_coin": purchased_staples + purchased_fuel,
                        "shared_pantry_stock": pantry_after,
                        "fuel_stock": fuel_after,
                    })),
                );
                if pantry_shortfall > 0 {
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::HouseholdBufferExhausted,
                        household_id.clone(),
                        vec![ActorRef {
                            actor_id: household_id.clone(),
                            actor_kind: "household".to_string(),
                        }],
                        Vec::new(),
                        vec!["household".to_string(), "buffer".to_string()],
                        Some(json!({
                            "household_id": household_id,
                            "shared_pantry_stock": pantry_after,
                            "shortfall": pantry_shortfall,
                        })),
                    );
                }
            }
        }

        let household_ids = self.households_by_id.keys().cloned().collect::<Vec<_>>();
        for household_id in household_ids {
            let Some(household_view) = self.households_by_id.get(&household_id) else {
                continue;
            };
            if household_view.rent_due_tick > tick {
                continue;
            }
            let members = household_view.member_npc_ids.clone();
            let payer_id = members.first().cloned();

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::RentDue,
                household_id.clone(),
                payer_id
                    .iter()
                    .map(|payer| ActorRef {
                        actor_id: payer.clone(),
                        actor_kind: "npc".to_string(),
                    })
                    .collect::<Vec<_>>(),
                Vec::new(),
                vec!["household".to_string(), "rent".to_string()],
                Some(json!({
                    "household_id": household_id,
                    "due_tick": tick,
                    "rent_cadence_ticks": household_view.rent_cadence_ticks,
                    "rent_amount": household_view.rent_amount,
                    "rent_reserve_coin": household_view.rent_reserve_coin,
                })),
            );

            let Some(mut household) = self.households_by_id.remove(&household_id) else {
                continue;
            };
            let rent_amount = household.rent_amount.max(1);
            let reserve_used = household.rent_reserve_coin.min(rent_amount);
            household.rent_reserve_coin -= reserve_used;
            let mut remaining_rent = rent_amount - reserve_used;
            for member_id in &members {
                if remaining_rent <= 0 {
                    break;
                }
                if let Some(economy) = self.npc_economy_by_id.get_mut(member_id) {
                    let payment = economy.wallet.max(0).min(remaining_rent);
                    economy.wallet -= payment;
                    remaining_rent -= payment;
                }
            }
            let paid = remaining_rent == 0;
            if paid {
                household.eviction_risk_score = (household.eviction_risk_score - 3).max(0);
                household.landlord_balance += rent_amount;
            } else {
                household.eviction_risk_score = (household.eviction_risk_score + 2).clamp(0, 100);
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::RentUnpaid,
                    household_id.clone(),
                    payer_id
                        .iter()
                        .map(|payer| ActorRef {
                            actor_id: payer.clone(),
                            actor_kind: "npc".to_string(),
                        })
                        .collect::<Vec<_>>(),
                    Vec::new(),
                    vec!["household".to_string(), "rent_unpaid".to_string()],
                    Some(json!({
                        "household_id": household_id,
                        "rent_amount": rent_amount,
                        "remaining_rent": remaining_rent,
                        "eviction_risk_score": household.eviction_risk_score,
                    })),
                );
            }
            household.rent_due_tick = tick + household.rent_cadence_ticks.max(24);
            let eviction_risk_score = household.eviction_risk_score;
            let rent_reserve_coin = household.rent_reserve_coin;
            let landlord_balance = household.landlord_balance;
            self.households_by_id.insert(household_id.clone(), household);

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::EvictionRiskChanged,
                household_id.clone(),
                Vec::new(),
                Vec::new(),
                vec!["household".to_string(), "eviction".to_string()],
                Some(json!({
                    "household_id": household_id,
                    "eviction_risk_score": eviction_risk_score,
                    "rent_paid": paid,
                    "rent_amount": rent_amount,
                    "rent_reserve_coin": rent_reserve_coin,
                    "reserve_used": reserve_used,
                    "landlord_balance": landlord_balance,
                })),
            );
        }

        let contract_ids = self.contracts_by_id.keys().cloned().collect::<Vec<_>>();
        for contract_id in contract_ids {
            let Some(mut state) = self.contracts_by_id.remove(&contract_id) else {
                continue;
            };
            if !state.contract.active || state.contract.next_payment_tick > tick {
                self.contracts_by_id.insert(contract_id, state);
                continue;
            }

            let payout_roll =
                self.deterministic_stream(tick, Phase::Commit, &contract_id, "contract_payout") % 100;
            let reliability = u64::from(state.contract.reliability_score);
            let settlement_id = state.contract.settlement_id.clone();
            let worker_id = state.contract.worker_id.clone();
            let employer_id = state.contract.employer_id.clone();
            let wage_amount = state.contract.wage_amount.max(1);
            let mut delayed = false;
            let mut breached = false;
            let mut terminated = false;
            let mut payout_reason = "reliability_roll";
            let liquidity_available = self
                .stock_by_settlement
                .get(&settlement_id)
                .map(|stock| stock.coin_reserve >= wage_amount)
                .unwrap_or(false);

            if payout_roll <= reliability && liquidity_available {
                if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                    stock.coin_reserve = (stock.coin_reserve - wage_amount).max(0);
                }
                if let Some(worker) = self.npc_economy_by_id.get_mut(&worker_id) {
                    worker.wallet += wage_amount;
                    worker.debt_balance = (worker.debt_balance - 1).max(0);
                    worker.apprenticeship_progress = (worker.apprenticeship_progress + 1).clamp(0, 200);
                    if matches!(state.contract.compensation_type, ContractCompensationType::Board | ContractCompensationType::Mixed) {
                        worker.food_reserve_days = (worker.food_reserve_days + 1).clamp(0, 14);
                    }
                }
                state.contract.reliability_score = state.contract.reliability_score.saturating_add(1).min(95);
            } else {
                delayed = true;
                if !liquidity_available {
                    payout_reason = "liquidity_shortfall";
                }
                if let Some(worker) = self.npc_economy_by_id.get_mut(&worker_id) {
                    worker.debt_balance += 1;
                }
                state.contract.reliability_score = state
                    .contract
                    .reliability_score
                    .saturating_sub(if liquidity_available { 5 } else { 3 });
                if state.contract.reliability_score < 30 {
                    breached = true;
                    state.contract.breached = true;
                }
                if state.contract.reliability_score < 20 {
                    terminated = true;
                    state.contract.active = false;
                    if let Some(worker) = self.npc_economy_by_id.get_mut(&worker_id) {
                        worker.employer_contract_id = None;
                    }
                }
            }
            state.contract.next_payment_tick += cadence_interval_ticks(state.contract.cadence);
            let reliability_score = state.contract.reliability_score;
            let stored_contract_id = state.contract.contract_id.clone();
            let compensation_type = state.contract.compensation_type;
            self.contracts_by_id.insert(contract_id.clone(), state);

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                if delayed {
                    EventType::WageDelayed
                } else {
                    EventType::WagePaid
                },
                settlement_id.clone(),
                vec![ActorRef {
                    actor_id: worker_id.clone(),
                    actor_kind: "npc".to_string(),
                }],
                vec![ActorRef {
                    actor_id: employer_id.clone(),
                    actor_kind: "employer".to_string(),
                }],
                vec!["labor".to_string(), "wages".to_string()],
                Some(json!({
                    "contract_id": stored_contract_id,
                    "employer_id": employer_id,
                    "worker_id": worker_id,
                    "wage_amount": wage_amount,
                    "compensation_type": compensation_type,
                    "reliability_score": reliability_score,
                    "payout_status": if delayed { "delayed" } else { "paid" },
                    "payout_reason": payout_reason,
                    "liquidity_available": liquidity_available,
                })),
            );
            if breached {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::ContractBreached,
                    settlement_id.clone(),
                    Vec::new(),
                    Vec::new(),
                    vec!["labor".to_string(), "breach".to_string()],
                    Some(json!({
                        "contract_id": contract_id.clone(),
                        "reliability_score": reliability_score,
                    })),
                );
            }
            if terminated {
                if let Some(market) = self.labor_market_by_settlement.get_mut(&settlement_id) {
                    market.open_roles = market.open_roles.saturating_add(1);
                    market.underemployment_index = (market.underemployment_index + 2).clamp(0, 100);
                }
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::EmploymentTerminated,
                    settlement_id,
                    Vec::new(),
                    Vec::new(),
                    vec!["labor".to_string(), "termination".to_string()],
                    Some(json!({
                        "contract_id": contract_id.clone(),
                    })),
                );
            }
        }

        let unemployed = self
            .npcs
            .iter()
            .filter(|npc| {
                self.npc_economy_by_id
                    .get(&npc.npc_id)
                    .and_then(|state| state.employer_contract_id.as_ref())
                    .is_none()
            })
            .map(|npc| (npc.npc_id.clone(), npc.location_id.clone()))
            .collect::<Vec<_>>();
        for (idx, (npc_id, location_id)) in unemployed.into_iter().enumerate() {
            let roll = self.deterministic_stream(tick, Phase::IntentUpdate, &npc_id, "job_seek") % 4;
            if roll != 0 {
                continue;
            }
            let market_update = if let Some(market) =
                self.labor_market_by_settlement.get_mut(&location_id)
            {
                if market.open_roles == 0 {
                    None
                } else {
                    let open_roles_before = market.open_roles;
                    let wage = market.wage_band_low.max(1);
                    market.open_roles = market.open_roles.saturating_sub(1);
                    market.underemployment_index = (market.underemployment_index - 1).max(0);
                    Some((open_roles_before, wage))
                }
            } else {
                None
            };
            let Some((open_roles_before, wage)) = market_update else {
                continue;
            };

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::JobSought,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: npc_id.clone(),
                    actor_kind: "npc".to_string(),
                }],
                Vec::new(),
                vec!["labor".to_string(), "job_seek".to_string()],
                Some(json!({
                    "npc_id": npc_id,
                    "open_roles": open_roles_before,
                })),
            );

            let contract_id = format!("contract:{}:{tick}:{idx}", location_id.replace(':', "_"));
            let cadence_roll =
                self.deterministic_stream(tick, Phase::IntentUpdate, &npc_id, "contract_cadence")
                    % 10;
            let cadence = if cadence_roll <= 5 {
                ContractCadence::Daily
            } else if cadence_roll <= 8 {
                ContractCadence::Weekly
            } else {
                ContractCadence::Monthly
            };
            let wage = match cadence {
                ContractCadence::Daily => wage.max(1),
                ContractCadence::Weekly => (wage * 6).max(8),
                ContractCadence::Monthly => (wage * 24).max(28),
            };
            let contract = EmploymentContractRecord {
                contract_id: contract_id.clone(),
                employer_id: format!("employer:{}", location_id),
                worker_id: npc_id.clone(),
                settlement_id: location_id.clone(),
                compensation_type: ContractCompensationType::Mixed,
                cadence,
                wage_amount: wage,
                reliability_score: 72,
                active: true,
                breached: false,
                next_payment_tick: tick + cadence_interval_ticks(cadence),
            };
            self.contracts_by_id.insert(contract_id.clone(), ContractState { contract });
            if let Some(economy) = self.npc_economy_by_id.get_mut(&npc_id) {
                economy.employer_contract_id = Some(contract_id.clone());
            }

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::ContractSigned,
                location_id,
                vec![ActorRef {
                    actor_id: npc_id,
                    actor_kind: "npc".to_string(),
                }],
                vec![ActorRef {
                    actor_id: contract_id,
                    actor_kind: "contract".to_string(),
                }],
                vec!["labor".to_string(), "contract".to_string()],
                None,
            );
        }
    }

    fn progress_production_and_supply_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let node_ids = self.production_nodes_by_id.keys().cloned().collect::<Vec<_>>();
        for node_id in node_ids {
            let Some(mut node) = self.production_nodes_by_id.remove(&node_id) else {
                continue;
            };
            let location_id = node.settlement_id.clone();
            let mut emitted_start = false;
            let mut emitted_completed = false;
            let mut emitted_spoilage = None;

            if tick % 12 == 0 {
                node.input_backlog += 1;
                emitted_start = true;
            }
            if node.input_backlog > 0 && tick % 6 == 0 {
                node.input_backlog -= 1;
                node.output_backlog += 1;
            }
            if node.output_backlog > 0 && tick % 8 == 0 {
                let completed_output = node.output_backlog;
                if let Some(stock) = self.stock_by_settlement.get_mut(&location_id) {
                    match node.node_kind.as_str() {
                        "farm" => stock.staples += completed_output * 2,
                        "workshop" => stock.craft_inputs += completed_output + 1,
                        _ => stock.fuel += completed_output + 1,
                    }
                }
                node.output_backlog = 0;
                emitted_completed = true;
            }
            node.spoilage_timer -= 1;
            if node.spoilage_timer <= 0 {
                let mut staples_after = None;
                if let Some(stock) = self.stock_by_settlement.get_mut(&location_id) {
                    if stock.staples > 0 {
                        stock.staples -= 1;
                        staples_after = Some(stock.staples);
                    }
                }
                node.spoilage_timer = 18;
                emitted_spoilage = staples_after;
            }

            let input_backlog = node.input_backlog;
            self.production_nodes_by_id.insert(node_id.clone(), node.clone());

            if emitted_start {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::ProductionStarted,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: node.node_id.clone(),
                        actor_kind: "production_node".to_string(),
                    }],
                    Vec::new(),
                    vec!["production".to_string(), node.node_kind.clone()],
                    Some(json!({
                        "node_id": node.node_id.clone(),
                        "input_backlog": input_backlog,
                    })),
                );
            }
            if emitted_completed {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::ProductionCompleted,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: node.node_id.clone(),
                        actor_kind: "production_node".to_string(),
                    }],
                    Vec::new(),
                    vec!["production".to_string(), "output".to_string()],
                    Some(json!({
                        "node_id": node.node_id.clone(),
                    })),
                );
            }
            if let Some(staples_after) = emitted_spoilage {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::SpoilageOccurred,
                    location_id,
                    vec![ActorRef {
                        actor_id: node.node_id.clone(),
                        actor_kind: "production_node".to_string(),
                    }],
                    Vec::new(),
                    vec!["production".to_string(), "spoilage".to_string()],
                    Some(json!({
                        "staples_after": staples_after,
                    })),
                );
            }
        }

        let settlements = self.stock_by_settlement.keys().cloned().collect::<Vec<_>>();
        for settlement_id in settlements {
            if tick % 24 == 0 {
                let route_support = self
                    .routes_by_id
                    .values()
                    .filter(|route| {
                        (route.origin_settlement_id == settlement_id
                            || route.destination_settlement_id == settlement_id)
                            && route.weather_window_open
                            && route.hazard_score <= 35
                    })
                    .count() as i64;
                let mut imported_staples = 0_i64;
                let mut imported_fuel = 0_i64;
                if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                    if stock.staples <= 4 && route_support > 0 {
                        imported_staples = (1 + route_support).clamp(1, 4);
                        stock.staples += imported_staples;
                    }
                    if stock.fuel <= 2 && route_support > 0 {
                        imported_fuel = route_support.clamp(1, 3);
                        stock.fuel += imported_fuel;
                    }
                }
                if imported_staples > 0 || imported_fuel > 0 {
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::StockRecovered,
                        settlement_id.clone(),
                        Vec::new(),
                        Vec::new(),
                        vec!["supply".to_string(), "import".to_string()],
                        Some(json!({
                            "settlement_id": settlement_id,
                            "imported_staples": imported_staples,
                            "imported_fuel": imported_fuel,
                            "route_support": route_support,
                        })),
                    );
                }
            }
            let Some((staples, price_pressure, shortage, recovered)) = ({
                if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                    stock.local_price_pressure = (14 - stock.staples).clamp(-10, 30);
                    let staples = stock.staples;
                    let price_pressure = stock.local_price_pressure;
                    Some((staples, price_pressure, staples <= 4, staples >= 10 && tick % 24 == 0))
                } else {
                    None
                }
            }) else {
                continue;
            };
            if shortage {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::StockShortage,
                    settlement_id.clone(),
                    Vec::new(),
                    Vec::new(),
                    vec!["supply".to_string(), "shortage".to_string()],
                    Some(json!({
                        "settlement_id": settlement_id,
                        "staples": staples,
                        "price_pressure": price_pressure,
                    })),
                );
            } else if recovered {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::StockRecovered,
                    settlement_id.clone(),
                    Vec::new(),
                    Vec::new(),
                    vec!["supply".to_string(), "recovery".to_string()],
                    Some(json!({
                        "settlement_id": settlement_id,
                        "staples": staples,
                    })),
                );
            }
        }
    }

    fn progress_relationship_and_belief_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let edge_keys = self.relationship_edges.keys().cloned().collect::<Vec<_>>();
        if !edge_keys.is_empty() {
            let updates = edge_keys.len().min(2);
            let start = tick as usize % edge_keys.len();
            for offset in 0..updates {
                let key = edge_keys[(start + offset) % edge_keys.len()].clone();
                let Some(mut edge) = self.relationship_edges.remove(&key) else {
                continue;
            };
                let household_stress = self
                    .npc_economy_by_id
                    .get(&edge.source_npc_id)
                    .map(|state| {
                        state.debt_balance
                            + (2 - state.food_reserve_days).max(0)
                            + if matches!(state.shelter_status, ShelterStatus::Unsheltered) {
                                2
                            } else {
                                0
                            }
                    })
                    .unwrap_or_default();
                let social_pressure = (self.pressure_index / 220).clamp(-3, 6);
                let trust_buffer =
                    ((edge.obligation + edge.attachment) / 24) - (edge.grievance / 12);
                let compatibility_bias = edge.compatibility_score / 8;
                let trait_support = self
                    .npc_traits_by_id
                    .get(&edge.source_npc_id)
                    .map(|profile| (profile.empathy + profile.sociability + profile.dutifulness - 150) / 44)
                    .unwrap_or(0);
                let shared_group = self.groups_by_id.values().any(|group| {
                    group
                        .member_npc_ids
                        .iter()
                        .any(|member| member == &edge.source_npc_id)
                        && group
                            .member_npc_ids
                            .iter()
                            .any(|member| member == &edge.target_npc_id)
                }) as i64;
                let shared_hardship = (self
                    .npc_economy_by_id
                    .get(&edge.source_npc_id)
                    .map(|state| state.food_reserve_days <= 1 || state.debt_balance >= 4)
                    .unwrap_or(false)
                    && self
                        .npc_economy_by_id
                        .get(&edge.target_npc_id)
                        .map(|state| state.food_reserve_days <= 1 || state.debt_balance >= 4)
                        .unwrap_or(false)) as i64;
                let source_action = self
                    .event_log
                    .iter()
                    .rev()
                    .take_while(|event| event.tick == tick)
                    .find(|event| {
                        event.event_type == EventType::NpcActionCommitted
                            && event
                                .actors
                                .first()
                                .map(|actor| actor.actor_id.as_str() == edge.source_npc_id.as_str())
                                .unwrap_or(false)
                    })
                    .and_then(|event| {
                        event.details
                            .as_ref()
                            .and_then(|details| details.get("chosen_action"))
                            .and_then(Value::as_str)
                    });
                let cooperative_bias = matches!(
                    source_action,
                    Some(
                        "share_meal"
                            | "mediate_dispute"
                            | "assist_neighbor"
                            | "work_for_food"
                            | "form_mutual_aid_group"
                    )
                ) as i64
                    * 2;
                let antisocial_bias = matches!(
                    source_action,
                    Some("steal_supplies" | "spread_accusation" | "avoid_patrols" | "fence_goods")
                ) as i64
                    * 2;
                let volatility = (self
                    .deterministic_stream(
                        tick,
                        Phase::MemoryBelief,
                        &edge.source_npc_id,
                        &edge.target_npc_id,
                    )
                    % 5) as i64
                    - 2;
                let trust_saturation = (edge.trust.max(0) / 20).clamp(0, 5);
                let baseline_drift = if tick % 24 == 0 {
                    if edge.trust < 0 {
                        1
                    } else if edge.trust > 50 {
                        -1
                    } else {
                        0
                    }
                } else {
                    0
                };
                let net_pressure =
                    social_pressure + household_stress - trust_buffer + volatility + antisocial_bias
                        - cooperative_bias
                        - trait_support
                        - compatibility_bias
                        - shared_group
                        - shared_hardship
                        + trust_saturation
                        + baseline_drift;
                let delta = if net_pressure >= 4 {
                    -1
                } else if net_pressure <= -3
                    && edge.trust < (58 + compatibility_bias.clamp(-4, 4))
                {
                    1
                } else if cooperative_bias >= 2 && volatility <= -1 && edge.trust < 60 {
                    1
                } else if antisocial_bias >= 2 && volatility >= 0 {
                    -1
                } else if tick % 24 == 0 && edge.trust > 55 {
                    -1
                } else if tick % 18 == 0 && edge.trust < 0 {
                    1
                } else {
                    0
                };

                if delta != 0 {
                    let trust_step = if net_pressure.abs() >= 6 { 2 } else { 1 };
                    edge.trust = (edge.trust + delta * trust_step).clamp(-100, 100);
                    edge.grievance =
                        (edge.grievance + if delta < 0 { 2 } else { -2 }).clamp(0, 100);
                    edge.obligation =
                        (edge.obligation + if delta > 0 { 1 } else { -2 }).clamp(0, 100);
                    edge.attachment =
                        (edge.attachment + if delta > 0 { 1 } else { -2 }).clamp(0, 100);
                    edge.respect = (edge.respect + if delta > 0 { 1 } else { -1 }).clamp(0, 100);
                    edge.recent_interaction_tick = tick;

                    let location = self
                        .npc_location_for(&edge.source_npc_id)
                        .unwrap_or_else(|| "region:crownvale".to_string());
                    let source_id = edge.source_npc_id.clone();
                    let target_id = edge.target_npc_id.clone();
                    let trust = edge.trust;
                    let obligation = edge.obligation;
                    let grievance = edge.grievance;

                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::TrustChanged,
                        location.clone(),
                        vec![ActorRef {
                            actor_id: source_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        vec![ActorRef {
                            actor_id: target_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        vec!["social".to_string(), "trust".to_string()],
                        Some(json!({
                            "trust": trust,
                            "obligation": obligation,
                            "grievance": grievance,
                            "net_pressure": net_pressure,
                            "trust_level": format!("{:?}", trust_level_from_score(trust)),
                        })),
                    );
                    if delta > 0 && obligation % 8 == 0 {
                        self.push_runtime_event(
                            tick,
                            sequence_in_tick,
                            causal_chain,
                            system_event_id,
                            EventType::ObligationCreated,
                            location.clone(),
                            vec![ActorRef {
                                actor_id: source_id.clone(),
                                actor_kind: "npc".to_string(),
                            }],
                            vec![ActorRef {
                                actor_id: target_id.clone(),
                                actor_kind: "npc".to_string(),
                            }],
                            vec!["social".to_string(), "obligation".to_string()],
                            Some(json!({
                                "obligation": obligation,
                            })),
                        );
                    }
                    if delta < 0 && grievance % 8 == 0 {
                        self.push_runtime_event(
                            tick,
                            sequence_in_tick,
                            causal_chain,
                            system_event_id,
                            EventType::GrievanceRecorded,
                            location,
                            vec![ActorRef {
                                actor_id: source_id,
                                actor_kind: "npc".to_string(),
                            }],
                            vec![ActorRef {
                                actor_id: target_id,
                                actor_kind: "npc".to_string(),
                            }],
                            vec!["social".to_string(), "grievance".to_string()],
                            Some(json!({
                                "grievance": grievance,
                            })),
                        );
                    }
                } else if tick % 24 == 0 {
                    if edge.trust > 20 {
                        edge.trust -= 1;
                    }
                    edge.obligation = (edge.obligation - 1).max(0);
                    edge.attachment = (edge.attachment - 1).max(0);
                    edge.grievance = (edge.grievance - 1).max(0);
                    let location = self
                        .npc_location_for(&edge.source_npc_id)
                        .unwrap_or_else(|| "region:crownvale".to_string());
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::TrustChanged,
                        location,
                        vec![ActorRef {
                            actor_id: edge.source_npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        vec![ActorRef {
                            actor_id: edge.target_npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        vec!["social".to_string(), "trust".to_string(), "stagnant".to_string()],
                        Some(json!({
                            "trust": edge.trust,
                            "obligation": edge.obligation,
                            "grievance": edge.grievance,
                            "net_pressure": net_pressure,
                            "stagnant": true,
                            "trust_level": format!("{:?}", trust_level_from_score(edge.trust)),
                        })),
                    );
                }
                self.relationship_edges.insert(key, edge);
            }
        }

        let npc_refs = self.npcs.clone();
        for npc in &npc_refs {
            let rumor_heat = self
                .scenario_state
                .rumor_heat_by_location
                .get(&npc.location_id)
                .copied()
                .unwrap_or(0);
            if rumor_heat <= 0 && tick % 24 != 0 {
                continue;
            }

            let claim_id = format!("claim:{}:{tick}", npc.npc_id);
            let baseline_confidence = (0.45 + (rumor_heat as f32 * 0.04)).clamp(0.2, 0.9);
            let mut event_type = EventType::BeliefFormed;
            let mut forgotten_claim_id = None;
            let institution = self
                .institutions_by_settlement
                .get(&npc.location_id)
                .cloned();
            let institution_correction = institution
                .as_ref()
                .map(|entry| (100 - entry.bias_level - entry.corruption_level).clamp(0, 80) as f32 / 2000.0)
                .unwrap_or(0.0);
            let (emitted_confidence, emitted_distortion, emitted_willingness) = {
                let claims = self.beliefs_by_npc.entry(npc.npc_id.clone()).or_default();
                let emitted = if let Some(existing) = claims
                    .iter_mut()
                    .find(|claim| claim.claim_text == "market rumor")
                {
                    existing.confidence = (existing.confidence
                        + if rumor_heat > 2 { 0.02 } else { -0.015 }
                        - institution_correction)
                        .clamp(0.1, 0.95);
                    existing.distortion_score = (existing.distortion_score
                        + if rumor_heat > 2 { 0.02 } else { -0.025 }
                        - institution_correction)
                        .clamp(0.0, 1.0);
                    existing.willingness_to_share = (existing.willingness_to_share
                        + if rumor_heat > 2 { 0.025 } else { -0.02 })
                        .clamp(0.0, 1.0);
                    event_type = EventType::BeliefUpdated;
                    (
                        existing.confidence,
                        existing.distortion_score,
                        existing.willingness_to_share,
                    )
                } else {
                    let distortion = if rumor_heat > 2 { 0.35 } else { 0.18 };
                    let willingness = if rumor_heat > 2 { 0.72 } else { 0.46 };
                    claims.push(BeliefClaimRuntime {
                        claim_id: claim_id.clone(),
                        npc_id: npc.npc_id.clone(),
                        settlement_id: npc.location_id.clone(),
                        confidence: baseline_confidence,
                        source: if rumor_heat > 2 {
                            BeliefSource::TrustedRumor
                        } else {
                            BeliefSource::Hearsay
                        },
                        distortion_score: distortion,
                        willingness_to_share: willingness,
                        truth_link: None,
                        claim_text: "market rumor".to_string(),
                    });
                    (baseline_confidence, distortion, willingness)
                };
                if tick % 48 == 0 && claims.len() > 2 {
                    forgotten_claim_id = Some(claims.remove(0).claim_id);
                }
                emitted
            };

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                event_type,
                npc.location_id.clone(),
                vec![ActorRef {
                    actor_id: npc.npc_id.clone(),
                    actor_kind: "npc".to_string(),
                }],
                Vec::new(),
                vec!["belief".to_string()],
                Some(json!({
                    "claim_id": claim_id,
                    "rumor_heat": rumor_heat,
                    "confidence": emitted_confidence,
                    "distortion_score": emitted_distortion,
                    "willingness_to_share": emitted_willingness,
                })),
            );

            if rumor_heat > 2 {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::RumorMutated,
                    npc.location_id.clone(),
                    vec![ActorRef {
                        actor_id: npc.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    Vec::new(),
                    vec!["belief".to_string(), "rumor".to_string()],
                    Some(json!({
                        "claim_id": claim_id,
                    })),
                );
            }
            if self
                .institutions_by_settlement
                .get(&npc.location_id)
                .map(|institution| institution.bias_level >= 60)
                .unwrap_or(false)
                && rumor_heat > 1
            {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::BeliefDisputed,
                    npc.location_id.clone(),
                    vec![ActorRef {
                        actor_id: npc.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    Vec::new(),
                    vec!["belief".to_string(), "disputed".to_string()],
                    Some(json!({
                        "claim_id": claim_id,
                    })),
                );
            }

            if let Some(forgotten) = forgotten_claim_id {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::BeliefForgotten,
                    npc.location_id.clone(),
                    vec![ActorRef {
                        actor_id: npc.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    Vec::new(),
                    vec!["belief".to_string(), "forgetting".to_string()],
                    Some(json!({
                        "claim_id": forgotten,
                    })),
                );
            }
        }
    }

    fn progress_institution_group_and_route_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let settlements = self
            .institutions_by_settlement
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for settlement_id in settlements {
            let law_cases = self
                .law_case_load_by_settlement
                .get(&settlement_id)
                .copied()
                .unwrap_or(0);
            let thefts_recent = self
                .event_log
                .iter()
                .rev()
                .take(48)
                .filter(|event| {
                    event.location_id == settlement_id && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let Some(mut institution) = self.institutions_by_settlement.remove(&settlement_id) else {
                continue;
            };
            let law_pressure = law_cases.clamp(0, 16);
            let theft_pressure = thefts_recent.clamp(0, 16);
            let social_fragment = (self.social_cohesion < -20) as i64;
            let target_capacity =
                (62 - law_pressure * 2 - theft_pressure / 3 + (self.social_cohesion > 20) as i64 * 4)
                    .clamp(35, 90);
            let target_corruption = (18 + theft_pressure + social_fragment * 6).clamp(5, 85);
            let target_bias = (14 + social_fragment * 8 + law_pressure / 2).clamp(5, 80);
            let target_latency =
                (3 + law_pressure / 2 + target_corruption / 25 - target_capacity / 40).clamp(1, 18);

            institution.enforcement_capacity =
                step_toward(institution.enforcement_capacity, target_capacity, 2);
            institution.corruption_level =
                step_toward(institution.corruption_level, target_corruption, 2);
            institution.bias_level = step_toward(institution.bias_level, target_bias, 2);
            institution.response_latency_ticks =
                step_toward(institution.response_latency_ticks, target_latency, 1);
            let corruption_level = institution.corruption_level;
            let enforcement_capacity = institution.enforcement_capacity;
            let bias_level = institution.bias_level;
            let response_latency_ticks = institution.response_latency_ticks;
            self.institutions_by_settlement
                .insert(settlement_id.clone(), institution);

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::InstitutionProfileUpdated,
                settlement_id.clone(),
                vec![ActorRef {
                    actor_id: format!("institution:{settlement_id}"),
                    actor_kind: "institution".to_string(),
                }],
                Vec::new(),
                vec!["institution".to_string()],
                Some(json!({
                    "enforcement_capacity": enforcement_capacity,
                    "corruption_level": corruption_level,
                    "bias_level": bias_level,
                    "response_latency_ticks": response_latency_ticks,
                    "law_cases": law_cases,
                    "thefts_recent": thefts_recent,
                })),
            );
            if corruption_level >= 75 && tick % 24 == 0 {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::InstitutionalErrorRecorded,
                    settlement_id.clone(),
                    vec![ActorRef {
                        actor_id: format!("institution:{settlement_id}"),
                        actor_kind: "institution".to_string(),
                    }],
                    Vec::new(),
                    vec!["institution".to_string(), "error".to_string()],
                    Some(json!({
                        "cause": "corruption_overload",
                    })),
                );
            }
            if law_cases == 0 && thefts_recent <= 1 && tick % 36 == 0 {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::InstitutionCaseResolved,
                    settlement_id,
                    Vec::new(),
                    Vec::new(),
                    vec!["institution".to_string(), "resolved".to_string()],
                    Some(json!({
                        "law_cases": law_cases,
                        "thefts_recent": thefts_recent,
                    })),
                );
            }
        }

        if tick % 24 == 0 {
            let mut settlements = self
                .npcs
                .iter()
                .map(|npc| npc.location_id.clone())
                .collect::<Vec<_>>();
            settlements.sort();
            settlements.dedup();
            for settlement_id in settlements {
                if self
                    .groups_by_id
                    .values()
                    .any(|group| group.settlement_id == settlement_id)
                {
                    continue;
                }
                let candidate_npcs = self
                    .npcs
                    .iter()
                    .filter(|npc| {
                        npc.location_id == settlement_id
                            || self.routes_by_id.values().any(|route| {
                                ((route.origin_settlement_id == settlement_id
                                    && route.destination_settlement_id == npc.location_id)
                                    || (route.destination_settlement_id == settlement_id
                                        && route.origin_settlement_id == npc.location_id))
                                    && route.weather_window_open
                                    && route.hazard_score <= 45
                            })
                    })
                    .map(|npc| npc.npc_id.clone())
                    .collect::<Vec<_>>();
                if candidate_npcs.len() < 2 {
                    continue;
                }
                let mut supportive_pairs = 0_i64;
                for left in &candidate_npcs {
                    for right in &candidate_npcs {
                        if left == right {
                            continue;
                        }
                        if let Some(edge) = self
                            .relationship_edges
                            .get(&(left.clone(), right.clone()))
                        {
                            if edge.trust >= 4 && edge.grievance <= 45 {
                                supportive_pairs += 1;
                            }
                        }
                    }
                }
                let group_interest = self
                    .event_log
                    .iter()
                    .rev()
                    .take(96)
                    .filter(|event| {
                        event.location_id == settlement_id
                            && event.event_type == EventType::NpcActionCommitted
                            && matches!(
                                event.details
                                    .as_ref()
                                    .and_then(|details| details.get("chosen_action"))
                                    .and_then(Value::as_str),
                                Some("form_mutual_aid_group" | "share_meal" | "mediate_dispute")
                            )
                    })
                    .count() as i64;
                if supportive_pairs == 0 && group_interest == 0 {
                    continue;
                }
                let shortage = self
                    .stock_by_settlement
                    .get(&settlement_id)
                    .map(|stock| (stock.staples <= 6) as i64)
                    .unwrap_or(0);
                let theft_pressure = self
                    .event_log
                    .iter()
                    .rev()
                    .take(72)
                    .filter(|event| {
                        event.location_id == settlement_id
                            && event.event_type == EventType::TheftCommitted
                    })
                    .count() as i64;
                let formation_score = supportive_pairs * 8
                    + group_interest * 6
                    + (self.social_cohesion.max(0) / 18)
                    + shortage * 5
                    - theft_pressure;
                let roll = (self.deterministic_stream(
                    tick,
                    Phase::Commit,
                    "group_formation",
                    &settlement_id,
                ) % 100) as i64;
                if formation_score + roll < 45 {
                    continue;
                }

                let Some(leader_npc_id) = candidate_npcs
                    .iter()
                    .max_by_key(|npc_id| {
                        let trust_out = self
                            .relationship_edges
                            .values()
                            .filter(|edge| edge.source_npc_id == **npc_id && edge.trust > 0)
                            .map(|edge| edge.trust)
                            .sum::<i64>();
                        let wallet = self
                            .npc_economy_by_id
                            .get(npc_id.as_str())
                            .map(|economy| economy.wallet)
                            .unwrap_or(0);
                        trust_out + wallet
                    })
                    .cloned()
                else {
                    continue;
                };

                let mut members = vec![leader_npc_id.clone()];
                for npc_id in &candidate_npcs {
                    if npc_id == &leader_npc_id {
                        continue;
                    }
                    if let Some(edge) = self
                        .relationship_edges
                        .get(&(leader_npc_id.clone(), npc_id.clone()))
                    {
                        if edge.trust >= -5 || edge.obligation >= 12 {
                            members.push(npc_id.clone());
                        }
                    }
                }
                if members.len() < 2 {
                    continue;
                }
                let shared_cause = if shortage > 0 {
                    "mutual_aid"
                } else if theft_pressure >= 2 {
                    "mutual_security"
                } else {
                    "craft_guild"
                };
                let group_id = format!(
                    "group:{}:{}:{}",
                    shared_cause,
                    settlement_id.replace(':', "_"),
                    tick
                );
                self.groups_by_id.insert(
                    group_id.clone(),
                    GroupRuntimeState {
                        group_id: group_id.clone(),
                        settlement_id: settlement_id.clone(),
                        leader_npc_id,
                        member_npc_ids: members.clone(),
                        norm_tags: vec![
                            format!("cause:{shared_cause}"),
                            "protect_members".to_string(),
                        ],
                        cohesion_score: (24 + supportive_pairs * 3).clamp(18, 55),
                        formed_tick: tick,
                        inertia_score: (20 + group_interest).clamp(20, 60),
                        shared_cause: shared_cause.to_string(),
                        infiltration_pressure: 2,
                    },
                );
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::GroupFormed,
                    settlement_id,
                    vec![ActorRef {
                        actor_id: group_id,
                        actor_kind: "group".to_string(),
                    }],
                    Vec::new(),
                    vec!["group".to_string(), "formation".to_string()],
                    Some(json!({
                        "shared_cause": shared_cause,
                        "member_count": members.len(),
                        "supportive_pairs": supportive_pairs,
                        "group_interest": group_interest,
                    })),
                );
            }
        }

        let group_ids = self.groups_by_id.keys().cloned().collect::<Vec<_>>();
        for group_id in group_ids {
            let Some(mut group) = self.groups_by_id.remove(&group_id) else {
                continue;
            };
            let group_age = tick.saturating_sub(group.formed_tick);
            let settlement_id = group.settlement_id.clone();
            let theft_pressure = self
                .event_log
                .iter()
                .rev()
                .take(48)
                .filter(|event| {
                    event.location_id == settlement_id && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let shortage = self
                .stock_by_settlement
                .get(&settlement_id)
                .map(|stock| (stock.staples <= 5) as i64)
                .unwrap_or(0);
            let hostile_to_leader = self
                .relationship_edges
                .values()
                .filter(|edge| edge.target_npc_id == group.leader_npc_id && edge.trust <= -25)
                .count() as i64;
            let cause_alignment = match group.shared_cause.as_str() {
                "mutual_aid" => {
                    if shortage > 0 {
                        2
                    } else {
                        -1
                    }
                }
                "mutual_security" => {
                    if theft_pressure > 0 {
                        1
                    } else {
                        -1
                    }
                }
                _ => {
                    if self.social_cohesion >= 0 {
                        1
                    } else {
                        -1
                    }
                }
            };
            let support_signal = self
                .event_log
                .iter()
                .rev()
                .take(48)
                .filter(|event| {
                    event.location_id == settlement_id
                        && event.event_type == EventType::NpcActionCommitted
                        && matches!(
                            event.details
                                .as_ref()
                                .and_then(|details| details.get("chosen_action"))
                                .and_then(Value::as_str),
                            Some(
                                "share_meal"
                                    | "assist_neighbor"
                                    | "mediate_dispute"
                                    | "form_mutual_aid_group"
                                    | "defend_patron"
                            )
                        )
                })
                .count() as i64;
            let stagnation_penalty = if support_signal == 0 { 2 } else { 0 };
            group.infiltration_pressure = (group.infiltration_pressure
                + hostile_to_leader
                + theft_pressure / 4
                - cause_alignment.max(0)
                + if support_signal <= 1 { 1 } else { 0 })
                .clamp(0, 100);
            let resilience = if group_age < 120 { 1 } else { 0 };
            let cohesion_delta = cause_alignment
                + if self.social_cohesion > 15 {
                    1
                } else if self.social_cohesion < -35 {
                    -1
                } else {
                    0
                }
                + resilience
                + (support_signal / 4)
                - (group.infiltration_pressure / 28)
                - stagnation_penalty
                - if group.cohesion_score > 70 && support_signal < 2 {
                    2
                } else {
                    0
                };
            group.cohesion_score = (group.cohesion_score + cohesion_delta).clamp(-60, 85);
            group.inertia_score =
                (group.inertia_score + cause_alignment + (support_signal / 6) - (group.infiltration_pressure / 26) - stagnation_penalty)
                    .clamp(-60, 80);
            let cohesion = group.cohesion_score;
            let member_count = group.member_npc_ids.len();
            let split = group_age >= 144
                && member_count >= 3
                && cohesion <= -30
                && group.infiltration_pressure >= 55
                && tick % 24 == 0;
            let dissolve = group_age >= 192
                && cohesion <= -45
                && group.inertia_score <= -16
                && (member_count <= 1 || cohesion <= -50)
                && (group.infiltration_pressure >= 70 || cause_alignment < 0)
                && tick % 24 == 0;
            if !dissolve {
                self.groups_by_id.insert(group_id.clone(), group.clone());
            }
            if tick % 12 == 0 || split || dissolve {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::GroupMembershipChanged,
                    settlement_id.clone(),
                    vec![ActorRef {
                        actor_id: group.group_id.clone(),
                        actor_kind: "group".to_string(),
                    }],
                    Vec::new(),
                    vec!["group".to_string(), "membership".to_string()],
                    Some(json!({
                        "group_age": group_age,
                        "shared_cause": group.shared_cause,
                        "infiltration_pressure": group.infiltration_pressure,
                        "cohesion_score": cohesion,
                        "member_count": member_count,
                    })),
                );
            }
            if split {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::GroupSplit,
                    settlement_id.clone(),
                    vec![ActorRef {
                        actor_id: group.group_id.clone(),
                        actor_kind: "group".to_string(),
                    }],
                    Vec::new(),
                    vec!["group".to_string(), "split".to_string()],
                    Some(json!({
                        "group_age": group_age,
                        "infiltration_pressure": group.infiltration_pressure,
                    })),
                );
            }
            if dissolve {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::GroupDissolved,
                    settlement_id,
                    vec![ActorRef {
                        actor_id: group_id,
                        actor_kind: "group".to_string(),
                    }],
                    Vec::new(),
                    vec!["group".to_string(), "dissolve".to_string()],
                    Some(json!({
                        "group_age": group_age,
                        "infiltration_pressure": group.infiltration_pressure,
                        "shared_cause": group.shared_cause,
                    })),
                );
            }
        }

        let route_ids = self.routes_by_id.keys().cloned().collect::<Vec<_>>();
        for route_id in route_ids {
            let Some(mut route) = self.routes_by_id.remove(&route_id) else {
                continue;
            };
            let previous_window = route.weather_window_open;
            let weather_roll =
                self.deterministic_stream(tick, Phase::Perception, &route.route_id, "weather");
            route.weather_window_open = weather_roll % 5 != 0;
            let local_thefts = self
                .event_log
                .iter()
                .rev()
                .take(48)
                .filter(|event| {
                    (event.location_id == route.origin_settlement_id
                        || event.location_id == route.destination_settlement_id)
                        && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let systemic_stress = (self.pressure_index / 320).clamp(0, 3);
            route.hazard_score = (route.hazard_score
                + if route.weather_window_open { -2 } else { 2 }
                + systemic_stress
                + (local_thefts / 6)
                - 1)
                .clamp(0, 100);
            route.travel_time_ticks =
                (18_i64 + route.hazard_score / 9 + if route.weather_window_open { 0 } else { 4 })
                    .max(8) as u64;
            let origin = route.origin_settlement_id.clone();
            let hazard = route.hazard_score;
            let travel = route.travel_time_ticks;
            let weather_window_open = route.weather_window_open;
            let actor_route_id = route.route_id.clone();
            self.routes_by_id.insert(route_id, route);

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::RouteRiskUpdated,
                origin.clone(),
                vec![ActorRef {
                    actor_id: actor_route_id.clone(),
                    actor_kind: "route".to_string(),
                }],
                Vec::new(),
                vec!["mobility".to_string(), "risk".to_string()],
                Some(json!({
                    "hazard_score": hazard,
                    "travel_time_ticks": travel,
                })),
            );
            if previous_window != weather_window_open {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::TravelWindowShifted,
                    origin,
                    vec![ActorRef {
                        actor_id: actor_route_id,
                        actor_kind: "route".to_string(),
                    }],
                    Vec::new(),
                    vec!["mobility".to_string(), "weather".to_string()],
                    Some(json!({
                        "weather_window_open": weather_window_open,
                    })),
                );
            }
        }
    }

    fn emit_narrative_why_summary(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let action_events = self
            .event_log
            .iter()
            .filter(|event| event.tick == tick && event.event_type == EventType::NpcActionCommitted)
            .cloned()
            .collect::<Vec<_>>();
        if action_events.is_empty() {
            return;
        }
        let focus_index = (self.deterministic_stream(tick, Phase::Commit, "narrative", "focus")
            % action_events.len() as u64) as usize;
        let action_event = action_events[focus_index].clone();

        let actor_id = action_event
            .actors
            .first()
            .map(|actor| actor.actor_id.clone())
            .unwrap_or_else(|| "npc_unknown".to_string());
        let reason = action_event
            .reason_packet_id
            .as_ref()
            .and_then(|reason_id| {
                self.reason_packet_log
                    .iter()
                    .find(|packet| packet.reason_packet_id == *reason_id)
            });
        let chosen_action = action_event
            .details
            .as_ref()
            .and_then(|details| details.get("chosen_action"))
            .and_then(Value::as_str)
            .unwrap_or("unknown_action");
        let motive_chain = reason
            .map(|packet| {
                let mut chain = if packet.why_chain.is_empty() {
                    packet.top_intents.clone()
                } else {
                    packet.why_chain.clone()
                };
                chain.retain(|step| {
                    step != "maintain_routine"
                        && step != "intent::maintain_routine"
                        && !step.starts_with("belief::season_cycle_tick_")
                });
                if chain.is_empty() {
                    if let Some(pressure) = packet.top_pressures.first() {
                        chain.push(format!("pressure::{pressure}"));
                    }
                    if let Some(intent) =
                        packet.top_intents.iter().find(|intent| intent.as_str() != "maintain_routine")
                    {
                        chain.push(format!("intent::{intent}"));
                    }
                }
                if chain.is_empty() {
                    chain.push(format!("verb::{chosen_action}"));
                }
                chain.truncate(3);
                chain
            })
            .unwrap_or_else(|| vec![format!("verb::{chosen_action}")]);
        let failed_alternatives = reason
            .map(|packet| {
                packet
                    .alternatives_considered
                    .iter()
                    .filter(|alternative| alternative.as_str() != packet.chosen_action.as_str())
                    .take(4)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let local_case_load = self
            .law_case_load_by_settlement
            .get(&action_event.location_id)
            .copied()
            .unwrap_or(0);
        let local_stock = self
            .stock_by_settlement
            .get(&action_event.location_id)
            .map(|stock| stock.staples)
            .unwrap_or(0);
        let social_consequence = if matches!(chosen_action, "steal_supplies" | "fence_goods") {
            "scarcity pressure tipped behavior into norm-breaking, raising future retaliation risk"
                .to_string()
        } else if matches!(chosen_action, "organize_watch" | "patrol_road")
            && local_case_load == 0
        {
            "high vigilance consumed social bandwidth without immediate threat signals".to_string()
        } else if matches!(chosen_action, "seek_shelter") && self.pressure_index < 140 {
            "precautionary shelter-seeking postponed livelihood recovery for another cycle".to_string()
        } else if matches!(chosen_action, "work_for_coin" | "work_for_food" | "tend_fields") {
            "livelihood maintenance reduced immediate household stress while keeping long-term obligations active"
                .to_string()
        } else if matches!(chosen_action, "form_mutual_aid_group" | "share_meal" | "mediate_dispute") {
            "cooperative behavior improved local support ties and reduced short-term volatility".to_string()
        } else if matches!(chosen_action, "spread_accusation" | "share_rumor") {
            "narrative competition intensified, shifting trust and credibility in uneven ways".to_string()
        } else if local_stock <= 4 {
            "resource scarcity amplified defensive behavior and narrowed acceptable choices".to_string()
        } else if self.social_cohesion < -20 {
            "outcome remained contested in fragmented local networks".to_string()
        } else {
            "local stress intensified after constrained choices".to_string()
        };
        let failed_alternatives = if failed_alternatives.is_empty() {
            narrative_default_alternatives(chosen_action)
        } else {
            failed_alternatives
        };
        let summary = NarrativeSummaryRuntime {
            summary_id: format!("why_{tick:06}_{:03}", *sequence_in_tick),
            tick,
            location_id: action_event.location_id.clone(),
            actor_id: actor_id.clone(),
            motive_chain: motive_chain.clone(),
            failed_alternatives: failed_alternatives.clone(),
            social_consequence: social_consequence.clone(),
        };
        self.narrative_summaries.push(summary.clone());
        if self.narrative_summaries.len() > 256 {
            self.narrative_summaries.remove(0);
        }

        self.push_runtime_event(
            tick,
            sequence_in_tick,
            causal_chain,
            system_event_id,
            EventType::NarrativeWhySummary,
            action_event.location_id,
            vec![ActorRef {
                actor_id,
                actor_kind: "npc".to_string(),
            }],
            Vec::new(),
            vec!["narrative".to_string(), "why_chain".to_string()],
            Some(json!({
                "summary_id": summary.summary_id,
                "chosen_action": chosen_action,
                "motive_chain": motive_chain,
                "failed_alternatives": failed_alternatives,
                "social_consequence": social_consequence,
                "pressure_index": self.pressure_index,
                "social_cohesion": self.social_cohesion,
            })),
        );
    }

    fn push_runtime_event(
        &mut self,
        tick: u64,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
        system_event_id: &str,
        event_type: EventType,
        location_id: String,
        actors: Vec<ActorRef>,
        targets: Vec<ActorRef>,
        tags: Vec<String>,
        details: Option<Value>,
    ) {
        let event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type,
            location_id,
            actors,
            reason_packet_id: None,
            caused_by: if causal_chain.is_empty() {
                vec![system_event_id.to_string()]
            } else {
                causal_chain.clone()
            },
            targets,
            tags,
            visibility: None,
            details,
        };
        causal_chain.push(event.event_id.clone());
        self.event_log.push(event);
        *sequence_in_tick += 1;
    }

    fn npc_location_for(&self, npc_id: &str) -> Option<String> {
        self.npcs
            .iter()
            .find(|npc| npc.npc_id == npc_id)
            .map(|npc| npc.location_id.clone())
    }

    fn build_context_constraints(
        &self,
        npc_id: &str,
        location_id: &str,
        law_case_load: i64,
        is_wanted: bool,
    ) -> Vec<String> {
        let mut constraints = Vec::new();
        if let Some(economy) = self.npc_economy_by_id.get(npc_id) {
            if economy.food_reserve_days <= 1 {
                constraints.push("food_reserve_critical".to_string());
            }
            if matches!(economy.shelter_status, ShelterStatus::Unsheltered) {
                constraints.push("shelter_absent".to_string());
            }
            if economy.debt_balance >= 4 {
                constraints.push("debt_overhang".to_string());
            }
        }
        if law_case_load >= 2 {
            constraints.push("law_attention_high".to_string());
        }
        if is_wanted {
            constraints.push("wanted_status_active".to_string());
        }
        if let Some(institution) = self.institutions_by_settlement.get(location_id) {
            if institution.corruption_level >= 60 {
                constraints.push("institutional_corruption_high".to_string());
            }
            if institution.response_latency_ticks >= 8 {
                constraints.push("institution_response_slow".to_string());
            }
        }
        if constraints.is_empty() {
            constraints.push("routine_constraints".to_string());
        }
        constraints
    }

    fn build_why_chain(
        &self,
        top_intents: &[String],
        top_beliefs: &[String],
        top_pressures: &[String],
        chosen_verb: AffordanceVerb,
    ) -> Vec<String> {
        let mut chain = Vec::new();
        if let Some(primary_intent) = top_intents
            .iter()
            .find(|intent| intent.as_str() != "maintain_routine")
            .or_else(|| top_intents.first())
        {
            chain.push(format!("intent::{primary_intent}"));
        }
        if let Some(primary_belief) = top_beliefs
            .iter()
            .find(|belief| !belief.starts_with("season_cycle_tick_"))
            .or_else(|| top_beliefs.first())
        {
            chain.push(format!("belief::{primary_belief}"));
        }
        if let Some(primary_pressure) = top_pressures.first() {
            chain.push(format!("pressure::{primary_pressure}"));
        }
        chain.push(format!("verb::{}", chosen_verb.as_str()));
        chain
    }

    fn build_expected_consequences(
        &self,
        location_id: &str,
        chosen_verb: AffordanceVerb,
    ) -> Vec<String> {
        let mut outcomes = match chosen_verb {
            AffordanceVerb::StealSupplies => vec![
                "short_term_resource_relief".to_string(),
                "higher_law_exposure".to_string(),
            ],
            AffordanceVerb::WorkForCoin | AffordanceVerb::WorkForFood => vec![
                "livelihood_stabilization".to_string(),
                "contract_dependency".to_string(),
            ],
            AffordanceVerb::SpreadAccusation => vec![
                "narrative_fragmentation".to_string(),
                "trust_volatility".to_string(),
            ],
            AffordanceVerb::FormMutualAidGroup => vec![
                "local_support_increase".to_string(),
                "group_norm_pressure".to_string(),
            ],
            _ => vec!["incremental_state_shift".to_string()],
        };
        if self
            .institutions_by_settlement
            .get(location_id)
            .map(|institution| institution.corruption_level >= 60)
            .unwrap_or(false)
        {
            outcomes.push("institutional_outcome_uncertain".to_string());
        }
        outcomes
    }

    fn household_pressure_signal(&self) -> i64 {
        let npc_count = self.npc_economy_by_id.len().max(1) as i64;
        let unsheltered = self
            .npc_economy_by_id
            .values()
            .filter(|economy| matches!(economy.shelter_status, ShelterStatus::Unsheltered))
            .count() as i64;
        let hunger = self
            .npc_economy_by_id
            .values()
            .filter(|economy| economy.food_reserve_days <= 1)
            .count() as i64;
        let eviction = self
            .households_by_id
            .values()
            .map(|household| household.eviction_risk_score / 20)
            .sum::<i64>();
        ((unsheltered + hunger + eviction) * 3 / npc_count).clamp(0, 8)
    }

    fn labor_pressure_signal(&self) -> i64 {
        let delayed = self
            .contracts_by_id
            .values()
            .filter(|contract| contract.contract.reliability_score < 45)
            .count() as i64;
        let underemployment = self
            .labor_market_by_settlement
            .values()
            .map(|market| market.underemployment_index / 10)
            .sum::<i64>();
        (delayed + underemployment).clamp(0, 8)
    }

    fn supply_pressure_signal(&self) -> i64 {
        self.stock_by_settlement
            .values()
            .map(|stock| stock.local_price_pressure / 12)
            .sum::<i64>()
            .clamp(-3, 8)
    }

    fn institution_pressure_signal(&self) -> i64 {
        self.institutions_by_settlement
            .values()
            .map(|institution| {
                (institution.corruption_level + institution.bias_level + institution.response_latency_ticks)
                    / 70
            })
            .sum::<i64>()
            .clamp(0, 8)
    }

    fn social_graph_pressure_signal(&self) -> i64 {
        if self.relationship_edges.is_empty() {
            return 0;
        }
        let total = self
            .relationship_edges
            .values()
            .map(|edge| (edge.grievance + edge.fear - edge.trust.max(0)) / 50)
            .sum::<i64>();
        (total / self.relationship_edges.len() as i64).clamp(-2, 8)
    }

    fn mobility_pressure_signal(&self) -> i64 {
        if self.routes_by_id.is_empty() {
            return 0;
        }
        let total = self
            .routes_by_id
            .values()
            .map(|route| route.hazard_score / 40 + if route.weather_window_open { 0 } else { 1 })
            .sum::<i64>();
        (total / self.routes_by_id.len() as i64).clamp(0, 6)
    }

    fn first_wanted_npc_at_location(&self, location_id: &str) -> Option<String> {
        self.npcs
            .iter()
            .find(|npc| {
                npc.location_id == location_id && self.wanted_npcs.contains(&npc.npc_id)
            })
            .map(|npc| npc.npc_id.clone())
    }

    fn first_wanted_npc_with_case(&self) -> Option<(String, String)> {
        self.npcs
            .iter()
            .find(|npc| {
                self.wanted_npcs.contains(&npc.npc_id)
                    && self
                        .law_case_load_by_settlement
                        .get(&npc.location_id)
                        .copied()
                        .unwrap_or(0)
                        > 0
            })
            .map(|npc| (npc.npc_id.clone(), npc.location_id.clone()))
    }

    fn npc_holds_stolen_item(&self, npc_id: &str) -> bool {
        self.item_registry
            .values()
            .any(|item| item.owner_id == npc_id && item.stolen)
    }

    fn stolen_item_owned_by_npc(&self, npc_id: &str) -> Option<String> {
        self.item_registry
            .iter()
            .find(|(_, item)| item.owner_id == npc_id && item.stolen)
            .map(|(item_id, _)| item_id.clone())
    }

    fn select_item_for_route(&self, origin_settlement_id: &str) -> Option<String> {
        self.item_registry
            .iter()
            .find(|(_, item)| item.location_id == origin_settlement_id)
            .map(|(item_id, _)| item_id.clone())
    }

    fn select_item_for_theft(
        &self,
        location_id: &str,
        thief_npc_id: &str,
        tick: u64,
    ) -> (String, String) {
        if let Some((item_id, owner_id)) = self
            .item_registry
            .iter()
            .find(|(_, item)| item.location_id == location_id && item.owner_id != thief_npc_id)
            .map(|(item_id, item)| (item_id.clone(), item.owner_id.clone()))
        {
            return (item_id, owner_id);
        }

        (
            format!(
                "item:cache:{}:{tick}",
                location_id.replace(':', "_")
            ),
            format!("store:{location_id}"),
        )
    }

    fn deterministic_stream(&self, tick: u64, phase: Phase, scope: &str, channel: &str) -> u64 {
        let mut seed = self.config.seed;
        seed = mix64(seed ^ tick);
        seed = mix64(seed ^ phase as u64);
        seed = mix64(seed ^ hash_bytes(scope.as_bytes()));
        mix64(seed ^ hash_bytes(channel.as_bytes()))
    }

    fn sync_queue_depth(&mut self) {
        self.status.queue_depth = self.queued_commands.len();
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

fn default_npcs() -> Vec<NpcAgent> {
    vec![
        NpcAgent {
            npc_id: "npc_001".to_string(),
            location_id: "settlement:greywall".to_string(),
        },
        NpcAgent {
            npc_id: "npc_002".to_string(),
            location_id: "settlement:millford".to_string(),
        },
        NpcAgent {
            npc_id: "npc_003".to_string(),
            location_id: "settlement:oakham".to_string(),
        },
    ]
}

fn default_items() -> BTreeMap<String, ItemRecord> {
    let mut items = BTreeMap::new();
    items.insert(
        "item:guild_ledger".to_string(),
        ItemRecord {
            owner_id: "store:settlement:millford".to_string(),
            location_id: "settlement:millford".to_string(),
            stolen: false,
            last_moved_tick: 0,
        },
    );
    items.insert(
        "item:bridge_tithe_box".to_string(),
        ItemRecord {
            owner_id: "store:settlement:greywall".to_string(),
            location_id: "settlement:greywall".to_string(),
            stolen: false,
            last_moved_tick: 0,
        },
    );
    items.insert(
        "item:grain_seal".to_string(),
        ItemRecord {
            owner_id: "store:settlement:oakham".to_string(),
            location_id: "settlement:oakham".to_string(),
            stolen: false,
            last_moved_tick: 0,
        },
    );
    items
}

fn default_npc_economy(npcs: &[NpcAgent]) -> BTreeMap<String, NpcEconomyState> {
    let mut states = BTreeMap::new();
    for (idx, npc) in npcs.iter().enumerate() {
        states.insert(
            npc.npc_id.clone(),
            NpcEconomyState {
                household_id: format!("household:{:03}", idx + 1),
                wallet: 6 - idx as i64,
                debt_balance: 1 + (idx as i64 % 2),
                food_reserve_days: 4 - idx as i64,
                shelter_status: ShelterStatus::Stable,
                dependents_count: if idx == 0 { 1 } else { 0 },
                apprenticeship_progress: (idx as i64) * 2,
                employer_contract_id: None,
            },
        );
    }
    states
}

fn default_npc_traits(npcs: &[NpcAgent]) -> BTreeMap<String, NpcTraitProfile> {
    npcs.iter()
        .map(|npc| {
            let base = hash_bytes(npc.npc_id.as_bytes());
            let lane = |salt: u64| -> i64 { 25 + (((mix64(base ^ salt) % 51) as i64).clamp(0, 50)) };
            (
                npc.npc_id.clone(),
                NpcTraitProfile {
                    risk_tolerance: lane(0xA1),
                    sociability: lane(0xB2),
                    dutifulness: lane(0xC3),
                    ambition: lane(0xD4),
                    empathy: lane(0xE5),
                    resilience: lane(0xF6),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
}

fn default_households(npcs: &[NpcAgent]) -> BTreeMap<String, HouseholdState> {
    let mut households = BTreeMap::new();
    for (idx, npc) in npcs.iter().enumerate() {
        let household_id = format!("household:{:03}", idx + 1);
        households.insert(
            household_id.clone(),
            HouseholdState {
                household_id,
                member_npc_ids: vec![npc.npc_id.clone()],
                shared_pantry_stock: 8 - idx as i64,
                fuel_stock: 6 - idx as i64,
                rent_due_tick: 24 * (8 + idx as u64 * 6),
                rent_cadence_ticks: 24 * 30,
                rent_amount: 12 + idx as i64,
                rent_reserve_coin: 0,
                landlord_balance: 0,
                eviction_risk_score: 8 + idx as i64 * 3,
            },
        );
    }
    households
}

fn default_contracts(npcs: &[NpcAgent]) -> BTreeMap<String, ContractState> {
    let mut contracts = BTreeMap::new();
    for (idx, npc) in npcs.iter().enumerate() {
        let settlement_id = npc.location_id.clone();
        let contract_id = format!("contract:seed:{:03}", idx + 1);
        let cadence = match idx % 3 {
            0 => ContractCadence::Daily,
            1 => ContractCadence::Weekly,
            _ => ContractCadence::Monthly,
        };
        let wage_amount = match cadence {
            ContractCadence::Daily => 2 + idx as i64,
            ContractCadence::Weekly => 12 + idx as i64 * 2,
            ContractCadence::Monthly => 45 + idx as i64 * 4,
        };
        let next_payment_tick = cadence_interval_ticks(cadence);
        contracts.insert(
            contract_id.clone(),
            ContractState {
                contract: EmploymentContractRecord {
                    contract_id: contract_id.clone(),
                    employer_id: format!("employer:{settlement_id}"),
                    worker_id: npc.npc_id.clone(),
                    settlement_id,
                    compensation_type: if idx % 2 == 0 {
                        ContractCompensationType::Mixed
                    } else {
                        ContractCompensationType::Coin
                    },
                    cadence,
                    wage_amount,
                    reliability_score: 70 - idx as u8 * 8,
                    active: true,
                    breached: false,
                    next_payment_tick,
                },
            },
        );
    }
    contracts
}

fn default_labor_markets() -> BTreeMap<String, LaborMarketState> {
    [
        ("settlement:greywall".to_string(), 6_u32, 2_i64, 5_i64, 16_i64),
        ("settlement:millford".to_string(), 4_u32, 2_i64, 4_i64, 18_i64),
        ("settlement:oakham".to_string(), 5_u32, 1_i64, 4_i64, 20_i64),
    ]
    .into_iter()
    .map(|(settlement_id, open_roles, wage_band_low, wage_band_high, underemployment_index)| {
        (
            settlement_id,
            LaborMarketState {
                open_roles,
                wage_band_low,
                wage_band_high,
                underemployment_index,
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
}

fn default_stock_ledgers() -> BTreeMap<String, StockLedgerState> {
    [
        (
            "settlement:greywall".to_string(),
            14_i64,
            9_i64,
            4_i64,
            5_i64,
            180_i64,
        ),
        (
            "settlement:millford".to_string(),
            11_i64,
            8_i64,
            3_i64,
            6_i64,
            165_i64,
        ),
        (
            "settlement:oakham".to_string(),
            13_i64,
            7_i64,
            3_i64,
            4_i64,
            170_i64,
        ),
    ]
    .into_iter()
    .map(|(settlement_id, staples, fuel, medicine, craft_inputs, coin_reserve)| {
        (
            settlement_id,
            StockLedgerState {
                staples,
                fuel,
                medicine,
                craft_inputs,
                local_price_pressure: 0,
                coin_reserve,
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
}

fn default_production_nodes() -> BTreeMap<String, ProductionNodeRuntimeState> {
    [
        (
            "node:farm:oakham".to_string(),
            "settlement:oakham".to_string(),
            "farm".to_string(),
        ),
        (
            "node:workshop:millford".to_string(),
            "settlement:millford".to_string(),
            "workshop".to_string(),
        ),
        (
            "node:kiln:greywall".to_string(),
            "settlement:greywall".to_string(),
            "kiln".to_string(),
        ),
    ]
    .into_iter()
    .map(|(node_id, settlement_id, node_kind)| {
        (
            node_id.clone(),
            ProductionNodeRuntimeState {
                node_id,
                settlement_id,
                node_kind,
                input_backlog: 2,
                output_backlog: 1,
                spoilage_timer: 14,
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
}

fn default_relationship_edges(npcs: &[NpcAgent]) -> BTreeMap<(String, String), RelationshipEdgeRuntime> {
    let mut edges = BTreeMap::new();
    for source in npcs {
        for target in npcs {
            if source.npc_id == target.npc_id {
                continue;
            }
            let pair_seed = hash_bytes(format!("{}:{}", source.npc_id, target.npc_id).as_bytes());
            let compatibility = ((mix64(pair_seed ^ 0xA5) % 41) as i64) - 20;
            let hobby_idx = (mix64(pair_seed ^ 0x5A) % 5) as usize;
            let hobby = ["dice", "hunting", "craft", "gossip", "prayer"][hobby_idx];
            edges.insert(
                (source.npc_id.clone(), target.npc_id.clone()),
                RelationshipEdgeRuntime {
                    source_npc_id: source.npc_id.clone(),
                    target_npc_id: target.npc_id.clone(),
                    trust: 2 + compatibility / 5,
                    attachment: 2 + (compatibility.max(0) / 8),
                    obligation: 2,
                    grievance: 1,
                    fear: 0,
                    respect: 3,
                    jealousy: 0,
                    recent_interaction_tick: 0,
                    compatibility_score: compatibility,
                    shared_context_tags: vec![format!("hobby:{hobby}")],
                    relation_tags: vec!["acquaintance".to_string(), format!("compat:{compatibility}")],
                },
            );
        }
    }
    edges
}

fn default_beliefs(npcs: &[NpcAgent]) -> BTreeMap<String, Vec<BeliefClaimRuntime>> {
    npcs.iter()
        .map(|npc| {
            (
                npc.npc_id.clone(),
                vec![BeliefClaimRuntime {
                    claim_id: format!("claim:{}:initial", npc.npc_id),
                    npc_id: npc.npc_id.clone(),
                    settlement_id: npc.location_id.clone(),
                    confidence: 0.62,
                    source: BeliefSource::Hearsay,
                    distortion_score: 0.2,
                    willingness_to_share: 0.4,
                    truth_link: None,
                    claim_text: "market rumor".to_string(),
                }],
            )
        })
        .collect::<BTreeMap<_, _>>()
}

fn default_institutions() -> BTreeMap<String, InstitutionRuntimeState> {
    [
        ("settlement:greywall".to_string(), 62_i64, 26_i64, 18_i64, 4_i64),
        ("settlement:millford".to_string(), 56_i64, 30_i64, 22_i64, 5_i64),
        ("settlement:oakham".to_string(), 58_i64, 24_i64, 20_i64, 4_i64),
    ]
    .into_iter()
    .map(|(settlement_id, enforcement_capacity, corruption_level, bias_level, response_latency)| {
        (
            settlement_id,
            InstitutionRuntimeState {
                enforcement_capacity,
                corruption_level,
                bias_level,
                response_latency_ticks: response_latency,
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
}

fn default_groups(npcs: &[NpcAgent]) -> BTreeMap<String, GroupRuntimeState> {
    let mut groups = BTreeMap::new();
    if let Some(leader) = npcs.first() {
        groups.insert(
            "group:lantern_circle".to_string(),
            GroupRuntimeState {
                group_id: "group:lantern_circle".to_string(),
                settlement_id: leader.location_id.clone(),
                leader_npc_id: leader.npc_id.clone(),
                member_npc_ids: vec![leader.npc_id.clone()],
                norm_tags: vec!["share_news".to_string(), "watch_roads".to_string()],
                cohesion_score: 34,
                formed_tick: 0,
                inertia_score: 22,
                shared_cause: "shared_watch".to_string(),
                infiltration_pressure: 4,
            },
        );
    }
    groups
}

fn default_routes() -> BTreeMap<String, MobilityRouteRuntimeState> {
    [
        (
            "route:greywall_millford".to_string(),
            "settlement:greywall".to_string(),
            "settlement:millford".to_string(),
        ),
        (
            "route:greywall_oakham".to_string(),
            "settlement:greywall".to_string(),
            "settlement:oakham".to_string(),
        ),
        (
            "route:millford_oakham".to_string(),
            "settlement:millford".to_string(),
            "settlement:oakham".to_string(),
        ),
    ]
    .into_iter()
    .map(|(route_id, origin_settlement_id, destination_settlement_id)| {
        (
            route_id.clone(),
            MobilityRouteRuntimeState {
                route_id,
                origin_settlement_id,
                destination_settlement_id,
                travel_time_ticks: 24,
                hazard_score: 14,
                weather_window_open: true,
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
}

fn build_top_intents(
    intent_seed: u64,
    pressure_index: i64,
    rumor_heat: i64,
    harvest_shock: i64,
    winter_severity: u8,
    removed_npc_count: usize,
    law_case_load: i64,
    is_wanted: bool,
    has_stolen_item: bool,
) -> Vec<String> {
    let mut intents = Vec::new();

    if law_case_load >= 1
        || is_wanted
        || (pressure_index >= 210 && intent_seed % 4 == 0)
    {
        push_unique(&mut intents, "stabilize_security");
    }

    if rumor_heat >= 2 {
        push_unique(&mut intents, "investigate_rumor");
    }

    if harvest_shock >= 2 {
        push_unique(&mut intents, "secure_food");
    }

    if winter_severity >= 60 {
        push_unique(&mut intents, "store_fuel");
    }

    if removed_npc_count > 0 {
        push_unique(&mut intents, "cover_missing_roles");
    }

    if law_case_load >= 2 || (pressure_index >= 240 && intent_seed % 5 == 0) {
        push_unique(&mut intents, "stabilize_security");
    }

    if is_wanted {
        push_unique(&mut intents, "evade_patrols");
    }

    if has_stolen_item {
        push_unique(&mut intents, "liquidate_goods");
    }

    match intent_seed % 4 {
        0 => {
            if !intents.iter().any(|intent| intent == "secure_food") {
                push_unique(&mut intents, "secure_food");
            }
        }
        1 => push_unique(&mut intents, "build_trust"),
        2 => push_unique(&mut intents, "patrol_routes"),
        _ => push_unique(&mut intents, "seek_opportunity"),
    }

    push_unique(&mut intents, "maintain_routine");
    intents
}

fn build_top_beliefs(
    tick: u64,
    intent_seed: u64,
    pressure_index: i64,
    rumor_heat: i64,
    harvest_shock: i64,
    winter_severity: u8,
    law_case_load: i64,
    is_wanted: bool,
    has_stolen_item: bool,
) -> Vec<String> {
    let mut beliefs = vec![format!("season_cycle_tick_{tick}")];

    if pressure_index > 0 {
        beliefs.push("local_tension_is_rising".to_string());
    }

    if rumor_heat >= 2 {
        beliefs.push("rumor_density_high".to_string());
    }

    if harvest_shock >= 2 {
        beliefs.push("grain_stores_are_thin".to_string());
    }

    if winter_severity >= 60 {
        beliefs.push("winter_supply_risk_rising".to_string());
    }

    if law_case_load > 0 {
        beliefs.push("watch_presence_increasing".to_string());
    }

    if is_wanted {
        beliefs.push("authorities_are_tracking_me".to_string());
    }

    if has_stolen_item {
        beliefs.push("fence_network_needed_for_goods".to_string());
    }

    if intent_seed % 3 == 0 {
        beliefs.push("rumors_spread_faster_near_market".to_string());
    } else {
        beliefs.push("road_safety_depends_on_patrols".to_string());
    }

    beliefs
}

fn top_pressures(
    pressure_index: i64,
    rumor_heat: i64,
    harvest_shock: i64,
    winter_severity: u8,
    law_case_load: i64,
    stolen_item_count: usize,
) -> Vec<String> {
    if pressure_index >= 170 {
        let mut pressures = vec![
            "material_high".to_string(),
            "security_high".to_string(),
            "social_rising".to_string(),
        ];
        if rumor_heat >= 2 {
            pressures.push("information_volatile".to_string());
        }
        if harvest_shock >= 2 {
            pressures.push("food_shock".to_string());
        }
        if winter_severity >= 60 {
            pressures.push("seasonal_strain".to_string());
        }
        if law_case_load >= 2 {
            pressures.push("institutional_crackdown".to_string());
        }
        if stolen_item_count > 0 {
            pressures.push("contraband_circulation".to_string());
        }
        pressures
    } else if pressure_index >= 90 {
        let mut pressures = vec![
            "material_moderate".to_string(),
            "security_moderate".to_string(),
        ];
        if rumor_heat >= 2 {
            pressures.push("information_rising".to_string());
        }
        if harvest_shock >= 2 {
            pressures.push("food_tight".to_string());
        }
        if law_case_load >= 1 {
            pressures.push("institutional_attention".to_string());
        }
        if stolen_item_count > 0 {
            pressures.push("contraband_flow".to_string());
        }
        pressures
    } else {
        let mut pressures = vec!["material_low".to_string(), "security_low".to_string()];
        if winter_severity >= 60 {
            pressures.push("cold_exposure_risk".to_string());
        }
        if law_case_load >= 1 {
            pressures.push("institutional_watch".to_string());
        }
        pressures
    }
}

fn build_action_candidates<F>(
    tick: u64,
    npc_id: &str,
    top_intents: &[String],
    pressure_index: i64,
    rumor_heat: i64,
    harvest_shock: i64,
    winter_severity: u8,
    law_case_load: i64,
    is_wanted: bool,
    has_stolen_item: bool,
    wallet: i64,
    debt_balance: i64,
    food_reserve_days: i64,
    dependents_count: i64,
    shelter_status: ShelterStatus,
    contract_reliability: i64,
    eviction_risk: i64,
    rent_due_in_ticks: i64,
    rent_shortfall: i64,
    rent_cadence_ticks: i64,
    trust_support: i64,
    local_theft_pressure: i64,
    local_support_need: i64,
    low_pressure_economic_opportunity: i64,
    profile: &NpcTraitProfile,
    stream: F,
) -> Vec<ActionCandidate>
where
    F: Fn(u64, &str, &str) -> u64,
{
    let security_need = law_case_load > 0 || local_theft_pressure >= 2 || is_wanted;
    let mut base = vec![
        candidate(AffordanceVerb::WorkForFood, 2, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::WorkForCoin, 3, -2, tick, npc_id, &stream),
        candidate(AffordanceVerb::SeekShelter, 0, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::TendFields, 2, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::CraftGoods, 2, 0, tick, npc_id, &stream),
        candidate(AffordanceVerb::ShareMeal, 1, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::FormMutualAidGroup, 1, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::DefendPatron, 1, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::SpreadAccusation, 1, 1, tick, npc_id, &stream),
        candidate(AffordanceVerb::MediateDispute, 1, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::TrainApprentice, 1, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::ShareRumor, 2, 1, tick, npc_id, &stream),
        candidate(AffordanceVerb::PatrolRoad, 2, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::Forage, 1, 0, tick, npc_id, &stream),
    ];

    if top_intents.iter().any(|intent| intent == "secure_food")
        && (food_reserve_days <= 1
            || (food_reserve_days <= 2 && dependents_count > 0)
            || (food_reserve_days <= 1 && profile.empathy >= 70 && trust_support >= 2))
    {
        base.push(candidate(
            AffordanceVerb::StealSupplies,
            2,
            1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents
        .iter()
        .any(|intent| intent == "investigate_rumor")
    {
        base.push(candidate(
            AffordanceVerb::InvestigateRumor,
            4,
            1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents.iter().any(|intent| intent == "store_fuel") {
        base.push(candidate(
            AffordanceVerb::GatherFirewood,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents
        .iter()
        .any(|intent| intent == "cover_missing_roles")
    {
        base.push(candidate(
            AffordanceVerb::CoverAbsentNeighbor,
            3,
            0,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents
        .iter()
        .any(|intent| intent == "stabilize_security")
        && security_need
    {
        base.push(candidate(
            AffordanceVerb::OrganizeWatch,
            2,
            -2,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents
        .iter()
        .any(|intent| intent == "liquidate_goods")
        || has_stolen_item
    {
        base.push(candidate(
            AffordanceVerb::FenceGoods,
            4,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents.iter().any(|intent| intent == "evade_patrols") || is_wanted {
        base.push(candidate(
            AffordanceVerb::AvoidPatrols,
            3,
            1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if rumor_heat >= 2 {
        base.push(candidate(
            AffordanceVerb::QuestionTravelers,
            3,
            1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::ShareRumor,
            4,
            1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::MediateDispute,
            2,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if harvest_shock >= 2 {
        base.push(candidate(
            AffordanceVerb::RationGrain,
            3,
            1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::Forage,
            4,
            -1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::WorkForFood,
            4,
            -2,
            tick,
            npc_id,
            &stream,
        ));
    }

    if winter_severity >= 65 {
        base.push(candidate(
            AffordanceVerb::RepairHearth,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if law_case_load >= 2 {
        base.push(candidate(
            AffordanceVerb::CollectTestimony,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if pressure_index <= 0 {
        base.push(candidate(
            AffordanceVerb::TradeVisit,
            2,
            0,
            tick,
            npc_id,
            &stream,
        ));
    }

    if pressure_index <= 90 && !security_need {
        base.push(candidate(
            AffordanceVerb::CraftGoods,
            3 + (low_pressure_economic_opportunity / 6).clamp(0, 2) as i32,
            -1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::TendFields,
            3 + (low_pressure_economic_opportunity / 8).clamp(0, 2) as i32,
            -1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::TradeVisit,
            3 + (low_pressure_economic_opportunity / 10).clamp(0, 1) as i32,
            0,
            tick,
            npc_id,
            &stream,
        ));
        if low_pressure_economic_opportunity >= 4 {
            base.push(candidate(
                AffordanceVerb::WorkForCoin,
                4,
                -1,
                tick,
                npc_id,
                &stream,
            ));
        }
    }

    if top_intents.iter().any(|intent| intent == "seek_income")
        || wallet <= 1
        || debt_balance >= 3
    {
        base.push(candidate(
            AffordanceVerb::WorkForCoin,
            4,
            -2,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::CraftGoods,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if food_reserve_days <= 1 {
        base.push(candidate(
            AffordanceVerb::WorkForFood,
            5,
            -2,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::Forage,
            4,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if matches!(shelter_status, ShelterStatus::Unsheltered)
        || top_intents.iter().any(|intent| intent == "seek_shelter")
    {
        base.push(candidate(
            AffordanceVerb::SeekShelter,
            4,
            -2,
            tick,
            npc_id,
            &stream,
        ));
    } else if matches!(shelter_status, ShelterStatus::Precarious)
        && (winter_severity >= 65 || wallet <= 0 || food_reserve_days <= 0)
    {
        base.push(candidate(
            AffordanceVerb::SeekShelter,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if rent_shortfall > 0 && wallet >= 2 && rent_due_in_ticks <= 6 {
        base.push(candidate(
            AffordanceVerb::PayRent,
            6,
            -2,
            tick,
            npc_id,
            &stream,
        ));
    } else if rent_shortfall > 0
        && wallet >= 3
        && rent_due_in_ticks <= (rent_cadence_ticks / 6).max(12)
        && eviction_risk >= 60
    {
        base.push(candidate(
            AffordanceVerb::PayRent,
            4,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    } else if rent_shortfall > 0 && rent_due_in_ticks <= 24 && wallet < 2 {
        base.push(candidate(
            AffordanceVerb::WorkForCoin,
            6,
            -2,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::WorkForFood,
            4,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if local_support_need > 0
        && trust_support > 0
        && food_reserve_days > 1
        && debt_balance < 4
    {
        base.push(candidate(
            AffordanceVerb::AssistNeighbor,
            2 + (local_support_need.min(3) as i32),
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if !security_need {
        base.push(candidate(
            AffordanceVerb::TradeVisit,
            3,
            0,
            tick,
            npc_id,
            &stream,
        ));
    }

    if food_reserve_days <= 1 {
        base.push(candidate(
            AffordanceVerb::ShareMeal,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if contract_reliability < 40 {
        base.push(candidate(
            AffordanceVerb::SpreadAccusation,
            3,
            1,
            tick,
            npc_id,
            &stream,
        ));
    }

    if trust_support > 0 {
        base.push(candidate(
            AffordanceVerb::DefendPatron,
            3,
            -1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::MediateDispute,
            2,
            -1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::FormMutualAidGroup,
            2,
            -1,
            tick,
            npc_id,
            &stream,
        ));
    }

    apply_personality_bias(
        &mut base,
        profile,
        shelter_status,
        is_wanted,
        security_need,
        local_support_need,
        food_reserve_days,
        debt_balance,
    );

    base
}

fn apply_personality_bias(
    candidates: &mut [ActionCandidate],
    profile: &NpcTraitProfile,
    shelter_status: ShelterStatus,
    is_wanted: bool,
    security_need: bool,
    local_support_need: i64,
    food_reserve_days: i64,
    debt_balance: i64,
) {
    for candidate in candidates {
        match candidate.verb {
            AffordanceVerb::PayRent => {
                if profile.dutifulness >= 65 {
                    candidate.score = candidate.score.saturating_add(160);
                }
                if profile.ambition >= 60 {
                    candidate.score = candidate.score.saturating_sub(120);
                }
            }
            AffordanceVerb::ShareMeal
            | AffordanceVerb::MediateDispute
            | AffordanceVerb::FormMutualAidGroup => {
                if profile.empathy >= 60 || profile.sociability >= 60 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(260);
                }
            }
            AffordanceVerb::AssistNeighbor => {
                if (profile.empathy >= 65 || profile.sociability >= 65) && local_support_need > 0 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(180);
                } else {
                    candidate.score = candidate.score.saturating_sub(700);
                }
                if debt_balance >= 3 || food_reserve_days <= 1 {
                    candidate.score = candidate.score.saturating_sub(900);
                }
            }
            AffordanceVerb::OrganizeWatch
            | AffordanceVerb::PatrolRoad
            | AffordanceVerb::CollectTestimony => {
                if profile.dutifulness >= 60 && security_need {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(220);
                } else if !security_need {
                    candidate.priority -= 2;
                    candidate.score = candidate.score.saturating_sub(1_400);
                }
            }
            AffordanceVerb::WorkForCoin | AffordanceVerb::CraftGoods => {
                if profile.ambition >= 60 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(180);
                }
                if debt_balance >= 3 {
                    candidate.score = candidate.score.saturating_add(260);
                }
            }
            AffordanceVerb::StealSupplies | AffordanceVerb::FenceGoods => {
                if profile.risk_tolerance <= 40 || profile.empathy >= 65 {
                    candidate.priority -= 2;
                    candidate.score = candidate.score.saturating_sub(1400);
                } else if profile.risk_tolerance >= 68 && food_reserve_days <= 1 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(240);
                }
            }
            AffordanceVerb::AvoidPatrols => {
                if !is_wanted || profile.dutifulness >= 60 {
                    candidate.priority -= 1;
                    candidate.score = candidate.score.saturating_sub(600);
                }
            }
            AffordanceVerb::SeekShelter => match shelter_status {
                ShelterStatus::Stable => {
                    candidate.priority -= 3;
                    candidate.score = candidate.score.saturating_sub(2200);
                }
                ShelterStatus::Precarious => {
                    if profile.resilience >= 60 && food_reserve_days >= 1 && !security_need {
                        candidate.priority -= 2;
                        candidate.score = candidate.score.saturating_sub(1300);
                    }
                }
                ShelterStatus::Unsheltered => {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(300);
                }
            },
            _ => {}
        }
    }
}

fn candidate<F>(
    verb: AffordanceVerb,
    priority: i32,
    pressure_effect: i64,
    tick: u64,
    npc_id: &str,
    stream: &F,
) -> ActionCandidate
where
    F: Fn(u64, &str, &str) -> u64,
{
    let action = verb.as_str();
    ActionCandidate {
        verb,
        action: action.to_string(),
        priority,
        score: stream(tick, npc_id, action) % 10_000,
        pressure_effect,
    }
}

fn choose_candidate(candidates: &[ActionCandidate]) -> Option<ActionCandidate> {
    candidates.iter().cloned().max_by(|left, right| {
        (left.priority, left.score, &left.action).cmp(&(right.priority, right.score, &right.action))
    })
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|current| current == value) {
        values.push(value.to_string());
    }
}

fn push_unique_motive(values: &mut Vec<MotiveFamily>, value: MotiveFamily) {
    if !values.iter().any(|current| current == &value) {
        values.push(value);
    }
}

fn push_unique_check(values: &mut Vec<FeasibilityCheck>, value: FeasibilityCheck) {
    if !values.iter().any(|current| current == &value) {
        values.push(value);
    }
}

fn trust_level_from_score(score: i64) -> TrustLevel {
    if score <= -40 {
        TrustLevel::Hostile
    } else if score <= -10 {
        TrustLevel::Distrusting
    } else if score <= 10 {
        TrustLevel::Cautious
    } else if score <= 30 {
        TrustLevel::Familiar
    } else if score <= 60 {
        TrustLevel::Trusted
    } else {
        TrustLevel::Bonded
    }
}

fn infer_motive_families(
    top_intents: &[String],
    chosen_verb: AffordanceVerb,
    is_wanted: bool,
    has_stolen_item: bool,
    law_case_load: i64,
    harvest_shock: i64,
    rumor_heat: i64,
) -> Vec<MotiveFamily> {
    let mut motives = Vec::new();

    if top_intents.iter().any(|intent| intent == "secure_food") || harvest_shock >= 2 {
        push_unique_motive(&mut motives, MotiveFamily::Survival);
    }

    if top_intents
        .iter()
        .any(|intent| intent == "stabilize_security")
        || law_case_load >= 2
    {
        push_unique_motive(&mut motives, MotiveFamily::Security);
        push_unique_motive(&mut motives, MotiveFamily::JusticeGrievance);
    }

    if top_intents.iter().any(|intent| intent == "build_trust") {
        push_unique_motive(&mut motives, MotiveFamily::Belonging);
        push_unique_motive(&mut motives, MotiveFamily::Attachment);
    }

    if top_intents.iter().any(|intent| intent == "seek_opportunity") {
        push_unique_motive(&mut motives, MotiveFamily::Ambition);
    }

    if top_intents
        .iter()
        .any(|intent| intent == "investigate_rumor")
        || rumor_heat >= 2
    {
        push_unique_motive(&mut motives, MotiveFamily::CuriosityMeaning);
    }

    if top_intents.iter().any(|intent| intent == "evade_patrols") || is_wanted {
        push_unique_motive(&mut motives, MotiveFamily::Coercion);
    }

    if has_stolen_item {
        push_unique_motive(&mut motives, MotiveFamily::Ambition);
    }

    match chosen_verb {
        AffordanceVerb::StealSupplies | AffordanceVerb::FenceGoods => {
            if harvest_shock >= 2 {
                push_unique_motive(&mut motives, MotiveFamily::Survival);
            } else {
                push_unique_motive(&mut motives, MotiveFamily::Ambition);
            }
            if is_wanted || has_stolen_item {
                push_unique_motive(&mut motives, MotiveFamily::Coercion);
            }
        }
        AffordanceVerb::OrganizeWatch
        | AffordanceVerb::PatrolRoad
        | AffordanceVerb::CollectTestimony => {
            push_unique_motive(&mut motives, MotiveFamily::Security);
            push_unique_motive(&mut motives, MotiveFamily::JusticeGrievance);
        }
        AffordanceVerb::ShareRumor
        | AffordanceVerb::InvestigateRumor
        | AffordanceVerb::QuestionTravelers => {
            push_unique_motive(&mut motives, MotiveFamily::CuriosityMeaning);
        }
        AffordanceVerb::WorkForCoin
        | AffordanceVerb::PayRent
        | AffordanceVerb::TendFields
        | AffordanceVerb::CraftGoods => {
            push_unique_motive(&mut motives, MotiveFamily::Security);
            push_unique_motive(&mut motives, MotiveFamily::Dignity);
        }
        AffordanceVerb::FormMutualAidGroup
        | AffordanceVerb::ShareMeal
        | AffordanceVerb::MediateDispute
        | AffordanceVerb::DefendPatron => {
            push_unique_motive(&mut motives, MotiveFamily::Belonging);
            push_unique_motive(&mut motives, MotiveFamily::Attachment);
        }
        AffordanceVerb::SpreadAccusation => {
            push_unique_motive(&mut motives, MotiveFamily::JusticeGrievance);
            push_unique_motive(&mut motives, MotiveFamily::Ideology);
        }
        AffordanceVerb::TrainApprentice => {
            push_unique_motive(&mut motives, MotiveFamily::CuriosityMeaning);
            push_unique_motive(&mut motives, MotiveFamily::Dignity);
        }
        AffordanceVerb::AssistNeighbor => {
            push_unique_motive(&mut motives, MotiveFamily::Attachment);
            push_unique_motive(&mut motives, MotiveFamily::Belonging);
        }
        _ => {}
    }

    if motives.is_empty() {
        motives.push(MotiveFamily::Survival);
    }

    motives
}

fn infer_feasibility_checks(
    pressure_index: i64,
    rumor_heat: i64,
    harvest_shock: i64,
    winter_severity: u8,
    law_case_load: i64,
    is_wanted: bool,
    has_stolen_item: bool,
) -> Vec<FeasibilityCheck> {
    let mut checks = Vec::new();

    if pressure_index >= 170 {
        push_unique_check(&mut checks, FeasibilityCheck::PressureIndexHigh);
    } else if pressure_index <= 85 {
        push_unique_check(&mut checks, FeasibilityCheck::PressureIndexLow);
    }

    if rumor_heat >= 2 {
        push_unique_check(&mut checks, FeasibilityCheck::RumorHeatHigh);
    }
    if harvest_shock >= 2 {
        push_unique_check(&mut checks, FeasibilityCheck::HarvestShockHigh);
    }
    if winter_severity >= 65 {
        push_unique_check(&mut checks, FeasibilityCheck::WinterSeverityHigh);
    }
    if law_case_load >= 2 {
        push_unique_check(&mut checks, FeasibilityCheck::LawCaseLoadHigh);
    }
    if is_wanted {
        push_unique_check(&mut checks, FeasibilityCheck::IsWanted);
    }
    if has_stolen_item {
        push_unique_check(&mut checks, FeasibilityCheck::HasStolenItem);
    }

    if !is_wanted && harvest_shock < 3 {
        push_unique_check(&mut checks, FeasibilityCheck::LivelihoodAvailable);
    }

    if law_case_load < 4 && rumor_heat < 5 {
        push_unique_check(&mut checks, FeasibilityCheck::MarketAccessible);
    }

    checks
}

fn apply_action_memory_bias(
    candidates: &mut [ActionCandidate],
    action_memory: Option<&ActionMemory>,
    is_wanted: bool,
    law_case_load: i64,
    tick: u64,
    shelter_status: ShelterStatus,
    food_reserve_days: i64,
    eviction_risk: i64,
    local_theft_pressure: i64,
    local_support_need: i64,
    profile: &NpcTraitProfile,
) {
    let Some(memory) = action_memory else {
        return;
    };

    for candidate in candidates {
        if memory.last_action.as_deref() == Some(candidate.action.as_str()) {
            let penalty = 1_200_u64 + u64::from(memory.repeat_streak) * 350;
            candidate.score = candidate.score.saturating_sub(penalty);
            if memory.repeat_streak >= 3 {
                candidate.score = candidate.score.saturating_sub(1_100);
            }
        } else {
            let novelty_bonus = 180_u64 + u64::from(memory.repeat_streak.min(4)) * 90;
            candidate.score = candidate.score.saturating_add(novelty_bonus);
        }

        if candidate.action == "organize_watch" && law_case_load == 0 && local_theft_pressure <= 1 {
            candidate.score = candidate.score.saturating_sub(1_800);
            if memory.repeat_streak >= 1 {
                candidate.score = candidate.score.saturating_sub(700);
            }
        }

        if candidate.action == "avoid_patrols" && !is_wanted {
            candidate.score = candidate.score.saturating_sub(900);
        }

        if candidate.action == "steal_supplies" {
            if food_reserve_days >= 2 {
                candidate.score = candidate.score.saturating_sub(2_300);
            }
            if let Some(last_theft_tick) = memory.last_theft_tick {
                if tick.saturating_sub(last_theft_tick) < 24 {
                    candidate.score = candidate.score.saturating_sub(3_000);
                } else if tick.saturating_sub(last_theft_tick) < 48 {
                    candidate.score = candidate.score.saturating_sub(1_200);
                }
            }
        }

        if candidate.action == "pay_rent" {
            if eviction_risk < 70 {
                candidate.score = candidate.score.saturating_sub(1_200);
            }
            if memory.last_action.as_deref() == Some("pay_rent")
                && tick.saturating_sub(memory.last_action_tick) < 12
            {
                candidate.score = candidate.score.saturating_sub(1_800);
            }
            if memory.repeat_streak >= 1 {
                let penalty = 900_u64 + u64::from(memory.repeat_streak) * 300;
                candidate.score = candidate.score.saturating_sub(penalty);
            }
        }

        if candidate.action == "assist_neighbor" {
            if local_support_need <= 0 {
                candidate.score = candidate.score.saturating_sub(1_600);
            }
            if memory.last_action.as_deref() == Some("assist_neighbor")
                && tick.saturating_sub(memory.last_action_tick) < 18
            {
                candidate.score = candidate.score.saturating_sub(1_900);
            }
        }

        if candidate.action == "seek_shelter" {
            match shelter_status {
                ShelterStatus::Stable => {
                    candidate.score = candidate.score.saturating_sub(2_600);
                }
                ShelterStatus::Precarious => {
                    if profile.resilience >= 60 && eviction_risk < 65 {
                        let penalty = 1_400_u64 + u64::from(memory.repeat_streak) * 300;
                        candidate.score = candidate.score.saturating_sub(penalty);
                    }
                }
                ShelterStatus::Unsheltered => {}
            }
        }
    }
}

fn sum_positive<'a, I>(values: I) -> i64
where
    I: IntoIterator<Item = &'a i64>,
{
    values.into_iter().map(|value| (*value).max(0)).sum()
}

fn step_toward(current: i64, target: i64, step: i64) -> i64 {
    if current < target {
        (current + step).min(target)
    } else if current > target {
        (current - step).max(target)
    } else {
        current
    }
}

fn winter_pressure_effect(severity: u8) -> i64 {
    if severity >= 75 {
        3
    } else if severity >= 60 {
        2
    } else if severity >= 45 {
        1
    } else if severity <= 20 {
        -1
    } else {
        0
    }
}

fn cadence_interval_ticks(cadence: ContractCadence) -> u64 {
    match cadence {
        ContractCadence::Daily => 24,
        ContractCadence::Weekly => 24 * 7,
        ContractCadence::Monthly => 24 * 30,
    }
}

fn narrative_default_alternatives(chosen_action: &str) -> Vec<String> {
    match chosen_action {
        "assist_neighbor" => vec!["work_for_coin".to_string(), "trade_visit".to_string()],
        "work_for_coin" => vec!["craft_goods".to_string(), "tend_fields".to_string()],
        "pay_rent" => vec!["work_for_coin".to_string(), "work_for_food".to_string()],
        "seek_shelter" => vec!["work_for_food".to_string(), "trade_visit".to_string()],
        "spread_accusation" => vec!["mediate_dispute".to_string(), "share_rumor".to_string()],
        _ => vec!["evaluate_other_options".to_string(), "delay_action".to_string()],
    }
}

fn increase_signal(map: &mut BTreeMap<String, i64>, key: &str, amount: i64) {
    if amount <= 0 {
        return;
    }

    let entry = map.entry(key.to_string()).or_insert(0);
    *entry += amount;
}

fn decrease_signal(map: &mut BTreeMap<String, i64>, key: &str, amount: i64) {
    if amount <= 0 {
        return;
    }

    if let Some(value) = map.get_mut(key) {
        *value -= amount;
        if *value <= 0 {
            map.remove(key);
        }
    }
}

fn apply_case_delta(map: &mut BTreeMap<String, i64>, settlement_id: &str, delta: i64) {
    if delta == 0 {
        return;
    }

    let next = map.get(settlement_id).copied().unwrap_or(0) + delta;
    if next <= 0 {
        map.remove(settlement_id);
    } else {
        map.insert(settlement_id.to_string(), next);
    }
}

fn decay_signal_map(map: &mut BTreeMap<String, i64>, decay: i64) {
    if decay <= 0 || map.is_empty() {
        return;
    }

    map.retain(|_, value| {
        *value -= decay;
        *value > 0
    });
}

fn pressure_delta_for_command(command: &Command) -> i64 {
    match &command.payload {
        CommandPayload::InjectRumor { .. } => 1,
        CommandPayload::InjectSpawnCaravan { .. } => -1,
        CommandPayload::InjectRemoveNpc { .. } => 2,
        CommandPayload::InjectForceBadHarvest { .. } => 3,
        CommandPayload::InjectSetWinterSeverity { severity } => {
            if *severity >= 60 {
                2
            } else {
                -1
            }
        }
        _ => 0,
    }
}

fn synthetic_timestamp(tick: u64, sequence_in_tick: u64) -> String {
    format!("tick-{tick:06}-seq-{sequence_in_tick:03}")
}

fn hash_bytes(input: &[u8]) -> u64 {
    // FNV-1a 64-bit
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{CommandType, RegionId};
    use serde_json::Value;

    #[test]
    fn stepping_advances_tick_and_emits_npc_actions() {
        let mut kernel = Kernel::new(RunConfig::default());
        assert_eq!(kernel.status().current_tick, 0);

        let did_step = kernel.step_tick();
        assert!(did_step);
        assert_eq!(kernel.status().current_tick, 1);

        assert!(kernel
            .events()
            .iter()
            .any(|event| event.event_type == EventType::NpcActionCommitted));
        assert!(!kernel.reason_packets().is_empty());
    }

    #[test]
    fn deterministic_replay_same_seed_same_commands() {
        let mut config = RunConfig::default();
        config.region_id = RegionId::Crownvale;

        let command = Command::new(
            "cmd_001",
            config.run_id.clone(),
            2,
            CommandType::InjectRumor,
            CommandPayload::InjectRumor {
                location_id: "settlement:greywall".to_string(),
                rumor_text: "Bandits on the pass".to_string(),
            },
        );

        let mut kernel_a = Kernel::new(config.clone());
        kernel_a.enqueue_command(command.clone(), 2);
        kernel_a.step_ticks(12);

        let mut kernel_b = Kernel::new(config);
        kernel_b.enqueue_command(command, 2);
        kernel_b.step_ticks(12);

        assert_eq!(kernel_a.events(), kernel_b.events());
        assert_eq!(kernel_a.reason_packets(), kernel_b.reason_packets());
        assert_eq!(kernel_a.state_hash(), kernel_b.state_hash());
    }

    #[test]
    fn command_ordering_uses_effective_tick_then_sequence() {
        let config = RunConfig::default();
        let run_id = config.run_id.clone();

        let mut kernel = Kernel::new(config);

        let later = Command::new(
            "cmd_later",
            run_id.clone(),
            1,
            CommandType::InjectForceBadHarvest,
            CommandPayload::InjectForceBadHarvest {
                settlement_id: "settlement:oakham".to_string(),
            },
        );

        let earlier = Command::new(
            "cmd_earlier",
            run_id,
            1,
            CommandType::InjectSpawnCaravan,
            CommandPayload::InjectSpawnCaravan {
                origin_settlement_id: "settlement:oakham".to_string(),
                destination_settlement_id: "settlement:millford".to_string(),
            },
        );

        kernel.enqueue_command(later, 5);
        kernel.enqueue_command(earlier, 4);
        kernel.run_to_tick(5);

        let ordered: Vec<&Event> = kernel
            .events()
            .iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    EventType::CaravanSpawned | EventType::BadHarvestForced
                )
            })
            .collect();

        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].tick, 4);
        assert_eq!(ordered[1].tick, 5);
    }

    #[test]
    fn npc_actions_include_reason_packet_references() {
        let mut kernel = Kernel::new(RunConfig::default());
        kernel.run_to_tick(4);

        let npc_events = kernel
            .events()
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .collect::<Vec<_>>();

        assert!(!npc_events.is_empty());
        assert!(npc_events
            .iter()
            .all(|event| event.reason_packet_id.as_ref().is_some()));

        let reason_ids = kernel
            .reason_packets()
            .iter()
            .map(|packet| packet.reason_packet_id.as_str())
            .collect::<std::collections::HashSet<_>>();

        assert!(npc_events.iter().all(|event| {
            let reason_id = event.reason_packet_id.as_ref().expect("reason id expected");
            reason_ids.contains(reason_id.as_str())
        }));

        assert!(kernel
            .reason_packets()
            .iter()
            .all(|packet| packet.chosen_verb.is_some()));
        assert!(kernel
            .reason_packets()
            .iter()
            .all(|packet| !packet.motive_families.is_empty()));
        assert!(kernel
            .reason_packets()
            .iter()
            .all(|packet| !packet.feasibility_checks.is_empty()));
    }

    #[test]
    fn scenario_injectors_create_persistent_pressure_signals() {
        let config = RunConfig::default();
        let run_id = config.run_id.clone();
        let mut kernel = Kernel::new(config);

        let commands = vec![
            Command::new(
                "cmd_rumor",
                run_id.clone(),
                1,
                CommandType::InjectRumor,
                CommandPayload::InjectRumor {
                    location_id: "settlement:greywall".to_string(),
                    rumor_text: "A hush falls over the bridge".to_string(),
                },
            ),
            Command::new(
                "cmd_caravan",
                run_id.clone(),
                1,
                CommandType::InjectSpawnCaravan,
                CommandPayload::InjectSpawnCaravan {
                    origin_settlement_id: "settlement:oakham".to_string(),
                    destination_settlement_id: "settlement:millford".to_string(),
                },
            ),
            Command::new(
                "cmd_harvest",
                run_id.clone(),
                1,
                CommandType::InjectForceBadHarvest,
                CommandPayload::InjectForceBadHarvest {
                    settlement_id: "settlement:oakham".to_string(),
                },
            ),
            Command::new(
                "cmd_winter",
                run_id,
                1,
                CommandType::InjectSetWinterSeverity,
                CommandPayload::InjectSetWinterSeverity { severity: 80 },
            ),
        ];

        for command in commands {
            kernel.enqueue_command(command, 1);
        }

        kernel.run_to_tick(8);

        let pressure_events = kernel
            .events()
            .iter()
            .filter(|event| event.event_type == EventType::PressureEconomyUpdated)
            .collect::<Vec<_>>();

        assert!(!pressure_events.is_empty());
        assert!(pressure_events
            .iter()
            .any(|event| detail_i64(event.details.as_ref(), "rumor_pressure") > 0));
        assert!(pressure_events
            .iter()
            .any(|event| detail_i64(event.details.as_ref(), "caravan_relief") > 0));
        assert!(pressure_events
            .iter()
            .any(|event| detail_i64(event.details.as_ref(), "harvest_pressure") > 0));
        assert!(pressure_events
            .iter()
            .any(|event| detail_i64(event.details.as_ref(), "winter_pressure") > 0));
        assert!(pressure_events
            .iter()
            .any(|event| detail_i64(event.details.as_ref(), "law_pressure") >= 0));
        assert!(pressure_events
            .iter()
            .any(|event| detail_i64(event.details.as_ref(), "stolen_item_count") >= 0));
    }

    #[test]
    fn remove_npc_reduces_future_action_count() {
        let base_config = RunConfig::default();

        let mut baseline = Kernel::new(base_config.clone());
        baseline.run_to_tick(4);
        let baseline_actions = baseline
            .events()
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .count();

        let run_id = base_config.run_id.clone();
        let mut with_removal = Kernel::new(base_config);
        with_removal.enqueue_command(
            Command::new(
                "cmd_remove",
                run_id,
                1,
                CommandType::InjectRemoveNpc,
                CommandPayload::InjectRemoveNpc {
                    npc_id: "npc_002".to_string(),
                },
            ),
            1,
        );
        with_removal.run_to_tick(4);

        let actions_after_removal = with_removal
            .events()
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .count();

        assert!(actions_after_removal < baseline_actions);
        assert!(with_removal
            .events()
            .iter()
            .any(|event| event.event_type == EventType::NpcRemoved));
    }

    #[test]
    fn caravan_command_moves_story_item_between_settlements() {
        let config = RunConfig::default();
        let run_id = config.run_id.clone();
        let mut kernel = Kernel::new(config);
        kernel.npcs.clear();
        kernel.npc_economy_by_id.clear();
        kernel.action_memory_by_npc.clear();

        kernel.enqueue_command(
            Command::new(
                "cmd_caravan_item_move",
                run_id,
                1,
                CommandType::InjectSpawnCaravan,
                CommandPayload::InjectSpawnCaravan {
                    origin_settlement_id: "settlement:oakham".to_string(),
                    destination_settlement_id: "settlement:millford".to_string(),
                },
            ),
            1,
        );

        kernel.run_to_tick(1);

        let event = kernel
            .events()
            .iter()
            .find(|entry| entry.event_type == EventType::CaravanSpawned)
            .expect("caravan event should exist");
        assert_eq!(
            detail_str(event.details.as_ref(), "moved_item_id"),
            Some("item:grain_seal".to_string())
        );

        let snapshot = kernel.snapshot_for_current_tick();
        let registry = snapshot
            .npc_state_refs
            .get("item_registry")
            .and_then(Value::as_array)
            .expect("snapshot item registry should be present");

        let moved_item = registry
            .iter()
            .find(|entry| entry.get("item_id").and_then(Value::as_str) == Some("item:grain_seal"))
            .expect("moved item should exist");

        assert_eq!(
            moved_item.get("location_id").and_then(Value::as_str),
            Some("settlement:millford")
        );
        assert_eq!(
            moved_item.get("owner_id").and_then(Value::as_str),
            Some("caravan:merchant")
        );
    }

    #[test]
    fn justice_loop_emits_investigation_and_arrest_events() {
        let mut kernel = Kernel::new(RunConfig::default());
        kernel
            .law_case_load_by_settlement
            .insert("settlement:greywall".to_string(), 3);
        kernel.wanted_npcs.insert("npc_001".to_string());

        let mut sequence_in_tick = 0_u64;
        let mut causal_chain = Vec::new();
        for tick in 1..=24 {
            kernel.progress_justice_loop(
                tick,
                "evt_seed_justice",
                &mut sequence_in_tick,
                &mut causal_chain,
            );
        }

        assert!(kernel
            .events()
            .iter()
            .any(|event| event.event_type == EventType::InvestigationProgressed));
        assert!(kernel
            .events()
            .iter()
            .any(|event| event.event_type == EventType::ArrestMade));
        assert!(!kernel.wanted_npcs.contains("npc_001"));
    }

    #[test]
    fn discovery_loop_emits_discovery_and_leverage_chain() {
        let mut kernel = Kernel::new(RunConfig::default());
        kernel.enqueue_command(
            Command::new(
                "cmd_rumor_discovery",
                kernel.run_id(),
                1,
                CommandType::InjectRumor,
                CommandPayload::InjectRumor {
                    location_id: "settlement:greywall".to_string(),
                    rumor_text: "A sealed stair below the watchtower.".to_string(),
                },
            ),
            1,
        );

        kernel.run_to_tick(24);

        let discovered = kernel
            .events()
            .iter()
            .find(|event| event.event_type == EventType::SiteDiscovered)
            .expect("discovery event should exist");
        let leverage = kernel
            .events()
            .iter()
            .find(|event| event.event_type == EventType::LeverageGained)
            .expect("leverage event should exist");

        assert!(leverage
            .caused_by
            .iter()
            .any(|cause| cause == &discovered.event_id));
    }

    #[test]
    fn relationship_loop_emits_shift_event() {
        let mut kernel = Kernel::new(RunConfig::default());
        kernel.run_to_tick(18);

        assert!(kernel
            .events()
            .iter()
            .any(|event| event.event_type == EventType::RelationshipShifted));
    }

    fn detail_i64(details: Option<&Value>, key: &str) -> i64 {
        details
            .and_then(|value| value.get(key))
            .and_then(Value::as_i64)
            .unwrap_or_default()
    }

    fn detail_str(details: Option<&Value>, key: &str) -> Option<String> {
        details
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .map(ToString::to_string)
    }
}
