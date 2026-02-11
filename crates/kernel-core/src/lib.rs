//! Deterministic phase executor and command queue with minimal NPC intent/action commits.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use contracts::{
    AccountingTransferState, ActionExecutionState, ActionTemplateDefinition, ActorRef,
    AffordanceVerb, AtomicOp, AtomicOpKind, BeliefClaimState, BeliefSource, Command,
    CommandPayload, CommitmentState, ContractCadence, ContractCompensationType,
    EmploymentContractRecord, Event, EventSynthesisTrace, EventType, FeasibilityCheck,
    GroupEntityState, HouseholdLedgerSnapshot, InstitutionProfileState, InstitutionQueueState,
    MarketClearingState, MobilityRouteState, MotiveFamily, NarrativeWhyChainSummary,
    NpcCapabilityProfile, NpcComposablePlanState, NpcDriveState, NpcHouseholdLedgerSnapshot,
    NpcOccupancyKind, NpcOccupancyState, OperatorDefinition, OpportunityState, PlanCandidateState,
    ProcessInstanceState, ProcessKind, ProcessStatus, ProductionNodeState, ReasonPacket,
    RelationshipEdgeState, RunConfig, RunMode, RunStatus, SettlementLaborMarketSnapshot,
    SettlementStockLedger, ShelterStatus, Snapshot, TimeBudgetState, TrustLevel, SCHEMA_VERSION_V1,
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
    profession: String,
    aspiration: String,
    social_class: String,
    temperament: String,
    hobbies: Vec<String>,
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
    health: i64,
    illness_ticks: i64,
}

#[derive(Debug, Clone)]
struct NpcTraitProfile {
    risk_tolerance: i64,
    sociability: i64,
    dutifulness: i64,
    ambition: i64,
    empathy: i64,
    resilience: i64,
    aggression: i64,
    patience: i64,
    honor: i64,
    generosity: i64,
    romance_drive: i64,
    gossip_drive: i64,
}

#[derive(Debug, Clone)]
struct ObservationRuntime {
    tick: u64,
    location_id: String,
    topic: String,
    salience: i64,
    source_event_id: String,
}

#[derive(Debug, Clone)]
struct ComposablePlanRuntimeState {
    npc_id: String,
    tick: u64,
    goal_family: String,
    template_id: String,
    base_action: String,
    composed_action: String,
    ops: Vec<AtomicOp>,
    rejected_alternatives: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProcessRuntimeState {
    process_id: String,
    kind: ProcessKind,
    status: ProcessStatus,
    stage: String,
    participants: Vec<String>,
    started_tick: u64,
    last_updated_tick: u64,
    last_event_id: Option<String>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
struct PairTensionState {
    anger: i64,
    losses: i64,
    coin_lost: i64,
    last_tick: u64,
}

#[derive(Debug, Clone, Default)]
struct PairEventMemory {
    last_conflict_tick: u64,
    last_romance_tick: u64,
    last_social_tick: u64,
}

#[derive(Debug, Clone)]
struct SocialReactionCandidate {
    reaction_key: &'static str,
    event_type: EventType,
    target_id: Option<String>,
    target_kind: Option<&'static str>,
    tags: Vec<String>,
    topic: Option<String>,
    weight: i64,
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

#[derive(Debug, Clone)]
struct OpportunityRuntimeState {
    opportunity_id: String,
    npc_id: String,
    location_id: String,
    action: String,
    source: String,
    opened_tick: u64,
    expires_tick: u64,
    utility_hint: i64,
    constraints: Vec<String>,
}

#[derive(Debug, Clone)]
struct CommitmentRuntimeState {
    commitment_id: String,
    npc_id: String,
    action_family: String,
    started_tick: u64,
    due_tick: u64,
    cadence_ticks: u64,
    progress_ticks: u64,
    inertia_score: i64,
    status: String,
}

#[derive(Debug, Clone)]
struct TimeBudgetRuntimeState {
    npc_id: String,
    tick: u64,
    sleep_hours: i64,
    work_hours: i64,
    care_hours: i64,
    travel_hours: i64,
    social_hours: i64,
    recovery_hours: i64,
    free_hours: i64,
}

#[derive(Debug, Clone)]
struct NpcDriveRuntimeState {
    npc_id: String,
    tick: u64,
    need_food: i64,
    need_shelter: i64,
    need_income: i64,
    need_safety: i64,
    need_belonging: i64,
    need_status: i64,
    need_recovery: i64,
    stress: i64,
    active_obligations: Vec<String>,
    active_aspirations: Vec<String>,
    moral_bounds: Vec<String>,
}

#[derive(Debug, Clone)]
struct NpcOccupancyRuntimeState {
    npc_id: String,
    tick: u64,
    occupancy: NpcOccupancyKind,
    state_tag: String,
    until_tick: u64,
    interruptible: bool,
    location_id: String,
    active_plan_id: Option<String>,
    active_operator_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ActionExecutionRuntimeState {
    execution_id: String,
    npc_id: String,
    tick: u64,
    plan_id: String,
    goal_id: String,
    action: String,
    active_step_index: usize,
    remaining_ticks: u64,
    interruption_policy: String,
    operator_chain_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct EventSynthesisTraceRuntimeState {
    trace_id: String,
    tick: u64,
    source_plan_id: String,
    source_operator_id: String,
    source_event_id: String,
    event_type: EventType,
    trigger_facts: Vec<String>,
    rejected_alternatives: Vec<String>,
}

#[derive(Debug, Clone)]
struct MarketClearingRuntimeState {
    settlement_id: String,
    staples_price_index: i64,
    fuel_price_index: i64,
    medicine_price_index: i64,
    wage_pressure: i64,
    shortage_score: i64,
    unmet_demand: i64,
    cleared_tick: u64,
    market_cleared: bool,
}

#[derive(Debug, Clone)]
struct AccountingTransferRuntimeState {
    transfer_id: String,
    tick: u64,
    settlement_id: String,
    from_account: String,
    to_account: String,
    resource_kind: String,
    amount: i64,
    cause_event_id: Option<String>,
}

#[derive(Debug, Clone)]
struct InstitutionQueueRuntimeState {
    settlement_id: String,
    pending_cases: i64,
    processed_cases: i64,
    dropped_cases: i64,
    avg_response_latency: i64,
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
    npc_capabilities_by_id: BTreeMap<String, NpcCapabilityProfile>,
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
    opportunities_by_npc: BTreeMap<String, Vec<OpportunityRuntimeState>>,
    commitments_by_npc: BTreeMap<String, CommitmentRuntimeState>,
    time_budget_by_npc: BTreeMap<String, TimeBudgetRuntimeState>,
    drives_by_npc: BTreeMap<String, NpcDriveRuntimeState>,
    occupancy_by_npc: BTreeMap<String, NpcOccupancyRuntimeState>,
    active_execution_by_npc: BTreeMap<String, ActionExecutionRuntimeState>,
    current_action_by_npc: BTreeMap<String, String>,
    operator_catalog: Vec<OperatorDefinition>,
    event_synthesis_trace: Vec<EventSynthesisTraceRuntimeState>,
    market_clearing_by_settlement: BTreeMap<String, MarketClearingRuntimeState>,
    accounting_transfers: Vec<AccountingTransferRuntimeState>,
    institution_queue_by_settlement: BTreeMap<String, InstitutionQueueRuntimeState>,
    observations_by_npc: BTreeMap<String, Vec<ObservationRuntime>>,
    active_plans_by_npc: BTreeMap<String, ComposablePlanRuntimeState>,
    process_instances_by_id: BTreeMap<String, ProcessRuntimeState>,
    tension_by_pair: BTreeMap<(String, String), PairTensionState>,
    pair_event_memory: BTreeMap<(String, String), PairEventMemory>,
    last_bandit_raid_tick_by_settlement: BTreeMap<String, u64>,
    settlement_conflict_cooldown_until: BTreeMap<String, u64>,
    last_social_channel_tick: BTreeMap<String, u64>,
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
    opportunities_by_npc: BTreeMap<String, Vec<OpportunityRuntimeState>>,
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
    goal_id: String,
    opportunities: Vec<OpportunityRuntimeState>,
    alternatives: Vec<ActionCandidate>,
    plan_candidates: Vec<PlanCandidateState>,
    chosen_action: Option<ActionCandidate>,
    chosen_plan_id: Option<String>,
    chosen_operator_chain_ids: Vec<String>,
    blocked_plan_ids: Vec<String>,
    started_new_step: bool,
    action_utility_score: i64,
    occupancy_hint: NpcOccupancyKind,
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
    goal_id: Option<String>,
    plan_id: Option<String>,
    operator_chain_ids: Vec<String>,
    blocked_plan_ids: Vec<String>,
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

        let npcs = default_npcs(config.seed, config.npc_count_min, config.npc_count_max);
        let mut npc_economy_by_id = default_npc_economy(&npcs, config.seed);
        let npc_traits_by_id = default_npc_traits(&npcs);
        let npc_capabilities_by_id = default_npc_capabilities(&npcs, &npc_traits_by_id);
        let households_by_id = default_households(&npcs, config.seed);
        let contracts_by_id = default_contracts(&npcs, config.seed);
        for (contract_id, state) in &contracts_by_id {
            if let Some(economy) = npc_economy_by_id.get_mut(&state.contract.worker_id) {
                economy.employer_contract_id = Some(contract_id.clone());
            }
        }
        let labor_market_by_settlement = default_labor_markets(&npcs, config.seed);
        let stock_by_settlement = default_stock_ledgers(&npcs, config.seed);
        let production_nodes_by_id = default_production_nodes(&npcs, config.seed);
        let relationship_edges = default_relationship_edges(&npcs);
        let beliefs_by_npc = default_beliefs(&npcs);
        let institutions_by_settlement = default_institutions(&npcs, config.seed);
        let groups_by_id = default_groups(&npcs, config.seed);
        let routes_by_id = default_routes();
        let market_clearing_by_settlement = default_market_clearing(&npcs);
        let institution_queue_by_settlement = default_institution_queue(&npcs);
        let item_registry = default_items(&npcs, config.seed);
        let operator_catalog = default_operator_catalog();

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
            item_registry,
            action_memory_by_npc: BTreeMap::new(),
            npc_economy_by_id,
            npc_traits_by_id,
            npc_capabilities_by_id,
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
            opportunities_by_npc: BTreeMap::new(),
            commitments_by_npc: BTreeMap::new(),
            time_budget_by_npc: BTreeMap::new(),
            drives_by_npc: BTreeMap::new(),
            occupancy_by_npc: BTreeMap::new(),
            active_execution_by_npc: BTreeMap::new(),
            current_action_by_npc: BTreeMap::new(),
            operator_catalog,
            event_synthesis_trace: Vec::new(),
            market_clearing_by_settlement,
            accounting_transfers: Vec::new(),
            institution_queue_by_settlement,
            observations_by_npc: BTreeMap::new(),
            active_plans_by_npc: BTreeMap::new(),
            process_instances_by_id: BTreeMap::new(),
            tension_by_pair: BTreeMap::new(),
            pair_event_memory: BTreeMap::new(),
            last_bandit_raid_tick_by_settlement: BTreeMap::new(),
            settlement_conflict_cooldown_until: BTreeMap::new(),
            last_social_channel_tick: BTreeMap::new(),
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
                profession: self
                    .npcs
                    .iter()
                    .find(|npc| npc.npc_id == *npc_id)
                    .map(|npc| npc.profession.clone())
                    .unwrap_or_else(|| "laborer".to_string()),
                aspiration: self
                    .npcs
                    .iter()
                    .find(|npc| npc.npc_id == *npc_id)
                    .map(|npc| npc.aspiration.clone())
                    .unwrap_or_else(|| "stability".to_string()),
                health: economy.health,
                illness_ticks: economy.illness_ticks,
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
        let capability_profiles = self
            .npc_capabilities_by_id
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let composable_plans = self
            .active_plans_by_npc
            .values()
            .map(|plan| NpcComposablePlanState {
                npc_id: plan.npc_id.clone(),
                tick: plan.tick,
                goal_family: plan.goal_family.clone(),
                template_id: plan.template_id.clone(),
                base_action: plan.base_action.clone(),
                composed_action: plan.composed_action.clone(),
                ops: plan.ops.clone(),
                rejected_alternatives: plan.rejected_alternatives.clone(),
            })
            .collect::<Vec<_>>();
        let process_instances = self
            .process_instances_by_id
            .values()
            .map(|process| ProcessInstanceState {
                process_id: process.process_id.clone(),
                kind: process.kind,
                status: process.status,
                stage: process.stage.clone(),
                participants: process.participants.clone(),
                started_tick: process.started_tick,
                last_updated_tick: process.last_updated_tick,
                last_event_id: process.last_event_id.clone(),
                metadata: process.metadata.clone(),
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
        let opportunities = self
            .opportunities_by_npc
            .values()
            .flat_map(|entries| entries.iter())
            .map(|entry| OpportunityState {
                opportunity_id: entry.opportunity_id.clone(),
                npc_id: entry.npc_id.clone(),
                location_id: entry.location_id.clone(),
                action: entry.action.clone(),
                source: entry.source.clone(),
                opened_tick: entry.opened_tick,
                expires_tick: entry.expires_tick,
                utility_hint: entry.utility_hint,
                constraints: entry.constraints.clone(),
            })
            .collect::<Vec<_>>();
        let commitments = self
            .commitments_by_npc
            .values()
            .map(|entry| CommitmentState {
                commitment_id: entry.commitment_id.clone(),
                npc_id: entry.npc_id.clone(),
                action_family: entry.action_family.clone(),
                started_tick: entry.started_tick,
                due_tick: entry.due_tick,
                cadence_ticks: entry.cadence_ticks,
                progress_ticks: entry.progress_ticks,
                inertia_score: entry.inertia_score,
                status: entry.status.clone(),
            })
            .collect::<Vec<_>>();
        let time_budgets = self
            .time_budget_by_npc
            .values()
            .map(|entry| TimeBudgetState {
                npc_id: entry.npc_id.clone(),
                tick: entry.tick,
                sleep_hours: entry.sleep_hours,
                work_hours: entry.work_hours,
                care_hours: entry.care_hours,
                travel_hours: entry.travel_hours,
                social_hours: entry.social_hours,
                recovery_hours: entry.recovery_hours,
                free_hours: entry.free_hours,
            })
            .collect::<Vec<_>>();
        let market_clearing = self
            .market_clearing_by_settlement
            .values()
            .map(|entry| MarketClearingState {
                settlement_id: entry.settlement_id.clone(),
                staples_price_index: entry.staples_price_index,
                fuel_price_index: entry.fuel_price_index,
                medicine_price_index: entry.medicine_price_index,
                wage_pressure: entry.wage_pressure,
                shortage_score: entry.shortage_score,
                unmet_demand: entry.unmet_demand,
                cleared_tick: entry.cleared_tick,
                market_cleared: entry.market_cleared,
            })
            .collect::<Vec<_>>();
        let accounting_transfers = self
            .accounting_transfers
            .iter()
            .rev()
            .take(256)
            .cloned()
            .map(|entry| AccountingTransferState {
                transfer_id: entry.transfer_id,
                tick: entry.tick,
                settlement_id: entry.settlement_id,
                from_account: entry.from_account,
                to_account: entry.to_account,
                resource_kind: entry.resource_kind,
                amount: entry.amount,
                cause_event_id: entry.cause_event_id,
            })
            .collect::<Vec<_>>();
        let institution_queue = self
            .institution_queue_by_settlement
            .values()
            .map(|entry| InstitutionQueueState {
                settlement_id: entry.settlement_id.clone(),
                pending_cases: entry.pending_cases,
                processed_cases: entry.processed_cases,
                dropped_cases: entry.dropped_cases,
                avg_response_latency: entry.avg_response_latency,
            })
            .collect::<Vec<_>>();
        let drives = self
            .drives_by_npc
            .values()
            .map(|entry| NpcDriveState {
                npc_id: entry.npc_id.clone(),
                tick: entry.tick,
                need_food: entry.need_food,
                need_shelter: entry.need_shelter,
                need_income: entry.need_income,
                need_safety: entry.need_safety,
                need_belonging: entry.need_belonging,
                need_status: entry.need_status,
                need_recovery: entry.need_recovery,
                stress: entry.stress,
                active_obligations: entry.active_obligations.clone(),
                active_aspirations: entry.active_aspirations.clone(),
                moral_bounds: entry.moral_bounds.clone(),
            })
            .collect::<Vec<_>>();
        let occupancy = self
            .occupancy_by_npc
            .values()
            .map(|entry| NpcOccupancyState {
                npc_id: entry.npc_id.clone(),
                tick: entry.tick,
                occupancy: entry.occupancy,
                state_tag: entry.state_tag.clone(),
                until_tick: entry.until_tick,
                interruptible: entry.interruptible,
                location_id: entry.location_id.clone(),
                active_plan_id: entry.active_plan_id.clone(),
                active_operator_id: entry.active_operator_id.clone(),
            })
            .collect::<Vec<_>>();
        let active_plans = self
            .active_execution_by_npc
            .values()
            .map(|entry| ActionExecutionState {
                execution_id: entry.execution_id.clone(),
                npc_id: entry.npc_id.clone(),
                tick: entry.tick,
                plan_id: entry.plan_id.clone(),
                goal_id: entry.goal_id.clone(),
                action: entry.action.clone(),
                active_step_index: entry.active_step_index,
                remaining_ticks: entry.remaining_ticks,
                interruption_policy: entry.interruption_policy.clone(),
                operator_chain_ids: entry.operator_chain_ids.clone(),
            })
            .collect::<Vec<_>>();
        let operator_effect_trace = self
            .event_synthesis_trace
            .iter()
            .rev()
            .take(256)
            .map(|entry| EventSynthesisTrace {
                trace_id: entry.trace_id.clone(),
                tick: entry.tick,
                run_id: self.status.run_id.clone(),
                source_plan_id: entry.source_plan_id.clone(),
                source_operator_id: entry.source_operator_id.clone(),
                source_event_id: entry.source_event_id.clone(),
                event_type: entry.event_type,
                trigger_facts: entry.trigger_facts.clone(),
                rejected_alternatives: entry.rejected_alternatives.clone(),
            })
            .collect::<Vec<_>>();
        let observations = self
            .observations_by_npc
            .iter()
            .map(|(npc_id, entries)| {
                let entry_values = entries
                    .iter()
                    .map(|entry| {
                        json!({
                            "tick": entry.tick,
                            "location_id": entry.location_id,
                            "topic": entry.topic,
                            "salience": entry.salience,
                            "source_event_id": entry.source_event_id,
                        })
                    })
                    .collect::<Vec<_>>();
                json!({
                    "npc_id": npc_id,
                    "entries": entry_values,
                })
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
                        "aggression": profile.aggression,
                        "patience": profile.patience,
                        "honor": profile.honor,
                        "generosity": profile.generosity,
                        "romance_drive": profile.romance_drive,
                        "gossip_drive": profile.gossip_drive,
                    }))
                    .collect::<Vec<_>>(),
                "npc_profiles": self
                    .npcs
                    .iter()
                    .map(|npc| json!({
                        "npc_id": npc.npc_id.clone(),
                        "location_id": npc.location_id.clone(),
                        "profession": npc.profession.clone(),
                        "aspiration": npc.aspiration.clone(),
                        "social_class": npc.social_class.clone(),
                        "temperament": npc.temperament.clone(),
                        "hobbies": npc.hobbies.clone(),
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
                "capabilities": capability_profiles.clone(),
                "composable_plans": composable_plans,
                "process_instances": process_instances,
                "narrative_summaries": narrative_summaries,
                "opportunities": opportunities,
                "commitments": commitments,
                "time_budgets": time_budgets,
                "drives": drives.clone(),
                "occupancy": occupancy.clone(),
                "active_plans": active_plans.clone(),
                "market_clearing": market_clearing,
                "accounting_transfers": accounting_transfers,
                "institution_queue": institution_queue,
                "operator_catalog": self.operator_catalog.clone(),
                "operator_effect_trace": operator_effect_trace.clone(),
                "observations": observations,
                "pair_tensions": self
                    .tension_by_pair
                    .iter()
                    .map(|((source_npc_id, target_npc_id), tension)| json!({
                        "source_npc_id": source_npc_id,
                        "target_npc_id": target_npc_id,
                        "anger": tension.anger,
                        "losses": tension.losses,
                        "coin_lost": tension.coin_lost,
                        "last_tick": tension.last_tick,
                    }))
                    .collect::<Vec<_>>(),
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
                        "aggression": profile.aggression,
                        "patience": profile.patience,
                        "honor": profile.honor,
                        "generosity": profile.generosity,
                        "romance_drive": profile.romance_drive,
                        "gossip_drive": profile.gossip_drive,
                    }))
                    .collect::<Vec<_>>(),
                "npc_profiles": self
                    .npcs
                    .iter()
                    .map(|npc| json!({
                        "npc_id": npc.npc_id.clone(),
                        "location_id": npc.location_id.clone(),
                        "profession": npc.profession.clone(),
                        "aspiration": npc.aspiration.clone(),
                        "social_class": npc.social_class.clone(),
                        "temperament": npc.temperament.clone(),
                        "hobbies": npc.hobbies.clone(),
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
                "npc_capabilities": capability_profiles,
                "drives": drives,
                "occupancy": occupancy,
                "active_plans": active_plans,
                "operator_effect_trace": operator_effect_trace,
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
                self.item_registry
                    .values()
                    .filter(|item| item.stolen)
                    .count(),
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
                    let (eviction_risk, rent_due_in_ticks, rent_amount) = self
                        .households_by_id
                        .get(&economy.household_id)
                        .map(|household| {
                            (
                                household.eviction_risk_score,
                                household.rent_due_tick.saturating_sub(execution.tick) as i64,
                                household.rent_amount,
                            )
                        })
                        .unwrap_or((0, 240, 0));
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
                    if rent_due_in_ticks <= 48 {
                        push_unique(&mut top_intents, "seek_income");
                        push_unique(&mut top_pressures, "rent_window_open");
                    }
                    if rent_due_in_ticks <= 24 && economy.wallet < rent_amount.max(2) {
                        push_unique(&mut top_intents, "seek_income");
                        push_unique(&mut top_pressures, "rent_deadline");
                    }
                    if rent_due_in_ticks <= 12 && economy.wallet < 2 {
                        push_unique(&mut top_intents, "seek_income");
                        push_unique(&mut top_pressures, "rent_deadline");
                    }
                }
                if economy.health <= 60 || economy.illness_ticks > 0 {
                    push_unique(&mut top_intents, "preserve_health");
                    push_unique(&mut top_pressures, "health_risk");
                }
                if economy.illness_ticks > 0 {
                    push_unique(&mut top_intents, "seek_treatment");
                    push_unique(&mut top_beliefs, "illness_is_active");
                }
                if economy.apprenticeship_progress >= 120 {
                    push_unique(&mut top_intents, "seek_opportunity");
                    push_unique(&mut top_beliefs, "skill_maturity_rising");
                } else if economy.apprenticeship_progress <= 20 {
                    push_unique(&mut top_beliefs, "skills_still_fragile");
                }
            }

            if let Some(contract_id) = economy.and_then(|value| value.employer_contract_id.as_ref())
            {
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

            if let Some(market) = self.labor_market_by_settlement.get(&npc.location_id) {
                let saturation_score = (market.underemployment_index / 12)
                    + if market.open_roles <= 1 {
                        3
                    } else if market.open_roles <= 2 {
                        2
                    } else if market.open_roles <= 3 {
                        1
                    } else {
                        0
                    };
                if saturation_score >= 5 {
                    push_unique(&mut top_pressures, "labor_market_saturated");
                    push_unique(&mut top_beliefs, "competition_for_coin_work_high");
                    push_unique(&mut top_intents, "seek_opportunity");
                } else if saturation_score <= 1 {
                    push_unique(&mut top_beliefs, "coin_work_market_open");
                }
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
                if profile.romance_drive >= 62 {
                    push_unique(&mut top_intents, "seek_companionship");
                }
                if profile.gossip_drive >= 58 {
                    push_unique(&mut top_intents, "exchange_stories");
                }
                if profile.aggression >= 65 && profile.patience <= 45 {
                    push_unique(&mut top_intents, "settle_grievance");
                }
                if profile.generosity >= 60 {
                    push_unique(&mut top_beliefs, "sharing_is_status_positive");
                }
                if profile.honor >= 62 {
                    push_unique(&mut top_beliefs, "public_reputation_matters");
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
                goal_id: "maintain_routine".to_string(),
                opportunities: Vec::new(),
                alternatives: Vec::new(),
                plan_candidates: Vec::new(),
                chosen_action: None,
                chosen_plan_id: None,
                chosen_operator_chain_ids: Vec::new(),
                blocked_plan_ids: Vec::new(),
                started_new_step: false,
                action_utility_score: 0,
                occupancy_hint: NpcOccupancyKind::Idle,
            });
        }

        decisions.sort_by(|a, b| a.npc_id.cmp(&b.npc_id));
        execution.npc_decisions = decisions;
    }

    fn phase_action_proposal(&mut self, execution: &mut TickExecution) {
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
            let health = economy.map(|entry| entry.health).unwrap_or(85);
            let illness_ticks = economy.map(|entry| entry.illness_ticks).unwrap_or(0);
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
            let (
                eviction_risk,
                rent_due_in_ticks,
                rent_amount,
                rent_reserve_coin,
                rent_cadence_ticks,
            ) = economy
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
            let local_conflict_pressure = self
                .event_log
                .iter()
                .rev()
                .take(96)
                .filter(|event| {
                    event.location_id == decision.location_id
                        && matches!(
                            event.event_type,
                            EventType::InsultExchanged
                                | EventType::PunchThrown
                                | EventType::BrawlStarted
                        )
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
                .map(|market| {
                    market.open_roles as i64 + (4 - market.underemployment_index / 10).max(0)
                })
                .unwrap_or(0)
                + self
                    .stock_by_settlement
                    .get(&decision.location_id)
                    .map(|stock| {
                        ((stock.staples / 6).max(0) + (stock.craft_inputs / 8).max(0)).min(8)
                    })
                    .unwrap_or(0);
            let default_profile = NpcTraitProfile {
                risk_tolerance: 50,
                sociability: 50,
                dutifulness: 50,
                ambition: 50,
                empathy: 50,
                resilience: 50,
                aggression: 50,
                patience: 50,
                honor: 50,
                generosity: 50,
                romance_drive: 50,
                gossip_drive: 50,
            };
            let profile = self
                .npc_traits_by_id
                .get(&decision.npc_id)
                .unwrap_or(&default_profile);

            let drive_state = build_drive_state(
                execution.tick,
                decision,
                profile,
                food_reserve_days,
                debt_balance,
                wallet,
                shelter_status,
                eviction_risk,
                law_case_load,
                illness_ticks,
                health,
                rent_due_in_ticks,
                rent_shortfall,
                local_conflict_pressure,
            );
            decision.goal_id = select_goal_id_from_drive(&drive_state);
            self.drives_by_npc
                .insert(decision.npc_id.clone(), drive_state);

            let time_budget =
                build_time_budget_for_tick(execution.tick, &decision.npc_id, profile, is_wanted);

            let mut alternatives = build_action_candidates(
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
                health,
                illness_ticks,
                dependents_count,
                shelter_status,
                contract_reliability,
                eviction_risk,
                rent_due_in_ticks,
                rent_shortfall,
                rent_cadence_ticks,
                trust_support,
                local_theft_pressure,
                local_conflict_pressure,
                local_support_need,
                low_pressure_economic_opportunity,
                profile,
                |tick, npc_id, action| {
                    self.deterministic_stream(tick, Phase::ActionProposal, npc_id, action)
                },
            );

            let action_memory = self.action_memory_by_npc.get(&decision.npc_id);
            let scarcity_pressure = decision.top_pressures.iter().any(|pressure| {
                matches!(
                    pressure.as_str(),
                    "food_tight" | "food_shock" | "seasonal_strain" | "cold_exposure_risk"
                )
            });
            let theft_desperation = wallet <= 2
                && !is_wanted
                && (debt_balance >= 4
                    || (rent_shortfall > 0 && rent_due_in_ticks <= 24 && eviction_risk >= 30)
                    || (food_reserve_days <= 1 && trust_support <= 1)
                    || (scarcity_pressure && trust_support <= 1 && wallet <= 1));
            apply_action_memory_bias(
                &mut alternatives,
                action_memory,
                is_wanted,
                law_case_load,
                execution.tick,
                shelter_status,
                food_reserve_days,
                debt_balance,
                eviction_risk,
                local_theft_pressure,
                local_support_need,
                theft_desperation,
                profile,
            );

            let opportunities = build_opportunities_from_candidates(
                execution.tick,
                decision,
                &alternatives,
                &time_budget,
                rent_due_in_ticks,
                rent_shortfall,
                law_case_load,
            );

            let constrained_alternatives = constrain_candidates_by_opportunities(
                execution.tick,
                &decision.npc_id,
                &alternatives,
                &opportunities,
            );

            decision.opportunities = opportunities.clone();
            decision.alternatives = constrained_alternatives;
            decision.plan_candidates = build_plan_candidates_from_alternatives(
                execution.tick,
                &decision.npc_id,
                &decision.goal_id,
                decision.alternatives.as_slice(),
                self.operator_catalog.as_slice(),
                law_case_load,
                local_conflict_pressure,
            );
            decision.blocked_plan_ids = decision
                .plan_candidates
                .iter()
                .filter(|candidate| !candidate.blocked_reasons.is_empty())
                .map(|candidate| candidate.plan_id.clone())
                .take(6)
                .collect::<Vec<_>>();
            execution
                .opportunities_by_npc
                .insert(decision.npc_id.clone(), opportunities);
        }
    }

    fn phase_action_resolution(&mut self, execution: &mut TickExecution) {
        execution.action_noise = self.deterministic_stream(
            execution.tick,
            Phase::ActionResolution,
            "npc_set",
            "action_resolution",
        );

        for decision in &mut execution.npc_decisions {
            decision.started_new_step = false;
            decision.chosen_plan_id = None;
            decision.chosen_operator_chain_ids.clear();
            decision.action_utility_score = 0;
            decision.occupancy_hint = NpcOccupancyKind::Idle;
            if let Some(occupancy) = self.occupancy_by_npc.get(&decision.npc_id) {
                if occupancy.until_tick > execution.tick
                    && occupancy.occupancy != NpcOccupancyKind::ExecutingPlanStep
                {
                    decision.chosen_action = None;
                    decision.occupancy_hint = occupancy.occupancy;
                    continue;
                }
            }
            let repetitive_streak = self
                .action_memory_by_npc
                .get(&decision.npc_id)
                .map(|memory| memory.repeat_streak)
                .unwrap_or_default();
            let emergency = decision_has_emergency_pressure(decision);
            if let Some(active_execution) = self.active_execution_by_npc.get(&decision.npc_id) {
                if active_execution.remaining_ticks > 0
                    && !should_interrupt_execution(
                        decision,
                        emergency,
                        execution.tick,
                        active_execution,
                    )
                {
                    if let Some(chosen) = candidate_from_action_name(
                        active_execution.action.as_str(),
                        execution.tick,
                        &decision.npc_id,
                        |tick, npc_id, action| {
                            self.deterministic_stream(tick, Phase::ActionResolution, npc_id, action)
                        },
                    ) {
                        decision.chosen_action = Some(chosen);
                        decision.chosen_plan_id = Some(active_execution.plan_id.clone());
                        decision.chosen_operator_chain_ids =
                            active_execution.operator_chain_ids.clone();
                        decision.action_utility_score = 0;
                        decision.occupancy_hint = NpcOccupancyKind::ExecutingPlanStep;
                        continue;
                    }
                }
            }

            let Some(chosen) =
                self.choose_candidate_for_decision(decision, execution.tick, repetitive_streak)
            else {
                decision.chosen_action = None;
                decision.occupancy_hint = NpcOccupancyKind::Resting;
                continue;
            };
            let chosen_score = self.candidate_context_utility(
                execution.tick,
                decision,
                &chosen,
                repetitive_streak,
            );
            let min_commit_threshold = min_commit_threshold_for_decision(
                self.drives_by_npc.get(&decision.npc_id),
                emergency,
            );
            if chosen_score < min_commit_threshold && !emergency {
                decision.chosen_action = None;
                decision.action_utility_score = chosen_score;
                decision.occupancy_hint = idle_occupancy_for_tick(execution.tick, &decision.npc_id);
                continue;
            }

            decision.action_utility_score = chosen_score;
            decision.chosen_action = Some(chosen.clone());
            decision.started_new_step = true;
            decision.occupancy_hint = NpcOccupancyKind::ExecutingPlanStep;
            let (plan_id, operator_chain_ids) = select_plan_for_action(
                decision.plan_candidates.as_slice(),
                chosen.action.as_str(),
                execution.tick,
                &decision.npc_id,
                |tick, npc_id, channel| {
                    self.deterministic_stream(tick, Phase::ActionResolution, npc_id, channel)
                },
            );
            decision.chosen_plan_id = Some(plan_id.clone());
            decision.chosen_operator_chain_ids = operator_chain_ids.clone();

            let mut ranked_alternatives = decision
                .alternatives
                .iter()
                .filter(|candidate| candidate.action != chosen.action)
                .cloned()
                .collect::<Vec<_>>();
            ranked_alternatives.sort_by(|left, right| {
                (right.priority, right.score, &right.action).cmp(&(
                    left.priority,
                    left.score,
                    &left.action,
                ))
            });
            let alternatives_considered = ranked_alternatives
                .into_iter()
                .take(8)
                .map(|candidate| candidate.action)
                .collect::<Vec<_>>();
            let composed_plan = self.compose_plan_for_choice(
                execution.tick,
                decision,
                &chosen,
                &alternatives_considered,
            );

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
            let mut why_chain = self.build_why_chain(
                &decision.top_intents,
                &decision.top_beliefs,
                &decision.top_pressures,
                chosen.verb,
            );
            why_chain.push(format!("template::{}", composed_plan.template_id));
            for op in composed_plan.ops.iter().take(5) {
                why_chain.push(format!("op::{:?}", op.kind));
            }
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
                goal_id: Some(decision.goal_id.clone()),
                plan_id: Some(plan_id.clone()),
                operator_chain_ids: operator_chain_ids.clone(),
                blocked_plan_ids: decision.blocked_plan_ids.clone(),
                selection_rationale: format!(
                    "selected goal '{}' action '{}' (utility={}) with composed action '{}'",
                    decision.goal_id, chosen.action, chosen_score, composed_plan.composed_action
                ),
            };

            if execution.tick % 4 == 0 {
                if let Some(opportunity) = decision
                    .opportunities
                    .iter()
                    .find(|entry| entry.action == chosen.action)
                {
                    execution.planned_events.push(PlannedEvent {
                        event_type: EventType::OpportunityOpened,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        targets: vec![ActorRef {
                            actor_id: opportunity.opportunity_id.clone(),
                            actor_kind: "opportunity".to_string(),
                        }],
                        caused_by: Vec::new(),
                        tags: vec![
                            "opportunity".to_string(),
                            "opened".to_string(),
                            "accepted_path".to_string(),
                        ],
                        details: Some(json!({
                            "opportunity_id": opportunity.opportunity_id,
                            "action": chosen.action,
                            "source": opportunity.source,
                            "utility_hint": opportunity.utility_hint,
                            "expires_tick": opportunity.expires_tick,
                        })),
                        reason_draft: None,
                        state_effects: PlannedStateEffects::default(),
                    });

                    execution.planned_events.push(PlannedEvent {
                        event_type: EventType::OpportunityAccepted,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        targets: vec![ActorRef {
                            actor_id: opportunity.opportunity_id.clone(),
                            actor_kind: "opportunity".to_string(),
                        }],
                        caused_by: Vec::new(),
                        tags: vec!["opportunity".to_string(), "accepted".to_string()],
                        details: Some(json!({
                            "opportunity_id": opportunity.opportunity_id,
                            "action": chosen.action,
                            "source": opportunity.source,
                            "utility_hint": opportunity.utility_hint,
                        })),
                        reason_draft: None,
                        state_effects: PlannedStateEffects::default(),
                    });
                }
            }

            if execution.tick % 24 == 0 {
                if let Some(rejected) = decision
                    .opportunities
                    .iter()
                    .find(|entry| entry.action != chosen.action)
                {
                    execution.planned_events.push(PlannedEvent {
                        event_type: EventType::OpportunityRejected,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        targets: vec![ActorRef {
                            actor_id: rejected.opportunity_id.clone(),
                            actor_kind: "opportunity".to_string(),
                        }],
                        caused_by: Vec::new(),
                        tags: vec!["opportunity".to_string(), "rejected".to_string()],
                        details: Some(json!({
                            "opportunity_id": rejected.opportunity_id,
                            "rejected_action": rejected.action,
                            "accepted_action": chosen.action,
                        })),
                        reason_draft: None,
                        state_effects: PlannedStateEffects::default(),
                    });
                }
            }

            if decision.started_new_step {
                execution.planned_events.push(PlannedEvent {
                    event_type: EventType::NpcActionCommitted,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec![
                        "npc_action".to_string(),
                        chosen.action.clone(),
                        canonical_action_name(chosen.action.as_str()).to_string(),
                    ],
                    details: Some(json!({
                        "chosen_action": canonical_action_name(chosen.action.as_str()),
                        "action_label": chosen.action,
                        "canonical_action": canonical_action_name(chosen.action.as_str()),
                        "action_variant": action_variant_name(chosen.action.as_str()),
                        "composed_action": composed_plan.composed_action,
                        "action_template_id": composed_plan.template_id,
                        "action_goal_family": composed_plan.goal_family,
                        "composed_ops": composed_plan.ops,
                        "top_intents": decision.top_intents,
                        "top_beliefs": decision.top_beliefs,
                        "top_pressures": decision.top_pressures,
                        "rejected_alternatives": composed_plan.rejected_alternatives,
                        "goal_id": decision.goal_id,
                        "plan_id": plan_id,
                        "operator_chain_ids": operator_chain_ids,
                        "blocked_plan_ids": decision.blocked_plan_ids,
                        "utility_score": chosen_score,
                    })),
                    reason_draft: Some(reason_draft),
                    state_effects: PlannedStateEffects::default(),
                });

                let consequence_events =
                    self.action_consequence_events(execution.tick, decision, &chosen);
                execution.planned_events.extend(consequence_events);
            }
        }
    }

    fn choose_candidate_for_decision(
        &self,
        decision: &NpcDecision,
        tick: u64,
        repetitive_streak: u32,
    ) -> Option<ActionCandidate> {
        if decision.alternatives.is_empty() {
            return None;
        }

        let mut ranked = decision
            .alternatives
            .iter()
            .map(|candidate| {
                (
                    self.candidate_context_utility(tick, decision, candidate, repetitive_streak),
                    candidate.clone(),
                )
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            (right.0, right.1.priority, right.1.score, &right.1.action).cmp(&(
                left.0,
                left.1.priority,
                left.1.score,
                &left.1.action,
            ))
        });
        let (mut chosen_score, mut chosen) = ranked.first().cloned()?;

        if let Some(commitment) = self.commitments_by_npc.get(&decision.npc_id) {
            let emergency = decision_has_emergency_pressure(decision);
            let commitment_due = tick >= commitment.due_tick;
            if !emergency && commitment.inertia_score > 0 {
                if let Some((continue_score, continue_candidate)) = ranked
                    .iter()
                    .find(|(_, candidate)| {
                        commitment_family_for_action(candidate.action.as_str())
                            == commitment.action_family
                    })
                    .cloned()
                {
                    let continuation_margin = (if commitment_due { 220 } else { 540 })
                        - i64::from(repetitive_streak.min(6)) * 60;
                    if commitment_family_for_action(chosen.action.as_str())
                        != commitment.action_family
                        && chosen_score <= continue_score + continuation_margin.max(120)
                    {
                        chosen_score = continue_score;
                        chosen = continue_candidate;
                    }
                }
            }
        }

        let emergency = decision_has_emergency_pressure(decision);
        let diversification_phase = (hash_bytes(decision.npc_id.as_bytes()) % 18) as u64;
        if !emergency && tick % 18 == diversification_phase {
            if let Some((alternative_score, alternative)) =
                ranked.iter().find(|(score, candidate)| {
                    canonical_action_name(candidate.action.as_str())
                        != canonical_action_name(chosen.action.as_str())
                        && !matches!(
                            commitment_family_for_action(candidate.action.as_str()),
                            "livelihood" | "rent" | "shelter"
                        )
                        && *score + 2_800 >= chosen_score
                })
            {
                chosen_score = *alternative_score;
                chosen = alternative.clone();
            }
        }
        if !emergency && repetitive_streak >= 2 && tick % 12 == 0 {
            let chosen_family = commitment_family_for_action(chosen.action.as_str());
            if let Some((_, alternative)) = ranked.iter().find(|(score, candidate)| {
                commitment_family_for_action(candidate.action.as_str()) != chosen_family
                    && canonical_action_name(candidate.action.as_str())
                        != canonical_action_name(chosen.action.as_str())
                    && *score + 2_400 >= chosen_score
            }) {
                chosen = alternative.clone();
            }
        } else if !emergency && repetitive_streak >= 4 && tick % 24 == 0 {
            if let Some((_, alternative)) = ranked.iter().find(|(score, candidate)| {
                canonical_action_name(candidate.action.as_str())
                    != canonical_action_name(chosen.action.as_str())
                    && *score + 800 >= chosen_score
                    && matches!(
                        canonical_action_name(candidate.action.as_str()),
                        "work_for_coin"
                            | "work_for_food"
                            | "trade_visit"
                            | "converse_neighbor"
                            | "lend_coin"
                            | "question_travelers"
                            | "mediate_dispute"
                            | "observe_notable_event"
                            | "forage"
                            | "share_rumor"
                            | "patrol_road"
                    )
            }) {
                chosen = alternative.clone();
            }
        }

        let scarcity_pressure = decision.top_pressures.iter().any(|pressure| {
            matches!(
                pressure.as_str(),
                "food_tight" | "food_shock" | "seasonal_strain" | "cold_exposure_risk"
            )
        });
        let financial_pressure = decision
            .top_pressures
            .iter()
            .any(|pressure| matches!(pressure.as_str(), "debt_strain" | "rent_deadline"));
        if canonical_action_name(chosen.action.as_str()) != "steal_supplies"
            && scarcity_pressure
            && financial_pressure
            && !decision
                .top_intents
                .iter()
                .any(|intent| intent == "stabilize_security")
        {
            if let Some((steal_score, steal_candidate)) = ranked
                .iter()
                .find(|(_, candidate)| canonical_action_name(candidate.action.as_str()) == "steal_supplies")
            {
                let roll = self.deterministic_stream(
                    tick,
                    Phase::ActionResolution,
                    &decision.npc_id,
                    "desperation_theft_override",
                ) % 100;
                if roll < 18 && *steal_score + 2_200 >= chosen_score {
                    chosen = steal_candidate.clone();
                }
            }
        }

        Some(chosen)
    }

    fn candidate_context_utility(
        &self,
        tick: u64,
        decision: &NpcDecision,
        candidate: &ActionCandidate,
        repetitive_streak: u32,
    ) -> i64 {
        let mut score = i64::from(candidate.priority) * 2_200 + candidate.score as i64;
        let action = canonical_action_name(candidate.action.as_str());
        let has_intent = |needle: &str| decision.top_intents.iter().any(|intent| intent == needle);
        let has_pressure = |needle: &str| {
            decision
                .top_pressures
                .iter()
                .any(|pressure| pressure == needle)
        };

        let economic_urgent = has_intent("seek_income")
            || has_intent("seek_opportunity")
            || has_pressure("debt_strain")
            || has_pressure("rent_deadline")
            || has_pressure("rent_window_open");
        let hunger_urgent = has_intent("secure_food") || has_pressure("household_hunger");
        let shelter_urgent = has_intent("seek_shelter") || has_pressure("shelter_instability");
        let social_urgent = has_intent("build_trust") || has_pressure("social_support_thin");
        let rumor_urgent = has_intent("investigate_rumor") || has_pressure("information_rising");
        let security_urgent = has_intent("stabilize_security")
            || has_pressure("institutional_corruption")
            || has_intent("evade_patrols");
        let labor_market_saturated = has_pressure("labor_market_saturated");
        let day_phase = tick % 24;
        let work_window = (6..=16).contains(&day_phase);
        let social_window = (17..=22).contains(&day_phase);
        let quiet_window = day_phase <= 5;
        let (same_action_count, total_action_count) =
            self.recent_action_stats(tick, &decision.location_id, action, 48);
        if total_action_count >= 30 {
            let action_share_pct = (same_action_count * 100) / total_action_count.max(1);
            if action_share_pct >= 28 {
                score -= ((action_share_pct - 28) * 85).clamp(0, 3_400);
            } else if action_share_pct <= 6 {
                score += ((6 - action_share_pct) * 18).clamp(0, 260);
            }
            if action_share_pct >= 45 {
                score -= 1_200;
            }
        }

        match action {
            "work_for_coin" => {
                if economic_urgent {
                    score += 1_500;
                }
                if has_pressure("debt_strain") {
                    score += 650;
                }
                if labor_market_saturated {
                    score -= if economic_urgent { 900 } else { 1_900 };
                }
                if !economic_urgent {
                    score -= 1_900;
                }
                if !economic_urgent && !work_window {
                    score -= 1_450;
                }
                if repetitive_streak >= 2 && !economic_urgent {
                    score -= 1_400 + i64::from(repetitive_streak) * 300;
                }
            }
            "work_for_food" | "forage" => {
                if hunger_urgent {
                    score += 1_600;
                } else {
                    score -= 450;
                }
            }
            "pay_rent" => {
                if has_pressure("rent_deadline")
                    || has_pressure("rent_window_open")
                    || shelter_urgent
                {
                    score += 2_000;
                } else {
                    score -= 1_400;
                }
            }
            "seek_shelter" => {
                if shelter_urgent {
                    score += 1_700;
                } else {
                    score -= 900;
                }
            }
            "craft_goods" | "tend_fields" => {
                if economic_urgent {
                    score += 900;
                }
                if social_urgent && !economic_urgent {
                    score -= 450;
                }
                if labor_market_saturated {
                    score += 900;
                }
                if work_window {
                    score += 350;
                } else if !economic_urgent {
                    score -= 700;
                }
                if total_action_count >= 24 && same_action_count * 100 / total_action_count >= 40 {
                    score -= 1_100;
                }
            }
            "trade_visit" => {
                if economic_urgent || rumor_urgent {
                    score += 800;
                }
                if labor_market_saturated {
                    score += 500;
                }
                if social_window {
                    score += 280;
                }
            }
            "converse_neighbor"
            | "lend_coin"
            | "share_meal"
            | "form_mutual_aid_group"
            | "mediate_dispute"
            | "court_romance" => {
                if social_urgent {
                    score += 1_300;
                } else {
                    score -= 500;
                }
                if social_window {
                    score += 700;
                } else if work_window {
                    score -= 280;
                }
            }
            "investigate_rumor" | "question_travelers" | "share_rumor" => {
                if rumor_urgent {
                    score += 1_200;
                } else {
                    score -= 600;
                }
            }
            "observe_notable_event" => {
                if rumor_urgent || social_urgent || security_urgent {
                    score += 1_050;
                } else {
                    score -= 350;
                }
                if (8..=20).contains(&day_phase) {
                    score += 250;
                }
            }
            "seek_treatment" => {
                if has_pressure("seasonal_strain")
                    || has_pressure("cold_exposure_risk")
                    || has_pressure("household_hunger")
                    || has_pressure("health_risk")
                    || has_intent("seek_treatment")
                    || has_intent("preserve_health")
                {
                    score += 1_100;
                } else {
                    score -= 300;
                }
            }
            "organize_watch" | "patrol_road" | "collect_testimony" | "defend_patron" => {
                if security_urgent {
                    score += 1_000;
                } else {
                    score -= 700;
                }
            }
            "steal_supplies" | "fence_goods" => {
                let opportunistic_drive = has_intent("seek_opportunity")
                    || has_pressure("labor_market_saturated")
                    || has_intent("settle_grievance");
                if hunger_urgent || economic_urgent {
                    score += 200;
                } else if opportunistic_drive {
                    score -= 850;
                } else {
                    score -= 2_600;
                }
                if has_pressure("rent_deadline") {
                    score += 900;
                }
                if has_pressure("debt_strain") {
                    score += 600;
                }
                if has_pressure("food_tight") || has_pressure("food_shock") {
                    score += 2_000;
                }
                if has_pressure("seasonal_strain") || has_pressure("cold_exposure_risk") {
                    score += 1_400;
                }
                if has_pressure("rent_deadline") && has_pressure("debt_strain") {
                    score += 900;
                }
                if labor_market_saturated {
                    score += 500;
                }
            }
            _ => {}
        }

        if action == "court_romance" {
            if same_action_count >= 6 {
                score -= 1_400;
            }
            if quiet_window {
                score -= 900;
            }
            if has_pressure("debt_strain") || has_pressure("rent_deadline") {
                score -= 700;
            }
        }

        if matches!(
            action,
            "craft_goods" | "tend_fields" | "work_for_coin"
        ) && social_window
            && !economic_urgent
            && !hunger_urgent
            && !shelter_urgent
        {
            score -= 650;
        }

        if !hunger_urgent && !shelter_urgent {
            if (16..=20).contains(&day_phase)
                && matches!(
                    action,
                    "share_meal"
                        | "form_mutual_aid_group"
                        | "mediate_dispute"
                        | "converse_neighbor"
                        | "lend_coin"
                        | "court_romance"
                )
            {
                score += 850;
            }
            if (8..=11).contains(&day_phase)
                && matches!(
                    action,
                    "investigate_rumor"
                        | "question_travelers"
                        | "share_rumor"
                        | "observe_notable_event"
                        | "patrol_road"
                        | "collect_testimony"
                )
            {
                score += 700;
            }
            if (16..=20).contains(&day_phase)
                && matches!(action, "craft_goods" | "tend_fields")
            {
                score -= 520;
            }
        }

        if let Some(memory) = self.action_memory_by_npc.get(&decision.npc_id) {
            if memory.last_action.as_deref() == Some(action) {
                score -= 900 + i64::from(repetitive_streak) * 450;
            } else {
                score += (i64::from(repetitive_streak) * 180).clamp(0, 700);
            }
        }

        if matches!(action, "craft_goods" | "tend_fields")
            && repetitive_streak >= 2
            && !hunger_urgent
            && !shelter_urgent
        {
            score -= 900 + i64::from(repetitive_streak) * 220;
        }

        score
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
        let projected_stolen =
            self.projected_stolen_item_count(execution.planned_events.as_slice());
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
        self.refresh_time_budgets_from_execution(
            execution.tick,
            execution.npc_decisions.as_slice(),
        );
        self.sync_action_execution_for_tick(execution.tick, execution.npc_decisions.as_slice());

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

        self.sync_opportunities_for_tick(
            execution.tick,
            &system_event_id,
            &mut sequence_in_tick,
            &mut causal_chain,
            &execution.opportunities_by_npc,
        );

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
                    goal_id: draft.goal_id.clone(),
                    plan_id: draft.plan_id.clone(),
                    operator_chain_ids: draft.operator_chain_ids.clone(),
                    blocked_plan_ids: draft.blocked_plan_ids.clone(),
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
            if let Some(last_event) = self.event_log.last().cloned() {
                self.record_event_synthesis_trace(execution.tick, &last_event);
            }
            self.apply_planned_state_effects(&planned.state_effects, execution.tick);
            sequence_in_tick += 1;
        }

        self.sync_commitments_for_tick(
            execution.tick,
            &system_event_id,
            &mut sequence_in_tick,
            &mut causal_chain,
            execution.npc_decisions.as_slice(),
        );

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

        self.emit_accounting_transfer_summary(
            execution.tick,
            &system_event_id,
            &mut sequence_in_tick,
            &mut causal_chain,
        );

        self.normalize_tick_event_ordering(execution.tick);
        self.refresh_composable_runtime_from_tick_events(execution.tick);
        self.reason_packet_log.extend(pending_reason_packets);

        self.last_tick_terminal_event_id = self
            .event_log
            .iter()
            .rev()
            .find(|event| event.tick == execution.tick)
            .map(|event| event.event_id.clone());
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
        self.state_hash =
            mix64(self.state_hash ^ sum_positive(self.law_case_load_by_settlement.values()) as u64);
        self.state_hash = mix64(self.state_hash ^ self.wanted_npcs.len() as u64);
        self.state_hash = mix64(
            self.state_hash
                ^ self
                    .item_registry
                    .values()
                    .filter(|item| item.stolen)
                    .count() as u64,
        );
        self.state_hash =
            mix64(self.state_hash ^ self.last_bandit_raid_tick_by_settlement.len() as u64);
        self.state_hash =
            mix64(self.state_hash ^ self.settlement_conflict_cooldown_until.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.pair_event_memory.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.last_social_channel_tick.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.households_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.contracts_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.relationship_edges.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.groups_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.routes_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.npc_capabilities_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.active_plans_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.process_instances_by_id.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.opportunities_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.commitments_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.time_budget_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.drives_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.occupancy_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.active_execution_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.current_action_by_npc.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.operator_catalog.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.event_synthesis_trace.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.market_clearing_by_settlement.len() as u64);
        self.state_hash =
            mix64(self.state_hash ^ self.institution_queue_by_settlement.len() as u64);
        self.state_hash = mix64(self.state_hash ^ self.accounting_transfers.len() as u64);
        self.state_hash = mix64(
            self.state_hash
                ^ self
                    .npc_economy_by_id
                    .values()
                    .map(|state| {
                        (state.wallet
                            + state.debt_balance
                            + state.food_reserve_days
                            + state.apprenticeship_progress
                            + state.health
                            + state.illness_ticks)
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
            self.state_hash = mix64(self.state_hash ^ profile.aggression.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.patience.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.honor.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.generosity.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.romance_drive.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.gossip_drive.max(0) as u64);
        }
        for (npc_id, profile) in &self.npc_capabilities_by_id {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ profile.physical.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.social.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.trade.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.combat.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.literacy.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.influence.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.stealth.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.care.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ profile.law.max(0) as u64);
        }
        for (npc_id, plan) in &self.active_plans_by_npc {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ plan.tick);
            self.state_hash = mix64(self.state_hash ^ hash_bytes(plan.goal_family.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(plan.template_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(plan.base_action.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(plan.composed_action.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ plan.ops.len() as u64);
            self.state_hash = mix64(self.state_hash ^ plan.rejected_alternatives.len() as u64);
        }
        for (npc_id, drive) in &self.drives_by_npc {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ drive.need_food.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ drive.need_shelter.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ drive.need_income.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ drive.need_safety.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ drive.need_recovery.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ drive.stress.max(0) as u64);
        }
        for (npc_id, occupancy) in &self.occupancy_by_npc {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ occupancy.tick);
            self.state_hash = mix64(self.state_hash ^ occupancy.until_tick);
            self.state_hash = mix64(self.state_hash ^ hash_bytes(occupancy.state_tag.as_bytes()));
            self.state_hash = mix64(
                self.state_hash
                    ^ match occupancy.occupancy {
                        NpcOccupancyKind::Idle => 1,
                        NpcOccupancyKind::Resting => 2,
                        NpcOccupancyKind::Loitering => 3,
                        NpcOccupancyKind::Traveling => 4,
                        NpcOccupancyKind::ExecutingPlanStep => 5,
                        NpcOccupancyKind::Recovering => 6,
                        NpcOccupancyKind::SocialPresence => 7,
                    },
            );
        }
        for (process_id, process) in &self.process_instances_by_id {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(process_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(process.stage.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ process.participants.len() as u64);
            self.state_hash = mix64(self.state_hash ^ process.started_tick);
            self.state_hash = mix64(self.state_hash ^ process.last_updated_tick);
            self.state_hash = mix64(
                self.state_hash
                    ^ match process.kind {
                        ProcessKind::Romance => 1,
                        ProcessKind::BusinessPartnership => 2,
                        ProcessKind::ConflictSpiral => 3,
                        ProcessKind::HouseholdFormation => 4,
                        ProcessKind::Apprenticeship => 5,
                    },
            );
            self.state_hash = mix64(
                self.state_hash
                    ^ match process.status {
                        ProcessStatus::Active => 1,
                        ProcessStatus::Dormant => 2,
                        ProcessStatus::Resolved => 3,
                        ProcessStatus::Failed => 4,
                    },
            );
        }
        for npc in &self.npcs {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc.npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc.profession.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc.aspiration.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc.social_class.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc.temperament.as_bytes()));
        }
        for (npc_id, observations) in &self.observations_by_npc {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ observations.len() as u64);
            if let Some(last) = observations.last() {
                self.state_hash = mix64(self.state_hash ^ last.tick);
                self.state_hash = mix64(self.state_hash ^ hash_bytes(last.topic.as_bytes()));
            }
        }
        for ((source_npc_id, target_npc_id), tension) in &self.tension_by_pair {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(source_npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(target_npc_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ tension.anger.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ tension.losses.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ tension.coin_lost.max(0) as u64);
            self.state_hash = mix64(self.state_hash ^ tension.last_tick);
        }
        for ((left, right), memory) in &self.pair_event_memory {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(left.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ hash_bytes(right.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ memory.last_conflict_tick);
            self.state_hash = mix64(self.state_hash ^ memory.last_romance_tick);
            self.state_hash = mix64(self.state_hash ^ memory.last_social_tick);
        }
        for (settlement_id, tick_mark) in &self.last_bandit_raid_tick_by_settlement {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(settlement_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ *tick_mark);
        }
        for (settlement_id, cooldown_until) in &self.settlement_conflict_cooldown_until {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(settlement_id.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ *cooldown_until);
        }
        for (channel_key, tick_mark) in &self.last_social_channel_tick {
            self.state_hash = mix64(self.state_hash ^ hash_bytes(channel_key.as_bytes()));
            self.state_hash = mix64(self.state_hash ^ *tick_mark);
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

    fn sync_action_execution_for_tick(&mut self, tick: u64, decisions: &[NpcDecision]) {
        self.current_action_by_npc.clear();
        let mut seen = BTreeSet::<String>::new();
        for decision in decisions {
            seen.insert(decision.npc_id.clone());
            if let Some(chosen) = decision.chosen_action.as_ref() {
                let canonical = canonical_action_name(chosen.action.as_str()).to_string();
                self.current_action_by_npc
                    .insert(decision.npc_id.clone(), canonical.clone());
                if decision.started_new_step {
                    let execution = ActionExecutionRuntimeState {
                        execution_id: format!("exec_{:06}_{}", tick, decision.npc_id),
                        npc_id: decision.npc_id.clone(),
                        tick,
                        plan_id: decision
                            .chosen_plan_id
                            .clone()
                            .unwrap_or_else(|| format!("plan_{:06}_{}", tick, decision.npc_id)),
                        goal_id: decision.goal_id.clone(),
                        action: canonical,
                        active_step_index: 0,
                        remaining_ticks: action_duration_ticks(chosen.action.as_str()),
                        interruption_policy: "soft".to_string(),
                        operator_chain_ids: decision.chosen_operator_chain_ids.clone(),
                    };
                    self.active_execution_by_npc
                        .insert(decision.npc_id.clone(), execution.clone());
                    self.occupancy_by_npc.insert(
                        decision.npc_id.clone(),
                        NpcOccupancyRuntimeState {
                            npc_id: decision.npc_id.clone(),
                            tick,
                            occupancy: NpcOccupancyKind::ExecutingPlanStep,
                            state_tag: format!("executing:{}", chosen.action),
                            until_tick: tick + execution.remaining_ticks,
                            interruptible: false,
                            location_id: decision.location_id.clone(),
                            active_plan_id: Some(execution.plan_id.clone()),
                            active_operator_id: execution.operator_chain_ids.first().cloned(),
                        },
                    );
                } else if let Some(execution) =
                    self.active_execution_by_npc.get_mut(&decision.npc_id)
                {
                    execution.tick = tick;
                    execution.active_step_index = execution.active_step_index.saturating_add(1);
                    execution.remaining_ticks = execution.remaining_ticks.saturating_sub(1);
                    if execution.remaining_ticks == 0 {
                        self.occupancy_by_npc.insert(
                            decision.npc_id.clone(),
                            NpcOccupancyRuntimeState {
                                npc_id: decision.npc_id.clone(),
                                tick,
                                occupancy: NpcOccupancyKind::Loitering,
                                state_tag: "cooldown_presence".to_string(),
                                until_tick: tick + 2,
                                interruptible: true,
                                location_id: decision.location_id.clone(),
                                active_plan_id: None,
                                active_operator_id: None,
                            },
                        );
                    } else {
                        self.occupancy_by_npc.insert(
                            decision.npc_id.clone(),
                            NpcOccupancyRuntimeState {
                                npc_id: decision.npc_id.clone(),
                                tick,
                                occupancy: NpcOccupancyKind::ExecutingPlanStep,
                                state_tag: format!("executing:{}", chosen.action),
                                until_tick: tick + execution.remaining_ticks,
                                interruptible: false,
                                location_id: decision.location_id.clone(),
                                active_plan_id: Some(execution.plan_id.clone()),
                                active_operator_id: execution.operator_chain_ids.first().cloned(),
                            },
                        );
                    }
                } else {
                    self.occupancy_by_npc.insert(
                        decision.npc_id.clone(),
                        NpcOccupancyRuntimeState {
                            npc_id: decision.npc_id.clone(),
                            tick,
                            occupancy: NpcOccupancyKind::ExecutingPlanStep,
                            state_tag: format!("executing:{}", chosen.action),
                            until_tick: tick + 1,
                            interruptible: true,
                            location_id: decision.location_id.clone(),
                            active_plan_id: decision.chosen_plan_id.clone(),
                            active_operator_id: decision.chosen_operator_chain_ids.first().cloned(),
                        },
                    );
                }
            } else {
                self.active_execution_by_npc.remove(&decision.npc_id);
                self.occupancy_by_npc.insert(
                    decision.npc_id.clone(),
                    NpcOccupancyRuntimeState {
                        npc_id: decision.npc_id.clone(),
                        tick,
                        occupancy: decision.occupancy_hint,
                        state_tag: occupancy_state_tag(decision.occupancy_hint).to_string(),
                        until_tick: tick + 2,
                        interruptible: true,
                        location_id: decision.location_id.clone(),
                        active_plan_id: None,
                        active_operator_id: None,
                    },
                );
            }
        }

        self.active_execution_by_npc
            .retain(|npc_id, _| seen.contains(npc_id));
        self.current_action_by_npc
            .retain(|npc_id, _| seen.contains(npc_id));
    }

    fn refresh_time_budgets_from_execution(&mut self, tick: u64, decisions: &[NpcDecision]) {
        for decision in decisions {
            let default_profile = NpcTraitProfile {
                risk_tolerance: 50,
                sociability: 50,
                dutifulness: 50,
                ambition: 50,
                empathy: 50,
                resilience: 50,
                aggression: 50,
                patience: 50,
                honor: 50,
                generosity: 50,
                romance_drive: 50,
                gossip_drive: 50,
            };
            let profile = self
                .npc_traits_by_id
                .get(&decision.npc_id)
                .unwrap_or(&default_profile);
            let mut budget = build_time_budget_for_tick(
                tick,
                &decision.npc_id,
                profile,
                self.wanted_npcs.contains(&decision.npc_id),
            );
            if let Some(chosen) = decision.chosen_action.as_ref() {
                match chosen.verb {
                    AffordanceVerb::WorkForCoin
                    | AffordanceVerb::WorkForFood
                    | AffordanceVerb::TendFields
                    | AffordanceVerb::CraftGoods => {
                        budget.work_hours = (budget.work_hours + 2).min(12);
                    }
                    AffordanceVerb::TradeVisit
                    | AffordanceVerb::PatrolRoad
                    | AffordanceVerb::InvestigateRumor => {
                        budget.travel_hours = (budget.travel_hours + 2).min(8);
                    }
                    AffordanceVerb::ShareMeal
                    | AffordanceVerb::ConverseNeighbor
                    | AffordanceVerb::LendCoin
                    | AffordanceVerb::CourtRomance
                    | AffordanceVerb::FormMutualAidGroup => {
                        budget.social_hours = (budget.social_hours + 1).min(8);
                    }
                    AffordanceVerb::SeekTreatment => {
                        budget.recovery_hours = (budget.recovery_hours + 2).min(8);
                    }
                    AffordanceVerb::ObserveNotableEvent => {
                        budget.free_hours = (budget.free_hours + 1).min(8);
                    }
                    _ => {}
                }
            } else {
                match decision.occupancy_hint {
                    NpcOccupancyKind::Resting | NpcOccupancyKind::Recovering => {
                        budget.recovery_hours = (budget.recovery_hours + 2).min(10);
                    }
                    NpcOccupancyKind::Loitering | NpcOccupancyKind::SocialPresence => {
                        budget.social_hours = (budget.social_hours + 1).min(8);
                    }
                    _ => {
                        budget.free_hours = (budget.free_hours + 2).min(10);
                    }
                }
            }
            budget.free_hours = (24
                - budget.sleep_hours
                - budget.work_hours
                - budget.care_hours
                - budget.travel_hours
                - budget.social_hours
                - budget.recovery_hours)
                .max(0);
            self.time_budget_by_npc
                .insert(decision.npc_id.clone(), budget);
        }
    }

    fn sync_opportunities_for_tick(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
        opportunities_by_npc: &BTreeMap<String, Vec<OpportunityRuntimeState>>,
    ) {
        let prior = self.opportunities_by_npc.clone();
        for (npc_id, current_entries) in opportunities_by_npc {
            let previous_entries = prior.get(npc_id).cloned().unwrap_or_default();
            let previous_ids = previous_entries
                .iter()
                .map(|entry| entry.opportunity_id.as_str())
                .collect::<BTreeSet<_>>();
            let mut merged = current_entries.clone();
            merged.sort_by(|left, right| right.utility_hint.cmp(&left.utility_hint));
            merged.truncate(3);

            if tick % 12 == 0 {
                for entry in merged.iter().take(1) {
                    if previous_ids.contains(entry.opportunity_id.as_str()) {
                        continue;
                    }
                    let event = Event {
                        schema_version: SCHEMA_VERSION_V1.to_string(),
                        run_id: self.status.run_id.clone(),
                        tick,
                        created_at: synthetic_timestamp(tick, *sequence_in_tick),
                        event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                        sequence_in_tick: *sequence_in_tick,
                        event_type: EventType::OpportunityOpened,
                        location_id: entry.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: entry.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        reason_packet_id: None,
                        caused_by: vec![system_event_id.to_string()],
                        targets: vec![ActorRef {
                            actor_id: entry.opportunity_id.clone(),
                            actor_kind: "opportunity".to_string(),
                        }],
                        tags: vec!["opportunity".to_string(), "opened".to_string()],
                        visibility: None,
                        details: Some(json!({
                            "opportunity_id": entry.opportunity_id,
                            "action": entry.action,
                            "source": entry.source,
                            "utility_hint": entry.utility_hint,
                            "constraints": entry.constraints,
                            "expires_tick": entry.expires_tick,
                        })),
                    };
                    causal_chain.push(event.event_id.clone());
                    self.event_log.push(event);
                    *sequence_in_tick += 1;
                }
            }

            if tick % 24 == 0 {
                if let Some(expired) = previous_entries
                    .iter()
                    .find(|entry| entry.expires_tick < tick)
                {
                    let event = Event {
                        schema_version: SCHEMA_VERSION_V1.to_string(),
                        run_id: self.status.run_id.clone(),
                        tick,
                        created_at: synthetic_timestamp(tick, *sequence_in_tick),
                        event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                        sequence_in_tick: *sequence_in_tick,
                        event_type: EventType::OpportunityExpired,
                        location_id: expired.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: expired.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        reason_packet_id: None,
                        caused_by: vec![system_event_id.to_string()],
                        targets: vec![ActorRef {
                            actor_id: expired.opportunity_id.clone(),
                            actor_kind: "opportunity".to_string(),
                        }],
                        tags: vec!["opportunity".to_string(), "expired".to_string()],
                        visibility: None,
                        details: Some(json!({
                            "opportunity_id": expired.opportunity_id,
                            "action": expired.action,
                            "expires_tick": expired.expires_tick,
                        })),
                    };
                    causal_chain.push(event.event_id.clone());
                    self.event_log.push(event);
                    *sequence_in_tick += 1;
                }
            }
            self.opportunities_by_npc.insert(npc_id.clone(), merged);
        }
    }

    fn sync_commitments_for_tick(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
        decisions: &[NpcDecision],
    ) {
        for decision in decisions {
            let Some(chosen) = decision.chosen_action.as_ref() else {
                continue;
            };
            let action_family = commitment_family_for_action(chosen.action.as_str()).to_string();
            match self.commitments_by_npc.get_mut(&decision.npc_id) {
                Some(existing) if existing.action_family == action_family => {
                    existing.progress_ticks = existing.progress_ticks.saturating_add(1);
                    if tick >= existing.due_tick {
                        existing.due_tick = tick + existing.cadence_ticks;
                        existing.inertia_score = (existing.inertia_score - 4).max(0);
                    } else if existing.inertia_score > 0 {
                        existing.inertia_score = (existing.inertia_score - 1).max(0);
                    }
                    if existing.progress_ticks == 2 || existing.progress_ticks % 8 == 0 {
                        let event = Event {
                            schema_version: SCHEMA_VERSION_V1.to_string(),
                            run_id: self.status.run_id.clone(),
                            tick,
                            created_at: synthetic_timestamp(tick, *sequence_in_tick),
                            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                            sequence_in_tick: *sequence_in_tick,
                            event_type: EventType::CommitmentContinued,
                            location_id: decision.location_id.clone(),
                            actors: vec![ActorRef {
                                actor_id: decision.npc_id.clone(),
                                actor_kind: "npc".to_string(),
                            }],
                            reason_packet_id: None,
                            caused_by: vec![system_event_id.to_string()],
                            targets: vec![ActorRef {
                                actor_id: existing.commitment_id.clone(),
                                actor_kind: "commitment".to_string(),
                            }],
                            tags: vec!["commitment".to_string(), "continued".to_string()],
                            visibility: None,
                            details: Some(json!({
                                "commitment_id": existing.commitment_id,
                                "action_family": existing.action_family,
                                "progress_ticks": existing.progress_ticks,
                                "due_tick": existing.due_tick,
                                "inertia_score": existing.inertia_score,
                            })),
                        };
                        causal_chain.push(event.event_id.clone());
                        self.event_log.push(event);
                        *sequence_in_tick += 1;
                    }
                }
                Some(existing) => {
                    let urgent_switch = decision_has_emergency_pressure(decision);
                    let commitment_due = tick >= existing.due_tick;
                    let switch_allowed =
                        urgent_switch || commitment_due || existing.inertia_score <= 0;
                    if !switch_allowed {
                        existing.progress_ticks = existing.progress_ticks.saturating_add(1);
                        if existing.inertia_score > 0 {
                            existing.inertia_score = (existing.inertia_score - 2).max(0);
                        }
                        if existing.progress_ticks == 2 || existing.progress_ticks % 8 == 0 {
                            let event = Event {
                                schema_version: SCHEMA_VERSION_V1.to_string(),
                                run_id: self.status.run_id.clone(),
                                tick,
                                created_at: synthetic_timestamp(tick, *sequence_in_tick),
                                event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                                sequence_in_tick: *sequence_in_tick,
                                event_type: EventType::CommitmentContinued,
                                location_id: decision.location_id.clone(),
                                actors: vec![ActorRef {
                                    actor_id: decision.npc_id.clone(),
                                    actor_kind: "npc".to_string(),
                                }],
                                reason_packet_id: None,
                                caused_by: vec![system_event_id.to_string()],
                                targets: vec![ActorRef {
                                    actor_id: existing.commitment_id.clone(),
                                    actor_kind: "commitment".to_string(),
                                }],
                                tags: vec!["commitment".to_string(), "continued".to_string()],
                                visibility: None,
                                details: Some(json!({
                                    "commitment_id": existing.commitment_id,
                                    "action_family": existing.action_family,
                                    "progress_ticks": existing.progress_ticks,
                                    "due_tick": existing.due_tick,
                                    "inertia_score": existing.inertia_score,
                                    "continuation_mode": "inertia_hold",
                                })),
                            };
                            causal_chain.push(event.event_id.clone());
                            self.event_log.push(event);
                            *sequence_in_tick += 1;
                        }
                        continue;
                    }
                    let broken_id = existing.commitment_id.clone();
                    let broken_action = existing.action_family.clone();
                    let matured_commitment =
                        existing.progress_ticks >= (existing.cadence_ticks / 2).max(1);
                    let transition_event_type =
                        if commitment_due || (!urgent_switch && matured_commitment) {
                            EventType::CommitmentCompleted
                        } else {
                            EventType::CommitmentBroken
                        };
                    let transition_tag = if transition_event_type == EventType::CommitmentCompleted
                    {
                        "completed"
                    } else {
                        "broken"
                    };
                    let broken_event = Event {
                        schema_version: SCHEMA_VERSION_V1.to_string(),
                        run_id: self.status.run_id.clone(),
                        tick,
                        created_at: synthetic_timestamp(tick, *sequence_in_tick),
                        event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                        sequence_in_tick: *sequence_in_tick,
                        event_type: transition_event_type,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        reason_packet_id: None,
                        caused_by: vec![system_event_id.to_string()],
                        targets: vec![ActorRef {
                            actor_id: broken_id.clone(),
                            actor_kind: "commitment".to_string(),
                        }],
                        tags: vec!["commitment".to_string(), transition_tag.to_string()],
                        visibility: None,
                        details: Some(json!({
                            "commitment_id": broken_id,
                            "previous_action_family": broken_action,
                            "transition_reason": if commitment_due { "due_rotation" } else if urgent_switch { "urgent_switch" } else { "inertia_release" },
                        })),
                    };
                    causal_chain.push(broken_event.event_id.clone());
                    self.event_log.push(broken_event);
                    *sequence_in_tick += 1;
                    *existing = build_commitment_state(tick, &decision.npc_id, &action_family);
                    let started_event = Event {
                        schema_version: SCHEMA_VERSION_V1.to_string(),
                        run_id: self.status.run_id.clone(),
                        tick,
                        created_at: synthetic_timestamp(tick, *sequence_in_tick),
                        event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                        sequence_in_tick: *sequence_in_tick,
                        event_type: EventType::CommitmentStarted,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        reason_packet_id: None,
                        caused_by: vec![system_event_id.to_string()],
                        targets: vec![ActorRef {
                            actor_id: existing.commitment_id.clone(),
                            actor_kind: "commitment".to_string(),
                        }],
                        tags: vec!["commitment".to_string(), "started".to_string()],
                        visibility: None,
                        details: Some(json!({
                            "commitment_id": existing.commitment_id,
                            "action_family": existing.action_family,
                            "due_tick": existing.due_tick,
                            "inertia_score": existing.inertia_score,
                        })),
                    };
                    causal_chain.push(started_event.event_id.clone());
                    self.event_log.push(started_event);
                    *sequence_in_tick += 1;
                }
                None => {
                    let commitment = build_commitment_state(tick, &decision.npc_id, &action_family);
                    let commitment_id = commitment.commitment_id.clone();
                    self.commitments_by_npc
                        .insert(decision.npc_id.clone(), commitment.clone());
                    let started_event = Event {
                        schema_version: SCHEMA_VERSION_V1.to_string(),
                        run_id: self.status.run_id.clone(),
                        tick,
                        created_at: synthetic_timestamp(tick, *sequence_in_tick),
                        event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                        sequence_in_tick: *sequence_in_tick,
                        event_type: EventType::CommitmentStarted,
                        location_id: decision.location_id.clone(),
                        actors: vec![ActorRef {
                            actor_id: decision.npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        reason_packet_id: None,
                        caused_by: vec![system_event_id.to_string()],
                        targets: vec![ActorRef {
                            actor_id: commitment_id,
                            actor_kind: "commitment".to_string(),
                        }],
                        tags: vec!["commitment".to_string(), "started".to_string()],
                        visibility: None,
                        details: Some(json!({
                            "action_family": commitment.action_family,
                            "due_tick": commitment.due_tick,
                            "inertia_score": commitment.inertia_score,
                        })),
                    };
                    causal_chain.push(started_event.event_id.clone());
                    self.event_log.push(started_event);
                    *sequence_in_tick += 1;
                }
            }
        }

        if tick % 72 == 0 {
            let npc_ids = self.commitments_by_npc.keys().cloned().collect::<Vec<_>>();
            for npc_id in npc_ids {
                let Some(commitment) = self.commitments_by_npc.get(&npc_id) else {
                    continue;
                };
                if commitment.progress_ticks < 48 {
                    continue;
                }
                let completed_id = commitment.commitment_id.clone();
                let completed_action = commitment.action_family.clone();
                let completed_event = Event {
                    schema_version: SCHEMA_VERSION_V1.to_string(),
                    run_id: self.status.run_id.clone(),
                    tick,
                    created_at: synthetic_timestamp(tick, *sequence_in_tick),
                    event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
                    sequence_in_tick: *sequence_in_tick,
                    event_type: EventType::CommitmentCompleted,
                    location_id: self
                        .npc_location_for(&npc_id)
                        .unwrap_or_else(|| "region:crownvale".to_string()),
                    actors: vec![ActorRef {
                        actor_id: npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    reason_packet_id: None,
                    caused_by: vec![system_event_id.to_string()],
                    targets: vec![ActorRef {
                        actor_id: completed_id,
                        actor_kind: "commitment".to_string(),
                    }],
                    tags: vec!["commitment".to_string(), "completed".to_string()],
                    visibility: None,
                    details: Some(json!({
                        "action_family": completed_action,
                    })),
                };
                causal_chain.push(completed_event.event_id.clone());
                self.event_log.push(completed_event);
                *sequence_in_tick += 1;
                self.commitments_by_npc.remove(&npc_id);
            }
        }
    }

    fn emit_accounting_transfer_summary(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let transfer_count = self
            .accounting_transfers
            .iter()
            .filter(|entry| entry.tick == tick)
            .count();
        if transfer_count == 0 {
            return;
        }
        let total_amount: i64 = self
            .accounting_transfers
            .iter()
            .filter(|entry| entry.tick == tick)
            .map(|entry| entry.amount.max(0))
            .sum();
        let event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type: EventType::AccountingTransferRecorded,
            location_id: "region:crownvale".to_string(),
            actors: Vec::new(),
            reason_packet_id: None,
            caused_by: vec![system_event_id.to_string()],
            targets: Vec::new(),
            tags: vec!["accounting".to_string(), "transfer".to_string()],
            visibility: None,
            details: Some(json!({
                "transfer_count": transfer_count,
                "total_amount": total_amount,
            })),
        };
        causal_chain.push(event.event_id.clone());
        self.event_log.push(event);
        *sequence_in_tick += 1;
    }

    fn record_accounting_transfer(
        &mut self,
        tick: u64,
        settlement_id: &str,
        from_account: impl Into<String>,
        to_account: impl Into<String>,
        resource_kind: impl Into<String>,
        amount: i64,
        cause_event_id: Option<String>,
    ) {
        if amount <= 0 {
            return;
        }
        let entry = AccountingTransferRuntimeState {
            transfer_id: format!("acct_{:06}_{}", tick, self.accounting_transfers.len()),
            tick,
            settlement_id: settlement_id.to_string(),
            from_account: from_account.into(),
            to_account: to_account.into(),
            resource_kind: resource_kind.into(),
            amount,
            cause_event_id,
        };
        self.accounting_transfers.push(entry);
        if self.accounting_transfers.len() > 512 {
            let overflow = self.accounting_transfers.len() - 512;
            self.accounting_transfers.drain(0..overflow);
        }
    }

    fn normalize_tick_event_ordering(&mut self, tick: u64) {
        let start = self
            .event_log
            .iter()
            .position(|event| event.tick == tick)
            .unwrap_or(self.event_log.len());
        if start >= self.event_log.len() {
            return;
        }
        let end = self.event_log.len();
        let mut id_map = BTreeMap::<String, String>::new();
        for (offset, event) in self.event_log[start..end].iter_mut().enumerate() {
            let old_id = event.event_id.clone();
            let sequence_in_tick = offset as u64;
            event.sequence_in_tick = sequence_in_tick;
            event.event_id = format!("evt_{:06}_{:03}", tick, sequence_in_tick);
            event.created_at = synthetic_timestamp(tick, sequence_in_tick);
            id_map.insert(old_id, event.event_id.clone());
        }
        for event in &mut self.event_log[start..end] {
            for cause in &mut event.caused_by {
                if let Some(remapped) = id_map.get(cause) {
                    *cause = remapped.clone();
                }
            }
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
                if economy.illness_ticks > 0 {
                    economy.illness_ticks = (economy.illness_ticks - 1).max(0);
                } else if economy.health < 95 {
                    economy.health += 1;
                }
            }
            for npc in &mut self.npcs {
                let Some(economy) = self.npc_economy_by_id.get(&npc.npc_id) else {
                    continue;
                };
                if economy.health <= 55 {
                    npc.aspiration = "recover_health".to_string();
                } else if economy.debt_balance >= 5 {
                    npc.aspiration = "clear_debts".to_string();
                } else if economy.wallet >= 12 && economy.food_reserve_days >= 3 {
                    npc.aspiration = "improve_status".to_string();
                } else if economy.apprenticeship_progress >= 96 {
                    npc.aspiration = "master_craft".to_string();
                } else {
                    npc.aspiration = "stable_household".to_string();
                }

                if economy.apprenticeship_progress >= 120 {
                    npc.profession = match npc.profession.as_str() {
                        "farmhand" => "farmer".to_string(),
                        "blacksmith" => "master_smith".to_string(),
                        "carter" => "merchant_caravaner".to_string(),
                        current => current.to_string(),
                    };
                }
            }
            for entries in self.observations_by_npc.values_mut() {
                entries.retain(|entry| entry.tick + 120 >= tick || entry.salience >= 7);
                if entries.len() > 24 {
                    let drop = entries.len() - 24;
                    entries.drain(0..drop);
                }
            }
            self.observations_by_npc
                .retain(|_, entries| !entries.is_empty());
            for tension in self.tension_by_pair.values_mut() {
                tension.anger = step_toward(tension.anger, 0, 2);
                if tension.losses > 0 {
                    tension.losses -= 1;
                }
                if tension.coin_lost > 0 {
                    tension.coin_lost -= 1;
                }
            }
            self.tension_by_pair.retain(|_, tension| {
                tension.anger > 0 || tension.losses > 0 || tension.coin_lost > 0
            });
            self.last_bandit_raid_tick_by_settlement
                .retain(|_, last_tick| last_tick.saturating_add(24 * 14) >= tick);
            self.settlement_conflict_cooldown_until
                .retain(|_, cooldown_until| *cooldown_until > tick);
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

        if tick % 6 == 0 {
            for entries in self.opportunities_by_npc.values_mut() {
                entries.retain(|entry| entry.expires_tick >= tick);
            }
            self.opportunities_by_npc
                .retain(|_, entries| !entries.is_empty());
        }

        if tick % 24 == 0 {
            for commitment in self.commitments_by_npc.values_mut() {
                if commitment.inertia_score > 0 {
                    commitment.inertia_score -= 1;
                }
            }
            for queue in self.institution_queue_by_settlement.values_mut() {
                queue.pending_cases = (queue.pending_cases - 1).max(0);
                queue.avg_response_latency = (queue.avg_response_latency - 1).max(1);
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

                let transfer = self
                    .select_item_for_route(origin_settlement_id)
                    .map(|item_id| ItemTransfer {
                        item_id: item_id.clone(),
                        owner_id: "caravan:merchant".to_string(),
                        location_id: destination_settlement_id.clone(),
                        stolen: false,
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
                base.state_effects
                    .item_transfers
                    .extend(rehomed_items.clone());
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
                        if let Some(suspect_id) =
                            self.first_wanted_npc_at_location(&decision.location_id)
                        {
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
            AffordanceVerb::WorkForFood => {
                planned.push(PlannedEvent {
                    event_type: EventType::JobSought,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("board_employer:{}", decision.location_id),
                        actor_kind: "employer".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["labor".to_string(), "board_work".to_string()],
                    details: Some(json!({
                        "worker_id": decision.npc_id,
                        "compensation_type": "board",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
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
            AffordanceVerb::ConverseNeighbor => {
                let target_id = self
                    .social_target_for_tick(
                        &decision.npc_id,
                        &decision.location_id,
                        -10,
                        tick,
                        "conversation",
                    )
                    .unwrap_or_else(|| format!("npc:neighbor:{}", decision.location_id));
                let topic = self.local_topic_for_npc(
                    &decision.npc_id,
                    &decision.location_id,
                    tick,
                    "conversation_topic",
                );
                planned.push(PlannedEvent {
                    event_type: EventType::ConversationHeld,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: target_id,
                        actor_kind: "npc".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "conversation".to_string()],
                    details: Some(json!({
                        "topic": topic,
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::LendCoin => {
                let target_id = self
                    .social_target_for_tick(
                        &decision.npc_id,
                        &decision.location_id,
                        5,
                        tick,
                        "lend_coin",
                    )
                    .unwrap_or_else(|| format!("npc:borrower:{}", decision.location_id));
                let amount = self
                    .npc_economy_by_id
                    .get(&decision.npc_id)
                    .map(|economy| economy.wallet.clamp(1, 2))
                    .unwrap_or(1);
                planned.push(PlannedEvent {
                    event_type: EventType::LoanExtended,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: target_id,
                        actor_kind: "npc".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "loan".to_string()],
                    details: Some(json!({
                        "amount": amount,
                        "currency": "coin",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::CourtRomance => {
                let target_id = self
                    .social_target_for_tick(
                        &decision.npc_id,
                        &decision.location_id,
                        6,
                        tick,
                        "court_romance",
                    )
                    .unwrap_or_else(|| format!("npc:court_target:{}", decision.location_id));
                let (trust, attachment, respect) = self
                    .relationship_edges
                    .get(&(decision.npc_id.clone(), target_id.clone()))
                    .map(|edge| (edge.trust, edge.attachment, edge.respect))
                    .unwrap_or((0, 0, 0));
                let stage = if attachment >= 72 && trust >= 52 {
                    "proposal_signal"
                } else if attachment >= 44 || trust >= 32 {
                    "courtship"
                } else {
                    "flirtation"
                };
                planned.push(PlannedEvent {
                    event_type: EventType::RomanceAdvanced,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: target_id,
                        actor_kind: "npc".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "romance".to_string()],
                    details: Some(json!({
                        "stage": stage,
                        "trust": trust,
                        "attachment": attachment,
                        "respect": respect,
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::SeekTreatment => {
                planned.push(PlannedEvent {
                    event_type: EventType::IllnessRecovered,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("healer:{}", decision.location_id),
                        actor_kind: "institution".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["health".to_string(), "treatment".to_string()],
                    details: Some(json!({
                        "source": "npc_action",
                        "treatment": "requested",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::ObserveNotableEvent => {
                let topic = self.local_topic_for_npc(
                    &decision.npc_id,
                    &decision.location_id,
                    tick,
                    "observation_topic",
                );
                planned.push(PlannedEvent {
                    event_type: EventType::ObservationLogged,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "observation".to_string()],
                    details: Some(json!({
                        "topic": topic,
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::TradeVisit => {
                planned.push(PlannedEvent {
                    event_type: EventType::ConversationHeld,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("merchant:{}", decision.location_id),
                        actor_kind: "npc".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["market".to_string(), "trade_visit".to_string()],
                    details: Some(json!({
                        "topic": "market_haggle",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
                planned.push(PlannedEvent {
                    event_type: EventType::BeliefUpdated,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["belief".to_string(), "market_signal".to_string()],
                    details: Some(json!({
                        "topic": "price_signal_observed",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::ShareRumor => {
                let topic = self.local_topic_for_npc(
                    &decision.npc_id,
                    &decision.location_id,
                    tick,
                    "rumor_topic",
                );
                planned.push(PlannedEvent {
                    event_type: EventType::RumorMutated,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["belief".to_string(), "rumor".to_string()],
                    details: Some(json!({
                        "topic": topic,
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::InvestigateRumor => {
                planned.push(PlannedEvent {
                    event_type: EventType::ConversationHeld,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("traveler:{}", decision.location_id),
                        actor_kind: "npc".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["information".to_string(), "inquiry".to_string()],
                    details: Some(json!({
                        "topic": "question_travelers",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
                planned.push(PlannedEvent {
                    event_type: EventType::BeliefUpdated,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["belief".to_string(), "inquiry".to_string()],
                    details: Some(json!({
                        "topic": "new_testimony_recorded",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::Forage | AffordanceVerb::GatherFirewood => {
                planned.push(PlannedEvent {
                    event_type: EventType::StockRecovered,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["supply".to_string(), "recovery".to_string()],
                    details: Some(json!({
                        "resource": if matches!(chosen.verb, AffordanceVerb::GatherFirewood) { "fuel" } else { "staples" },
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::RationGrain | AffordanceVerb::RepairHearth => {
                planned.push(PlannedEvent {
                    event_type: EventType::HouseholdConsumptionApplied,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["household".to_string(), "survival".to_string()],
                    details: Some(json!({
                        "mode": if matches!(chosen.verb, AffordanceVerb::RepairHearth) { "heat_repair" } else { "rationing" },
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::CoverAbsentNeighbor => {
                planned.push(PlannedEvent {
                    event_type: EventType::ObligationCreated,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: vec![ActorRef {
                        actor_id: format!("neighbor:{}", decision.location_id),
                        actor_kind: "npc".to_string(),
                    }],
                    caused_by: Vec::new(),
                    tags: vec!["social".to_string(), "coverage".to_string()],
                    details: Some(json!({
                        "kind": "cover_absent_neighbor",
                        "source": "npc_action",
                    })),
                    reason_draft: None,
                    state_effects: PlannedStateEffects::default(),
                });
            }
            AffordanceVerb::AvoidPatrols => {
                planned.push(PlannedEvent {
                    event_type: EventType::ObservationLogged,
                    location_id: decision.location_id.clone(),
                    actors: vec![ActorRef {
                        actor_id: decision.npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    targets: Vec::new(),
                    caused_by: Vec::new(),
                    tags: vec!["security".to_string(), "evasion".to_string()],
                    details: Some(json!({
                        "topic": "evaded_guard_patrol",
                        "source": "npc_action",
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
        let canonical = event
            .details
            .as_ref()
            .and_then(|details| details.get("canonical_action"))
            .and_then(Value::as_str)
            .unwrap_or_else(|| canonical_action_name(chosen_action));

        let memory = self
            .action_memory_by_npc
            .entry(actor_id.to_string())
            .or_default();
        if memory.last_action.as_deref() == Some(canonical) {
            memory.repeat_streak = memory.repeat_streak.saturating_add(1);
        } else {
            memory.repeat_streak = 0;
        }
        memory.last_action = Some(canonical.to_string());
        memory.last_action_tick = event.tick;
        if canonical == "steal_supplies" {
            memory.last_theft_tick = Some(event.tick);
        }
    }

    fn record_event_synthesis_trace(&mut self, tick: u64, event: &Event) {
        let details = event.details.as_ref();
        let event_actor_id = event.actors.first().map(|actor| actor.actor_id.clone());
        let Some(source_plan_id) = details
            .and_then(|value| value.get("plan_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                self.active_execution_by_npc
                    .values()
                    .find(|execution| {
                        event_actor_id
                            .as_ref()
                            .map(|actor_id| execution.npc_id == *actor_id)
                            .unwrap_or(false)
                    })
                    .map(|execution| execution.plan_id.clone())
            })
        else {
            return;
        };
        let source_operator_id = details
            .and_then(|value| value.get("operator_chain_ids"))
            .and_then(Value::as_array)
            .and_then(|entries| entries.first())
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                self.active_execution_by_npc
                    .values()
                    .find(|execution| execution.plan_id == source_plan_id)
                    .and_then(|execution| execution.operator_chain_ids.first().cloned())
            })
            .unwrap_or_else(|| "op:unknown".to_string());
        let trigger_facts = details
            .and_then(|value| value.get("top_pressures"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let rejected_alternatives = details
            .and_then(|value| value.get("rejected_alternatives"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.event_synthesis_trace
            .push(EventSynthesisTraceRuntimeState {
                trace_id: format!("trace_{:06}_{}", tick, self.event_synthesis_trace.len()),
                tick,
                source_plan_id,
                source_operator_id,
                source_event_id: event.event_id.clone(),
                event_type: event.event_type,
                trigger_facts,
                rejected_alternatives,
            });
        if self.event_synthesis_trace.len() > 1024 {
            let overflow = self.event_synthesis_trace.len() - 1024;
            self.event_synthesis_trace.drain(0..overflow);
        }
    }

    fn refresh_composable_runtime_from_tick_events(&mut self, tick: u64) {
        let tick_events = self
            .event_log
            .iter()
            .filter(|event| event.tick == tick)
            .cloned()
            .collect::<Vec<_>>();
        for event in &tick_events {
            self.record_composable_state_from_event(event);
        }
    }

    fn record_composable_state_from_event(&mut self, event: &Event) {
        if event.event_type == EventType::NpcActionCommitted {
            let actor_id = event
                .actors
                .iter()
                .find(|actor| actor.actor_kind == "npc")
                .map(|actor| actor.actor_id.clone());
            if let Some(actor_id) = actor_id {
                let details = event.details.as_ref();
                let base_action = details
                    .and_then(|value| {
                        value
                            .get("canonical_action")
                            .and_then(Value::as_str)
                            .or_else(|| value.get("chosen_action").and_then(Value::as_str))
                    })
                    .map(canonical_action_name)
                    .unwrap_or("maintain_routine")
                    .to_string();
                let composed_action = details
                    .and_then(|value| value.get("composed_action"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| base_action.clone());
                let goal_family = details
                    .and_then(|value| value.get("action_goal_family"))
                    .and_then(Value::as_str)
                    .unwrap_or("maintain_routine")
                    .to_string();
                let template_id = details
                    .and_then(|value| value.get("action_template_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("template/unknown")
                    .to_string();
                let ops = details
                    .and_then(|value| value.get("composed_ops"))
                    .cloned()
                    .and_then(|value| serde_json::from_value::<Vec<AtomicOp>>(value).ok())
                    .unwrap_or_default();
                let rejected_alternatives = details
                    .and_then(|value| value.get("rejected_alternatives"))
                    .and_then(Value::as_array)
                    .map(|entries| {
                        entries
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                self.active_plans_by_npc.insert(
                    actor_id.clone(),
                    ComposablePlanRuntimeState {
                        npc_id: actor_id,
                        tick: event.tick,
                        goal_family,
                        template_id,
                        base_action,
                        composed_action,
                        ops,
                        rejected_alternatives,
                    },
                );
            }
        }

        let process_descriptor = match event.event_type {
            EventType::RomanceAdvanced => {
                let stage = event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("stage"))
                    .and_then(Value::as_str)
                    .unwrap_or("courtship");
                let status = if stage == "wedding" {
                    ProcessStatus::Resolved
                } else {
                    ProcessStatus::Active
                };
                Some((ProcessKind::Romance, "romance", status, stage))
            }
            EventType::ContractSigned => Some((
                ProcessKind::BusinessPartnership,
                "business",
                ProcessStatus::Active,
                "contract_signed",
            )),
            EventType::ContractBreached => Some((
                ProcessKind::BusinessPartnership,
                "business",
                ProcessStatus::Failed,
                "contract_breached",
            )),
            EventType::EmploymentTerminated => Some((
                ProcessKind::BusinessPartnership,
                "business",
                ProcessStatus::Resolved,
                "employment_terminated",
            )),
            EventType::InsultExchanged => Some((
                ProcessKind::ConflictSpiral,
                "conflict",
                ProcessStatus::Active,
                "verbal_conflict",
            )),
            EventType::PunchThrown => Some((
                ProcessKind::ConflictSpiral,
                "conflict",
                ProcessStatus::Active,
                "physical_conflict",
            )),
            EventType::BrawlStarted => Some((
                ProcessKind::ConflictSpiral,
                "conflict",
                ProcessStatus::Active,
                "brawl_started",
            )),
            EventType::BrawlStopped => Some((
                ProcessKind::ConflictSpiral,
                "conflict",
                ProcessStatus::Resolved,
                "brawl_stopped",
            )),
            _ => None,
        };

        if let Some((kind, kind_tag, status, stage)) = process_descriptor {
            self.upsert_process_instance_from_event(event, kind, kind_tag, status, stage);
        }
    }

    fn upsert_process_instance_from_event(
        &mut self,
        event: &Event,
        kind: ProcessKind,
        kind_tag: &str,
        status: ProcessStatus,
        stage: &str,
    ) {
        let participants = self.process_participants_for_event(event);
        if participants.is_empty() {
            return;
        }
        let process_id = self.process_id_for_event(event, kind_tag, &participants);
        let mut metadata = BTreeMap::new();
        metadata.insert("kind".to_string(), kind_tag.to_string());
        metadata.insert("location_id".to_string(), event.location_id.clone());
        metadata.insert(
            "last_event_type".to_string(),
            format!("{:?}", event.event_type),
        );
        if let Some(contract_id) = event
            .details
            .as_ref()
            .and_then(|details| details.get("contract_id"))
            .and_then(Value::as_str)
        {
            metadata.insert("contract_id".to_string(), contract_id.to_string());
        }
        if let Some(stage_value) = event
            .details
            .as_ref()
            .and_then(|details| details.get("stage"))
            .and_then(Value::as_str)
        {
            metadata.insert("stage".to_string(), stage_value.to_string());
        }

        if let Some(existing) = self.process_instances_by_id.get_mut(&process_id) {
            existing.status = status;
            existing.stage = stage.to_string();
            existing.last_updated_tick = event.tick;
            existing.last_event_id = Some(event.event_id.clone());
            existing.participants = participants;
            for (key, value) in metadata {
                existing.metadata.insert(key, value);
            }
            return;
        }

        self.process_instances_by_id.insert(
            process_id.clone(),
            ProcessRuntimeState {
                process_id,
                kind,
                status,
                stage: stage.to_string(),
                participants,
                started_tick: event.tick,
                last_updated_tick: event.tick,
                last_event_id: Some(event.event_id.clone()),
                metadata,
            },
        );
    }

    fn process_participants_for_event(&self, event: &Event) -> Vec<String> {
        let mut participants = event
            .actors
            .iter()
            .chain(event.targets.iter())
            .filter(|actor| actor.actor_kind == "npc")
            .map(|actor| actor.actor_id.clone())
            .collect::<BTreeSet<_>>();

        if participants.is_empty() {
            if let Some(contract_id) = event
                .details
                .as_ref()
                .and_then(|details| details.get("contract_id"))
                .and_then(Value::as_str)
            {
                if let Some(contract) = self.contracts_by_id.get(contract_id) {
                    participants.insert(contract.contract.worker_id.clone());
                    participants.insert(contract.contract.employer_id.clone());
                }
            }
        }

        participants.into_iter().collect()
    }

    fn process_id_for_event(
        &self,
        event: &Event,
        kind_tag: &str,
        participants: &[String],
    ) -> String {
        if !participants.is_empty() {
            return format!("process:{kind_tag}:{}", participants.join("|"));
        }
        let fallback = event
            .details
            .as_ref()
            .and_then(|details| details.get("contract_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| event.actors.first().map(|actor| actor.actor_id.clone()))
            .unwrap_or_else(|| event.location_id.clone());
        format!("process:{kind_tag}:{fallback}")
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
        if !self.deterministic_window_fire(tick, 10, "relationship") || self.npcs.len() < 2 {
            return;
        }

        let mut candidates = Vec::<(String, String, String, i64, i64)>::new();
        for source in &self.npcs {
            for target in &self.npcs {
                if source.npc_id >= target.npc_id {
                    continue;
                }
                let Some(edge_ab) = self
                    .relationship_edges
                    .get(&(source.npc_id.clone(), target.npc_id.clone()))
                else {
                    continue;
                };
                let Some(edge_ba) = self
                    .relationship_edges
                    .get(&(target.npc_id.clone(), source.npc_id.clone()))
                else {
                    continue;
                };
                let connected = source.location_id == target.location_id
                    || self.routes_by_id.values().any(|route| {
                        ((route.origin_settlement_id == source.location_id
                            && route.destination_settlement_id == target.location_id)
                            || (route.origin_settlement_id == target.location_id
                                && route.destination_settlement_id == source.location_id))
                            && route.weather_window_open
                            && route.hazard_score <= 55
                    });
                if !connected {
                    continue;
                }
                let pair_key = canonical_pair_key(&source.npc_id, &target.npc_id);
                let memory = self
                    .pair_event_memory
                    .get(&pair_key)
                    .cloned()
                    .unwrap_or_default();
                let recency_penalty = if memory.last_social_tick > 0
                    && tick.saturating_sub(memory.last_social_tick) < 18
                {
                    10
                } else {
                    0
                };
                let bond = (edge_ab.trust
                    + edge_ba.trust
                    + edge_ab.attachment
                    + edge_ba.attachment
                    + edge_ab.respect
                    + edge_ba.respect
                    - edge_ab.grievance
                    - edge_ba.grievance)
                    / 2;
                let volatility = (edge_ab.grievance
                    + edge_ba.grievance
                    + edge_ab.fear
                    + edge_ba.fear
                    + edge_ab.jealousy
                    + edge_ba.jealousy)
                    / 4;
                let weight =
                    (12 + (bond.abs() / 5) + volatility / 3 - recency_penalty).clamp(1, 96);
                candidates.push((
                    source.npc_id.clone(),
                    target.npc_id.clone(),
                    source.location_id.clone(),
                    bond,
                    weight,
                ));
            }
        }
        if candidates.is_empty() {
            return;
        }
        let weights = candidates
            .iter()
            .map(|(_, _, _, _, weight)| *weight)
            .collect::<Vec<_>>();
        let pick_roll =
            self.deterministic_stream(tick, Phase::ActionResolution, "relationship", "pair_pick");
        let Some(pair_idx) = pick_weighted_index(pick_roll, &weights) else {
            return;
        };
        let (actor_id, target_id, location_id, bond, _) = candidates[pair_idx].clone();
        let betrayal_pressure = self.pressure_index / 40
            + self.wanted_npcs.len() as i64
            + (sum_positive(self.law_case_load_by_settlement.values()) / 5)
            + (-bond).max(0) / 18;
        let betrayal_threshold = (28_i64 + betrayal_pressure).clamp(12, 88) as u64;
        let roll =
            self.deterministic_stream(tick, Phase::ActionResolution, "relationship", "shift_kind")
                % 100;
        let (shift_kind, trust_delta, grievance_delta) = if roll < betrayal_threshold {
            ("betrayal", -3_i64, 4_i64)
        } else {
            ("cooperation", 3_i64, -2_i64)
        };
        if let Some(edge) = self
            .relationship_edges
            .get_mut(&(actor_id.clone(), target_id.clone()))
        {
            edge.trust = (edge.trust + trust_delta).clamp(-100, 100);
            edge.grievance = (edge.grievance + grievance_delta).clamp(0, 100);
            edge.recent_interaction_tick = tick;
        }
        if let Some(edge) = self
            .relationship_edges
            .get_mut(&(target_id.clone(), actor_id.clone()))
        {
            edge.trust = (edge.trust + trust_delta / 2).clamp(-100, 100);
            edge.grievance = (edge.grievance + grievance_delta / 2).clamp(0, 100);
            edge.recent_interaction_tick = tick;
        }
        let memory = self.pair_memory_mut(&actor_id, &target_id);
        memory.last_social_tick = tick;
        self.social_cohesion = (self.social_cohesion + trust_delta).clamp(-200, 200);

        let relationship_event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: format!("evt_{:06}_{:03}", tick, *sequence_in_tick),
            sequence_in_tick: *sequence_in_tick,
            event_type: EventType::RelationshipShifted,
            location_id,
            actors: vec![ActorRef {
                actor_id: actor_id.clone(),
                actor_kind: "npc".to_string(),
            }],
            reason_packet_id: None,
            caused_by: vec![system_event_id.to_string()],
            targets: vec![ActorRef {
                actor_id: target_id,
                actor_kind: "npc".to_string(),
            }],
            tags: vec!["social".to_string(), shift_kind.to_string()],
            visibility: None,
            details: Some(json!({
                "shift_kind": shift_kind,
                "trust_delta": trust_delta,
                "bond_signal": bond,
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
        let offset =
            self.deterministic_stream(cycle + 1, Phase::PreTick, "window_scheduler", channel)
                % window;
        tick == cycle_start_tick + offset
    }

    fn hidden_site_for_location(&self, location_id: &str, tick: u64) -> String {
        let node_site = "site:forgotten_waystone";
        let ruin_site = "site:ashen_vault";
        let watch_post = "site:old_watch_post";
        let roll =
            self.deterministic_stream(tick, Phase::Perception, location_id, "site_choice") % 100;

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
        self.progress_social_dynamics_loop(tick, system_event_id, sequence_in_tick, causal_chain);
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
        let action_by_npc = self.current_action_by_npc.clone();
        let shelter_seekers = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| {
                (action.as_str() == "seek_shelter").then_some(npc_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let livelihood_workers = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| {
                matches!(
                    action.as_str(),
                    "work_for_food" | "work_for_coin" | "tend_fields"
                )
                .then_some(npc_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let rent_contributors = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| {
                (action.as_str() == "pay_rent").then_some(npc_id.clone())
            })
            .collect::<Vec<_>>();
        let lenders = action_by_npc
            .iter()
            .filter_map(|(npc_id, action)| {
                (action.as_str() == "lend_coin").then_some(npc_id.clone())
            })
            .collect::<Vec<_>>();
        for npc_id in rent_contributors {
            let household_id = self
                .npc_economy_by_id
                .get(&npc_id)
                .map(|economy| economy.household_id.clone());
            if let Some(household_id) = household_id {
                if let Some(household) = self.households_by_id.get_mut(&household_id) {
                    let target_step = (household.rent_amount / 6).clamp(1, 4);
                    let mut contributed = 0_i64;
                    if let Some(economy) = self.npc_economy_by_id.get_mut(&npc_id) {
                        if economy.wallet > 0 {
                            contributed = economy.wallet.min(target_step);
                            economy.wallet -= contributed;
                        }
                    }
                    if contributed <= 0 {
                        continue;
                    }
                    let cap = (household.rent_amount * 2).max(4);
                    household.rent_reserve_coin =
                        (household.rent_reserve_coin + contributed).min(cap);
                    self.record_accounting_transfer(
                        tick,
                        &self
                            .npc_location_for(&npc_id)
                            .unwrap_or_else(|| "settlement:greywall".to_string()),
                        format!("npc:{npc_id}"),
                        format!("household_reserve:{household_id}"),
                        "coin",
                        contributed,
                        None,
                    );
                }
            }
        }
        for lender_id in lenders {
            let Some(location_id) = self.npc_location_for(&lender_id) else {
                continue;
            };
            let Some(borrower_id) = self.social_target_for(&lender_id, &location_id, 0) else {
                continue;
            };
            let lend_amount = {
                let Some(lender) = self.npc_economy_by_id.get_mut(&lender_id) else {
                    continue;
                };
                if lender.wallet <= 2 {
                    continue;
                }
                let amount = lender.wallet.saturating_sub(2).min(2);
                lender.wallet -= amount;
                amount
            };
            if lend_amount <= 0 {
                continue;
            }
            if let Some(borrower) = self.npc_economy_by_id.get_mut(&borrower_id) {
                borrower.wallet += lend_amount;
                borrower.debt_balance = (borrower.debt_balance + lend_amount).clamp(0, 40);
            }
            self.record_accounting_transfer(
                tick,
                &location_id,
                format!("npc:{lender_id}"),
                format!("npc:{borrower_id}"),
                "coin",
                lend_amount,
                None,
            );
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
                        self.record_accounting_transfer(
                            tick,
                            &settlement_id,
                            format!("household:{household_id}"),
                            format!("market:{settlement_id}"),
                            "coin",
                            purchased_staples,
                            None,
                        );
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
                        self.record_accounting_transfer(
                            tick,
                            &settlement_id,
                            format!("household:{household_id}"),
                            format!("market:{settlement_id}"),
                            "coin",
                            purchased_fuel,
                            None,
                        );
                    }
                }

                let pantry_consumed = pantry_need.min(household.shared_pantry_stock.max(0));
                let fuel_consumed = fuel_need.min(household.fuel_stock.max(0));
                household.shared_pantry_stock =
                    (household.shared_pantry_stock - pantry_consumed).max(0);
                household.fuel_stock = (household.fuel_stock - fuel_consumed).max(0);
                self.record_accounting_transfer(
                    tick,
                    &settlement_id,
                    format!("pantry:{household_id}"),
                    format!("consumption:{household_id}"),
                    "staples",
                    pantry_consumed,
                    None,
                );
                self.record_accounting_transfer(
                    tick,
                    &settlement_id,
                    format!("fuel_store:{household_id}"),
                    format!("consumption:{household_id}"),
                    "fuel",
                    fuel_consumed,
                    None,
                );
                let pantry_shortfall = pantry_need - pantry_consumed;
                let fuel_shortfall = fuel_need - fuel_consumed;
                let mut health_transitions = Vec::<(String, EventType, i64, i64, String)>::new();

                for npc_id in &members {
                    let resilience = self
                        .npc_traits_by_id
                        .get(npc_id)
                        .map(|profile| profile.resilience)
                        .unwrap_or(50);
                    let recovery_roll =
                        (self.deterministic_stream(tick, Phase::Commit, npc_id, "shelter_recovery")
                            % 100) as i64;
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
                    let illness_roll =
                        (self.deterministic_stream(tick, Phase::Commit, npc_id, "illness_roll")
                            % 100) as i64;
                    if let Some(economy) = self.npc_economy_by_id.get_mut(npc_id) {
                        if spot_coin_payout > 0 {
                            economy.wallet += spot_coin_payout;
                        }
                        let illness_before = economy.illness_ticks;
                        let health_before = economy.health;
                        if matches!(chosen_action, Some("tend_fields" | "craft_goods")) {
                            household.shared_pantry_stock =
                                (household.shared_pantry_stock + 1).clamp(0, 12);
                        }
                        if matches!(chosen_action, Some("work_for_food")) {
                            household.shared_pantry_stock =
                                (household.shared_pantry_stock + 1).clamp(0, 12);
                            economy.food_reserve_days =
                                (economy.food_reserve_days + 2).clamp(0, 14);
                        }
                        if livelihood_workers.contains(npc_id) {
                            economy.food_reserve_days =
                                (economy.food_reserve_days + 1).clamp(0, 14);
                        }
                        if pantry_shortfall > 0 {
                            economy.food_reserve_days = (economy.food_reserve_days - 1).max(0);
                        } else {
                            economy.food_reserve_days =
                                (economy.food_reserve_days + 1).clamp(0, 14);
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

                        let winter_penalty = if self.scenario_state.winter_severity >= 70 {
                            1
                        } else {
                            0
                        };
                        let hardship = pantry_shortfall + fuel_shortfall + winter_penalty;
                        if matches!(chosen_action, Some("seek_treatment")) {
                            economy.illness_ticks = (economy.illness_ticks - 2).max(0);
                            economy.health = (economy.health + 4).clamp(20, 100);
                        }
                        if economy.illness_ticks == 0 && hardship > 0 {
                            let illness_threshold =
                                (12 + hardship * 8 - (resilience / 8)).clamp(6, 55);
                            if illness_roll < illness_threshold {
                                economy.illness_ticks = (8 + hardship * 2).clamp(6, 24);
                                economy.health = (economy.health - (2 + hardship)).clamp(20, 100);
                                health_transitions.push((
                                    npc_id.clone(),
                                    EventType::IllnessContracted,
                                    economy.health,
                                    economy.illness_ticks,
                                    "hardship_exposure".to_string(),
                                ));
                            }
                        } else if economy.illness_ticks > 0 {
                            let treatment_bonus = if matches!(chosen_action, Some("seek_treatment"))
                            {
                                2
                            } else {
                                1
                            };
                            economy.illness_ticks =
                                (economy.illness_ticks - treatment_bonus).max(0);
                            let health_loss = if hardship > 0 { 2 } else { 1 };
                            economy.health = (economy.health - health_loss).clamp(20, 100);
                            if economy.illness_ticks == 0 {
                                economy.health = (economy.health + 3).clamp(20, 100);
                                health_transitions.push((
                                    npc_id.clone(),
                                    EventType::IllnessRecovered,
                                    economy.health,
                                    economy.illness_ticks,
                                    "natural_recovery".to_string(),
                                ));
                            }
                        } else if hardship == 0 {
                            economy.health = (economy.health + 1).clamp(20, 100);
                        }
                        if illness_before != economy.illness_ticks
                            || health_before != economy.health
                        {
                            if economy.health <= 35 && economy.illness_ticks > 0 {
                                health_transitions.push((
                                    npc_id.clone(),
                                    EventType::IllnessContracted,
                                    economy.health,
                                    economy.illness_ticks,
                                    "acute_decline".to_string(),
                                ));
                            }
                        }
                    }
                }
                if pantry_shortfall > 0 || fuel_shortfall > 0 {
                    household.eviction_risk_score =
                        (household.eviction_risk_score + 1).clamp(0, 100);
                } else {
                    household.eviction_risk_score = (household.eviction_risk_score - 1).max(0);
                }
                let pantry_after = household.shared_pantry_stock;
                let fuel_after = household.fuel_stock;
                self.households_by_id
                    .insert(household_id.clone(), household);
                for (npc_id, event_type, health, illness_ticks, cause) in health_transitions {
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        event_type,
                        settlement_id.clone(),
                        vec![ActorRef {
                            actor_id: npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        Vec::new(),
                        vec!["health".to_string()],
                        Some(json!({
                            "npc_id": npc_id,
                            "health": health,
                            "illness_ticks": illness_ticks,
                            "cause": cause,
                        })),
                    );
                }

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
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::RentPaid,
                    household_id.clone(),
                    payer_id
                        .iter()
                        .map(|payer| ActorRef {
                            actor_id: payer.clone(),
                            actor_kind: "npc".to_string(),
                        })
                        .collect::<Vec<_>>(),
                    Vec::new(),
                    vec!["household".to_string(), "rent_paid".to_string()],
                    Some(json!({
                        "household_id": household_id,
                        "rent_amount": rent_amount,
                        "reserve_used": reserve_used,
                        "landlord_balance": household.landlord_balance,
                    })),
                );
                self.record_accounting_transfer(
                    tick,
                    &self
                        .npc_location_for(&members.first().cloned().unwrap_or_default())
                        .unwrap_or_else(|| "settlement:greywall".to_string()),
                    format!("household_rent_pool:{household_id}"),
                    format!("landlord:{household_id}"),
                    "coin",
                    rent_amount,
                    None,
                );
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
            self.households_by_id
                .insert(household_id.clone(), household);

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
                self.deterministic_stream(tick, Phase::Commit, &contract_id, "contract_payout")
                    % 100;
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
                    worker.apprenticeship_progress =
                        (worker.apprenticeship_progress + 1).clamp(0, 200);
                    if matches!(
                        state.contract.compensation_type,
                        ContractCompensationType::Board | ContractCompensationType::Mixed
                    ) {
                        worker.food_reserve_days = (worker.food_reserve_days + 1).clamp(0, 14);
                    }
                }
                state.contract.reliability_score =
                    state.contract.reliability_score.saturating_add(1).min(95);
                self.record_accounting_transfer(
                    tick,
                    &settlement_id,
                    format!("employer:{settlement_id}"),
                    format!("npc:{worker_id}"),
                    "coin",
                    wage_amount,
                    None,
                );
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
            let roll =
                self.deterministic_stream(tick, Phase::IntentUpdate, &npc_id, "job_seek") % 4;
            if roll != 0 {
                continue;
            }
            let market_update =
                if let Some(market) = self.labor_market_by_settlement.get_mut(&location_id) {
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
            self.contracts_by_id
                .insert(contract_id.clone(), ContractState { contract });
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
        let node_ids = self
            .production_nodes_by_id
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for node_id in node_ids {
            let Some(mut node) = self.production_nodes_by_id.remove(&node_id) else {
                continue;
            };
            let location_id = node.settlement_id.clone();
            let mut emitted_start = false;
            let mut emitted_completed = false;
            let mut emitted_spoilage = None;

            let (demand_signal, storage_pressure) = self
                .stock_by_settlement
                .get(&location_id)
                .map(|stock| match node.node_kind.as_str() {
                    "farm" => {
                        let demand = if stock.staples <= 8 {
                            3
                        } else if stock.staples <= 14 {
                            2
                        } else if stock.staples <= 24 {
                            1
                        } else {
                            0
                        };
                        let pressure = (stock.staples - 32).max(0);
                        (demand, pressure)
                    }
                    "workshop" => {
                        let demand = if stock.craft_inputs <= 8 {
                            2
                        } else if stock.craft_inputs <= 16 {
                            1
                        } else {
                            0
                        };
                        let pressure = (stock.craft_inputs - 48).max(0);
                        (demand, pressure)
                    }
                    _ => {
                        let demand = if stock.fuel <= 5 {
                            2
                        } else if stock.fuel <= 12 {
                            1
                        } else {
                            0
                        };
                        let pressure = (stock.fuel - 40).max(0);
                        (demand, pressure)
                    }
                })
                .unwrap_or((1, 0));

            if tick % 12 == 0 && demand_signal > 0 {
                node.input_backlog = (node.input_backlog + demand_signal.min(2)).clamp(0, 10);
                emitted_start = true;
            }
            if node.input_backlog > 0 && tick % 6 == 0 {
                let transform = (1 + demand_signal / 2).clamp(1, 2);
                let transformed = node.input_backlog.min(transform);
                node.input_backlog -= transformed;
                node.output_backlog = (node.output_backlog + transformed).clamp(0, 10);
            }
            if node.output_backlog > 0 && tick % 8 == 0 {
                let completed_output = node.output_backlog.min((1 + demand_signal).clamp(1, 3));
                if let Some(stock) = self.stock_by_settlement.get_mut(&location_id) {
                    match node.node_kind.as_str() {
                        "farm" => {
                            let gain = completed_output * if demand_signal >= 2 { 2 } else { 1 };
                            stock.staples += gain;
                        }
                        "workshop" => stock.craft_inputs += completed_output,
                        _ => stock.fuel += completed_output,
                    }
                }
                node.output_backlog = (node.output_backlog - completed_output).max(0);
                emitted_completed = completed_output > 0;
            }
            if storage_pressure > 0 && node.output_backlog > 0 {
                node.output_backlog = (node.output_backlog - 1).max(0);
            }
            node.spoilage_timer -= 1;
            if node.spoilage_timer <= 0 {
                let mut resource_after = None;
                let mut resource_kind = "staples";
                let mut amount_lost = 0_i64;
                if let Some(stock) = self.stock_by_settlement.get_mut(&location_id) {
                    match node.node_kind.as_str() {
                        "farm" => {
                            let overflow = (stock.staples - 30).max(0);
                            amount_lost = (1 + overflow / 24).clamp(0, stock.staples.max(0));
                            stock.staples -= amount_lost;
                            resource_after = Some(stock.staples);
                            resource_kind = "staples";
                        }
                        "workshop" => {
                            let overflow = (stock.craft_inputs - 44).max(0);
                            amount_lost = (1 + overflow / 28).clamp(0, stock.craft_inputs.max(0));
                            stock.craft_inputs -= amount_lost;
                            resource_after = Some(stock.craft_inputs);
                            resource_kind = "craft_inputs";
                        }
                        _ => {
                            let overflow = (stock.fuel - 36).max(0);
                            amount_lost = (1 + overflow / 24).clamp(0, stock.fuel.max(0));
                            stock.fuel -= amount_lost;
                            resource_after = Some(stock.fuel);
                            resource_kind = "fuel";
                        }
                    }
                }
                if let Some(remaining) = resource_after {
                    if amount_lost > 0 {
                        self.record_accounting_transfer(
                            tick,
                            &location_id,
                            format!("stock:{location_id}"),
                            format!("spoilage:{location_id}"),
                            resource_kind,
                            amount_lost,
                            None,
                        );
                    }
                    emitted_spoilage = Some(remaining);
                }
                node.spoilage_timer = if storage_pressure > 0 { 12 } else { 18 };
            }

            let input_backlog = node.input_backlog;
            self.production_nodes_by_id
                .insert(node_id.clone(), node.clone());

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
            if let Some(resource_remaining) = emitted_spoilage {
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
                        "resource_remaining": resource_remaining,
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
                    self.record_accounting_transfer(
                        tick,
                        &settlement_id,
                        format!("route_network:{settlement_id}"),
                        format!("stock:{settlement_id}"),
                        "staples",
                        imported_staples,
                        None,
                    );
                    self.record_accounting_transfer(
                        tick,
                        &settlement_id,
                        format!("route_network:{settlement_id}"),
                        format!("stock:{settlement_id}"),
                        "fuel",
                        imported_fuel,
                        None,
                    );
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

                let mut overstock_staples = 0_i64;
                let mut overstock_fuel = 0_i64;
                let mut overstock_inputs = 0_i64;
                let mut maintenance_fee = 0_i64;
                if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                    let staples_cap = 36 + route_support * 2;
                    let fuel_cap = 28 + route_support;
                    let inputs_cap = 44 + route_support * 2;
                    if stock.staples > staples_cap {
                        let overflow = stock.staples - staples_cap;
                        overstock_staples = (overflow / 3).max(1).min(stock.staples);
                        stock.staples -= overstock_staples;
                    }
                    if stock.fuel > fuel_cap {
                        let overflow = stock.fuel - fuel_cap;
                        overstock_fuel = (overflow / 3).max(1).min(stock.fuel);
                        stock.fuel -= overstock_fuel;
                    }
                    if stock.craft_inputs > inputs_cap {
                        let overflow = stock.craft_inputs - inputs_cap;
                        overstock_inputs = (overflow / 4).max(1).min(stock.craft_inputs);
                        stock.craft_inputs -= overstock_inputs;
                    }
                    let volume =
                        stock.staples.max(0) + stock.fuel.max(0) + stock.craft_inputs.max(0);
                    maintenance_fee = (volume / 120).clamp(0, 4).min(stock.coin_reserve.max(0));
                    stock.coin_reserve -= maintenance_fee;
                }
                if overstock_staples > 0 || overstock_fuel > 0 || overstock_inputs > 0 {
                    if overstock_staples > 0 {
                        self.record_accounting_transfer(
                            tick,
                            &settlement_id,
                            format!("stock:{settlement_id}"),
                            format!("overstock_decay:{settlement_id}"),
                            "staples",
                            overstock_staples,
                            None,
                        );
                    }
                    if overstock_fuel > 0 {
                        self.record_accounting_transfer(
                            tick,
                            &settlement_id,
                            format!("stock:{settlement_id}"),
                            format!("overstock_decay:{settlement_id}"),
                            "fuel",
                            overstock_fuel,
                            None,
                        );
                    }
                    if overstock_inputs > 0 {
                        self.record_accounting_transfer(
                            tick,
                            &settlement_id,
                            format!("stock:{settlement_id}"),
                            format!("overstock_decay:{settlement_id}"),
                            "craft_inputs",
                            overstock_inputs,
                            None,
                        );
                    }
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::SpoilageOccurred,
                        settlement_id.clone(),
                        Vec::new(),
                        Vec::new(),
                        vec!["supply".to_string(), "overstock_decay".to_string()],
                        Some(json!({
                            "settlement_id": settlement_id,
                            "staples_lost": overstock_staples,
                            "fuel_lost": overstock_fuel,
                            "craft_inputs_lost": overstock_inputs,
                        })),
                    );
                }
                if maintenance_fee > 0 {
                    self.record_accounting_transfer(
                        tick,
                        &settlement_id,
                        format!("stock:{settlement_id}"),
                        format!("maintenance:{settlement_id}"),
                        "coin",
                        maintenance_fee,
                        None,
                    );
                }
            }
            let Some((staples, price_pressure, shortage, recovered)) = ({
                if let Some(stock) = self.stock_by_settlement.get_mut(&settlement_id) {
                    stock.local_price_pressure = (14 - stock.staples).clamp(-10, 30);
                    let staples = stock.staples;
                    let price_pressure = stock.local_price_pressure;
                    Some((
                        staples,
                        price_pressure,
                        staples <= 4,
                        staples >= 10 && tick % 24 == 0,
                    ))
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

            if tick % 24 == 0 {
                let (shortage_score, wage_pressure, unmet_demand, market_cleared, prices) = self
                    .stock_by_settlement
                    .get(&settlement_id)
                    .map(|stock| {
                        let shortage_score = ((6 - stock.staples).max(0)
                            + (3 - stock.fuel).max(0)
                            + (2 - stock.medicine).max(0))
                        .clamp(0, 24);
                        let wage_pressure = self
                            .labor_market_by_settlement
                            .get(&settlement_id)
                            .map(|market| (market.underemployment_index / 8).clamp(0, 20))
                            .unwrap_or(0);
                        let unmet_demand = (shortage_score + wage_pressure / 2).clamp(0, 30);
                        let market_cleared = shortage_score <= 6 && unmet_demand <= 8;
                        let staples_price_index = 100 + shortage_score * 4;
                        let fuel_price_index = 100 + (shortage_score * 3 / 2);
                        let medicine_price_index = 100 + (shortage_score * 5 / 2);
                        (
                            shortage_score,
                            wage_pressure,
                            unmet_demand,
                            market_cleared,
                            (staples_price_index, fuel_price_index, medicine_price_index),
                        )
                    })
                    .unwrap_or((0, 0, 0, true, (100, 100, 100)));

                self.market_clearing_by_settlement.insert(
                    settlement_id.clone(),
                    MarketClearingRuntimeState {
                        settlement_id: settlement_id.clone(),
                        staples_price_index: prices.0,
                        fuel_price_index: prices.1,
                        medicine_price_index: prices.2,
                        wage_pressure,
                        shortage_score,
                        unmet_demand,
                        cleared_tick: tick,
                        market_cleared,
                    },
                );
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    if market_cleared {
                        EventType::MarketCleared
                    } else {
                        EventType::MarketFailed
                    },
                    settlement_id.clone(),
                    Vec::new(),
                    Vec::new(),
                    vec!["market".to_string(), "clearing".to_string()],
                    Some(json!({
                        "settlement_id": settlement_id,
                        "shortage_score": shortage_score,
                        "wage_pressure": wage_pressure,
                        "unmet_demand": unmet_demand,
                        "staples_price_index": prices.0,
                        "fuel_price_index": prices.1,
                        "medicine_price_index": prices.2,
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
            let updates = (edge_keys.len() / 96).clamp(4, 48);
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
                    .map(|profile| {
                        (profile.empathy + profile.sociability + profile.dutifulness - 150) / 44
                    })
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
                let reverse_trust = self
                    .relationship_edges
                    .get(&(edge.target_npc_id.clone(), edge.source_npc_id.clone()))
                    .map(|reverse| reverse.trust)
                    .unwrap_or(0);
                let source_action = self
                    .current_action_by_npc
                    .get(&edge.source_npc_id)
                    .map(String::as_str);
                let cooperative_bias = matches!(
                    source_action,
                    Some(
                        "share_meal"
                            | "mediate_dispute"
                            | "converse_neighbor"
                            | "lend_coin"
                            | "work_for_food"
                            | "court_romance"
                            | "form_mutual_aid_group"
                    )
                ) as i64
                    * 2;
                let antisocial_bias = matches!(
                    source_action,
                    Some("steal_supplies" | "spread_accusation" | "avoid_patrols" | "fence_goods")
                ) as i64
                    * 2;
                let volatility = (self.deterministic_stream(
                    tick,
                    Phase::MemoryBelief,
                    &edge.source_npc_id,
                    &edge.target_npc_id,
                ) % 5) as i64
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
                let net_pressure = social_pressure + household_stress - trust_buffer
                    + volatility
                    + antisocial_bias
                    - cooperative_bias
                    - trait_support
                    - compatibility_bias
                    - shared_group
                    - shared_hardship
                    + trust_saturation
                    + baseline_drift;
                let mut delta = if net_pressure >= 3 {
                    -1
                } else if net_pressure <= -2 && edge.trust < (58 + compatibility_bias.clamp(-4, 4))
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
                if delta > 0 && reverse_trust <= -35 {
                    delta = 0;
                } else if delta < 0 && reverse_trust >= 45 {
                    delta = 0;
                }

                if delta != 0 {
                    let trust_step = if net_pressure.abs() >= 8 {
                        3
                    } else if net_pressure.abs() >= 5 {
                        2
                    } else {
                        1
                    };
                    edge.trust = (edge.trust + delta * trust_step).clamp(-100, 100);
                    edge.grievance =
                        (edge.grievance + if delta < 0 { 3 } else { -2 }).clamp(0, 100);
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
                    let trust_target = (edge.compatibility_score / 2 + shared_group * 6
                        - (household_stress / 3)
                        + (reverse_trust / 8))
                        .clamp(-35, 55);
                    edge.trust = step_toward(
                        edge.trust,
                        trust_target,
                        if edge.trust.abs() >= 70 { 2 } else { 1 },
                    );
                    let obligation_target = if edge.trust > 20 {
                        (10 + edge.trust / 4).clamp(6, 40)
                    } else {
                        0
                    };
                    let grievance_target = if edge.trust < -15 {
                        ((-edge.trust) / 3).clamp(4, 40)
                    } else {
                        0
                    };
                    edge.obligation = step_toward(edge.obligation, obligation_target, 2);
                    edge.attachment = step_toward(edge.attachment, obligation_target / 2, 2);
                    edge.grievance = step_toward(edge.grievance, grievance_target, 2);
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
                .map(|entry| {
                    (100 - entry.bias_level - entry.corruption_level).clamp(0, 80) as f32 / 2000.0
                })
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
            let (trusted_links, hostile_links) = self
                .relationship_edges
                .values()
                .filter(|edge| edge.source_npc_id == npc.npc_id)
                .fold((0_i64, 0_i64), |(trusted, hostile), edge| {
                    (
                        trusted + (edge.trust >= 20) as i64,
                        hostile + (edge.trust <= -20) as i64,
                    )
                });
            let institution_bias = self
                .institutions_by_settlement
                .get(&npc.location_id)
                .map(|institution| institution.bias_level)
                .unwrap_or(0);
            let institution_corruption = self
                .institutions_by_settlement
                .get(&npc.location_id)
                .map(|institution| institution.corruption_level)
                .unwrap_or(0);
            let contested_signal = emitted_distortion >= 0.28
                || (emitted_confidence >= 0.25
                    && emitted_confidence <= 0.78
                    && hostile_links > trusted_links);
            if contested_signal
                && (rumor_heat > 0 || institution_bias >= 30 || institution_corruption >= 35)
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
                        "trusted_links": trusted_links,
                        "hostile_links": hostile_links,
                        "institution_bias": institution_bias,
                        "institution_corruption": institution_corruption,
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

    fn progress_social_dynamics_loop(
        &mut self,
        tick: u64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        let settlements = self
            .npcs
            .iter()
            .map(|npc| npc.location_id.clone())
            .collect::<BTreeSet<_>>();
        let recent_events = self
            .event_log
            .iter()
            .rev()
            .take(512)
            .cloned()
            .collect::<Vec<_>>();

        for settlement_id in settlements {
            let nearby_locations = self
                .routes_by_id
                .values()
                .filter(|route| route.weather_window_open && route.hazard_score <= 60)
                .filter_map(|route| {
                    if route.origin_settlement_id == settlement_id {
                        Some(route.destination_settlement_id.clone())
                    } else if route.destination_settlement_id == settlement_id {
                        Some(route.origin_settlement_id.clone())
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>();
            let local_npcs = self
                .npcs
                .iter()
                .filter(|npc| {
                    npc.location_id == settlement_id || nearby_locations.contains(&npc.location_id)
                })
                .map(|npc| npc.npc_id.clone())
                .collect::<Vec<_>>();
            if local_npcs.len() < 2 {
                continue;
            }
            let interaction_npcs = local_npcs
                .iter()
                .filter(|npc_id| {
                    let sociability = self
                        .npc_traits_by_id
                        .get(*npc_id)
                        .map(|profile| profile.sociability)
                        .unwrap_or(50);
                    let resident_bonus = self
                        .npc_location_for(npc_id)
                        .map(|location| (location == settlement_id) as i64 * 24)
                        .unwrap_or(0);
                    let presence_threshold = (12 + sociability / 2 + resident_bonus).clamp(12, 72);
                    let presence_roll = (self.deterministic_stream(
                        tick,
                        Phase::Commit,
                        "social_presence",
                        &format!("{settlement_id}:{npc_id}"),
                    ) % 100) as i64;
                    presence_roll < presence_threshold
                })
                .cloned()
                .collect::<Vec<_>>();

            let thefts_recent = recent_events
                .iter()
                .filter(|event| {
                    event.location_id == settlement_id
                        && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let mut thefts_today = self
                .event_log
                .iter()
                .filter(|event| {
                    event.tick / 24 == tick / 24
                        && event.location_id == settlement_id
                        && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let conflicts_recent = recent_events
                .iter()
                .filter(|event| {
                    event.location_id == settlement_id
                        && matches!(
                            event.event_type,
                            EventType::InsultExchanged
                                | EventType::PunchThrown
                                | EventType::BrawlStarted
                        )
                })
                .count() as i64;

            let rumor_heat = self
                .scenario_state
                .rumor_heat_by_location
                .get(&settlement_id)
                .copied()
                .unwrap_or(0)
                .max(0);
            let civic_energy = interaction_npcs.len() as i64
                + rumor_heat
                + thefts_recent * 2
                + conflicts_recent * 2;
            if civic_energy >= 14 {
                let civic_key = format!("{settlement_id}:civic");
                let last_civic_tick = self
                    .last_social_channel_tick
                    .get(&civic_key)
                    .copied()
                    .unwrap_or(0);
                let civic_gap = tick.saturating_sub(last_civic_tick);
                if civic_gap >= 18 {
                    let civic_roll = (self.deterministic_stream(
                        tick,
                        Phase::Commit,
                        "civic_signal",
                        &settlement_id,
                    ) % 100) as i64;
                    let civic_threshold =
                        (8 + civic_energy / 3 - (civic_gap / 24) as i64).clamp(6, 34);
                    if civic_roll < civic_threshold {
                        let is_festival = civic_energy >= 22
                            || self.deterministic_stream(
                                tick,
                                Phase::Commit,
                                "civic_flavor",
                                &settlement_id,
                            ) % 3
                                == 0;
                        let (topic, actor_id, tags) = if is_festival {
                            (
                                "seasonal_festival",
                                format!("festival_host:{settlement_id}"),
                                vec!["civic".to_string(), "festival".to_string()],
                            )
                        } else {
                            (
                                "mayor_tour",
                                format!("mayor:{settlement_id}"),
                                vec!["civic".to_string(), "mayor_tour".to_string()],
                            )
                        };
                        self.push_runtime_event(
                            tick,
                            sequence_in_tick,
                            causal_chain,
                            system_event_id,
                            EventType::ObservationLogged,
                            settlement_id.clone(),
                            vec![ActorRef {
                                actor_id,
                                actor_kind: "institution".to_string(),
                            }],
                            Vec::new(),
                            tags,
                            Some(json!({
                                "topic": topic,
                                "attendance_estimate": interaction_npcs.len(),
                                "civic_energy": civic_energy,
                            })),
                        );
                        self.last_social_channel_tick.insert(civic_key, tick);
                    }
                }
            }
            let bandit_phase = self.deterministic_stream(
                0,
                Phase::Commit,
                "bandit_activity_phase",
                &settlement_id,
            ) % 24;
            let last_raid_tick = self
                .last_bandit_raid_tick_by_settlement
                .get(&settlement_id)
                .copied()
                .unwrap_or(0);
            let raid_cooldown_ticks = (24 * 5) as u64;
            let bandit_roll =
                (self.deterministic_stream(tick, Phase::Commit, "bandit_activity", &settlement_id)
                    % 100) as i64;
            let bandit_threshold = (2
                + (self.pressure_index / 140).clamp(0, 4)
                + (thefts_recent / 5).clamp(0, 3)
                + (conflicts_recent / 7).clamp(0, 2))
            .clamp(2, 10);
            if tick % 24 == bandit_phase
                && tick.saturating_sub(last_raid_tick) >= raid_cooldown_ticks
                && thefts_today == 0
                && bandit_roll < bandit_threshold
            {
                let victim_owner = format!("store:{settlement_id}");
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::TheftCommitted,
                    settlement_id.clone(),
                    vec![ActorRef {
                        actor_id: format!("bandit:{}", settlement_id.replace(':', "_")),
                        actor_kind: "bandit".to_string(),
                    }],
                    vec![ActorRef {
                        actor_id: victim_owner.clone(),
                        actor_kind: "owner".to_string(),
                    }],
                    vec!["bandit".to_string(), "raid".to_string()],
                    Some(json!({
                        "item_id": format!("loot:{}:{tick}", settlement_id.replace(':', "_")),
                        "from_owner_id": victim_owner,
                        "to_owner_id": "bandit_group",
                        "wanted": true,
                        "source": "bandit_activity",
                    })),
                );
                apply_case_delta(&mut self.law_case_load_by_settlement, &settlement_id, 1);
                self.last_bandit_raid_tick_by_settlement
                    .insert(settlement_id.clone(), tick);
                thefts_today += 1;
            }

            let theft_candidates = interaction_npcs
                .iter()
                .filter_map(|npc_id| {
                    let economy = self.npc_economy_by_id.get(npc_id)?;
                    let traits = self.npc_traits_by_id.get(npc_id)?;
                    if self.wanted_npcs.contains(npc_id)
                        || self.npc_holds_stolen_item(npc_id)
                        || economy.wallet > 5
                    {
                        return None;
                    }
                    let household_pressure = self
                        .households_by_id
                        .get(&economy.household_id)
                        .map(|household| household.eviction_risk_score / 14)
                        .unwrap_or(0);
                    let trust_support = self
                        .relationship_edges
                        .values()
                        .filter(|edge| edge.source_npc_id == *npc_id && edge.trust >= 22)
                        .count() as i64;
                    let hardship = economy.debt_balance
                        + (2 - economy.food_reserve_days).max(0) * 2
                        + household_pressure;
                    let pull = hardship * 3 + traits.risk_tolerance / 5 + traits.ambition / 8
                        - traits.empathy / 10
                        - trust_support * 3
                        - thefts_recent * 3;
                    if pull < 18 {
                        return None;
                    }
                    let recent_theft_penalty = self
                        .action_memory_by_npc
                        .get(npc_id)
                        .and_then(|memory| memory.last_theft_tick)
                        .map(|last| {
                            if tick.saturating_sub(last) < 96 {
                                34
                            } else {
                                0
                            }
                        })
                        .unwrap_or(0);
                    let weight = (pull - recent_theft_penalty).clamp(0, 96);
                    (weight > 0).then_some((npc_id.clone(), weight))
                })
                .collect::<Vec<_>>();
            let last_global_theft_tick = self
                .last_social_channel_tick
                .get("global:theft")
                .copied()
                .unwrap_or(0);
            if !theft_candidates.is_empty()
                && thefts_recent <= 2
                && thefts_today == 0
                && tick.saturating_sub(last_global_theft_tick) >= 24
            {
                let weights = theft_candidates
                    .iter()
                    .map(|(_, weight)| *weight)
                    .collect::<Vec<_>>();
                let pick_roll = self.deterministic_stream(
                    tick,
                    Phase::Commit,
                    "npc_theft_candidate",
                    &settlement_id,
                );
                if let Some(idx) = pick_weighted_index(pick_roll, &weights) {
                    let thief_id = theft_candidates[idx].0.clone();
                    let theft_roll = (self.deterministic_stream(
                        tick,
                        Phase::Commit,
                        "npc_theft_trigger",
                        &format!("{settlement_id}:{thief_id}"),
                    ) % 100) as i64;
                    let theft_threshold =
                        (1 + theft_candidates[idx].1 / 14 - thefts_recent * 2).clamp(1, 8);
                    if theft_roll < theft_threshold {
                        let (item_id, from_owner_id) =
                            self.select_item_for_theft(&settlement_id, &thief_id, tick);
                        self.item_registry.insert(
                            item_id.clone(),
                            ItemRecord {
                                owner_id: thief_id.clone(),
                                location_id: settlement_id.clone(),
                                stolen: true,
                                last_moved_tick: tick,
                            },
                        );
                        self.wanted_npcs.insert(thief_id.clone());
                        apply_case_delta(&mut self.law_case_load_by_settlement, &settlement_id, 2);
                        self.push_runtime_event(
                            tick,
                            sequence_in_tick,
                            causal_chain,
                            system_event_id,
                            EventType::TheftCommitted,
                            settlement_id.clone(),
                            vec![ActorRef {
                                actor_id: thief_id.clone(),
                                actor_kind: "npc".to_string(),
                            }],
                            vec![ActorRef {
                                actor_id: from_owner_id.clone(),
                                actor_kind: "owner".to_string(),
                            }],
                            vec![
                                "crime".to_string(),
                                "theft".to_string(),
                                "opportunistic".to_string(),
                            ],
                            Some(json!({
                                "item_id": item_id,
                                "from_owner_id": from_owner_id,
                                "to_owner_id": thief_id,
                                "source": "social_dynamics",
                                "theft_threshold": theft_threshold,
                            })),
                        );
                        self.last_social_channel_tick
                            .insert("global:theft".to_string(), tick);
                        self.last_social_channel_tick
                            .insert(format!("{settlement_id}:theft"), tick);
                        let memory = self.action_memory_by_npc.entry(thief_id).or_default();
                        memory.last_theft_tick = Some(tick);
                    }
                }
            }

            self.emit_settlement_context_nudge(
                tick,
                &settlement_id,
                interaction_npcs.as_slice(),
                thefts_recent,
                conflicts_recent,
                civic_energy,
                system_event_id,
                sequence_in_tick,
                causal_chain,
            );
        }

        let notable = self
            .event_log
            .iter()
            .filter(|event| event.tick == tick)
            .filter(|event| {
                matches!(
                    event.event_type,
                    EventType::TheftCommitted
                        | EventType::ArrestMade
                        | EventType::InsultExchanged
                        | EventType::PunchThrown
                        | EventType::BrawlStarted
                        | EventType::GuardsDispatched
                        | EventType::StockShortage
                        | EventType::MarketFailed
                        | EventType::StockRecovered
                        | EventType::MarketCleared
                        | EventType::RomanceAdvanced
                        | EventType::ConversationHeld
                        | EventType::CaravanSpawned
                        | EventType::NpcActionCommitted
                )
            })
            .filter(|event| {
                if event.event_type != EventType::NpcActionCommitted {
                    return true;
                }
                matches!(
                    event
                        .details
                        .as_ref()
                        .and_then(|details| {
                            details
                                .get("canonical_action")
                                .and_then(Value::as_str)
                                .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                        })
                        .map(canonical_action_name),
                    Some(
                        "converse_neighbor"
                            | "lend_coin"
                            | "court_romance"
                            | "steal_supplies"
                            | "pay_rent"
                            | "seek_treatment"
                            | "trade_visit"
                    )
                )
            })
            .map(|event| {
                (
                    event.event_id.clone(),
                    event.location_id.clone(),
                    event.event_type,
                    event.tags.clone(),
                    event.details.clone(),
                    event.actors.first().map(|actor| actor.actor_id.clone()),
                    event.targets.first().map(|target| target.actor_id.clone()),
                    event
                        .details
                        .as_ref()
                        .and_then(|details| {
                            details
                                .get("canonical_action")
                                .and_then(Value::as_str)
                                .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                        })
                        .map(canonical_action_name)
                        .map(str::to_string),
                )
            })
            .collect::<Vec<_>>();
        if notable.is_empty() {
            return;
        }

        let npcs = self.npcs.clone();
        for npc in npcs {
            let nearby_locations = self
                .routes_by_id
                .values()
                .filter(|route| route.weather_window_open && route.hazard_score <= 60)
                .filter_map(|route| {
                    if route.origin_settlement_id == npc.location_id {
                        Some(route.destination_settlement_id.clone())
                    } else if route.destination_settlement_id == npc.location_id {
                        Some(route.origin_settlement_id.clone())
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>();
            let local_notable = notable
                .iter()
                .filter(|(_, location_id, _, _, _, _, _, _)| {
                    location_id == &npc.location_id || nearby_locations.contains(location_id)
                })
                .collect::<Vec<_>>();
            if local_notable.is_empty() {
                continue;
            }
            let profile = self.npc_traits_by_id.get(&npc.npc_id).cloned();
            let awareness_threshold = 28
                + profile
                    .as_ref()
                    .map(|entry| entry.sociability / 4)
                    .unwrap_or(10)
                + profile
                    .as_ref()
                    .map(|entry| entry.gossip_drive / 5)
                    .unwrap_or(10);
            let awareness_roll = (self.deterministic_stream(
                tick,
                Phase::Commit,
                "observation_awareness",
                &npc.npc_id,
            ) % 100) as i64;
            if awareness_roll > awareness_threshold.clamp(12, 72) {
                continue;
            }

            let notable_weights = local_notable
                .iter()
                .map(|(_, _, event_type, _, _, _, _, _)| match event_type {
                    EventType::BrawlStarted | EventType::ArrestMade => 18,
                    EventType::PunchThrown | EventType::TheftCommitted => 16,
                    EventType::GuardsDispatched => 14,
                    EventType::StockShortage | EventType::MarketFailed => 13,
                    EventType::RomanceAdvanced => 11,
                    EventType::ConversationHeld => 8,
                    EventType::StockRecovered | EventType::MarketCleared => 7,
                    EventType::NpcActionCommitted => 6,
                    _ => 5,
                })
                .collect::<Vec<_>>();
            let idx = pick_weighted_index(
                self.deterministic_stream(tick, Phase::Commit, "observation_pick", &npc.npc_id),
                &notable_weights,
            )
            .unwrap_or(0);
            let (
                source_event_id,
                _,
                source_event_type,
                source_tags,
                source_details,
                source_actor_id,
                source_target_id,
                chosen_action,
            ) = local_notable[idx];
            let topic = observation_topic_for_event(
                *source_event_type,
                source_tags,
                source_details.as_ref(),
                chosen_action.as_deref(),
            );
            let salience = match source_event_type {
                EventType::BrawlStarted | EventType::ArrestMade => 9,
                EventType::PunchThrown | EventType::TheftCommitted => 8,
                EventType::StockShortage | EventType::MarketFailed => 7,
                EventType::RomanceAdvanced => 6,
                EventType::GuardsDispatched => 8,
                EventType::ConversationHeld => 5,
                EventType::StockRecovered | EventType::MarketCleared => 5,
                EventType::NpcActionCommitted => 5,
                _ => 5,
            };
            let memory = ObservationRuntime {
                tick,
                location_id: npc.location_id.clone(),
                topic: topic.clone(),
                salience,
                source_event_id: source_event_id.clone(),
            };
            let entry = self
                .observations_by_npc
                .entry(npc.npc_id.clone())
                .or_default();
            if let Some(previous) = entry.last() {
                let min_gap = if salience >= 8 { 3 } else { 8 };
                if previous.topic == topic && tick.saturating_sub(previous.tick) < 18 {
                    continue;
                }
                if tick.saturating_sub(previous.tick) < min_gap {
                    continue;
                }
            }
            entry.push(memory);
            if entry.len() > 24 {
                entry.remove(0);
            }

            self.push_runtime_event(
                tick,
                sequence_in_tick,
                causal_chain,
                system_event_id,
                EventType::ObservationLogged,
                npc.location_id.clone(),
                vec![ActorRef {
                    actor_id: npc.npc_id.clone(),
                    actor_kind: "npc".to_string(),
                }],
                Vec::new(),
                vec!["social".to_string(), "observation".to_string()],
                Some(json!({
                    "topic": topic,
                    "salience": salience,
                    "source_event_id": source_event_id,
                    "source_tags": source_tags,
                    "source_action": chosen_action,
                })),
            );

            self.emit_dynamic_reaction_from_observation(
                tick,
                &npc.location_id,
                &npc.npc_id,
                source_event_id,
                *source_event_type,
                source_tags,
                source_details.as_ref(),
                source_actor_id.as_deref(),
                source_target_id.as_deref(),
                chosen_action.as_deref(),
                salience,
                sequence_in_tick,
                causal_chain,
                system_event_id,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_settlement_context_nudge(
        &mut self,
        tick: u64,
        settlement_id: &str,
        interaction_npcs: &[String],
        thefts_recent: i64,
        conflicts_recent: i64,
        civic_energy: i64,
        system_event_id: &str,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
    ) {
        if interaction_npcs.is_empty() {
            return;
        }
        let channel_key = format!("{settlement_id}:context_nudge");
        let last_tick = self
            .last_social_channel_tick
            .get(&channel_key)
            .copied()
            .unwrap_or(0);
        let min_gap = if thefts_recent > 0 || conflicts_recent > 0 {
            4
        } else {
            8
        };
        if tick.saturating_sub(last_tick) < min_gap {
            return;
        }

        let mut candidates = vec![
            (
                "public_chat",
                EventType::ConversationHeld,
                (10 + civic_energy / 2 + interaction_npcs.len() as i64 / 2).clamp(4, 80),
            ),
            (
                "street_notice",
                EventType::ObservationLogged,
                (8 + thefts_recent * 2 + conflicts_recent * 2 + civic_energy / 4).clamp(2, 80),
            ),
        ];
        if thefts_recent > 0 || conflicts_recent > 1 {
            candidates.push((
                "guard_attention",
                EventType::GuardsDispatched,
                (4 + thefts_recent * 3 + conflicts_recent * 3).clamp(1, 70),
            ));
        }
        if interaction_npcs.len() >= 2 && civic_energy >= 10 {
            candidates.push((
                "mutual_aid",
                EventType::LoanExtended,
                (3 + civic_energy / 3 - conflicts_recent).clamp(1, 32),
            ));
        }
        let weights = candidates
            .iter()
            .map(|(_, _, weight)| *weight)
            .collect::<Vec<_>>();
        let pick_roll =
            self.deterministic_stream(tick, Phase::Commit, "context_nudge_pick", settlement_id);
        let Some(idx) = pick_weighted_index(pick_roll, &weights) else {
            return;
        };
        let (nudge_key, event_type, _) = candidates[idx];

        let source_idx =
            (self.deterministic_stream(tick, Phase::Commit, "context_nudge_actor", settlement_id)
                as usize)
                % interaction_npcs.len();
        let source_npc_id = interaction_npcs[source_idx].clone();
        let mut target_npc_id = None;
        if interaction_npcs.len() >= 2 {
            let mut target_idx = (self.deterministic_stream(
                tick,
                Phase::Commit,
                "context_nudge_target",
                &source_npc_id,
            ) as usize)
                % interaction_npcs.len();
            if target_idx == source_idx {
                target_idx = (target_idx + 1) % interaction_npcs.len();
            }
            target_npc_id = Some(interaction_npcs[target_idx].clone());
        }

        match event_type {
            EventType::ConversationHeld => {
                let topic_pool = [
                    "market_prices",
                    "workday_exhaustion",
                    "road_rumors",
                    "local_craft",
                    "harvest_timing",
                    "household_matters",
                ];
                let topic_idx = (self.deterministic_stream(
                    tick,
                    Phase::Commit,
                    "context_nudge_topic",
                    settlement_id,
                ) as usize)
                    % topic_pool.len();
                if let Some(target_id) = target_npc_id.clone() {
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::ConversationHeld,
                        settlement_id.to_string(),
                        vec![ActorRef {
                            actor_id: source_npc_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        vec![ActorRef {
                            actor_id: target_id.clone(),
                            actor_kind: "npc".to_string(),
                        }],
                        vec![
                            "social".to_string(),
                            "context_nudge".to_string(),
                            "conversation".to_string(),
                        ],
                        Some(json!({
                            "topic": topic_pool[topic_idx],
                            "source": nudge_key,
                        })),
                    );
                    if let Some(edge) = self
                        .relationship_edges
                        .get_mut(&(source_npc_id.clone(), target_id.clone()))
                    {
                        edge.trust = (edge.trust + 1).clamp(-100, 100);
                        edge.recent_interaction_tick = tick;
                    }
                    let memory = self.pair_memory_mut(&source_npc_id, &target_id);
                    memory.last_social_tick = tick;
                }
            }
            EventType::ObservationLogged => {
                let topic = if thefts_recent > conflicts_recent {
                    "watch_patrols"
                } else if conflicts_recent > 0 {
                    "street_tension"
                } else if civic_energy > 16 {
                    "public_gathering"
                } else {
                    "quiet_evening"
                };
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::ObservationLogged,
                    settlement_id.to_string(),
                    vec![ActorRef {
                        actor_id: source_npc_id,
                        actor_kind: "npc".to_string(),
                    }],
                    Vec::new(),
                    vec!["social".to_string(), "context_nudge".to_string()],
                    Some(json!({
                        "topic": topic,
                        "source": nudge_key,
                        "civic_energy": civic_energy,
                    })),
                );
            }
            EventType::GuardsDispatched => {
                let wanted_target = self
                    .first_wanted_npc_at_location(settlement_id)
                    .or(target_npc_id.clone());
                if let Some(target_id) = wanted_target {
                    self.push_runtime_event(
                        tick,
                        sequence_in_tick,
                        causal_chain,
                        system_event_id,
                        EventType::GuardsDispatched,
                        settlement_id.to_string(),
                        vec![ActorRef {
                            actor_id: format!("guard_unit:{settlement_id}"),
                            actor_kind: "institution".to_string(),
                        }],
                        vec![ActorRef {
                            actor_id: target_id,
                            actor_kind: "npc".to_string(),
                        }],
                        vec![
                            "institution".to_string(),
                            "context_nudge".to_string(),
                            "response".to_string(),
                        ],
                        Some(json!({
                            "source": nudge_key,
                            "thefts_recent": thefts_recent,
                            "conflicts_recent": conflicts_recent,
                        })),
                    );
                    apply_case_delta(&mut self.law_case_load_by_settlement, settlement_id, 1);
                }
            }
            EventType::LoanExtended => {
                let Some(target_id) = target_npc_id.clone() else {
                    return;
                };
                let lender_wallet = self
                    .npc_economy_by_id
                    .get(&source_npc_id)
                    .map(|economy| economy.wallet)
                    .unwrap_or(0);
                let borrower_wallet = self
                    .npc_economy_by_id
                    .get(&target_id)
                    .map(|economy| economy.wallet)
                    .unwrap_or(0);
                if lender_wallet <= 3 || borrower_wallet >= 3 {
                    return;
                }
                let amount = lender_wallet.min(2).max(1);
                if let Some(lender) = self.npc_economy_by_id.get_mut(&source_npc_id) {
                    lender.wallet -= amount;
                }
                if let Some(borrower) = self.npc_economy_by_id.get_mut(&target_id) {
                    borrower.wallet += amount;
                    borrower.debt_balance = (borrower.debt_balance + amount).clamp(0, 300);
                }
                self.record_accounting_transfer(
                    tick,
                    settlement_id,
                    format!("npc:{source_npc_id}"),
                    format!("npc:{target_id}"),
                    "coin",
                    amount,
                    None,
                );
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::LoanExtended,
                    settlement_id.to_string(),
                    vec![ActorRef {
                        actor_id: source_npc_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    vec![ActorRef {
                        actor_id: target_id.clone(),
                        actor_kind: "npc".to_string(),
                    }],
                    vec![
                        "social".to_string(),
                        "context_nudge".to_string(),
                        "aid".to_string(),
                    ],
                    Some(json!({
                        "amount": amount,
                        "currency": "coin",
                        "source": nudge_key,
                    })),
                );
                if let Some(edge) = self
                    .relationship_edges
                    .get_mut(&(source_npc_id.clone(), target_id.clone()))
                {
                    edge.trust = (edge.trust + 2).clamp(-100, 100);
                    edge.obligation = (edge.obligation + 1).clamp(0, 100);
                    edge.recent_interaction_tick = tick;
                }
                let memory = self.pair_memory_mut(&source_npc_id, &target_id);
                memory.last_social_tick = tick;
            }
            _ => {}
        }
        self.last_social_channel_tick.insert(channel_key, tick);
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
                    event.location_id == settlement_id
                        && event.event_type == EventType::TheftCommitted
                })
                .count() as i64;
            let Some(mut institution) = self.institutions_by_settlement.remove(&settlement_id)
            else {
                continue;
            };
            let law_pressure = law_cases.clamp(0, 16);
            let theft_pressure = thefts_recent.clamp(0, 16);
            let social_fragment = (self.social_cohesion < -20) as i64;
            let target_capacity = (62 - law_pressure * 2 - theft_pressure / 3
                + (self.social_cohesion > 20) as i64 * 4)
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

            if tick % 24 == 0 {
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
            }

            let queue_state = self
                .institution_queue_by_settlement
                .entry(settlement_id.clone())
                .or_insert_with(|| InstitutionQueueRuntimeState {
                    settlement_id: settlement_id.clone(),
                    pending_cases: 0,
                    processed_cases: 0,
                    dropped_cases: 0,
                    avg_response_latency: 2,
                });
            queue_state.pending_cases = law_cases.max(0);
            queue_state.processed_cases +=
                (enforcement_capacity / 18).max(0) + (law_cases == 0) as i64;
            if corruption_level >= 70 {
                queue_state.dropped_cases += (law_cases / 3).max(1);
            }
            queue_state.avg_response_latency =
                step_toward(queue_state.avg_response_latency, response_latency_ticks, 1);
            let pending_cases = queue_state.pending_cases;
            let processed_cases = queue_state.processed_cases;
            let dropped_cases = queue_state.dropped_cases;
            let avg_response_latency = queue_state.avg_response_latency;
            if tick % 24 == 0 {
                self.push_runtime_event(
                    tick,
                    sequence_in_tick,
                    causal_chain,
                    system_event_id,
                    EventType::InstitutionQueueUpdated,
                    settlement_id.clone(),
                    vec![ActorRef {
                        actor_id: format!("institution_queue:{settlement_id}"),
                        actor_kind: "institution_queue".to_string(),
                    }],
                    Vec::new(),
                    vec!["institution".to_string(), "queue".to_string()],
                    Some(json!({
                        "settlement_id": settlement_id,
                        "pending_cases": pending_cases,
                        "processed_cases": processed_cases,
                        "dropped_cases": dropped_cases,
                        "avg_response_latency": avg_response_latency,
                    })),
                );
            }
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
                let recent_group_churn = self
                    .event_log
                    .iter()
                    .rev()
                    .take(180)
                    .filter(|event| {
                        event.location_id == settlement_id
                            && matches!(
                                event.event_type,
                                EventType::GroupSplit
                                    | EventType::GroupDissolved
                                    | EventType::GroupFormed
                            )
                    })
                    .count() as i64;
                if recent_group_churn >= 2 {
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
                        if let Some(edge) =
                            self.relationship_edges.get(&(left.clone(), right.clone()))
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
                                event
                                    .details
                                    .as_ref()
                                    .and_then(|details| {
                                        details
                                            .get("canonical_action")
                                            .and_then(Value::as_str)
                                            .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                                    })
                                    .map(canonical_action_name),
                                Some("form_mutual_aid_group" | "share_meal" | "mediate_dispute")
                            )
                    })
                    .count() as i64;
                if supportive_pairs == 0 && group_interest == 0 {
                    continue;
                }
                let sustained_interest = self
                    .event_log
                    .iter()
                    .rev()
                    .take(192)
                    .filter(|event| {
                        event.location_id == settlement_id
                            && event.event_type == EventType::NpcActionCommitted
                            && matches!(
                                event
                                    .details
                                    .as_ref()
                                    .and_then(|details| {
                                        details
                                            .get("canonical_action")
                                            .and_then(Value::as_str)
                                            .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                                    })
                                    .map(canonical_action_name),
                                Some(
                                    "form_mutual_aid_group"
                                        | "share_meal"
                                        | "converse_neighbor"
                                        | "lend_coin"
                                        | "mediate_dispute"
                                        | "court_romance"
                                )
                            )
                    })
                    .count() as i64;
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
                    + sustained_interest * 2
                    + (self.social_cohesion.max(0) / 18)
                    + shortage * 5
                    - theft_pressure
                    - recent_group_churn * 6;
                let roll = (self.deterministic_stream(
                    tick,
                    Phase::Commit,
                    "group_formation",
                    &settlement_id,
                ) % 100) as i64;
                if formation_score + roll < 62 {
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
                        inertia_score: (26 + group_interest + sustained_interest / 2).clamp(24, 72),
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
                    event.location_id == settlement_id
                        && event.event_type == EventType::TheftCommitted
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
                            event
                                .details
                                .as_ref()
                                .and_then(|details| {
                                    details
                                        .get("canonical_action")
                                        .and_then(Value::as_str)
                                        .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                                })
                                .map(canonical_action_name),
                            Some(
                                "share_meal"
                                    | "converse_neighbor"
                                    | "lend_coin"
                                    | "mediate_dispute"
                                    | "form_mutual_aid_group"
                                    | "defend_patron"
                            )
                        )
                })
                .count() as i64;
            let stagnation_penalty = if support_signal == 0 { 2 } else { 0 };
            group.infiltration_pressure =
                (group.infiltration_pressure + hostile_to_leader + theft_pressure / 4
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
            group.inertia_score = (group.inertia_score + cause_alignment + (support_signal / 6)
                - (group.infiltration_pressure / 26)
                - stagnation_penalty)
                .clamp(-60, 80);
            let cohesion = group.cohesion_score;
            let member_count = group.member_npc_ids.len();
            let split = group_age >= 288
                && member_count >= 3
                && cohesion <= -40
                && group.infiltration_pressure >= 62
                && support_signal <= 1
                && tick % 48 == 0;
            let dissolve = group_age >= 432
                && cohesion <= -52
                && group.inertia_score <= -24
                && (member_count <= 1 || cohesion <= -50)
                && (group.infiltration_pressure >= 70 || cause_alignment < 0)
                && support_signal == 0
                && tick % 48 == 0;
            if !dissolve {
                self.groups_by_id.insert(group_id.clone(), group.clone());
            }
            if tick % 24 == 0 || split || dissolve {
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
            let previous_hazard = route.hazard_score;
            let previous_travel_time = route.travel_time_ticks;
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

            let risk_changed =
                (hazard - previous_hazard).abs() >= 4 || travel.abs_diff(previous_travel_time) >= 4;
            if tick % 12 == 0 || risk_changed {
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
            }
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
            .and_then(|details| {
                details
                    .get("canonical_action")
                    .and_then(Value::as_str)
                    .or_else(|| details.get("chosen_action").and_then(Value::as_str))
            })
            .map(canonical_action_name)
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
                    if let Some(intent) = packet
                        .top_intents
                        .iter()
                        .find(|intent| intent.as_str() != "maintain_routine")
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
        } else if matches!(chosen_action, "organize_watch" | "patrol_road") && local_case_load == 0
        {
            "high vigilance consumed social bandwidth without immediate threat signals".to_string()
        } else if matches!(chosen_action, "seek_shelter") && self.pressure_index < 140 {
            "precautionary shelter-seeking postponed livelihood recovery for another cycle"
                .to_string()
        } else if matches!(
            chosen_action,
            "work_for_coin" | "work_for_food" | "tend_fields"
        ) {
            "livelihood maintenance reduced immediate household stress while keeping long-term obligations active"
                .to_string()
        } else if matches!(
            chosen_action,
            "form_mutual_aid_group" | "share_meal" | "mediate_dispute"
        ) {
            "cooperative behavior improved local support ties and reduced short-term volatility"
                .to_string()
        } else if matches!(chosen_action, "spread_accusation" | "share_rumor") {
            "narrative competition intensified, shifting trust and credibility in uneven ways"
                .to_string()
        } else if local_stock <= 4 {
            "resource scarcity amplified defensive behavior and narrowed acceptable choices"
                .to_string()
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
            AffordanceVerb::ConverseNeighbor | AffordanceVerb::ObserveNotableEvent => vec![
                "social_information_diffusion".to_string(),
                "relationship_reweighting".to_string(),
            ],
            AffordanceVerb::LendCoin => vec![
                "short_term_household_relief".to_string(),
                "future_obligation_created".to_string(),
            ],
            AffordanceVerb::CourtRomance => vec![
                "bond_strength_shift".to_string(),
                "reputation_effects".to_string(),
            ],
            AffordanceVerb::SeekTreatment => vec![
                "health_stabilization_attempt".to_string(),
                "time_budget_tradeoff".to_string(),
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

    fn compose_plan_for_choice(
        &self,
        tick: u64,
        decision: &NpcDecision,
        chosen: &ActionCandidate,
        alternatives_considered: &[String],
    ) -> NpcComposablePlanState {
        let templates = self.action_templates_for_choice(chosen.action.as_str());
        let template_idx = if templates.is_empty() {
            0
        } else {
            (self.deterministic_stream(
                tick,
                Phase::ActionResolution,
                &decision.npc_id,
                "compose_template",
            ) % templates.len() as u64) as usize
        };
        let template = templates
            .get(template_idx)
            .cloned()
            .unwrap_or(ActionTemplateDefinition {
                template_id: format!("fallback/{}", chosen.action),
                base_action: chosen.action.clone(),
                goal_family: decision
                    .top_intents
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "maintain_routine".to_string()),
                op_sequence: vec![AtomicOpKind::Perceive, AtomicOpKind::WorkShift],
                required_capability: None,
                optional_tags: vec!["fallback".to_string()],
            });

        let tone = self.compose_tone_for_npc(&decision.npc_id);
        let target_id = self.social_target_for_tick(
            &decision.npc_id,
            &decision.location_id,
            -15,
            tick,
            "compose_target",
        );
        let ops = template
            .op_sequence
            .iter()
            .enumerate()
            .map(|(idx, kind)| {
                self.bind_atomic_op(
                    tick,
                    &decision.npc_id,
                    &decision.location_id,
                    *kind,
                    target_id.clone(),
                    &tone,
                    idx as u64,
                )
            })
            .collect::<Vec<_>>();

        let target_fragment = target_id
            .as_ref()
            .map(|value| value.replace(':', "_"))
            .unwrap_or_else(|| "self".to_string());
        let day_phase = match tick % 24 {
            0..=5 => "night_watch",
            6..=11 => "morning",
            12..=17 => "afternoon",
            _ => "evening",
        };
        let topic_fragment = self
            .local_topic_for_npc(
                &decision.npc_id,
                &decision.location_id,
                tick,
                "compose_topic_fragment",
            )
            .replace(':', "_")
            .replace(' ', "_");
        let pressure_fragment = if decision.top_pressures.is_empty() {
            "steady".to_string()
        } else {
            decision
                .top_pressures
                .iter()
                .take(2)
                .map(|value| value.replace(':', "_"))
                .collect::<Vec<_>>()
                .join("+")
        };
        let composed_action = format!(
            "{}::{}::{}::{}::{}::{}::{}",
            chosen.action,
            template.template_id,
            tone,
            target_fragment,
            day_phase,
            topic_fragment,
            pressure_fragment
        );

        NpcComposablePlanState {
            npc_id: decision.npc_id.clone(),
            tick,
            goal_family: template.goal_family,
            template_id: template.template_id,
            base_action: chosen.action.clone(),
            composed_action,
            ops,
            rejected_alternatives: alternatives_considered.iter().take(6).cloned().collect(),
        }
    }

    fn compose_tone_for_npc(&self, npc_id: &str) -> String {
        let temperament = self
            .npcs
            .iter()
            .find(|npc| npc.npc_id == npc_id)
            .map(|npc| npc.temperament.as_str())
            .unwrap_or("steady");
        let traits = self.npc_traits_by_id.get(npc_id);
        let aggression = traits.map(|profile| profile.aggression).unwrap_or(40);
        let patience = traits.map(|profile| profile.patience).unwrap_or(40);
        let empathy = traits.map(|profile| profile.empathy).unwrap_or(40);

        if aggression >= 70 && patience <= 35 {
            return "heated".to_string();
        }
        if empathy >= 62 && patience >= 55 {
            return "gentle".to_string();
        }
        match temperament {
            "guarded" | "suspicious" => "guarded".to_string(),
            "bold" | "restless" => "assertive".to_string(),
            "warm" | "sociable" => "warm".to_string(),
            _ => "neutral".to_string(),
        }
    }

    fn bind_atomic_op(
        &self,
        tick: u64,
        npc_id: &str,
        location_id: &str,
        kind: AtomicOpKind,
        target_id: Option<String>,
        tone: &str,
        op_index: u64,
    ) -> AtomicOp {
        let roll = self.deterministic_stream(
            tick,
            Phase::ActionResolution,
            npc_id,
            &format!("op_intensity_{op_index}"),
        );
        let intensity = (25 + (roll % 71)) as u8;
        let item_kind = match kind {
            AtomicOpKind::GiftOffer => Some("gift_small".to_string()),
            AtomicOpKind::Buy | AtomicOpKind::Sell | AtomicOpKind::Barter => {
                Some("trade_goods".to_string())
            }
            AtomicOpKind::LendCoin | AtomicOpKind::BorrowCoin | AtomicOpKind::PayRent => {
                Some("coin".to_string())
            }
            AtomicOpKind::SeekTreatment => Some("medicine".to_string()),
            _ => None,
        };

        AtomicOp {
            kind,
            target_id,
            item_kind,
            location_id: Some(location_id.to_string()),
            tone: Some(tone.to_string()),
            intensity,
        }
    }

    fn action_templates_for_choice(&self, action: &str) -> Vec<ActionTemplateDefinition> {
        let action = canonical_action_name(action);
        match action {
            "court_romance" => vec![
                ActionTemplateDefinition {
                    template_id: "romance/observe_approach_flirt".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_companionship".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Perceive,
                        AtomicOpKind::Approach,
                        AtomicOpKind::Flirt,
                        AtomicOpKind::Converse,
                    ],
                    required_capability: Some("social".to_string()),
                    optional_tags: vec!["romance".to_string(), "approach".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "romance/gift_and_promise".to_string(),
                    base_action: action.to_string(),
                    goal_family: "build_trust".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Approach,
                        AtomicOpKind::GiftOffer,
                        AtomicOpKind::Promise,
                        AtomicOpKind::Converse,
                    ],
                    required_capability: Some("social".to_string()),
                    optional_tags: vec!["romance".to_string(), "trust".to_string()],
                },
            ],
            "trade_visit" => vec![
                ActionTemplateDefinition {
                    template_id: "trade/market_scan_and_barter".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_income".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Observe,
                        AtomicOpKind::TradeVisit,
                        AtomicOpKind::Barter,
                        AtomicOpKind::Sell,
                    ],
                    required_capability: Some("trade".to_string()),
                    optional_tags: vec!["market".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "trade/negotiate_and_buy".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_opportunity".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::TradeVisit,
                        AtomicOpKind::Negotiate,
                        AtomicOpKind::AgreeTerms,
                        AtomicOpKind::Buy,
                    ],
                    required_capability: Some("trade".to_string()),
                    optional_tags: vec!["market".to_string(), "deal".to_string()],
                },
            ],
            "converse_neighbor" => vec![
                ActionTemplateDefinition {
                    template_id: "social/greet_and_exchange".to_string(),
                    base_action: action.to_string(),
                    goal_family: "exchange_stories".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Approach,
                        AtomicOpKind::Greet,
                        AtomicOpKind::Converse,
                        AtomicOpKind::ShareInfo,
                    ],
                    required_capability: Some("social".to_string()),
                    optional_tags: vec!["social".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "social/ask_and_share_rumor".to_string(),
                    base_action: action.to_string(),
                    goal_family: "investigate_rumor".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Approach,
                        AtomicOpKind::AskQuestion,
                        AtomicOpKind::ShareRumor,
                        AtomicOpKind::Converse,
                    ],
                    required_capability: Some("social".to_string()),
                    optional_tags: vec!["social".to_string(), "rumor".to_string()],
                },
            ],
            "work_for_coin" => vec![
                ActionTemplateDefinition {
                    template_id: "labor/contract_shift".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_income".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Travel,
                        AtomicOpKind::WorkShift,
                        AtomicOpKind::ReceiveWage,
                    ],
                    required_capability: Some("physical".to_string()),
                    optional_tags: vec!["labor".to_string(), "income".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "labor/day_hire_piecework".to_string(),
                    base_action: action.to_string(),
                    goal_family: "clear_debt".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Negotiate,
                        AtomicOpKind::WorkShift,
                        AtomicOpKind::ReceiveWage,
                    ],
                    required_capability: Some("trade".to_string()),
                    optional_tags: vec!["labor".to_string(), "debt".to_string()],
                },
            ],
            "craft_goods" => vec![
                ActionTemplateDefinition {
                    template_id: "craft/produce_then_sell".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_income".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Craft,
                        AtomicOpKind::TradeVisit,
                        AtomicOpKind::Sell,
                    ],
                    required_capability: Some("trade".to_string()),
                    optional_tags: vec!["craft".to_string(), "market".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "craft/commission_cycle".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_opportunity".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Negotiate,
                        AtomicOpKind::Craft,
                        AtomicOpKind::AgreeTerms,
                    ],
                    required_capability: Some("trade".to_string()),
                    optional_tags: vec!["craft".to_string(), "commission".to_string()],
                },
            ],
            "steal_supplies" => vec![
                ActionTemplateDefinition {
                    template_id: "crime/scout_steal_flee".to_string(),
                    base_action: action.to_string(),
                    goal_family: "secure_food".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Observe,
                        AtomicOpKind::Approach,
                        AtomicOpKind::Stealth,
                        AtomicOpKind::Flee,
                    ],
                    required_capability: Some("stealth".to_string()),
                    optional_tags: vec!["crime".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "crime/threaten_and_take".to_string(),
                    base_action: action.to_string(),
                    goal_family: "evade_patrols".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Threaten,
                        AtomicOpKind::Strike,
                        AtomicOpKind::Flee,
                    ],
                    required_capability: Some("combat".to_string()),
                    optional_tags: vec!["crime".to_string(), "violent".to_string()],
                },
            ],
            "tend_fields" => vec![
                ActionTemplateDefinition {
                    template_id: "farm/survey_and_sow".to_string(),
                    base_action: action.to_string(),
                    goal_family: "secure_food".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Observe,
                        AtomicOpKind::Farm,
                        AtomicOpKind::WorkShift,
                    ],
                    required_capability: Some("physical".to_string()),
                    optional_tags: vec!["farm".to_string(), "staples".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "farm/harvest_and_store".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_income".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Farm,
                        AtomicOpKind::TradeVisit,
                        AtomicOpKind::Sell,
                    ],
                    required_capability: Some("physical".to_string()),
                    optional_tags: vec!["farm".to_string(), "harvest".to_string()],
                },
            ],
            "work_for_food" => vec![
                ActionTemplateDefinition {
                    template_id: "labor/board_shift".to_string(),
                    base_action: action.to_string(),
                    goal_family: "secure_food".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Travel,
                        AtomicOpKind::WorkShift,
                        AtomicOpKind::Eat,
                    ],
                    required_capability: Some("physical".to_string()),
                    optional_tags: vec!["labor".to_string(), "board".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "labor/farm_for_board".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_shelter".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Farm,
                        AtomicOpKind::WorkShift,
                        AtomicOpKind::SeekShelter,
                    ],
                    required_capability: Some("physical".to_string()),
                    optional_tags: vec!["labor".to_string(), "board".to_string()],
                },
            ],
            "forage" => vec![
                ActionTemplateDefinition {
                    template_id: "forage/woodland_route".to_string(),
                    base_action: action.to_string(),
                    goal_family: "secure_food".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Travel,
                        AtomicOpKind::Forage,
                        AtomicOpKind::TradeVisit,
                    ],
                    required_capability: Some("physical".to_string()),
                    optional_tags: vec!["forage".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "forage/night_edge".to_string(),
                    base_action: action.to_string(),
                    goal_family: "seek_opportunity".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Observe,
                        AtomicOpKind::Forage,
                        AtomicOpKind::Flee,
                    ],
                    required_capability: Some("stealth".to_string()),
                    optional_tags: vec!["forage".to_string(), "risk".to_string()],
                },
            ],
            "investigate_rumor" => vec![
                ActionTemplateDefinition {
                    template_id: "info/interview_and_crosscheck".to_string(),
                    base_action: action.to_string(),
                    goal_family: "investigate_rumor".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::AskQuestion,
                        AtomicOpKind::Converse,
                        AtomicOpKind::ShareInfo,
                    ],
                    required_capability: Some("social".to_string()),
                    optional_tags: vec!["information".to_string()],
                },
                ActionTemplateDefinition {
                    template_id: "info/street_observe_verify".to_string(),
                    base_action: action.to_string(),
                    goal_family: "exchange_stories".to_string(),
                    op_sequence: vec![
                        AtomicOpKind::Observe,
                        AtomicOpKind::Approach,
                        AtomicOpKind::AskQuestion,
                    ],
                    required_capability: Some("social".to_string()),
                    optional_tags: vec!["information".to_string(), "street".to_string()],
                },
            ],
            _ => vec![ActionTemplateDefinition {
                template_id: format!("routine/{action}"),
                base_action: action.to_string(),
                goal_family: "maintain_routine".to_string(),
                op_sequence: vec![AtomicOpKind::Perceive, AtomicOpKind::WorkShift],
                required_capability: None,
                optional_tags: vec!["routine".to_string()],
            }],
        }
    }

    fn recent_action_stats(
        &self,
        tick: u64,
        location_id: &str,
        action: &str,
        window_ticks: u64,
    ) -> (i64, i64) {
        let canonical = canonical_action_name(action);
        let mut total = 0_i64;
        let mut same_action = 0_i64;
        for event in self.event_log.iter().rev() {
            if event.tick + window_ticks < tick {
                break;
            }
            if event.location_id != location_id || event.event_type != EventType::NpcActionCommitted
            {
                continue;
            }
            total += 1;
            if event
                .details
                .as_ref()
                .and_then(|details| {
                    details
                        .get("canonical_action")
                        .and_then(Value::as_str)
                        .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                })
                .map(canonical_action_name)
                == Some(canonical)
            {
                same_action += 1;
            }
        }
        (same_action, total)
    }

    fn local_topic_for_npc(
        &self,
        npc_id: &str,
        location_id: &str,
        tick: u64,
        channel: &str,
    ) -> String {
        if let Some(last_observation) = self.observations_by_npc.get(npc_id).and_then(|entries| {
            entries
                .iter()
                .rev()
                .find(|entry| tick.saturating_sub(entry.tick) <= 48)
        }) {
            return last_observation.topic.clone();
        }

        let nearby_locations = self
            .routes_by_id
            .values()
            .filter(|route| route.weather_window_open && route.hazard_score <= 60)
            .filter_map(|route| {
                if route.origin_settlement_id == location_id {
                    Some(route.destination_settlement_id.clone())
                } else if route.destination_settlement_id == location_id {
                    Some(route.origin_settlement_id.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        let recent = self
            .event_log
            .iter()
            .rev()
            .take(192)
            .filter(|event| {
                event.tick <= tick
                    && (event.location_id == location_id
                        || nearby_locations.contains(&event.location_id))
                    && matches!(
                        event.event_type,
                        EventType::TheftCommitted
                            | EventType::ArrestMade
                            | EventType::InsultExchanged
                            | EventType::PunchThrown
                            | EventType::BrawlStarted
                            | EventType::RomanceAdvanced
                            | EventType::ConversationHeld
                            | EventType::NpcActionCommitted
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        if let Some(source) = recent.first() {
            let chosen_action = source
                .details
                .as_ref()
                .and_then(|details| {
                    details
                        .get("canonical_action")
                        .and_then(Value::as_str)
                        .or_else(|| details.get("chosen_action").and_then(Value::as_str))
                })
                .map(canonical_action_name);
            return observation_topic_for_event(
                source.event_type,
                &source.tags,
                source.details.as_ref(),
                chosen_action,
            );
        }

        let fallback_roll = self.deterministic_stream(tick, Phase::Commit, npc_id, channel) % 5;
        match fallback_roll {
            0 => "weather_and_roads".to_string(),
            1 => "market_prices".to_string(),
            2 => "watch_patrols".to_string(),
            3 => "neighbor_work".to_string(),
            _ => "daily_activity".to_string(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_dynamic_reaction_from_observation(
        &mut self,
        tick: u64,
        location_id: &str,
        observer_npc_id: &str,
        source_event_id: &str,
        source_event_type: EventType,
        source_tags: &[String],
        source_details: Option<&Value>,
        source_actor_id: Option<&str>,
        source_target_id: Option<&str>,
        source_action: Option<&str>,
        salience: i64,
        sequence_in_tick: &mut u64,
        causal_chain: &mut Vec<String>,
        system_event_id: &str,
    ) {
        let Some(profile) = self.npc_traits_by_id.get(observer_npc_id).cloned() else {
            return;
        };
        let source_topic = observation_topic_for_event(
            source_event_type,
            source_tags,
            source_details,
            source_action,
        );
        let source_actor = source_actor_id
            .filter(|actor_id| *actor_id != observer_npc_id)
            .filter(|actor_id| self.npc_economy_by_id.contains_key(*actor_id))
            .map(ToString::to_string);
        let source_target = source_target_id
            .filter(|target_id| *target_id != observer_npc_id)
            .filter(|target_id| self.npc_economy_by_id.contains_key(*target_id))
            .map(ToString::to_string);
        let institution_corruption = self
            .institutions_by_settlement
            .get(location_id)
            .map(|institution| institution.corruption_level)
            .unwrap_or(25);
        let observer_wallet = self
            .npc_economy_by_id
            .get(observer_npc_id)
            .map(|entry| entry.wallet)
            .unwrap_or(0);
        let is_conflict_signal = matches!(
            source_event_type,
            EventType::TheftCommitted
                | EventType::InsultExchanged
                | EventType::PunchThrown
                | EventType::BrawlStarted
                | EventType::ArrestMade
                | EventType::GuardsDispatched
        );
        let is_social_signal = matches!(
            source_event_type,
            EventType::ConversationHeld
                | EventType::RomanceAdvanced
                | EventType::ObservationLogged
                | EventType::NpcActionCommitted
        );
        let activation_roll = (self.deterministic_stream(
            tick,
            Phase::Commit,
            observer_npc_id,
            &format!("dynamic_reaction_activation:{source_event_id}"),
        ) % 100) as i64;
        let activation_threshold = (4
            + salience * 2
            + profile.sociability / 11
            + profile.empathy / 12
            + profile.gossip_drive / 13)
            .clamp(6, 42);
        if activation_roll >= activation_threshold {
            return;
        }

        let relation_snapshot = |target_id: &str| -> (i64, i64, i64, i64, i64) {
            self.relationship_edges
                .get(&(observer_npc_id.to_string(), target_id.to_string()))
                .map(|edge| {
                    (
                        edge.trust,
                        edge.attachment,
                        edge.respect,
                        edge.grievance,
                        edge.fear,
                    )
                })
                .unwrap_or((0, 0, 0, 0, 0))
        };

        let mut candidates = Vec::<SocialReactionCandidate>::new();

        if let Some(target_id) = self.social_target_for_tick(
            observer_npc_id,
            location_id,
            if salience >= 8 { 10 } else { 2 },
            tick,
            "dynamic_reaction_discuss",
        ) {
            let (trust, attachment, respect, grievance, _) = relation_snapshot(&target_id);
            let weight = (8
                + profile.sociability / 4
                + profile.gossip_drive / 5
                + trust.max(0) / 6
                + attachment / 8
                + respect / 10
                + salience * 2
                - grievance / 8)
                .clamp(1, 96);
            candidates.push(SocialReactionCandidate {
                reaction_key: "discuss_notable",
                event_type: EventType::ConversationHeld,
                target_id: Some(target_id),
                target_kind: Some("npc"),
                tags: vec![
                    "social".to_string(),
                    "reaction".to_string(),
                    "discussion".to_string(),
                ],
                topic: Some(source_topic.clone()),
                weight,
            });
        }

        if let Some(actor_id) = source_actor.clone() {
            let (trust, attachment, respect, grievance, fear) = relation_snapshot(&actor_id);
            let praise_weight = (profile.empathy / 3
                + profile.sociability / 4
                + respect.max(0) / 2
                + attachment.max(0) / 3
                + trust.max(0) / 3
                + salience
                - grievance.max(0) / 4)
                .clamp(0, 96);
            if praise_weight >= 14 {
                candidates.push(SocialReactionCandidate {
                    reaction_key: "praise_observed",
                    event_type: EventType::ConversationHeld,
                    target_id: Some(actor_id.clone()),
                    target_kind: Some("npc"),
                    tags: vec![
                        "social".to_string(),
                        "reaction".to_string(),
                        "praise".to_string(),
                    ],
                    topic: Some("offer_praise".to_string()),
                    weight: praise_weight,
                });
            }

            if is_social_signal {
                let collaboration_weight = (profile.dutifulness / 3
                    + profile.ambition / 4
                    + trust.max(0) / 2
                    + attachment.max(0) / 4
                    + salience
                    - fear.max(0) / 3)
                    .clamp(0, 96);
                if collaboration_weight >= 16 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "propose_collaboration",
                        event_type: EventType::ObligationCreated,
                        target_id: Some(actor_id.clone()),
                        target_kind: Some("npc"),
                        tags: vec![
                            "social".to_string(),
                            "reaction".to_string(),
                            "cooperation".to_string(),
                        ],
                        topic: Some("propose_joint_work".to_string()),
                        weight: collaboration_weight,
                    });
                }

                let challenge_weight = (profile.aggression / 2
                    + profile.ambition / 3
                    + grievance.max(0)
                    + salience
                    - profile.patience / 3
                    - fear.max(0) / 2
                    - trust.max(0) / 3)
                    .clamp(0, 96);
                if challenge_weight >= 22 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "status_challenge",
                        event_type: EventType::ConversationHeld,
                        target_id: Some(actor_id.clone()),
                        target_kind: Some("npc"),
                        tags: vec![
                            "social".to_string(),
                            "reaction".to_string(),
                            "challenge".to_string(),
                        ],
                        topic: Some("status_challenge".to_string()),
                        weight: challenge_weight,
                    });
                }
            }

            if !is_conflict_signal {
                let disparage_weight = (profile.gossip_drive / 3
                    + profile.aggression / 4
                    + grievance.max(0)
                    + salience
                    - profile.honor / 4
                    - trust.max(0) / 3)
                    .clamp(0, 96);
                if disparage_weight >= 18 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "cast_doubt",
                        event_type: EventType::BeliefDisputed,
                        target_id: Some(actor_id),
                        target_kind: Some("npc"),
                        tags: vec![
                            "belief".to_string(),
                            "reaction".to_string(),
                            "doubt".to_string(),
                        ],
                        topic: Some("cast_public_doubt".to_string()),
                        weight: disparage_weight,
                    });
                }
            }
        }

        if is_conflict_signal {
            if let Some(target_id) = source_target.clone() {
                let (trust, attachment, _, grievance, _) = relation_snapshot(&target_id);
                let support_weight = (profile.empathy / 3
                    + profile.generosity / 5
                    + trust.max(0) / 3
                    + attachment / 3
                    + salience * 2
                    - profile.aggression / 7
                    - grievance / 8)
                    .clamp(0, 96);
                if support_weight >= 10 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "support_victim",
                        event_type: EventType::ConversationHeld,
                        target_id: Some(target_id.clone()),
                        target_kind: Some("npc"),
                        tags: vec![
                            "social".to_string(),
                            "reaction".to_string(),
                            "support".to_string(),
                        ],
                        topic: Some("offer_support".to_string()),
                        weight: support_weight,
                    });
                }

                if observer_wallet >= 3 {
                    let hardship = self
                        .npc_economy_by_id
                        .get(&target_id)
                        .map(|economy| {
                            economy.debt_balance
                                + (2 - economy.food_reserve_days).max(0) * 2
                                + match economy.shelter_status {
                                    ShelterStatus::Unsheltered => 3,
                                    ShelterStatus::Precarious => 2,
                                    ShelterStatus::Stable => 0,
                                }
                        })
                        .unwrap_or(0);
                    let loan_weight = (profile.generosity / 3
                        + profile.empathy / 4
                        + hardship * 4
                        + trust.max(0) / 5)
                        .clamp(0, 96);
                    if loan_weight >= 16 {
                        candidates.push(SocialReactionCandidate {
                            reaction_key: "offer_loan",
                            event_type: EventType::LoanExtended,
                            target_id: Some(target_id),
                            target_kind: Some("npc"),
                            tags: vec![
                                "social".to_string(),
                                "reaction".to_string(),
                                "aid".to_string(),
                            ],
                            topic: Some("emergency_loan".to_string()),
                            weight: loan_weight,
                        });
                    }
                }
            }
        }

        if let Some(actor_id) = source_actor.clone() {
            let (trust, _, _, grievance, fear) = relation_snapshot(&actor_id);
            if is_conflict_signal {
                let report_weight = (profile.dutifulness / 2 + profile.honor / 3 + salience * 2
                    - institution_corruption / 5)
                    .clamp(0, 96);
                if report_weight >= 14 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "report_authority",
                        event_type: EventType::GuardsDispatched,
                        target_id: Some(actor_id.clone()),
                        target_kind: Some("npc"),
                        tags: vec![
                            "institution".to_string(),
                            "reaction".to_string(),
                            "report".to_string(),
                        ],
                        topic: Some("report_incident".to_string()),
                        weight: report_weight,
                    });
                }

                let insult_weight = (profile.aggression / 2 + grievance * 2 + salience
                    - profile.patience / 2
                    - trust.max(0)
                    - fear / 2)
                    .clamp(0, 96);
                if insult_weight >= 16 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "escalate_verbal",
                        event_type: EventType::InsultExchanged,
                        target_id: Some(actor_id.clone()),
                        target_kind: Some("npc"),
                        tags: vec![
                            "conflict".to_string(),
                            "reaction".to_string(),
                            "verbal".to_string(),
                        ],
                        topic: Some("reactive_insult".to_string()),
                        weight: insult_weight,
                    });
                }

                let strike_weight = (profile.aggression + grievance * 2 + salience * 3
                    - profile.patience
                    - trust.max(0)
                    - fear)
                    .clamp(0, 120);
                if strike_weight >= 52 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "retaliate_strike",
                        event_type: EventType::PunchThrown,
                        target_id: Some(actor_id.clone()),
                        target_kind: Some("npc"),
                        tags: vec![
                            "conflict".to_string(),
                            "reaction".to_string(),
                            "physical".to_string(),
                        ],
                        topic: Some("reactive_strike".to_string()),
                        weight: strike_weight,
                    });
                }
            }

            if is_social_signal {
                let romance_weight = (profile.romance_drive / 3
                    + profile.sociability / 4
                    + trust.max(0) / 2
                    + salience
                    - grievance)
                    .clamp(0, 96);
                if romance_weight >= 20 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "romance_interest",
                        event_type: EventType::RomanceAdvanced,
                        target_id: Some(actor_id),
                        target_kind: Some("npc"),
                        tags: vec![
                            "social".to_string(),
                            "reaction".to_string(),
                            "romance".to_string(),
                        ],
                        topic: Some("reaction_interest".to_string()),
                        weight: romance_weight,
                    });
                }
            }
        }

        if is_conflict_signal {
            if let (Some(actor_id), Some(target_id)) = (source_actor.clone(), source_target.clone())
            {
                let (actor_trust, _, _, actor_grievance, _) = relation_snapshot(&actor_id);
                let (target_trust, _, _, target_grievance, _) = relation_snapshot(&target_id);
                let mediation_target =
                    if actor_trust + actor_grievance <= target_trust + target_grievance {
                        actor_id
                    } else {
                        target_id
                    };
                let mediate_weight =
                    ((profile.empathy + profile.dutifulness + profile.patience) / 3 + salience * 2
                        - profile.aggression / 4)
                        .clamp(0, 96);
                if mediate_weight >= 14 {
                    candidates.push(SocialReactionCandidate {
                        reaction_key: "mediate_conflict",
                        event_type: EventType::ConversationHeld,
                        target_id: Some(mediation_target),
                        target_kind: Some("npc"),
                        tags: vec![
                            "conflict".to_string(),
                            "reaction".to_string(),
                            "mediation".to_string(),
                        ],
                        topic: Some("mediate_tension".to_string()),
                        weight: mediate_weight,
                    });
                }
            }
        }

        if candidates.is_empty() {
            return;
        }
        let weights = candidates
            .iter()
            .map(|candidate| candidate.weight)
            .collect::<Vec<_>>();
        let pick_roll = self.deterministic_stream(
            tick,
            Phase::Commit,
            observer_npc_id,
            &format!("dynamic_reaction_pick:{source_event_id}"),
        );
        let Some(idx) = pick_weighted_index(pick_roll, &weights) else {
            return;
        };
        let candidate = candidates[idx].clone();

        let mut loan_amount = 0_i64;
        if candidate.reaction_key == "offer_loan" {
            if let Some(target_id) = candidate.target_id.as_ref() {
                if self.npc_economy_by_id.contains_key(target_id) {
                    if let Some(lender) = self.npc_economy_by_id.get_mut(observer_npc_id) {
                        loan_amount = lender.wallet.max(0).min(2);
                        lender.wallet -= loan_amount;
                    }
                    if loan_amount > 0 {
                        if let Some(borrower) = self.npc_economy_by_id.get_mut(target_id) {
                            borrower.wallet += loan_amount;
                            borrower.debt_balance =
                                (borrower.debt_balance + loan_amount).clamp(0, 250);
                        } else if let Some(lender) = self.npc_economy_by_id.get_mut(observer_npc_id)
                        {
                            lender.wallet += loan_amount;
                            loan_amount = 0;
                        }
                    }
                }
            }
            if loan_amount <= 0 {
                return;
            }
        }

        let mut details = json!({
            "topic": candidate.topic.clone().unwrap_or_else(|| source_topic.clone()),
            "reaction_key": candidate.reaction_key,
            "source_event_id": source_event_id,
            "source_event_type": format!("{:?}", source_event_type),
            "salience": salience,
        });
        if let Some(action) = source_action {
            details["source_action"] = json!(action);
        }
        if loan_amount > 0 {
            details["amount"] = json!(loan_amount);
            details["currency"] = json!("coin");
        }
        if candidate.event_type == EventType::PunchThrown {
            details["escalation_score"] = json!(candidate.weight);
        }
        if candidate.event_type == EventType::RomanceAdvanced {
            details["stage"] = json!("reaction_interest");
        }

        let mut actors = vec![ActorRef {
            actor_id: observer_npc_id.to_string(),
            actor_kind: "npc".to_string(),
        }];
        if candidate.reaction_key == "report_authority" {
            actors.push(ActorRef {
                actor_id: format!("guard_unit:{location_id}"),
                actor_kind: "institution".to_string(),
            });
        }
        let targets = candidate
            .target_id
            .as_ref()
            .map(|target_id| {
                vec![ActorRef {
                    actor_id: target_id.clone(),
                    actor_kind: candidate.target_kind.unwrap_or("npc").to_string(),
                }]
            })
            .unwrap_or_default();

        self.push_runtime_event(
            tick,
            sequence_in_tick,
            causal_chain,
            system_event_id,
            candidate.event_type,
            location_id.to_string(),
            actors,
            targets,
            candidate.tags.clone(),
            Some(details),
        );

        if loan_amount > 0 {
            if let Some(target_id) = candidate.target_id.as_ref() {
                self.record_accounting_transfer(
                    tick,
                    location_id,
                    format!("npc:{observer_npc_id}"),
                    format!("npc:{target_id}"),
                    "coin",
                    loan_amount,
                    Some(source_event_id.to_string()),
                );
            }
        }

        if let Some(target_id) = candidate.target_id.as_ref() {
            match candidate.event_type {
                EventType::ConversationHeld => {
                    if let Some(edge) = self
                        .relationship_edges
                        .get_mut(&(observer_npc_id.to_string(), target_id.clone()))
                    {
                        edge.trust = (edge.trust + 1).clamp(-100, 100);
                        edge.respect = (edge.respect + 1).clamp(0, 100);
                        edge.recent_interaction_tick = tick;
                    }
                }
                EventType::LoanExtended => {
                    if let Some(edge) = self
                        .relationship_edges
                        .get_mut(&(observer_npc_id.to_string(), target_id.clone()))
                    {
                        edge.trust = (edge.trust + 2).clamp(-100, 100);
                        edge.attachment = (edge.attachment + 2).clamp(0, 100);
                        edge.obligation = (edge.obligation + 2).clamp(0, 100);
                        edge.recent_interaction_tick = tick;
                    }
                    if let Some(reverse) = self
                        .relationship_edges
                        .get_mut(&(target_id.clone(), observer_npc_id.to_string()))
                    {
                        reverse.trust = (reverse.trust + 1).clamp(-100, 100);
                        reverse.obligation = (reverse.obligation + 1).clamp(0, 100);
                        reverse.recent_interaction_tick = tick;
                    }
                }
                EventType::RomanceAdvanced => {
                    if let Some(edge) = self
                        .relationship_edges
                        .get_mut(&(observer_npc_id.to_string(), target_id.clone()))
                    {
                        edge.trust = (edge.trust + 1).clamp(-100, 100);
                        edge.attachment = (edge.attachment + 2).clamp(0, 100);
                        edge.respect = (edge.respect + 1).clamp(0, 100);
                        edge.recent_interaction_tick = tick;
                    }
                }
                EventType::InsultExchanged => {
                    if let Some(edge) = self
                        .relationship_edges
                        .get_mut(&(observer_npc_id.to_string(), target_id.clone()))
                    {
                        edge.trust = (edge.trust - 3).clamp(-100, 100);
                        edge.grievance = (edge.grievance + 4).clamp(0, 100);
                        edge.recent_interaction_tick = tick;
                    }
                    if let Some(reverse) = self
                        .relationship_edges
                        .get_mut(&(target_id.clone(), observer_npc_id.to_string()))
                    {
                        reverse.fear = (reverse.fear + 2).clamp(0, 100);
                        reverse.grievance = (reverse.grievance + 1).clamp(0, 100);
                        reverse.recent_interaction_tick = tick;
                    }
                }
                EventType::PunchThrown => {
                    if let Some(edge) = self
                        .relationship_edges
                        .get_mut(&(observer_npc_id.to_string(), target_id.clone()))
                    {
                        edge.trust = (edge.trust - 6).clamp(-100, 100);
                        edge.grievance = (edge.grievance + 8).clamp(0, 100);
                        edge.respect = (edge.respect - 2).clamp(0, 100);
                        edge.recent_interaction_tick = tick;
                    }
                    if let Some(reverse) = self
                        .relationship_edges
                        .get_mut(&(target_id.clone(), observer_npc_id.to_string()))
                    {
                        reverse.fear = (reverse.fear + 6).clamp(0, 100);
                        reverse.grievance = (reverse.grievance + 3).clamp(0, 100);
                        reverse.trust = (reverse.trust - 2).clamp(-100, 100);
                        reverse.recent_interaction_tick = tick;
                    }
                    apply_case_delta(&mut self.law_case_load_by_settlement, location_id, 1);
                }
                EventType::GuardsDispatched => {
                    apply_case_delta(&mut self.law_case_load_by_settlement, location_id, 1);
                    let wanted_roll = (self.deterministic_stream(
                        tick,
                        Phase::Commit,
                        observer_npc_id,
                        &format!("report_wanted:{source_event_id}:{target_id}"),
                    ) % 100) as i64;
                    let wanted_threshold =
                        (18 + salience * 3 - institution_corruption / 5).clamp(8, 60);
                    if wanted_roll < wanted_threshold {
                        self.wanted_npcs.insert(target_id.clone());
                    }
                }
                _ => {}
            }
            let memory = self.pair_memory_mut(observer_npc_id, target_id);
            memory.last_social_tick = tick;
            if matches!(
                candidate.event_type,
                EventType::InsultExchanged | EventType::PunchThrown
            ) {
                memory.last_conflict_tick = tick;
            }
            if candidate.event_type == EventType::RomanceAdvanced {
                memory.last_romance_tick = tick;
            }
        }
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
                (institution.corruption_level
                    + institution.bias_level
                    + institution.response_latency_ticks)
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
            .find(|npc| npc.location_id == location_id && self.wanted_npcs.contains(&npc.npc_id))
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

    fn social_target_for(
        &self,
        source_npc_id: &str,
        location_id: &str,
        trust_floor: i64,
    ) -> Option<String> {
        let nearby_locations = self
            .routes_by_id
            .values()
            .filter(|route| route.weather_window_open && route.hazard_score <= 55)
            .filter_map(|route| {
                if route.origin_settlement_id == location_id {
                    Some(route.destination_settlement_id.clone())
                } else if route.destination_settlement_id == location_id {
                    Some(route.origin_settlement_id.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        let trusted = self
            .relationship_edges
            .values()
            .filter(|edge| edge.source_npc_id == source_npc_id && edge.trust >= trust_floor)
            .filter(|edge| {
                self.npc_location_for(&edge.target_npc_id)
                    .map(|location| location == location_id || nearby_locations.contains(&location))
                    .unwrap_or(false)
            })
            .max_by_key(|edge| edge.trust)
            .map(|edge| edge.target_npc_id.clone());
        if trusted.is_some() {
            return trusted;
        }

        self.social_fallback_target_for(source_npc_id, location_id, &nearby_locations, 0, "legacy")
    }

    fn social_fallback_target_for(
        &self,
        source_npc_id: &str,
        location_id: &str,
        nearby_locations: &BTreeSet<String>,
        tick: u64,
        channel: &str,
    ) -> Option<String> {
        let mut candidates = self
            .npcs
            .iter()
            .filter(|npc| {
                (npc.location_id == location_id || nearby_locations.contains(&npc.location_id))
                    && npc.npc_id != source_npc_id
            })
            .map(|npc| {
                let pair_key = canonical_pair_key(source_npc_id, &npc.npc_id);
                let memory = self
                    .pair_event_memory
                    .get(&pair_key)
                    .cloned()
                    .unwrap_or_default();
                let recency_penalty = if memory.last_social_tick > 0
                    && tick > 0
                    && tick.saturating_sub(memory.last_social_tick) < 8
                {
                    9
                } else {
                    0
                };
                let relationship = self
                    .relationship_edges
                    .get(&(source_npc_id.to_string(), npc.npc_id.clone()));
                let weight = if let Some(edge) = relationship {
                    (8 + edge.trust.max(0) / 4
                        + edge.attachment / 7
                        + edge.respect / 9
                        + edge.compatibility_score.max(0) / 8
                        - edge.grievance / 7
                        - recency_penalty)
                        .clamp(1, 96)
                } else {
                    (12 - recency_penalty).clamp(1, 32)
                };
                (npc.npc_id.clone(), weight)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return None;
        }
        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        let weights = candidates
            .iter()
            .map(|(_, weight)| *weight)
            .collect::<Vec<_>>();
        let roll = self.deterministic_stream(
            tick,
            Phase::Commit,
            source_npc_id,
            &format!("social_fallback_target:{channel}:{location_id}"),
        );
        let idx = pick_weighted_index(roll, &weights)?;
        candidates.get(idx).map(|(target_id, _)| target_id.clone())
    }

    fn social_target_for_tick(
        &self,
        source_npc_id: &str,
        location_id: &str,
        trust_floor: i64,
        tick: u64,
        channel: &str,
    ) -> Option<String> {
        let nearby_locations = self
            .routes_by_id
            .values()
            .filter(|route| route.weather_window_open && route.hazard_score <= 55)
            .filter_map(|route| {
                if route.origin_settlement_id == location_id {
                    Some(route.destination_settlement_id.clone())
                } else if route.destination_settlement_id == location_id {
                    Some(route.origin_settlement_id.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        let candidates = self
            .relationship_edges
            .values()
            .filter(|edge| edge.source_npc_id == source_npc_id && edge.trust >= trust_floor)
            .filter(|edge| {
                self.npc_location_for(&edge.target_npc_id)
                    .map(|location| location == location_id || nearby_locations.contains(&location))
                    .unwrap_or(false)
            })
            .map(|edge| {
                let pair_key = canonical_pair_key(source_npc_id, &edge.target_npc_id);
                let memory = self
                    .pair_event_memory
                    .get(&pair_key)
                    .cloned()
                    .unwrap_or_default();
                let recency_penalty = if memory.last_social_tick > 0
                    && tick.saturating_sub(memory.last_social_tick) < 10
                {
                    10
                } else {
                    0
                };
                let weight = (8
                    + edge.trust / 4
                    + edge.attachment / 6
                    + edge.respect / 8
                    + edge.obligation / 10
                    - recency_penalty)
                    .clamp(1, 96);
                (edge.target_npc_id.clone(), weight)
            })
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            return self.social_fallback_target_for(
                source_npc_id,
                location_id,
                &nearby_locations,
                tick,
                channel,
            );
        }
        let weights = candidates
            .iter()
            .map(|(_, weight)| *weight)
            .collect::<Vec<_>>();
        let roll = self.deterministic_stream(
            tick,
            Phase::Commit,
            source_npc_id,
            &format!("social_target:{channel}:{location_id}"),
        );
        let idx = pick_weighted_index(roll, &weights)?;
        candidates.get(idx).map(|(target_id, _)| target_id.clone())
    }

    fn pair_memory_mut(&mut self, left_npc_id: &str, right_npc_id: &str) -> &mut PairEventMemory {
        let key = canonical_pair_key(left_npc_id, right_npc_id);
        self.pair_event_memory.entry(key).or_default()
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
            .find(|(_, item)| {
                item.location_id == location_id
                    && item.owner_id != thief_npc_id
                    && !item.owner_id.starts_with("fence:")
                    && item.last_moved_tick + 24 <= tick
            })
            .map(|(item_id, item)| (item_id.clone(), item.owner_id.clone()))
        {
            return (item_id, owner_id);
        }

        (
            format!("item:cache:{}:{tick}", location_id.replace(':', "_")),
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

fn settlement_catalog() -> [&'static str; 3] {
    [
        "settlement:greywall",
        "settlement:millford",
        "settlement:oakham",
    ]
}

fn settlement_ids_from_npcs(npcs: &[NpcAgent]) -> Vec<String> {
    let ids = npcs
        .iter()
        .map(|npc| npc.location_id.clone())
        .collect::<BTreeSet<_>>();
    if ids.is_empty() {
        return settlement_catalog()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
    }
    ids.into_iter().collect::<Vec<_>>()
}

fn resolve_npc_count(seed: u64, npc_count_min: u16, npc_count_max: u16) -> usize {
    let min = usize::from(npc_count_min.max(3));
    let max = usize::from(npc_count_max.max(npc_count_min).max(3));
    if min == max {
        return min;
    }
    let span = (max - min + 1) as u64;
    min + (mix64(seed ^ 0xA9D1_5B3C) % span) as usize
}

fn default_npcs(seed: u64, npc_count_min: u16, npc_count_max: u16) -> Vec<NpcAgent> {
    let target_count = resolve_npc_count(seed, npc_count_min, npc_count_max);
    let settlements = settlement_catalog();
    let professions = [
        "blacksmith",
        "farmhand",
        "carter",
        "tanner",
        "miller",
        "fletcher",
        "scribe",
        "guard",
        "innkeeper",
        "herbalist",
        "fisher",
        "mason",
    ];
    let aspirations = [
        "stable_household",
        "open_workshop",
        "grow_trade_network",
        "pay_off_debt",
        "gain_status",
        "master_craft",
        "protect_family",
        "secure_patronage",
    ];
    let social_classes = ["commoner", "free_worker", "free_craft", "retainer"];
    let temperaments = [
        "steady",
        "restless",
        "sociable",
        "guarded",
        "pragmatic",
        "hotheaded",
    ];
    let hobbies = [
        "craft",
        "dice",
        "hunting",
        "gossip",
        "music",
        "prayer",
        "drinking",
        "cards",
        "poetry",
        "horsecare",
    ];

    let mut npcs = vec![
        NpcAgent {
            npc_id: "npc_001".to_string(),
            location_id: "settlement:greywall".to_string(),
            profession: "blacksmith".to_string(),
            aspiration: "open_workshop".to_string(),
            social_class: "free_craft".to_string(),
            temperament: "steady".to_string(),
            hobbies: vec!["craft".to_string(), "dice".to_string()],
        },
        NpcAgent {
            npc_id: "npc_002".to_string(),
            location_id: "settlement:millford".to_string(),
            profession: "farmhand".to_string(),
            aspiration: "secure_household".to_string(),
            social_class: "commoner".to_string(),
            temperament: "sociable".to_string(),
            hobbies: vec!["gossip".to_string(), "music".to_string()],
        },
        NpcAgent {
            npc_id: "npc_003".to_string(),
            location_id: "settlement:oakham".to_string(),
            profession: "carter".to_string(),
            aspiration: "grow_trade_network".to_string(),
            social_class: "free_worker".to_string(),
            temperament: "restless".to_string(),
            hobbies: vec!["hunting".to_string(), "dice".to_string()],
        },
    ];

    for idx in 3..target_count {
        let idx_u64 = idx as u64;
        let entropy = mix64(seed ^ (idx_u64.wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        let settlement_idx = (entropy % settlements.len() as u64) as usize;
        let profession = professions[(mix64(entropy ^ 0x11) % professions.len() as u64) as usize];
        let aspiration = aspirations[(mix64(entropy ^ 0x22) % aspirations.len() as u64) as usize];
        let social_class =
            social_classes[(mix64(entropy ^ 0x33) % social_classes.len() as u64) as usize];
        let temperament =
            temperaments[(mix64(entropy ^ 0x44) % temperaments.len() as u64) as usize];
        let hobby_a = (mix64(entropy ^ 0x55) % hobbies.len() as u64) as usize;
        let hobby_b = (hobby_a + 1 + (mix64(entropy ^ 0x66) % (hobbies.len() as u64 - 1)) as usize)
            % hobbies.len();
        npcs.push(NpcAgent {
            npc_id: format!("npc_{:03}", idx + 1),
            location_id: settlements[settlement_idx].to_string(),
            profession: profession.to_string(),
            aspiration: aspiration.to_string(),
            social_class: social_class.to_string(),
            temperament: temperament.to_string(),
            hobbies: vec![hobbies[hobby_a].to_string(), hobbies[hobby_b].to_string()],
        });
    }

    npcs.truncate(target_count);
    npcs
}

fn default_items(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, ItemRecord> {
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
    for npc in npcs {
        let entropy = mix64(seed ^ hash_bytes(npc.npc_id.as_bytes()) ^ 0xC1);
        let item_id = format!("item:kit:{}", npc.npc_id);
        items.insert(
            item_id,
            ItemRecord {
                owner_id: format!("npc:{}", npc.npc_id),
                location_id: npc.location_id.clone(),
                stolen: false,
                last_moved_tick: entropy % 8,
            },
        );
    }
    items
}

fn default_npc_economy(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, NpcEconomyState> {
    let mut states = BTreeMap::new();
    for (idx, npc) in npcs.iter().enumerate() {
        let entropy = mix64(seed ^ hash_bytes(npc.npc_id.as_bytes()) ^ 0x5A);
        let wallet = 2 + (entropy % 12) as i64;
        let debt = (mix64(entropy ^ 0x11) % 5) as i64;
        let food = 2 + (mix64(entropy ^ 0x22) % 8) as i64;
        let shelter_roll = (mix64(entropy ^ 0x33) % 100) as i64;
        let shelter_status = if shelter_roll < 10 {
            ShelterStatus::Unsheltered
        } else if shelter_roll < 36 {
            ShelterStatus::Precarious
        } else {
            ShelterStatus::Stable
        };
        let dependents = (mix64(entropy ^ 0x44) % 3) as u8;
        let apprenticeship = (mix64(entropy ^ 0x55) % 128) as i64;
        let health = 58 + (mix64(entropy ^ 0x66) % 43) as i64;
        states.insert(
            npc.npc_id.clone(),
            NpcEconomyState {
                household_id: format!("household:{:03}", idx + 1),
                wallet,
                debt_balance: debt,
                food_reserve_days: food,
                shelter_status,
                dependents_count: dependents,
                apprenticeship_progress: apprenticeship,
                employer_contract_id: None,
                health,
                illness_ticks: 0,
            },
        );
    }
    states
}

fn default_npc_traits(npcs: &[NpcAgent]) -> BTreeMap<String, NpcTraitProfile> {
    npcs.iter()
        .map(|npc| {
            let base = hash_bytes(npc.npc_id.as_bytes());
            let lane =
                |salt: u64| -> i64 { 25 + (((mix64(base ^ salt) % 51) as i64).clamp(0, 50)) };
            (
                npc.npc_id.clone(),
                NpcTraitProfile {
                    risk_tolerance: lane(0xA1),
                    sociability: lane(0xB2),
                    dutifulness: lane(0xC3),
                    ambition: lane(0xD4),
                    empathy: lane(0xE5),
                    resilience: lane(0xF6),
                    aggression: lane(0x17),
                    patience: lane(0x28),
                    honor: lane(0x39),
                    generosity: lane(0x4A),
                    romance_drive: lane(0x5B),
                    gossip_drive: lane(0x6C),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
}

fn default_npc_capabilities(
    npcs: &[NpcAgent],
    traits_by_id: &BTreeMap<String, NpcTraitProfile>,
) -> BTreeMap<String, NpcCapabilityProfile> {
    npcs.iter()
        .map(|npc| {
            let profile = traits_by_id
                .get(&npc.npc_id)
                .cloned()
                .unwrap_or(NpcTraitProfile {
                    risk_tolerance: 40,
                    sociability: 40,
                    dutifulness: 40,
                    ambition: 40,
                    empathy: 40,
                    resilience: 40,
                    aggression: 40,
                    patience: 40,
                    honor: 40,
                    generosity: 40,
                    romance_drive: 40,
                    gossip_drive: 40,
                });

            let mut capability = NpcCapabilityProfile {
                npc_id: npc.npc_id.clone(),
                physical: (35 + profile.resilience / 2).clamp(10, 100),
                social: (30 + profile.sociability / 2 + profile.empathy / 4).clamp(10, 100),
                trade: (25 + profile.ambition / 2 + profile.patience / 5).clamp(10, 100),
                combat: (20 + profile.aggression / 2 + profile.risk_tolerance / 4).clamp(5, 100),
                literacy: (18 + profile.patience / 3 + profile.honor / 4).clamp(5, 100),
                influence: (20 + profile.sociability / 3 + profile.ambition / 3).clamp(5, 100),
                stealth: (20 + profile.risk_tolerance / 3 + (100 - profile.empathy) / 5)
                    .clamp(5, 100),
                care: (20 + profile.empathy / 2 + profile.generosity / 3).clamp(5, 100),
                law: (15 + profile.dutifulness / 2 + profile.honor / 3).clamp(5, 100),
            };

            match npc.profession.as_str() {
                "guard" | "watchman" | "bailiff" => {
                    capability.combat = (capability.combat + 22).clamp(5, 100);
                    capability.law = (capability.law + 20).clamp(5, 100);
                }
                "blacksmith" | "cooper" | "weaver" | "stonemason" => {
                    capability.trade = (capability.trade + 20).clamp(5, 100);
                    capability.physical = (capability.physical + 10).clamp(5, 100);
                }
                "farmer" | "farmhand" | "woodcutter" => {
                    capability.physical = (capability.physical + 18).clamp(5, 100);
                    capability.care = (capability.care + 6).clamp(5, 100);
                }
                "merchant" | "innkeeper" | "carter" => {
                    capability.trade = (capability.trade + 18).clamp(5, 100);
                    capability.social = (capability.social + 12).clamp(5, 100);
                    capability.influence = (capability.influence + 10).clamp(5, 100);
                }
                "healer" => {
                    capability.care = (capability.care + 24).clamp(5, 100);
                    capability.literacy = (capability.literacy + 14).clamp(5, 100);
                }
                "scribe" | "clerk" => {
                    capability.literacy = (capability.literacy + 22).clamp(5, 100);
                    capability.law = (capability.law + 12).clamp(5, 100);
                }
                "noble" | "retainer" => {
                    capability.influence = (capability.influence + 24).clamp(5, 100);
                    capability.social = (capability.social + 12).clamp(5, 100);
                }
                _ => {}
            }

            (npc.npc_id.clone(), capability)
        })
        .collect::<BTreeMap<_, _>>()
}

fn default_households(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, HouseholdState> {
    let mut households = BTreeMap::new();
    for (idx, npc) in npcs.iter().enumerate() {
        let entropy = mix64(seed ^ hash_bytes(npc.npc_id.as_bytes()) ^ 0x7D);
        let pantry = 4 + (entropy % 12) as i64;
        let fuel = 3 + (mix64(entropy ^ 0x10) % 9) as i64;
        let rent_due_tick = 24 * (5 + (mix64(entropy ^ 0x20) % 28));
        let rent_amount = 8 + (mix64(entropy ^ 0x30) % 18) as i64;
        let eviction_risk = 4 + (mix64(entropy ^ 0x40) % 37) as i64;
        let household_id = format!("household:{:03}", idx + 1);
        households.insert(
            household_id.clone(),
            HouseholdState {
                household_id,
                member_npc_ids: vec![npc.npc_id.clone()],
                shared_pantry_stock: pantry,
                fuel_stock: fuel,
                rent_due_tick,
                rent_cadence_ticks: 24 * 30,
                rent_amount,
                rent_reserve_coin: 0,
                landlord_balance: 0,
                eviction_risk_score: eviction_risk,
            },
        );
    }
    households
}

fn default_contracts(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, ContractState> {
    let mut contracts = BTreeMap::new();
    for (idx, npc) in npcs.iter().enumerate() {
        let entropy = mix64(seed ^ hash_bytes(npc.npc_id.as_bytes()) ^ 0x3F);
        let settlement_id = npc.location_id.clone();
        let contract_id = format!("contract:seed:{:03}", idx + 1);
        let cadence = match mix64(entropy ^ 0x01) % 3 {
            0 => ContractCadence::Daily,
            1 => ContractCadence::Weekly,
            _ => ContractCadence::Monthly,
        };
        let wage_amount = match cadence {
            ContractCadence::Daily => 2 + (mix64(entropy ^ 0x02) % 4) as i64,
            ContractCadence::Weekly => 11 + (mix64(entropy ^ 0x03) % 10) as i64,
            ContractCadence::Monthly => 40 + (mix64(entropy ^ 0x04) % 32) as i64,
        };
        let payment_phase = (mix64(entropy ^ 0x05) % 8) as u64;
        let next_payment_tick = cadence_interval_ticks(cadence) + payment_phase;
        let reliability = (46 + (mix64(entropy ^ 0x06) % 50) as i64
            - (mix64(entropy ^ 0x07) % 8) as i64)
            .clamp(30, 95) as u8;
        contracts.insert(
            contract_id.clone(),
            ContractState {
                contract: EmploymentContractRecord {
                    contract_id: contract_id.clone(),
                    employer_id: format!("employer:{settlement_id}"),
                    worker_id: npc.npc_id.clone(),
                    settlement_id,
                    compensation_type: if mix64(entropy ^ 0x08) % 2 == 0 {
                        ContractCompensationType::Mixed
                    } else {
                        ContractCompensationType::Coin
                    },
                    cadence,
                    wage_amount,
                    reliability_score: reliability,
                    active: true,
                    breached: false,
                    next_payment_tick,
                },
            },
        );
    }
    contracts
}

fn default_labor_markets(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, LaborMarketState> {
    let settlements = settlement_ids_from_npcs(npcs);
    let mut by_settlement = BTreeMap::<String, usize>::new();
    for npc in npcs {
        *by_settlement.entry(npc.location_id.clone()).or_default() += 1;
    }
    let mut markets = BTreeMap::new();
    for settlement_id in settlements {
        let pop = *by_settlement.get(&settlement_id).unwrap_or(&0) as i64;
        let entropy = mix64(seed ^ hash_bytes(settlement_id.as_bytes()) ^ 0x90);
        let base_roles = (pop * 3 / 5).max(2);
        let open_roles = (base_roles + (entropy % 4) as i64).max(2) as u32;
        let wage_band_low = 1 + (mix64(entropy ^ 0x01) % 3) as i64;
        let wage_band_high = wage_band_low + 2 + (mix64(entropy ^ 0x02) % 3) as i64;
        let underemployment_index = ((pop - i64::from(open_roles)).max(0) * 5
            + (mix64(entropy ^ 0x03) % 11) as i64)
            .clamp(6, 96);
        markets.insert(
            settlement_id,
            LaborMarketState {
                open_roles,
                wage_band_low,
                wage_band_high,
                underemployment_index,
            },
        );
    }
    markets
}

fn default_stock_ledgers(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, StockLedgerState> {
    let settlements = settlement_ids_from_npcs(npcs);
    let mut by_settlement = BTreeMap::<String, usize>::new();
    for npc in npcs {
        *by_settlement.entry(npc.location_id.clone()).or_default() += 1;
    }
    let mut stocks = BTreeMap::new();
    for settlement_id in settlements {
        let pop = *by_settlement.get(&settlement_id).unwrap_or(&0) as i64;
        let entropy = mix64(seed ^ hash_bytes(settlement_id.as_bytes()) ^ 0xA7);
        stocks.insert(
            settlement_id.clone(),
            StockLedgerState {
                staples: 18 + pop * 2 + (entropy % 6) as i64,
                fuel: 10 + pop + (mix64(entropy ^ 0x01) % 5) as i64,
                medicine: 4 + pop / 3 + (mix64(entropy ^ 0x02) % 3) as i64,
                craft_inputs: 7 + pop / 2 + (mix64(entropy ^ 0x03) % 5) as i64,
                local_price_pressure: 0,
                coin_reserve: 180 + pop * 16 + (mix64(entropy ^ 0x04) % 40) as i64,
            },
        );
    }
    stocks
}

fn default_production_nodes(
    npcs: &[NpcAgent],
    seed: u64,
) -> BTreeMap<String, ProductionNodeRuntimeState> {
    let settlements = settlement_ids_from_npcs(npcs);
    let mut nodes = BTreeMap::new();
    for settlement_id in settlements {
        for node_kind in ["farm", "workshop", "kiln"] {
            let node_id = format!(
                "node:{node_kind}:{}",
                settlement_id.replace("settlement:", "")
            );
            let entropy = mix64(seed ^ hash_bytes(node_id.as_bytes()) ^ 0xBC);
            nodes.insert(
                node_id.clone(),
                ProductionNodeRuntimeState {
                    node_id,
                    settlement_id: settlement_id.clone(),
                    node_kind: node_kind.to_string(),
                    input_backlog: 2 + (entropy % 3) as i64,
                    output_backlog: 1 + (mix64(entropy ^ 0x01) % 3) as i64,
                    spoilage_timer: 10 + (mix64(entropy ^ 0x02) % 8) as i64,
                },
            );
        }
    }
    nodes
}

fn default_relationship_edges(
    npcs: &[NpcAgent],
) -> BTreeMap<(String, String), RelationshipEdgeRuntime> {
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
                    relation_tags: vec![
                        "acquaintance".to_string(),
                        format!("compat:{compatibility}"),
                    ],
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

fn default_institutions(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, InstitutionRuntimeState> {
    let settlements = settlement_ids_from_npcs(npcs);
    settlements
        .into_iter()
        .map(|settlement_id| {
            let entropy = mix64(seed ^ hash_bytes(settlement_id.as_bytes()) ^ 0xD3);
            (
                settlement_id,
                InstitutionRuntimeState {
                    enforcement_capacity: 48 + (entropy % 25) as i64,
                    corruption_level: 15 + (mix64(entropy ^ 0x01) % 22) as i64,
                    bias_level: 10 + (mix64(entropy ^ 0x02) % 18) as i64,
                    response_latency_ticks: 3 + (mix64(entropy ^ 0x03) % 4) as i64,
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
}

fn default_groups(npcs: &[NpcAgent], seed: u64) -> BTreeMap<String, GroupRuntimeState> {
    let mut groups = BTreeMap::new();
    if let Some(leader) = npcs.first() {
        let entropy = mix64(seed ^ hash_bytes(leader.npc_id.as_bytes()) ^ 0xE1);
        let mut members = npcs
            .iter()
            .filter(|npc| npc.location_id == leader.location_id)
            .take(3)
            .map(|npc| npc.npc_id.clone())
            .collect::<Vec<_>>();
        if !members.iter().any(|member| member == &leader.npc_id) {
            members.push(leader.npc_id.clone());
        }
        groups.insert(
            "group:lantern_circle".to_string(),
            GroupRuntimeState {
                group_id: "group:lantern_circle".to_string(),
                settlement_id: leader.location_id.clone(),
                leader_npc_id: leader.npc_id.clone(),
                member_npc_ids: members,
                norm_tags: vec!["share_news".to_string(), "watch_roads".to_string()],
                cohesion_score: 24 + (entropy % 18) as i64,
                formed_tick: 0,
                inertia_score: 18 + (mix64(entropy ^ 0x01) % 16) as i64,
                shared_cause: "shared_watch".to_string(),
                infiltration_pressure: 2 + (mix64(entropy ^ 0x02) % 6) as i64,
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
    .map(
        |(route_id, origin_settlement_id, destination_settlement_id)| {
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
        },
    )
    .collect::<BTreeMap<_, _>>()
}

fn default_market_clearing(npcs: &[NpcAgent]) -> BTreeMap<String, MarketClearingRuntimeState> {
    let mut map = BTreeMap::new();
    for settlement_id in settlement_ids_from_npcs(npcs) {
        map.insert(
            settlement_id.clone(),
            MarketClearingRuntimeState {
                settlement_id,
                staples_price_index: 100,
                fuel_price_index: 100,
                medicine_price_index: 100,
                wage_pressure: 0,
                shortage_score: 0,
                unmet_demand: 0,
                cleared_tick: 0,
                market_cleared: true,
            },
        );
    }
    map
}

fn default_institution_queue(npcs: &[NpcAgent]) -> BTreeMap<String, InstitutionQueueRuntimeState> {
    let mut map = BTreeMap::new();
    for settlement_id in settlement_ids_from_npcs(npcs) {
        map.insert(
            settlement_id.clone(),
            InstitutionQueueRuntimeState {
                settlement_id,
                pending_cases: 0,
                processed_cases: 0,
                dropped_cases: 0,
                avg_response_latency: 2,
            },
        );
    }
    map
}

fn operator_family_for_action(action: &str) -> &'static str {
    match action {
        "work_for_coin" | "work_for_food" | "tend_fields" | "craft_goods" | "trade_visit"
        | "forage" | "gather_firewood" => "livelihood",
        "pay_rent" | "seek_shelter" | "share_meal" | "ration_grain" | "repair_hearth" => {
            "household"
        }
        "converse_neighbor"
        | "lend_coin"
        | "court_romance"
        | "form_mutual_aid_group"
        | "mediate_dispute"
        | "assist_neighbor"
        | "defend_patron" => "social",
        "investigate_rumor"
        | "share_rumor"
        | "observe_notable_event"
        | "question_travelers"
        | "spread_accusation"
        | "collect_testimony" => "information",
        "patrol_road" | "organize_watch" | "avoid_patrols" => "security",
        "seek_treatment" | "train_apprentice" | "cover_absent_neighbor" => "health",
        "steal_supplies" | "fence_goods" => "illicit",
        _ => "leisure",
    }
}

fn default_operator_catalog() -> Vec<OperatorDefinition> {
    let actions = [
        "work_for_food",
        "work_for_coin",
        "pay_rent",
        "seek_shelter",
        "tend_fields",
        "craft_goods",
        "share_meal",
        "form_mutual_aid_group",
        "defend_patron",
        "spread_accusation",
        "mediate_dispute",
        "train_apprentice",
        "share_rumor",
        "patrol_road",
        "assist_neighbor",
        "converse_neighbor",
        "lend_coin",
        "court_romance",
        "seek_treatment",
        "observe_notable_event",
        "forage",
        "steal_supplies",
        "investigate_rumor",
        "gather_firewood",
        "cover_absent_neighbor",
        "organize_watch",
        "fence_goods",
        "avoid_patrols",
        "question_travelers",
        "ration_grain",
        "repair_hearth",
        "collect_testimony",
        "trade_visit",
    ];
    let phases = [
        ("scan", 1_u64),
        ("approach", 1_u64),
        ("execute", 2_u64),
        ("followup", 1_u64),
    ];
    let mut operators = Vec::new();
    for action in actions {
        let family = operator_family_for_action(action);
        for (phase, duration_ticks) in phases {
            let risk = match family {
                "illicit" => 70,
                "security" => 48,
                "information" => 35,
                "social" => 28,
                _ => 18,
            } + if phase == "execute" { 8 } else { 0 };
            let visibility = match family {
                "illicit" => 62,
                "security" => 66,
                "social" => 58,
                _ => 46,
            };
            let preconditions = vec![
                format!("family:{family}"),
                format!("action:{action}"),
                format!("phase:{phase}"),
            ];
            let effects = vec![
                format!(
                    "effect:{action}:{}",
                    if phase == "execute" {
                        "primary"
                    } else {
                        "support"
                    }
                ),
                format!("occupancy:{phase}"),
            ];
            let mut resource_delta = BTreeMap::new();
            resource_delta.insert("time".to_string(), -(duration_ticks as i64));
            if matches!(family, "livelihood" | "household") && phase == "execute" {
                resource_delta.insert("coin".to_string(), 1);
            }
            if family == "illicit" && phase == "execute" {
                resource_delta.insert("risk".to_string(), 2);
            }
            operators.push(OperatorDefinition {
                operator_id: format!("op:{family}:{action}:{phase}"),
                family: family.to_string(),
                action: action.to_string(),
                duration_ticks,
                risk,
                visibility,
                preconditions,
                effects,
                resource_delta,
            });
        }
    }
    operators
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

    if law_case_load >= 1 || is_wanted || (pressure_index >= 210 && intent_seed % 4 == 0) {
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
            pressures.push("seasonal_strain".to_string());
        }
        if harvest_shock >= 2 {
            pressures.push("food_tight".to_string());
        }
        if law_case_load >= 1 {
            pressures.push("institutional_watch".to_string());
        }
        pressures
    }
}

fn build_time_budget_for_tick(
    tick: u64,
    npc_id: &str,
    profile: &NpcTraitProfile,
    is_wanted: bool,
) -> TimeBudgetRuntimeState {
    let entropy = mix64(hash_bytes(
        format!("{tick}:{npc_id}:time_budget").as_bytes(),
    ));
    let sleep_hours = 6 + (entropy % 3) as i64;
    let base_work = 6 + ((entropy >> 8) % 4) as i64;
    let social_hours = 1 + ((profile.sociability.max(0) as u64 + (entropy >> 16) % 4) % 4) as i64;
    let care_hours = 1 + ((profile.empathy.max(0) as u64 + (entropy >> 20) % 3) % 3) as i64;
    let travel_hours = 1 + ((entropy >> 24) % 3) as i64 + is_wanted as i64;
    let recovery_hours = 1 + ((100 - profile.resilience.clamp(0, 100)) / 25);
    let work_hours = base_work.clamp(4, 12);
    let free_hours =
        (24 - sleep_hours - work_hours - care_hours - travel_hours - social_hours - recovery_hours)
            .max(0);
    TimeBudgetRuntimeState {
        npc_id: npc_id.to_string(),
        tick,
        sleep_hours,
        work_hours,
        care_hours,
        travel_hours,
        social_hours,
        recovery_hours,
        free_hours,
    }
}

fn build_drive_state(
    tick: u64,
    decision: &NpcDecision,
    profile: &NpcTraitProfile,
    food_reserve_days: i64,
    debt_balance: i64,
    wallet: i64,
    shelter_status: ShelterStatus,
    eviction_risk: i64,
    law_case_load: i64,
    illness_ticks: i64,
    health: i64,
    rent_due_in_ticks: i64,
    rent_shortfall: i64,
    local_conflict_pressure: i64,
) -> NpcDriveRuntimeState {
    let need_food = ((2 - food_reserve_days).max(0) * 24 + illness_ticks / 2).clamp(0, 100);
    let need_shelter = match shelter_status {
        ShelterStatus::Unsheltered => 85,
        ShelterStatus::Precarious => 45 + eviction_risk / 2,
        ShelterStatus::Stable => (eviction_risk / 3).clamp(0, 30),
    }
    .clamp(0, 100);
    let need_income = (debt_balance * 9
        + rent_shortfall * 2
        + if rent_due_in_ticks <= 48 { 18 } else { 0 }
        + (2 - wallet).max(0) * 8)
        .clamp(0, 100);
    let need_safety = (law_case_load * 8
        + local_conflict_pressure * 5
        + if decision
            .top_intents
            .iter()
            .any(|intent| intent == "evade_patrols")
        {
            16
        } else {
            0
        })
    .clamp(0, 100);
    let need_belonging = (40 + profile.sociability / 2 - profile.aggression / 3).clamp(0, 100);
    let need_status = (35 + profile.ambition / 2 + profile.honor / 3).clamp(0, 100);
    let need_recovery =
        ((60 - health).max(0) + illness_ticks * 2 + (35 - profile.resilience).max(0)).clamp(0, 100);
    let stress = ((need_food + need_shelter + need_income + need_safety + need_recovery) / 5
        + (profile.patience <= 35) as i64 * 8)
        .clamp(0, 100);

    NpcDriveRuntimeState {
        npc_id: decision.npc_id.clone(),
        tick,
        need_food,
        need_shelter,
        need_income,
        need_safety,
        need_belonging,
        need_status,
        need_recovery,
        stress,
        active_obligations: decision
            .top_intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.as_str(),
                    "secure_food"
                        | "stabilize_security"
                        | "cover_missing_roles"
                        | "patrol_routes"
                        | "evade_patrols"
                )
            })
            .cloned()
            .collect::<Vec<_>>(),
        active_aspirations: decision
            .top_intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.as_str(),
                    "seek_opportunity" | "build_trust" | "seek_companionship" | "exchange_stories"
                )
            })
            .cloned()
            .collect::<Vec<_>>(),
        moral_bounds: vec![
            if profile.honor >= 55 {
                "honor_bound".to_string()
            } else {
                "pragmatic_bound".to_string()
            },
            if profile.empathy >= 55 {
                "prosocial_bound".to_string()
            } else {
                "self_preserving_bound".to_string()
            },
        ],
    }
}

fn select_goal_id_from_drive(drive: &NpcDriveRuntimeState) -> String {
    let mut goals = vec![
        (
            "survive",
            drive.need_food + drive.need_shelter + drive.need_recovery,
        ),
        (
            "stabilize_household",
            drive.need_income + drive.need_shelter,
        ),
        ("earn", drive.need_income + drive.need_status / 2),
        (
            "socialize",
            drive.need_belonging + (100 - drive.need_safety) / 3,
        ),
        ("status", drive.need_status + drive.need_income / 4),
        ("investigate", drive.need_safety / 2 + drive.stress / 4),
        ("recover", drive.need_recovery + drive.stress / 3),
    ];
    goals.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    goals
        .first()
        .map(|(goal_id, _)| (*goal_id).to_string())
        .unwrap_or_else(|| "maintain_routine".to_string())
}

fn min_commit_threshold_for_decision(drive: Option<&NpcDriveRuntimeState>, emergency: bool) -> i64 {
    if emergency {
        return -1_000_000;
    }
    let Some(drive) = drive else {
        return 3_000;
    };
    let pressure =
        (drive.need_food + drive.need_shelter + drive.need_income + drive.need_safety) / 4;
    let recovery_bias = drive.need_recovery / 6;
    let social_pull = drive.need_belonging / 4;
    let mut baseline = 4_200 - pressure * 26 + recovery_bias + social_pull / 2;
    if pressure <= 25 {
        baseline += 500;
    } else if pressure <= 40 {
        baseline += 250;
    }
    baseline.clamp(1_100, 5_000)
}

fn idle_occupancy_for_tick(tick: u64, npc_id: &str) -> NpcOccupancyKind {
    let roll = mix64(hash_bytes(format!("{tick}:{npc_id}:idle_kind").as_bytes())) % 100;
    if roll < 30 {
        NpcOccupancyKind::Resting
    } else if roll < 70 {
        NpcOccupancyKind::Loitering
    } else {
        NpcOccupancyKind::SocialPresence
    }
}

fn should_interrupt_execution(
    _decision: &NpcDecision,
    emergency: bool,
    tick: u64,
    execution: &ActionExecutionRuntimeState,
) -> bool {
    if emergency {
        return true;
    }
    if execution.remaining_ticks <= 1 {
        return true;
    }
    let is_rare_window = matches!(execution.action.as_str(), "pay_rent" | "seek_treatment");
    if is_rare_window && tick + execution.remaining_ticks > execution.tick + 24 {
        return true;
    }
    false
}

fn candidate_from_action_name<F>(
    action: &str,
    tick: u64,
    npc_id: &str,
    stream: F,
) -> Option<ActionCandidate>
where
    F: Fn(u64, &str, &str) -> u64,
{
    let verb = match canonical_action_name(action) {
        "work_for_food" => AffordanceVerb::WorkForFood,
        "work_for_coin" => AffordanceVerb::WorkForCoin,
        "pay_rent" => AffordanceVerb::PayRent,
        "seek_shelter" => AffordanceVerb::SeekShelter,
        "tend_fields" => AffordanceVerb::TendFields,
        "craft_goods" => AffordanceVerb::CraftGoods,
        "share_meal" => AffordanceVerb::ShareMeal,
        "form_mutual_aid_group" => AffordanceVerb::FormMutualAidGroup,
        "defend_patron" => AffordanceVerb::DefendPatron,
        "spread_accusation" => AffordanceVerb::SpreadAccusation,
        "mediate_dispute" => AffordanceVerb::MediateDispute,
        "train_apprentice" => AffordanceVerb::TrainApprentice,
        "share_rumor" => AffordanceVerb::ShareRumor,
        "patrol_road" => AffordanceVerb::PatrolRoad,
        "assist_neighbor" => AffordanceVerb::AssistNeighbor,
        "converse_neighbor" => AffordanceVerb::ConverseNeighbor,
        "lend_coin" => AffordanceVerb::LendCoin,
        "court_romance" => AffordanceVerb::CourtRomance,
        "seek_treatment" => AffordanceVerb::SeekTreatment,
        "observe_notable_event" => AffordanceVerb::ObserveNotableEvent,
        "forage" => AffordanceVerb::Forage,
        "steal_supplies" => AffordanceVerb::StealSupplies,
        "investigate_rumor" => AffordanceVerb::InvestigateRumor,
        "gather_firewood" => AffordanceVerb::GatherFirewood,
        "cover_absent_neighbor" => AffordanceVerb::CoverAbsentNeighbor,
        "organize_watch" => AffordanceVerb::OrganizeWatch,
        "fence_goods" => AffordanceVerb::FenceGoods,
        "avoid_patrols" => AffordanceVerb::AvoidPatrols,
        "question_travelers" => AffordanceVerb::QuestionTravelers,
        "ration_grain" => AffordanceVerb::RationGrain,
        "repair_hearth" => AffordanceVerb::RepairHearth,
        "collect_testimony" => AffordanceVerb::CollectTestimony,
        "trade_visit" => AffordanceVerb::TradeVisit,
        _ => return None,
    };
    Some(candidate(verb, 2, 0, tick, npc_id, &stream))
}

fn select_plan_for_action<F>(
    plans: &[PlanCandidateState],
    action: &str,
    tick: u64,
    npc_id: &str,
    stream: F,
) -> (String, Vec<String>)
where
    F: Fn(u64, &str, &str) -> u64,
{
    let mut matching = plans
        .iter()
        .filter(|plan| plan.action == action && plan.blocked_reasons.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    matching.sort_by(|left, right| {
        right
            .utility_score
            .cmp(&left.utility_score)
            .then_with(|| left.risk_score.cmp(&right.risk_score))
            .then_with(|| {
                left.operator_chain_ids
                    .len()
                    .cmp(&right.operator_chain_ids.len())
            })
            .then_with(|| left.plan_id.cmp(&right.plan_id))
    });
    if matching.is_empty() {
        return (
            format!("plan_{:06}_{npc_id}_{action}", tick),
            vec![format!("op:{action}:step_0")],
        );
    }
    let idx =
        (stream(tick, npc_id, &format!("plan_pick:{action}")) % (matching.len() as u64)) as usize;
    let chosen = matching.get(idx).unwrap_or(&matching[0]);
    (chosen.plan_id.clone(), chosen.operator_chain_ids.clone())
}

fn build_plan_candidates_from_alternatives(
    tick: u64,
    npc_id: &str,
    goal_id: &str,
    alternatives: &[ActionCandidate],
    operator_catalog: &[OperatorDefinition],
    law_case_load: i64,
    local_conflict_pressure: i64,
) -> Vec<PlanCandidateState> {
    alternatives
        .iter()
        .enumerate()
        .map(|(idx, action)| {
            let canonical_action = canonical_action_name(action.action.as_str());
            let operator_chain_ids = operator_catalog
                .iter()
                .filter(|entry| entry.action == canonical_action)
                .take(6)
                .map(|entry| entry.operator_id.clone())
                .collect::<Vec<_>>();
            let mut blocked_reasons = Vec::<String>::new();
            if operator_chain_ids.is_empty() {
                blocked_reasons.push("no_operator_path".to_string());
            }
            if law_case_load >= 6
                && matches!(
                    canonical_action,
                    "steal_supplies" | "fence_goods" | "avoid_patrols"
                )
            {
                blocked_reasons.push("law_pressure_block".to_string());
            }
            if local_conflict_pressure >= 5
                && matches!(
                    canonical_action,
                    "court_romance" | "share_meal" | "converse_neighbor"
                )
            {
                blocked_reasons.push("social_instability_block".to_string());
            }
            PlanCandidateState {
                plan_id: format!("plan_{:06}_{npc_id}_{idx}", tick),
                npc_id: npc_id.to_string(),
                tick,
                goal_id: goal_id.to_string(),
                action: action.action.clone(),
                utility_score: i64::from(action.priority) * 100 + (action.score / 100) as i64,
                risk_score: matches!(
                    canonical_action,
                    "steal_supplies" | "fence_goods" | "spread_accusation" | "avoid_patrols"
                ) as i64
                    * 35,
                temporal_fit: (6 - action_cadence_window(action.action.as_str()).min(6) as i64)
                    .max(0),
                operator_chain_ids,
                blocked_reasons,
            }
        })
        .collect::<Vec<_>>()
}

fn action_cadence_window(action: &str) -> u64 {
    match canonical_action_name(action) {
        "work_for_coin" | "work_for_food" | "tend_fields" | "craft_goods" => 4,
        "converse_neighbor" | "share_meal" | "mediate_dispute" | "observe_notable_event" => 4,
        "lend_coin" => 12,
        "court_romance" => 24,
        "seek_treatment" => 8,
        "pay_rent" => 24,
        "steal_supplies" => 24,
        "form_mutual_aid_group" => 24,
        "seek_shelter" => 8,
        "spread_accusation" => 12,
        "trade_visit" => 6,
        _ => 1,
    }
}

fn action_duration_ticks(action: &str) -> u64 {
    match canonical_action_name(action) {
        "work_for_coin" | "work_for_food" | "tend_fields" | "craft_goods" => 3,
        "trade_visit" | "patrol_road" | "investigate_rumor" => 2,
        "seek_treatment" | "seek_shelter" => 2,
        "pay_rent" => 1,
        "converse_neighbor" | "share_meal" | "court_romance" | "lend_coin" => 2,
        "steal_supplies" | "fence_goods" | "avoid_patrols" => 2,
        _ => 1,
    }
}

fn occupancy_state_tag(occupancy: NpcOccupancyKind) -> &'static str {
    match occupancy {
        NpcOccupancyKind::Idle => "idle_bench",
        NpcOccupancyKind::Resting => "rest_home",
        NpcOccupancyKind::Loitering => "watch_street",
        NpcOccupancyKind::Traveling => "travel_wait",
        NpcOccupancyKind::ExecutingPlanStep => "executing_plan_step",
        NpcOccupancyKind::Recovering => "recovering",
        NpcOccupancyKind::SocialPresence => "quiet_social_presence",
    }
}

fn canonical_action_name(action: &str) -> &str {
    action.split_once("::").map(|(base, _)| base).unwrap_or(action)
}

fn action_variant_name(action: &str) -> Option<&str> {
    action.split_once("::").map(|(_, variant)| variant)
}

fn build_opportunities_from_candidates(
    tick: u64,
    decision: &NpcDecision,
    candidates: &[ActionCandidate],
    time_budget: &TimeBudgetRuntimeState,
    rent_due_in_ticks: i64,
    rent_shortfall: i64,
    law_case_load: i64,
) -> Vec<OpportunityRuntimeState> {
    let mut ranked = candidates.to_vec();
    ranked.sort_by(|left, right| {
        (right.priority, right.score, &right.action).cmp(&(left.priority, left.score, &left.action))
    });
    let mut selected = Vec::<ActionCandidate>::new();
    let mut selected_actions = BTreeSet::<String>::new();
    let mut source_counts = BTreeMap::<&'static str, usize>::new();

    let mut source_heads = BTreeMap::<&'static str, ActionCandidate>::new();
    for candidate in &ranked {
        let source = opportunity_source_for_verb(candidate.verb);
        source_heads
            .entry(source)
            .or_insert_with(|| candidate.clone());
    }
    for source in [
        "labor_market",
        "production_cycle",
        "social_network",
        "institution_signal",
        "local_context",
    ] {
        let Some(candidate) = source_heads.get(source) else {
            continue;
        };
        let canonical = canonical_action_name(candidate.action.as_str()).to_string();
        if selected_actions.insert(canonical) {
            selected.push(candidate.clone());
            *source_counts.entry(source).or_insert(0) += 1;
        }
    }

    for candidate in &ranked {
        let source = opportunity_source_for_verb(candidate.verb);
        let quota = opportunity_source_quota(source);
        let canonical = canonical_action_name(candidate.action.as_str());
        if selected_actions.contains(canonical) {
            continue;
        }
        if source_counts.get(source).copied().unwrap_or_default() >= quota {
            continue;
        }
        selected_actions.insert(canonical.to_string());
        *source_counts.entry(source).or_insert(0) += 1;
        selected.push(candidate.clone());
        if selected.len() >= 7 {
            break;
        }
    }

    let theft_pressure_active = decision.top_pressures.iter().any(|pressure| {
        matches!(
            pressure.as_str(),
            "rent_deadline"
                | "debt_strain"
                | "household_hunger"
                | "food_tight"
                | "food_shock"
                | "seasonal_strain"
                | "cold_exposure_risk"
        )
    });
    let theft_edge_open = theft_pressure_active
        && law_case_load <= 4
                && ((rent_shortfall > 0 && rent_due_in_ticks <= 48)
            || decision.top_pressures.iter().any(|pressure| {
                matches!(
                    pressure.as_str(),
                    "food_tight" | "food_shock" | "seasonal_strain" | "cold_exposure_risk"
                )
            }));
    if theft_edge_open {
        if let Some(illicit_candidate) = ranked
            .iter()
            .find(|candidate| canonical_action_name(candidate.action.as_str()) == "steal_supplies")
            .cloned()
        {
            let canonical = canonical_action_name(illicit_candidate.action.as_str());
            if !selected_actions.contains(canonical) {
                if selected.len() >= 7 {
                    if let Some(removed) = selected.pop() {
                        selected_actions.remove(canonical_action_name(removed.action.as_str()));
                    }
                }
                selected_actions.insert(canonical.to_string());
                selected.push(illicit_candidate);
            }
        }
    }

    if selected.len() < 4 {
        for candidate in ranked {
            if selected_actions.insert(canonical_action_name(candidate.action.as_str()).to_string()) {
                selected.push(candidate);
            }
            if selected.len() >= 4 {
                break;
            }
        }
    }

    selected
        .into_iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let source = opportunity_source_for_verb(candidate.verb);
            let mut constraints = Vec::new();
            if rent_shortfall > 0 && rent_due_in_ticks <= 24 {
                constraints.push("rent_due_window_active".to_string());
            }
            if law_case_load > 0 {
                constraints.push("institutional_attention_active".to_string());
            }
            if time_budget.free_hours <= 1 {
                constraints.push("time_budget_tight".to_string());
            }
            OpportunityRuntimeState {
                opportunity_id: format!("opp_{:06}_{}_{}", tick, decision.npc_id, idx),
                npc_id: decision.npc_id.clone(),
                location_id: decision.location_id.clone(),
                action: candidate.action.clone(),
                source: source.to_string(),
                opened_tick: tick,
                expires_tick: tick + action_cadence_window(candidate.action.as_str()).max(4),
                utility_hint: i64::from(candidate.priority) * 100 + (candidate.score / 100) as i64,
                constraints,
            }
        })
        .collect::<Vec<_>>()
}

fn cadence_phase_offset(npc_id: &str, action: &str, cadence: u64) -> u64 {
    if cadence <= 1 {
        return 0;
    }
    let seed = format!("{npc_id}:{action}:cadence_phase");
    mix64(hash_bytes(seed.as_bytes())) % cadence
}

fn cadence_exact_window_open(tick: u64, npc_id: &str, action: &str) -> bool {
    let cadence = action_cadence_window(action).max(1);
    if cadence <= 1 {
        return true;
    }
    (tick + cadence_phase_offset(npc_id, action, cadence)) % cadence == 0
}

fn cadence_relaxed_window_open(tick: u64, npc_id: &str, action: &str) -> bool {
    let cadence = action_cadence_window(action).max(1);
    if cadence <= 1 {
        return true;
    }
    let cadence_offset = (tick + cadence_phase_offset(npc_id, action, cadence)) % cadence;
    if matches!(action, "pay_rent" | "steal_supplies") {
        return cadence_offset == 0;
    }
    if cadence >= 12 {
        cadence_offset <= 1 || cadence.saturating_sub(cadence_offset) <= 1
    } else {
        cadence_offset <= 2 || cadence.saturating_sub(cadence_offset) <= 2
    }
}

fn constrain_candidates_by_opportunities(
    tick: u64,
    npc_id: &str,
    candidates: &[ActionCandidate],
    opportunities: &[OpportunityRuntimeState],
) -> Vec<ActionCandidate> {
    let allowed = opportunities
        .iter()
        .map(|entry| entry.action.as_str())
        .collect::<BTreeSet<_>>();
    let mut constrained = candidates
        .iter()
        .filter(|candidate| {
            allowed.contains(candidate.action.as_str())
                && cadence_exact_window_open(tick, npc_id, candidate.action.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    if constrained.len() >= 3 {
        return constrained;
    }

    let mut ranked_relaxed = candidates
        .iter()
        .filter(|candidate| allowed.contains(candidate.action.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    ranked_relaxed.sort_by(|left, right| {
        (right.priority, right.score, &right.action).cmp(&(left.priority, left.score, &left.action))
    });
    let mut seen = constrained
        .iter()
        .map(|candidate| candidate.action.clone())
        .collect::<BTreeSet<_>>();
    for candidate in ranked_relaxed {
        if seen.contains(candidate.action.as_str()) {
            continue;
        }
        if cadence_relaxed_window_open(tick, npc_id, candidate.action.as_str()) {
            seen.insert(candidate.action.clone());
            constrained.push(candidate);
        }
        if constrained.len() >= 4 {
            return constrained;
        }
    }

    constrained
}

fn opportunity_source_for_verb(verb: AffordanceVerb) -> &'static str {
    match verb {
        AffordanceVerb::WorkForCoin | AffordanceVerb::WorkForFood => "labor_market",
        AffordanceVerb::PayRent | AffordanceVerb::SeekShelter | AffordanceVerb::SeekTreatment => {
            "household_need"
        }
        AffordanceVerb::TendFields | AffordanceVerb::CraftGoods => "production_cycle",
        AffordanceVerb::PatrolRoad | AffordanceVerb::CollectTestimony => "institution_signal",
        AffordanceVerb::ShareMeal
        | AffordanceVerb::ConverseNeighbor
        | AffordanceVerb::LendCoin
        | AffordanceVerb::CourtRomance
        | AffordanceVerb::FormMutualAidGroup => "social_network",
        _ => "local_context",
    }
}

fn opportunity_source_quota(source: &str) -> usize {
    match source {
        "production_cycle" => 2,
        "labor_market" => 2,
        "household_need" => 2,
        "social_network" => 2,
        "institution_signal" => 1,
        _ => 2,
    }
}

fn commitment_family_for_action(action: &str) -> &'static str {
    match canonical_action_name(action) {
        "pay_rent" => "rent",
        "seek_shelter" => "shelter",
        "work_for_coin" | "work_for_food" | "tend_fields" | "craft_goods" | "trade_visit"
        | "forage" => "livelihood",
        "converse_neighbor"
        | "lend_coin"
        | "court_romance"
        | "share_meal"
        | "form_mutual_aid_group"
        | "mediate_dispute"
        | "defend_patron"
        | "train_apprentice" => "social",
        "investigate_rumor"
        | "share_rumor"
        | "question_travelers"
        | "spread_accusation"
        | "observe_notable_event" => "information",
        "patrol_road" | "organize_watch" | "collect_testimony" | "avoid_patrols" => "security",
        "steal_supplies" | "fence_goods" => "illicit",
        "repair_hearth" | "gather_firewood" | "ration_grain" | "seek_treatment" => "survival",
        _ => "general",
    }
}

fn build_commitment_state(tick: u64, npc_id: &str, action_family: &str) -> CommitmentRuntimeState {
    let cadence_ticks = match action_family {
        "rent" => 24 * 3,
        "shelter" => 12,
        "livelihood" => 8,
        "social" => 6,
        "information" => 6,
        "security" => 8,
        "survival" => 8,
        "illicit" => 6,
        _ => 6,
    };
    let inertia_score = match action_family {
        "rent" => 48,
        "shelter" => 26,
        "livelihood" => 20,
        "social" => 12,
        "information" => 10,
        "security" => 16,
        "survival" => 18,
        "illicit" => 8,
        _ => 10,
    };
    CommitmentRuntimeState {
        commitment_id: format!("commit_{:06}_{npc_id}_{}", tick, action_family),
        npc_id: npc_id.to_string(),
        action_family: action_family.to_string(),
        started_tick: tick,
        due_tick: tick + cadence_ticks,
        cadence_ticks,
        progress_ticks: 1,
        inertia_score,
        status: "active".to_string(),
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
    health: i64,
    illness_ticks: i64,
    dependents_count: i64,
    shelter_status: ShelterStatus,
    contract_reliability: i64,
    eviction_risk: i64,
    rent_due_in_ticks: i64,
    rent_shortfall: i64,
    rent_cadence_ticks: i64,
    trust_support: i64,
    local_theft_pressure: i64,
    local_conflict_pressure: i64,
    local_support_need: i64,
    low_pressure_economic_opportunity: i64,
    profile: &NpcTraitProfile,
    stream: F,
) -> Vec<ActionCandidate>
where
    F: Fn(u64, &str, &str) -> u64,
{
    let security_need =
        law_case_load > 0 || local_theft_pressure >= 2 || local_conflict_pressure >= 2 || is_wanted;
    let social_safe = local_conflict_pressure <= 2 && law_case_load <= 3;
    let day_phase = tick % 24;
    let work_window = (6..=16).contains(&day_phase);
    let social_window = (17..=22).contains(&day_phase);
    let quiet_window = day_phase <= 5;
    let mut base = vec![
        candidate(AffordanceVerb::WorkForFood, 2, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::WorkForCoin, 3, -2, tick, npc_id, &stream),
        candidate(AffordanceVerb::SeekShelter, 0, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::TendFields, 2, -1, tick, npc_id, &stream),
        candidate(AffordanceVerb::CraftGoods, 2, 0, tick, npc_id, &stream),
        candidate(AffordanceVerb::ShareMeal, 1, -1, tick, npc_id, &stream),
        candidate(
            AffordanceVerb::FormMutualAidGroup,
            1,
            -1,
            tick,
            npc_id,
            &stream,
        ),
        candidate(AffordanceVerb::DefendPatron, 1, -1, tick, npc_id, &stream),
        candidate(
            AffordanceVerb::SpreadAccusation,
            1,
            1,
            tick,
            npc_id,
            &stream,
        ),
        candidate(AffordanceVerb::MediateDispute, 1, -1, tick, npc_id, &stream),
        candidate(
            AffordanceVerb::TrainApprentice,
            1,
            -1,
            tick,
            npc_id,
            &stream,
        ),
        candidate(AffordanceVerb::ShareRumor, 2, 1, tick, npc_id, &stream),
        candidate(AffordanceVerb::PatrolRoad, 2, -1, tick, npc_id, &stream),
        candidate(
            AffordanceVerb::ConverseNeighbor,
            2,
            -1,
            tick,
            npc_id,
            &stream,
        ),
        candidate(
            AffordanceVerb::ObserveNotableEvent,
            1,
            0,
            tick,
            npc_id,
            &stream,
        ),
        candidate(AffordanceVerb::Forage, 1, 0, tick, npc_id, &stream),
    ];

    if social_window && social_safe && food_reserve_days >= 2 && !is_wanted {
        base.push(candidate(
            AffordanceVerb::ConverseNeighbor,
            4,
            -1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::ObserveNotableEvent,
            3,
            0,
            tick,
            npc_id,
            &stream,
        ));
        if trust_support > 0 {
            base.push(candidate(
                AffordanceVerb::ShareMeal,
                3,
                -1,
                tick,
                npc_id,
                &stream,
            ));
        }
    }

    if work_window && !security_need && !quiet_window {
        base.push(candidate(
            AffordanceVerb::TradeVisit,
            3,
            0,
            tick,
            npc_id,
            &stream,
        ));
    }

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

    let constrained_theft_window = !is_wanted
        && !has_stolen_item
        && law_case_load <= 4
        && wallet <= 3
        && profile.risk_tolerance >= 50
        && trust_support <= 2
        && local_theft_pressure <= 2
        && ((food_reserve_days <= 2)
            || (rent_shortfall > 0 && rent_due_in_ticks <= 24 && eviction_risk >= 30))
        && ((rent_shortfall > 0 && rent_due_in_ticks <= 48)
            || debt_balance >= 4
            || eviction_risk >= 70
            || harvest_shock >= 2
            || winter_severity >= 70);
    if constrained_theft_window {
        let theft_roll = stream(tick, npc_id, "edge_theft_window") % 100;
        if theft_roll < 25 {
            base.push(candidate(
                AffordanceVerb::StealSupplies,
                3,
                1,
                tick,
                npc_id,
                &stream,
            ));
        }
    }

    let opportunistic_theft_window = !is_wanted
        && !has_stolen_item
        && law_case_load <= 3
        && wallet <= 0
        && profile.risk_tolerance >= 60
        && rent_shortfall > 0
        && rent_due_in_ticks <= 72;
    if opportunistic_theft_window {
        let theft_roll = stream(tick, npc_id, "opportunistic_theft_window") % 100;
        if theft_roll < 8 {
            base.push(candidate(
                AffordanceVerb::StealSupplies,
                4,
                1,
                tick,
                npc_id,
                &stream,
            ));
        }
    }

    let volitional_theft_window = !is_wanted
        && !has_stolen_item
        && law_case_load <= 3
        && profile.risk_tolerance >= 74
        && profile.ambition >= 64
        && profile.empathy <= 42
        && top_intents
            .iter()
            .any(|intent| intent == "seek_opportunity")
        && trust_support <= 1
        && wallet <= 5;
    if volitional_theft_window {
        let theft_roll = stream(tick, npc_id, "volitional_theft_window") % 100;
        if theft_roll < 12 {
            base.push(candidate(
                AffordanceVerb::StealSupplies,
                5,
                1,
                tick,
                npc_id,
                &stream,
            ));
        }
    }

    let scarcity_theft_window = !is_wanted
        && !has_stolen_item
        && law_case_load <= 4
        && local_theft_pressure <= 2
        && trust_support <= 2
        && profile.risk_tolerance >= 52
        && wallet <= 2
        && (harvest_shock >= 2 || winter_severity >= 70);
    if scarcity_theft_window {
        let theft_roll = stream(tick, npc_id, "scarcity_theft_window") % 100;
        if theft_roll < 35 {
            base.push(candidate(
                AffordanceVerb::StealSupplies,
                5,
                1,
                tick,
                npc_id,
                &stream,
            ));
        }
    }

    let stress_desperation_theft = !is_wanted
        && !has_stolen_item
        && law_case_load <= 4
        && (harvest_shock >= 2 || winter_severity >= 70)
        && (wallet <= 2 || debt_balance >= 3 || rent_shortfall > 0)
        && trust_support <= 2;
    if stress_desperation_theft {
        base.push(candidate(
            AffordanceVerb::StealSupplies,
            6,
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
        .any(|intent| intent == "seek_companionship")
        && social_safe
        && food_reserve_days >= 2
        && !matches!(shelter_status, ShelterStatus::Unsheltered)
    {
        let romance_phase = stream(0, npc_id, "romance_phase") % 6;
        base.push(candidate(
            AffordanceVerb::CourtRomance,
            (if social_window && tick % 6 == romance_phase {
                3
            } else {
                1
            }) + ((profile.romance_drive / 20).clamp(0, 2) as i32),
            0,
            tick,
            npc_id,
            &stream,
        ));
    }

    if top_intents
        .iter()
        .any(|intent| intent == "exchange_stories")
        || rumor_heat >= 2
        || local_conflict_pressure >= 2
    {
        base.push(candidate(
            AffordanceVerb::ObserveNotableEvent,
            2,
            1,
            tick,
            npc_id,
            &stream,
        ));
        base.push(candidate(
            AffordanceVerb::ConverseNeighbor,
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

    if top_intents.iter().any(|intent| intent == "liquidate_goods") || has_stolen_item {
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

    if health <= 65
        || illness_ticks > 0
        || (winter_severity >= 70 && matches!(shelter_status, ShelterStatus::Precarious))
    {
        base.push(candidate(
            AffordanceVerb::SeekTreatment,
            4,
            -2,
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
        if low_pressure_economic_opportunity >= 6
            && (wallet <= 1 || debt_balance >= 2 || rent_shortfall > 0)
        {
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

    if top_intents.iter().any(|intent| intent == "seek_income") || wallet <= 1 || debt_balance >= 3
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

    if rent_shortfall > 0 && wallet >= 1 && rent_due_in_ticks <= 24 {
        let priority = if rent_due_in_ticks <= 6 { 6 } else { 5 };
        base.push(candidate(
            AffordanceVerb::PayRent,
            priority,
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

    if local_support_need > 0 && trust_support > 0 && food_reserve_days > 1 && debt_balance < 4 {
        base.push(candidate(
            AffordanceVerb::ConverseNeighbor,
            2 + (local_support_need.min(2) as i32),
            -1,
            tick,
            npc_id,
            &stream,
        ));
        if wallet >= 3 && profile.generosity >= 55 {
            base.push(candidate(
                AffordanceVerb::LendCoin,
                2 + (local_support_need.min(2) as i32),
                -1,
                tick,
                npc_id,
                &stream,
            ));
        }
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

    apply_contextual_action_variants(
        &mut base,
        tick,
        npc_id,
        top_intents,
        pressure_index,
        harvest_shock,
        winter_severity,
        profile,
        &stream,
    );

    base
}

fn apply_personality_bias(
    candidates: &mut [ActionCandidate],
    profile: &NpcTraitProfile,
    shelter_status: ShelterStatus,
    is_wanted: bool,
    security_need: bool,
    _local_support_need: i64,
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
            | AffordanceVerb::FormMutualAidGroup
            | AffordanceVerb::ConverseNeighbor => {
                if profile.empathy >= 60 || profile.sociability >= 60 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(260);
                }
            }
            AffordanceVerb::LendCoin => {
                if profile.generosity >= 58 && profile.empathy >= 50 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(220);
                } else {
                    candidate.score = candidate.score.saturating_sub(900);
                }
                if debt_balance >= 3 || food_reserve_days <= 1 {
                    candidate.score = candidate.score.saturating_sub(1_000);
                }
            }
            AffordanceVerb::CourtRomance => {
                if profile.romance_drive >= 58 && profile.sociability >= 45 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(180);
                } else {
                    candidate.score = candidate.score.saturating_sub(700);
                }
                if profile.aggression >= 70 && profile.patience <= 40 {
                    candidate.score = candidate.score.saturating_sub(450);
                }
            }
            AffordanceVerb::ObserveNotableEvent => {
                if profile.gossip_drive >= 55 || profile.ambition >= 55 {
                    candidate.score = candidate.score.saturating_add(180);
                }
            }
            AffordanceVerb::SeekTreatment => {
                if profile.patience >= 45 || profile.resilience <= 45 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(220);
                }
            }
            AffordanceVerb::AssistNeighbor => {
                candidate.score = candidate.score.saturating_sub(4_000);
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
                if profile.risk_tolerance <= 35 || profile.empathy >= 78 {
                    candidate.priority -= 2;
                    candidate.score = candidate.score.saturating_sub(1400);
                } else if profile.risk_tolerance >= 62 && food_reserve_days <= 2 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(240);
                }
                if profile.risk_tolerance >= 74 && profile.ambition >= 64 && profile.empathy <= 42 {
                    candidate.priority += 1;
                    candidate.score = candidate.score.saturating_add(900);
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

fn apply_contextual_action_variants<F>(
    candidates: &mut [ActionCandidate],
    tick: u64,
    npc_id: &str,
    top_intents: &[String],
    pressure_index: i64,
    harvest_shock: i64,
    winter_severity: u8,
    profile: &NpcTraitProfile,
    stream: &F,
) where
    F: Fn(u64, &str, &str) -> u64,
{
    const WORK_VARIANTS: &[&str] = &[
        "shift_dawn_labor",
        "shift_market_porter",
        "shift_stable_hand",
        "shift_tavern_help",
        "shift_quarry_load",
    ];
    const CRAFT_VARIANTS: &[&str] = &[
        "forge_nails",
        "mend_tools",
        "carve_woodware",
        "repair_harness",
        "fletch_arrows",
    ];
    const FIELD_VARIANTS: &[&str] = &[
        "plow_furrow",
        "water_rows",
        "weed_plot",
        "fertilize_soil",
        "harvest_patch",
        "store_harvest",
    ];
    const TALK_VARIANTS: &[&str] = &[
        "chat_weather",
        "debate_taxes",
        "trade_gossip",
        "discuss_prices",
        "share_story",
        "joke_exchange",
    ];
    const TRADE_VARIANTS: &[&str] = &[
        "haggle_staples",
        "sell_surplus",
        "buy_medicine",
        "compare_prices",
        "book_caravan",
    ];
    const WATCH_VARIANTS: &[&str] = &[
        "inspect_gate",
        "check_bridge",
        "escort_traveler",
        "scan_alley",
        "inspect_wagon",
    ];
    const ROMANCE_VARIANTS: &[&str] = &[
        "exchange_glance",
        "offer_flower",
        "walk_together",
        "sing_verse",
        "request_courtship",
        "offer_token",
    ];
    const THEFT_VARIANTS: &[&str] = &[
        "lift_purse",
        "snatch_loaf",
        "break_storage",
        "pick_pocket",
    ];
    const OBSERVE_VARIANTS: &[&str] = &[
        "watch_street",
        "notice_stranger",
        "observe_horseman",
        "spot_argument",
        "watch_market_crowd",
        "observe_civic_round",
    ];
    const SHELTER_VARIANTS: &[&str] = &[
        "ask_patron_bed",
        "offer_labor_for_board",
        "seek_common_hall",
        "negotiate_cot",
    ];

    let has_social_intent = top_intents
        .iter()
        .any(|intent| matches!(intent.as_str(), "build_trust" | "seek_companionship" | "exchange_stories"));
    let scarcity_pressure = pressure_index >= 120 || harvest_shock >= 2 || winter_severity >= 60;
    let day_phase = tick % 24;

    for (idx, candidate) in candidates.iter_mut().enumerate() {
        let variants: &[&str] = match candidate.verb {
            AffordanceVerb::WorkForCoin => WORK_VARIANTS,
            AffordanceVerb::CraftGoods => CRAFT_VARIANTS,
            AffordanceVerb::TendFields => FIELD_VARIANTS,
            AffordanceVerb::ConverseNeighbor => TALK_VARIANTS,
            AffordanceVerb::TradeVisit => TRADE_VARIANTS,
            AffordanceVerb::PatrolRoad | AffordanceVerb::OrganizeWatch => WATCH_VARIANTS,
            AffordanceVerb::CourtRomance => ROMANCE_VARIANTS,
            AffordanceVerb::StealSupplies => THEFT_VARIANTS,
            AffordanceVerb::ObserveNotableEvent => OBSERVE_VARIANTS,
            AffordanceVerb::SeekShelter => SHELTER_VARIANTS,
            _ => &[],
        };

        if variants.is_empty() {
            continue;
        }

        let social_bias = (profile.sociability + profile.empathy + profile.gossip_drive) / 3;
        let channel = format!(
            "variant:{}:{}:{}:{}:{}",
            candidate.verb.as_str(),
            idx,
            day_phase,
            scarcity_pressure as i32,
            has_social_intent as i32
        );
        let mut pick = (stream(tick, npc_id, &channel) as usize) % variants.len();
        if matches!(candidate.verb, AffordanceVerb::CourtRomance) && !has_social_intent {
            pick = pick.min(2);
        }
        if matches!(candidate.verb, AffordanceVerb::StealSupplies) && !scarcity_pressure {
            pick = pick.min(1);
        }
        if matches!(candidate.verb, AffordanceVerb::ConverseNeighbor) && social_bias < 45 {
            pick = pick.saturating_sub(1);
        }
        let base = candidate.verb.as_str();
        candidate.action = format!("{base}::{}", variants[pick]);
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
        score: 1_200 + (stream(tick, npc_id, action) % 256),
        pressure_effect,
    }
}

fn decision_has_emergency_pressure(decision: &NpcDecision) -> bool {
    decision.top_intents.iter().any(|intent| {
        matches!(
            intent.as_str(),
            "secure_food" | "seek_shelter" | "evade_patrols"
        )
    }) || decision.top_pressures.iter().any(|pressure| {
        matches!(
            pressure.as_str(),
            "household_hunger" | "shelter_instability"
        )
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

    if top_intents
        .iter()
        .any(|intent| intent == "seek_opportunity")
    {
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
        | AffordanceVerb::ConverseNeighbor
        | AffordanceVerb::LendCoin
        | AffordanceVerb::MediateDispute
        | AffordanceVerb::DefendPatron => {
            push_unique_motive(&mut motives, MotiveFamily::Belonging);
            push_unique_motive(&mut motives, MotiveFamily::Attachment);
        }
        AffordanceVerb::CourtRomance => {
            push_unique_motive(&mut motives, MotiveFamily::Attachment);
            push_unique_motive(&mut motives, MotiveFamily::Ambition);
        }
        AffordanceVerb::SeekTreatment => {
            push_unique_motive(&mut motives, MotiveFamily::Survival);
            push_unique_motive(&mut motives, MotiveFamily::Security);
        }
        AffordanceVerb::ObserveNotableEvent => {
            push_unique_motive(&mut motives, MotiveFamily::CuriosityMeaning);
        }
        AffordanceVerb::SpreadAccusation => {
            push_unique_motive(&mut motives, MotiveFamily::JusticeGrievance);
            push_unique_motive(&mut motives, MotiveFamily::Ideology);
        }
        AffordanceVerb::TrainApprentice => {
            push_unique_motive(&mut motives, MotiveFamily::CuriosityMeaning);
            push_unique_motive(&mut motives, MotiveFamily::Dignity);
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
    debt_balance: i64,
    eviction_risk: i64,
    local_theft_pressure: i64,
    local_support_need: i64,
    theft_desperation: bool,
    profile: &NpcTraitProfile,
) {
    let Some(memory) = action_memory else {
        return;
    };

    for candidate in candidates {
        let action = canonical_action_name(candidate.action.as_str());
        if memory.last_action.as_deref() == Some(action) {
            let penalty = 1_200_u64 + u64::from(memory.repeat_streak) * 350;
            candidate.score = candidate.score.saturating_sub(penalty);
            if memory.repeat_streak >= 3 {
                candidate.score = candidate.score.saturating_sub(1_100);
            }
        } else {
            let novelty_bonus = 180_u64 + u64::from(memory.repeat_streak.min(4)) * 90;
            candidate.score = candidate.score.saturating_add(novelty_bonus);
        }

        if action == "organize_watch" && law_case_load == 0 && local_theft_pressure <= 1 {
            candidate.score = candidate.score.saturating_sub(1_800);
            if memory.repeat_streak >= 1 {
                candidate.score = candidate.score.saturating_sub(700);
            }
        }

        if action == "avoid_patrols" && !is_wanted {
            candidate.score = candidate.score.saturating_sub(900);
        }

        if action == "steal_supplies" {
            if !theft_desperation && food_reserve_days >= 2 && eviction_risk < 75 {
                candidate.score = candidate.score.saturating_sub(2_300);
            }
            if theft_desperation && profile.risk_tolerance >= 55 && law_case_load <= 2 {
                candidate.score = candidate.score.saturating_add(1_100);
            }
            if eviction_risk >= 80
                && local_support_need > 0
                && profile.risk_tolerance >= 60
                && law_case_load <= 2
            {
                candidate.score = candidate.score.saturating_add(550);
            }
            if let Some(last_theft_tick) = memory.last_theft_tick {
                if tick.saturating_sub(last_theft_tick) < 24 {
                    candidate.score = candidate.score.saturating_sub(3_000);
                } else if tick.saturating_sub(last_theft_tick) < 48 {
                    candidate.score = candidate.score.saturating_sub(1_200);
                }
            }
        }

        if action == "pay_rent" {
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

        if action == "converse_neighbor" || action == "lend_coin" {
            if local_support_need <= 0 {
                candidate.score = candidate.score.saturating_sub(1_600);
            }
            if memory.last_action.as_deref() == Some(action)
                && tick.saturating_sub(memory.last_action_tick) < 18
            {
                candidate.score = candidate.score.saturating_sub(1_900);
            }
        }

        if action == "court_romance" {
            if food_reserve_days <= 1 || debt_balance >= 4 {
                candidate.score = candidate.score.saturating_sub(1_300);
            }
            if memory.last_action.as_deref() == Some("court_romance")
                && tick.saturating_sub(memory.last_action_tick) < 24
            {
                candidate.score = candidate.score.saturating_sub(1_700);
            }
        }

        if action == "observe_notable_event"
            && memory.last_action.as_deref() == Some("observe_notable_event")
            && tick.saturating_sub(memory.last_action_tick) < 4
        {
            candidate.score = candidate.score.saturating_sub(900);
        }

        if action == "seek_treatment" && food_reserve_days >= 3 && debt_balance <= 1 {
            candidate.score = candidate.score.saturating_sub(450);
        }

        if action == "seek_shelter" {
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
    match canonical_action_name(chosen_action) {
        "converse_neighbor" => vec!["work_for_coin".to_string(), "trade_visit".to_string()],
        "lend_coin" => vec!["converse_neighbor".to_string(), "share_meal".to_string()],
        "court_romance" => vec!["converse_neighbor".to_string(), "share_meal".to_string()],
        "work_for_coin" => vec!["craft_goods".to_string(), "tend_fields".to_string()],
        "pay_rent" => vec!["work_for_coin".to_string(), "work_for_food".to_string()],
        "seek_shelter" => vec!["work_for_food".to_string(), "trade_visit".to_string()],
        "spread_accusation" => vec!["mediate_dispute".to_string(), "share_rumor".to_string()],
        _ => vec![
            "evaluate_other_options".to_string(),
            "delay_action".to_string(),
        ],
    }
}

fn social_topic_for_action(action: &str) -> &'static str {
    match canonical_action_name(action) {
        "work_for_coin" => "took_paid_shift",
        "work_for_food" => "worked_for_board",
        "tend_fields" => "fieldwork_progress",
        "craft_goods" => "workshop_output",
        "trade_visit" => "market_visit",
        "converse_neighbor" => "street_conversation",
        "lend_coin" => "small_loan",
        "court_romance" => "courtship_gesture",
        "share_rumor" => "rumor_exchange",
        "investigate_rumor" => "rumor_check",
        "observe_notable_event" => "public_noticing",
        "seek_treatment" => "healer_visit",
        "steal_supplies" => "petty_theft",
        "pay_rent" => "rent_settlement",
        "form_mutual_aid_group" => "group_coordination",
        "mediate_dispute" => "dispute_mediation",
        "patrol_road" | "organize_watch" => "watch_activity",
        _ => "daily_activity",
    }
}

fn observation_topic_for_event(
    event_type: EventType,
    source_tags: &[String],
    details: Option<&Value>,
    chosen_action: Option<&str>,
) -> String {
    if let Some(topic) = details
        .and_then(|entry| entry.get("topic"))
        .and_then(Value::as_str)
    {
        return topic.to_string();
    }
    if source_tags.iter().any(|tag| tag == "festival") {
        return "seasonal_festival".to_string();
    }
    if source_tags.iter().any(|tag| tag == "mayor_tour") {
        return "civic_visit".to_string();
    }

    match event_type {
        EventType::TheftCommitted => "witnessed_theft".to_string(),
        EventType::ArrestMade => "watched_arrest".to_string(),
        EventType::BrawlStarted => "saw_brawl".to_string(),
        EventType::PunchThrown => "saw_punch".to_string(),
        EventType::InsultExchanged => "heard_insults".to_string(),
        EventType::GuardsDispatched => "guards_called".to_string(),
        EventType::StockShortage | EventType::MarketFailed => "market_stress".to_string(),
        EventType::StockRecovered | EventType::MarketCleared => "market_recovery".to_string(),
        EventType::RomanceAdvanced => "public_courtship".to_string(),
        EventType::ConversationHeld => "street_conversation".to_string(),
        EventType::NpcActionCommitted => chosen_action
            .map(social_topic_for_action)
            .unwrap_or("daily_activity")
            .to_string(),
        _ => "street_notice".to_string(),
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

fn canonical_pair_key(left_npc_id: &str, right_npc_id: &str) -> (String, String) {
    if left_npc_id <= right_npc_id {
        (left_npc_id.to_string(), right_npc_id.to_string())
    } else {
        (right_npc_id.to_string(), left_npc_id.to_string())
    }
}

fn pick_weighted_index(roll: u64, weights: &[i64]) -> Option<usize> {
    if weights.is_empty() {
        return None;
    }
    let total = weights
        .iter()
        .map(|weight| (*weight).max(0) as u64)
        .sum::<u64>();
    if total == 0 {
        return Some((roll % (weights.len() as u64)) as usize);
    }
    let mut cursor = roll % total;
    for (idx, weight) in weights.iter().enumerate() {
        let clamped = (*weight).max(0) as u64;
        if clamped == 0 {
            continue;
        }
        if cursor < clamped {
            return Some(idx);
        }
        cursor -= clamped;
    }
    Some(weights.len() - 1)
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

    fn test_config() -> RunConfig {
        let mut config = RunConfig::default();
        config.npc_count_min = 3;
        config.npc_count_max = 3;
        config
    }

    #[test]
    fn stepping_advances_tick_and_emits_npc_actions() {
        let mut kernel = Kernel::new(test_config());
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
        let mut config = test_config();
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
        let config = test_config();
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
        let mut kernel = Kernel::new(test_config());
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
    fn planner_reason_packets_include_goal_plan_and_operator_linkage() {
        let mut kernel = Kernel::new(test_config());
        kernel.run_to_tick(12);

        let action_packets = kernel
            .reason_packets()
            .iter()
            .filter(|packet| packet.chosen_verb.is_some())
            .collect::<Vec<_>>();
        assert!(
            !action_packets.is_empty(),
            "expected at least one action-linked reason packet"
        );
        assert!(
            action_packets.iter().all(|packet| packet
                .goal_id
                .as_ref()
                .map(|value| !value.is_empty())
                .unwrap_or(false)),
            "all action-linked reason packets must include goal_id"
        );
        assert!(
            action_packets.iter().all(|packet| packet
                .plan_id
                .as_ref()
                .map(|value| !value.is_empty())
                .unwrap_or(false)),
            "all action-linked reason packets must include plan_id"
        );
        assert!(
            action_packets
                .iter()
                .all(|packet| !packet.operator_chain_ids.is_empty()),
            "all action-linked reason packets must include operator_chain_ids"
        );

        let action_events = kernel
            .events()
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .collect::<Vec<_>>();
        assert!(!action_events.is_empty(), "expected npc action commits");
        assert!(action_events.iter().all(|event| {
            let details = event.details.as_ref();
            let has_goal = details
                .and_then(|value| value.get("goal_id"))
                .and_then(Value::as_str)
                .map(|value| !value.is_empty())
                .unwrap_or(false);
            let has_plan = details
                .and_then(|value| value.get("plan_id"))
                .and_then(Value::as_str)
                .map(|value| !value.is_empty())
                .unwrap_or(false);
            let has_operator_chain = details
                .and_then(|value| value.get("operator_chain_ids"))
                .and_then(Value::as_array)
                .map(|entries| !entries.is_empty())
                .unwrap_or(false);
            has_goal && has_plan && has_operator_chain
        }));
    }

    #[test]
    fn action_commits_are_not_forced_per_npc_per_tick() {
        let mut kernel = Kernel::new(test_config());
        let horizon_ticks = 48_u64;
        kernel.run_to_tick(horizon_ticks);

        let action_events = kernel
            .events()
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .collect::<Vec<_>>();

        let max_forced = horizon_ticks as usize * 3;
        assert!(
            action_events.len() < max_forced,
            "expected fewer commits than forced-per-npc-per-tick maximum: commits={} max_forced={}",
            action_events.len(),
            max_forced
        );

        let mut commits_by_tick = std::collections::BTreeMap::<u64, usize>::new();
        for event in &action_events {
            *commits_by_tick.entry(event.tick).or_insert(0) += 1;
        }
        let ticks_without_commits = (1..=horizon_ticks)
            .filter(|tick| !commits_by_tick.contains_key(tick))
            .count();
        assert!(
            ticks_without_commits > 0,
            "expected at least one tick with no new action commit"
        );
    }

    #[test]
    fn scenario_injectors_create_persistent_pressure_signals() {
        let config = test_config();
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
        let base_config = test_config();

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
        let config = test_config();
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
        let mut kernel = Kernel::new(test_config());
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
        let mut kernel = Kernel::new(test_config());
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
        let mut kernel = Kernel::new(test_config());
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
