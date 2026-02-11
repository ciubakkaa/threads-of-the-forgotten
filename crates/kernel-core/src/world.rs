//! AgentWorld engine: top-level simulation engine replacing the monolithic Kernel.
//!
//! Composes all subsystems (spatial, economy, social, institutions, agents,
//! scheduler, conflict resolver, observation bus) into a single `AgentWorld`
//! that drives the simulation via discrete-event scheduling.

use std::collections::BTreeMap;

use contracts::agency::{
    ActivePlan, AgentAction, AgentDrivesSnapshot, AgentOccupancySnapshot, AgentSnapshot,
    Aspiration, CandidatePlan, DriveKind, EconomyLedgerSnapshot, Goal, Observation,
    PlanningMode, ReasonEnvelope, RejectedPlan, SchedulerSnapshot, WakeReason,
    WorldMutation, WorldStateSnapshot,
};
use contracts::{
    Command, CommandPayload, Event, EventType, RunConfig, RunMode, RunStatus, Snapshot,
    SCHEMA_VERSION_V1,
};
use serde_json::{json, Value};

use crate::agent::{NpcAgent, TickResult};
use crate::economy::{EconomyLedger, MarketState};
use crate::institution::InstitutionState;
use crate::operator::{default_catalog, OperatorCatalog, WorldView};
use crate::perception::ObservationBus;
use crate::planner::{AgentRng, OccupancyState, PlannerConfig};
use crate::scheduler::{AgentScheduler, ConflictResolver};
use crate::social::{GroupState, RumorNetwork, SocialGraph};
use crate::spatial::{Location, SpatialModel, WeatherState};

// ---------------------------------------------------------------------------
// 17.1 — WorldState: aggregates all subsystems
// ---------------------------------------------------------------------------

/// Shared mutable world state aggregating all simulation subsystems.
///
/// Replaces the scattered BTreeMaps in the old `Kernel` with a structured
/// composition of spatial, economic, social, institutional, and environmental
/// subsystems.
#[derive(Debug, Clone)]
pub struct WorldState {
    // Spatial
    pub spatial: SpatialModel,

    // Economic
    pub economy: EconomyLedger,
    pub items: BTreeMap<String, ItemRecord>,
    pub production_nodes: BTreeMap<String, ProductionNodeRecord>,
    pub markets: BTreeMap<String, MarketState>,

    // Social
    pub social_graph: SocialGraph,
    pub rumor_network: RumorNetwork,
    pub groups: BTreeMap<String, GroupState>,

    // Institutional
    pub institutions: BTreeMap<String, InstitutionState>,

    // Environmental
    pub weather: WeatherState,

    // Scenario
    pub scenario: ScenarioState,
}

/// A tracked item in the world.
#[derive(Debug, Clone)]
pub struct ItemRecord {
    pub item_id: String,
    pub owner_id: String,
    pub location_id: String,
    pub stolen: bool,
    pub last_moved_tick: u64,
}

/// A production node record.
#[derive(Debug, Clone)]
pub struct ProductionNodeRecord {
    pub node_id: String,
    pub settlement_id: String,
    pub node_kind: String,
    pub input_backlog: i64,
    pub output_backlog: i64,
    pub spoilage_timer: i64,
}

/// Scenario injection state.
#[derive(Debug, Clone, Default)]
pub struct ScenarioState {
    pub rumor_heat_by_location: BTreeMap<String, i64>,
    pub caravan_flow_by_settlement: BTreeMap<String, i64>,
    pub harvest_shock_by_settlement: BTreeMap<String, i64>,
    pub winter_severity: u8,
}

impl WorldState {
    /// Create a new empty world state.
    pub fn new() -> Self {
        Self {
            spatial: SpatialModel::new(),
            economy: EconomyLedger::default(),
            items: BTreeMap::new(),
            production_nodes: BTreeMap::new(),
            markets: BTreeMap::new(),
            social_graph: SocialGraph::new(),
            rumor_network: RumorNetwork::default(),
            groups: BTreeMap::new(),
            institutions: BTreeMap::new(),
            weather: WeatherState::default(),
            scenario: ScenarioState::default(),
        }
    }

    /// Build a `WorldView` for the planner/operator catalog from current state.
    pub fn to_world_view(&self, agents: &BTreeMap<String, NpcAgent>) -> WorldView {
        let mut npcs_by_location: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for agent in agents.values() {
            npcs_by_location
                .entry(agent.location_id.clone())
                .or_default()
                .push(agent.id.clone());
        }

        let location_ids: Vec<String> = self.spatial.locations().keys().cloned().collect();

        let mut objects_by_location: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, item) in &self.items {
            objects_by_location
                .entry(item.location_id.clone())
                .or_default()
                .push(id.clone());
        }

        let mut institutions_by_location: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (id, inst) in &self.institutions {
            institutions_by_location
                .entry(inst.settlement_id.clone())
                .or_default()
                .push(id.clone());
        }

        let resources_by_location: BTreeMap<String, Vec<String>> = BTreeMap::new();

        // Build facts from economy accounts.
        let mut facts: BTreeMap<String, i64> = BTreeMap::new();
        for (account_id, balance) in self.economy.accounts() {
            facts.insert(format!("money:{}", account_id), balance.money);
            facts.insert(format!("food:{}", account_id), balance.food);
        }

        WorldView {
            npcs_by_location,
            location_ids,
            objects_by_location,
            institutions_by_location,
            resources_by_location,
            facts,
        }
    }
}

impl Default for WorldState {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Inspection types (17.3)
// ---------------------------------------------------------------------------

/// Inspection data for a single NPC.
#[derive(Debug, Clone)]
pub struct NpcInspection {
    pub agent_id: String,
    pub location_id: String,
    pub drives: Vec<(DriveKind, i64)>,
    pub active_plan: Option<ActivePlan>,
    pub occupancy: OccupancyState,
    pub relationships: Vec<(String, contracts::agency::RelationshipEdge)>,
    pub beliefs: Vec<contracts::agency::BeliefClaim>,
    pub recent_reason_envelope: Option<ReasonEnvelope>,
}

/// Inspection data for a settlement.
#[derive(Debug, Clone)]
pub struct SettlementInspection {
    pub settlement_id: String,
    pub economy_snapshot: EconomyLedgerSnapshot,
    pub institution_queues: Vec<(String, usize)>,
    pub market_state: Option<MarketState>,
    pub local_npc_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// 17.2 — AgentWorld: top-level engine replacing Kernel
// ---------------------------------------------------------------------------

/// Top-level simulation engine. Owns the world state, scheduler, all agent
/// instances, and drives the simulation via discrete-event scheduling.
#[derive(Debug)]
pub struct AgentWorld {
    pub config: RunConfig,
    pub status: RunStatus,
    seed: u64,
    scheduler: AgentScheduler,
    world: WorldState,
    agents: BTreeMap<String, NpcAgent>,
    observation_bus: ObservationBus,
    operator_catalog: OperatorCatalog,
    conflict_resolver: ConflictResolver,
    event_log: Vec<Event>,
    reason_envelopes: Vec<ReasonEnvelope>,
    next_event_sequence: u64,
}

impl AgentWorld {
    /// Initialize all subsystems, create agents, seed scheduler.
    pub fn new(config: RunConfig) -> Self {
        let seed = config.seed;
        let max_ticks = config.max_ticks();

        let status = RunStatus {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: config.run_id.clone(),
            current_tick: 0,
            max_ticks,
            mode: RunMode::Paused,
            queue_depth: 0,
        };

        let mut world = WorldState::new();
        world.rumor_network = RumorNetwork::new(config.agency.rumor_distortion_probability);

        // Create a default location for agents.
        world.spatial.add_location(Location {
            location_id: "crownvale".into(),
            name: "Crownvale".into(),
            tags: vec!["settlement".into()],
        });

        // Create agents with economy accounts.
        let mut agents = BTreeMap::new();
        let (npc_min, npc_max) = config.normalized_npc_bounds();
        let npc_count = {
            let mut rng = AgentRng::new(seed);
            let range = (npc_max - npc_min) as i64;
            if range <= 0 {
                npc_min as usize
            } else {
                (npc_min as i64 + rng.next_u64() as i64 % (range + 1)) as usize
            }
        };

        for i in 0..npc_count {
            let npc_id = format!("npc_{}", i);
            let agent = NpcAgent::new(
                npc_id.clone(),
                default_identity(seed, i),
                Aspiration {
                    current: "stability".into(),
                    previous: None,
                    changed_at_tick: None,
                    change_cause: None,
                },
                "crownvale".into(),
            );
            agents.insert(npc_id.clone(), agent);

            // Give each agent an economy account.
            world.economy.source(
                &npc_id,
                contracts::agency::ResourceKind::Money,
                100,
                "init",
                0,
            );
            world.economy.source(
                &npc_id,
                contracts::agency::ResourceKind::Food,
                50,
                "init",
                0,
            );
        }

        // Seed the scheduler with initial wake events for all agents.
        let mut scheduler = AgentScheduler::new(seed, max_ticks);
        for agent_id in agents.keys() {
            scheduler.schedule_wake(agent_id.clone(), 1, WakeReason::Idle);
        }

        Self {
            config,
            status,
            seed,
            scheduler,
            world,
            agents,
            observation_bus: ObservationBus::new(),
            operator_catalog: default_catalog(),
            conflict_resolver: ConflictResolver::new(),
            event_log: Vec::new(),
            reason_envelopes: Vec::new(),
            next_event_sequence: 0,
        }
    }

    /// Advance one scheduling window: pop event → wake agent → agent tick →
    /// conflict resolution → commit/reject → broadcast observations →
    /// schedule next wake.
    ///
    /// Returns `true` if the simulation can continue, `false` if complete.
    pub fn step(&mut self) -> bool {
        if self.status.mode != RunMode::Running {
            return false;
        }

        // Clear expired occupancies so the scheduler stops skipping agents
        // whose multi-tick operators have finished.
        self.clear_expired_occupancies();

        let event = match self.scheduler.pop_next() {
            Some(e) => e,
            None => {
                self.status.mode = RunMode::Paused;
                return false;
            }
        };

        let tick = event.wake_tick;
        self.status.current_tick = tick;

        // Emit AgentWake event.
        self.emit_event(tick, EventType::NpcActionCommitted, &event.agent_id, "crownvale", None);

        // Build world view and run agent tick.
        let world_view = self.world.to_world_view(&self.agents);
        let observations = self
            .observation_bus
            .drain_for_agent(&event.agent_id, &self.agent_location(&event.agent_id));

        let planner_config = PlannerConfig {
            beam_width: self.config.agency.planning_beam_width as usize,
            planning_horizon: self.config.agency.planning_horizon as usize,
            idle_threshold: self.config.agency.idle_threshold,
        };

        let agent_seed = deterministic_agent_seed(self.seed, &event.agent_id, tick, 0);
        let mut rng = AgentRng::new(agent_seed);

        let tick_result = {
            let social = &self.world.social_graph;
            let agent = match self.agents.get_mut(&event.agent_id) {
                Some(a) => a,
                None => {
                    // Agent no longer exists; skip.
                    return self.scheduler.has_pending();
                }
            };
            agent.tick(
                &world_view,
                &observations,
                &self.operator_catalog,
                Some(social),
                &planner_config,
                tick,
                &mut rng,
            )
        };

        // Handle aspiration change events.
        if let Some(ref change) = tick_result.aspiration_change {
            self.emit_event(
                tick,
                EventType::NpcActionCommitted,
                &change.agent_id,
                "crownvale",
                Some(json!({
                    "agency_event": "aspiration_changed",
                    "old": change.old_aspiration,
                    "new": change.new_aspiration,
                    "cause": change.cause,
                })),
            );
        }

        // Process the agent's action.
        let agent_id = event.agent_id.clone();
        self.process_action(&agent_id, tick, &tick_result, 0);

        // Schedule next wake.
        let next_tick = tick + 1;
        if next_tick <= self.scheduler.max_ticks() {
            self.scheduler
                .schedule_wake(agent_id, next_tick, WakeReason::PlanStepComplete);
        }

        self.status.queue_depth = self.scheduler.queue_len();
        self.scheduler.has_pending()
    }

    /// Check all occupied agents and clear those whose occupancy has expired.
    fn clear_expired_occupancies(&mut self) {
        let current_tick = self.status.current_tick;
        // Peek at the next event tick to use as the reference point.
        let ref_tick = self.scheduler.peek_next_tick().unwrap_or(current_tick);
        let expired: Vec<String> = self
            .agents
            .iter()
            .filter(|(id, agent)| {
                self.scheduler.is_occupied(id) && !agent.occupancy.is_occupied(ref_tick)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            self.scheduler.clear_occupied(&id);
        }
    }

    /// Advance N scheduling windows. Returns the number of windows actually advanced.
    pub fn step_n(&mut self, n: u64) -> u64 {
        let mut count = 0;
        for _ in 0..n {
            if !self.step() {
                break;
            }
            count += 1;
        }
        count
    }

    /// Run until the given tick. Returns the final tick reached.
    pub fn run_to_tick(&mut self, target_tick: u64) -> u64 {
        while self.status.current_tick < target_tick {
            if !self.step() {
                break;
            }
        }
        self.status.current_tick
    }

    /// Start the simulation.
    pub fn start(&mut self) {
        if !self.status.is_complete() {
            self.status.mode = RunMode::Running;
        }
    }

    /// Pause the simulation.
    pub fn pause(&mut self) {
        self.status.mode = RunMode::Paused;
    }

    /// Access the event log.
    pub fn events(&self) -> &[Event] {
        &self.event_log
    }

    /// Access reason envelopes.
    pub fn reason_envelopes(&self) -> &[ReasonEnvelope] {
        &self.reason_envelopes
    }

    /// Access the world state.
    pub fn world(&self) -> &WorldState {
        &self.world
    }

    /// Access agents.
    pub fn agents(&self) -> &BTreeMap<String, NpcAgent> {
        &self.agents
    }

    /// Current tick.
    pub fn current_tick(&self) -> u64 {
        self.status.current_tick
    }

    // -----------------------------------------------------------------------
    // 17.3 — Snapshot, inspection, and scenario injection
    // -----------------------------------------------------------------------

    /// Serialize all agent states, world state, scheduler state, economy ledger.
    pub fn snapshot(&self) -> Snapshot {
        let agent_snapshots: Vec<AgentSnapshot> = self
            .agents
            .values()
            .map(|a| AgentSnapshot {
                agent_id: a.id.clone(),
                identity: a.identity.clone(),
                drives: AgentDrivesSnapshot {
                    food: a.drives.food.clone(),
                    shelter: a.drives.shelter.clone(),
                    income: a.drives.income.clone(),
                    safety: a.drives.safety.clone(),
                    belonging: a.drives.belonging.clone(),
                    status: a.drives.status.clone(),
                    health: a.drives.health.clone(),
                },
                beliefs: a.beliefs.claims().to_vec(),
                memories: a.memory.memories().to_vec(),
                active_plan: a.active_plan.clone(),
                occupancy: AgentOccupancySnapshot {
                    kind: a
                        .occupancy
                        .active_operator_id
                        .clone()
                        .unwrap_or_else(|| "idle".into()),
                    until_tick: a.occupancy.occupied_until,
                    interruptible: a.occupancy.interruptible,
                },
                aspiration: a.aspiration.clone(),
                location_id: a.location_id.clone(),
                next_wake_tick: self
                    .scheduler
                    .peek_next_tick()
                    .unwrap_or(self.status.current_tick),
            })
            .collect();

        let scheduler_snapshot = SchedulerSnapshot {
            current_tick: self.scheduler.current_tick(),
            events: Vec::new(), // Priority queue contents not easily serializable
        };

        let economy_snapshot = self.world.economy.snapshot();

        // Build world state snapshot.
        let social_edges: Vec<(String, String, contracts::agency::RelationshipEdge)> = self
            .world
            .social_graph
            .edges()
            .iter()
            .map(|((a, b), e)| (a.clone(), b.clone(), e.clone()))
            .collect();

        let world_state_snapshot = WorldStateSnapshot {
            locations: self
                .world
                .spatial
                .locations()
                .iter()
                .map(|(k, v)| (k.clone(), json!({"name": v.name, "tags": v.tags})))
                .collect(),
            routes: self
                .world
                .spatial
                .routes()
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        json!({
                            "origin": v.origin_id,
                            "destination": v.destination_id,
                            "travel_time": v.travel_time,
                        }),
                    )
                })
                .collect(),
            social_edges,
            groups: self
                .world
                .groups
                .iter()
                .map(|(k, g)| {
                    (
                        k.clone(),
                        json!({
                            "leader": g.leader_id,
                            "members": g.members,
                            "cohesion": g.cohesion,
                        }),
                    )
                })
                .collect(),
            institutions: self
                .world
                .institutions
                .iter()
                .map(|(k, inst)| {
                    (
                        k.clone(),
                        json!({
                            "capacity": inst.capacity,
                            "quality": inst.quality,
                            "corruption": inst.corruption,
                            "queue_depth": inst.queue_depth(),
                        }),
                    )
                })
                .collect(),
            weather: json!({
                "condition": format!("{:?}", self.world.weather.condition),
                "travel_multiplier": self.world.weather.travel_time_multiplier,
                "risk_modifier": self.world.weather.risk_modifier,
            }),
        };

        Snapshot {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.config.run_id.clone(),
            tick: self.status.current_tick,
            created_at: String::new(),
            snapshot_id: format!("snap-{}", self.status.current_tick),
            world_state_hash: format!("{}", self.event_log.len()),
            region_state: json!({
                "agents": agent_snapshots.len(),
                "scheduler": scheduler_snapshot,
                "world_state": world_state_snapshot,
            }),
            settlement_states: json!({
                "economy": economy_snapshot,
            }),
            npc_state_refs: serde_json::to_value(&agent_snapshots).unwrap_or(json!([])),
            diff_from_prev_snapshot: None,
            perf_stats: None,
        }
    }

    /// Apply a command as a WorldState mutation perceivable by agents at the
    /// affected location.
    pub fn inject_command(&mut self, command: Command) {
        let tick = self.status.current_tick;
        match &command.payload {
            CommandPayload::InjectRumor {
                location_id,
                rumor_text,
            } => {
                self.observation_bus.broadcast(Observation {
                    event_id: format!("inject-rumor-{}", tick),
                    tick,
                    location_id: location_id.clone(),
                    event_type: "rumor_injected".into(),
                    actors: vec![],
                    visibility: 80,
                    details: json!({"rumor": rumor_text}),
                });
                self.world
                    .scenario
                    .rumor_heat_by_location
                    .entry(location_id.clone())
                    .and_modify(|h| *h += 10)
                    .or_insert(10);
            }
            CommandPayload::InjectSpawnCaravan {
                origin_settlement_id,
                destination_settlement_id,
            } => {
                self.observation_bus.broadcast(Observation {
                    event_id: format!("inject-caravan-{}", tick),
                    tick,
                    location_id: origin_settlement_id.clone(),
                    event_type: "caravan_arrived".into(),
                    actors: vec![],
                    visibility: 90,
                    details: json!({
                        "origin": origin_settlement_id,
                        "destination": destination_settlement_id,
                    }),
                });
                self.world
                    .scenario
                    .caravan_flow_by_settlement
                    .entry(origin_settlement_id.clone())
                    .and_modify(|f| *f += 1)
                    .or_insert(1);
            }
            CommandPayload::InjectForceBadHarvest { settlement_id } => {
                self.observation_bus.broadcast(Observation {
                    event_id: format!("inject-harvest-{}", tick),
                    tick,
                    location_id: settlement_id.clone(),
                    event_type: "bad_harvest".into(),
                    actors: vec![],
                    visibility: 100,
                    details: json!({"settlement": settlement_id}),
                });
                self.world
                    .scenario
                    .harvest_shock_by_settlement
                    .entry(settlement_id.clone())
                    .and_modify(|s| *s += 1)
                    .or_insert(1);
            }
            CommandPayload::InjectSetWinterSeverity { severity } => {
                self.world.scenario.winter_severity = *severity;
            }
            CommandPayload::InjectRemoveNpc { npc_id } => {
                // Remove the agent and broadcast an observation so nearby NPCs perceive it.
                let location = self.agent_location_owned(npc_id);
                self.agents.remove(npc_id);
                self.observation_bus.broadcast(Observation {
                    event_id: format!("inject-remove-npc-{}-{}", npc_id, tick),
                    tick,
                    location_id: location,
                    event_type: "npc_removed".into(),
                    actors: vec![],
                    visibility: 100,
                    details: json!({"npc_id": npc_id}),
                });
            }
            _ => {} // Control commands handled elsewhere
        }
    }

    /// Return drives, active plan, occupancy, relationships, beliefs,
    /// and recent ReasonEnvelope for an NPC.
    pub fn inspect_npc(&self, npc_id: &str) -> Option<NpcInspection> {
        let agent = self.agents.get(npc_id)?;

        let drives = vec![
            (DriveKind::Food, agent.drives.food.current),
            (DriveKind::Shelter, agent.drives.shelter.current),
            (DriveKind::Income, agent.drives.income.current),
            (DriveKind::Safety, agent.drives.safety.current),
            (DriveKind::Belonging, agent.drives.belonging.current),
            (DriveKind::Status, agent.drives.status.current),
            (DriveKind::Health, agent.drives.health.current),
        ];

        let relationships: Vec<(String, contracts::agency::RelationshipEdge)> = self
            .world
            .social_graph
            .neighbors(npc_id)
            .into_iter()
            .map(|(id, edge)| (id.to_string(), edge.clone()))
            .collect();

        let recent_envelope = self
            .reason_envelopes
            .iter()
            .rev()
            .find(|e| e.agent_id == npc_id)
            .cloned();

        Some(NpcInspection {
            agent_id: npc_id.to_string(),
            location_id: agent.location_id.clone(),
            drives,
            active_plan: agent.active_plan.clone(),
            occupancy: agent.occupancy.clone(),
            relationships,
            beliefs: agent.beliefs.claims().to_vec(),
            recent_reason_envelope: recent_envelope,
        })
    }

    /// Return economic state, institution queues, market state, local NPC activity.
    pub fn inspect_settlement(&self, settlement_id: &str) -> Option<SettlementInspection> {
        // Check if the settlement exists as a location.
        if self.world.spatial.get_location(settlement_id).is_none() {
            return None;
        }

        let institution_queues: Vec<(String, usize)> = self
            .world
            .institutions
            .iter()
            .filter(|(_, inst)| inst.settlement_id == settlement_id)
            .map(|(id, inst)| (id.clone(), inst.queue_depth()))
            .collect();

        let market_state = self.world.markets.get(settlement_id).cloned();

        let local_npc_ids: Vec<String> = self
            .agents
            .values()
            .filter(|a| a.location_id == settlement_id)
            .map(|a| a.id.clone())
            .collect();

        Some(SettlementInspection {
            settlement_id: settlement_id.to_string(),
            economy_snapshot: self.world.economy.snapshot(),
            institution_queues,
            market_state,
            local_npc_ids,
        })
    }

    // -----------------------------------------------------------------------
    // 17.4 — ReasonEnvelope recording and causal chain tracking
    // -----------------------------------------------------------------------

    /// Record a ReasonEnvelope for a committed NPC action.
    fn record_reason_envelope(
        &mut self,
        agent_id: &str,
        tick: u64,
        goal: &Goal,
        selected_plan: &CandidatePlan,
        rejected: &[CandidatePlan],
        scores: &[(String, i64)],
        mode: PlanningMode,
    ) {
        let agent = match self.agents.get(agent_id) {
            Some(a) => a,
            None => return,
        };

        let drive_pressures = vec![
            (DriveKind::Food, agent.drives.food.current),
            (DriveKind::Shelter, agent.drives.shelter.current),
            (DriveKind::Income, agent.drives.income.current),
            (DriveKind::Safety, agent.drives.safety.current),
            (DriveKind::Belonging, agent.drives.belonging.current),
            (DriveKind::Status, agent.drives.status.current),
            (DriveKind::Health, agent.drives.health.current),
        ];

        let operator_chain: Vec<String> = selected_plan
            .steps
            .iter()
            .map(|s| s.operator_id.clone())
            .collect();

        let rejected_alternatives: Vec<RejectedPlan> = rejected
            .iter()
            .map(|r| {
                let score = scores
                    .iter()
                    .find(|(id, _)| id == &r.plan_id)
                    .map(|(_, s)| *s)
                    .unwrap_or(0);
                RejectedPlan {
                    plan_id: r.plan_id.clone(),
                    goal: r.goal.clone(),
                    score,
                    rejection_reason: "lower utility score".into(),
                }
            })
            .collect();

        let envelope = ReasonEnvelope {
            agent_id: agent_id.to_string(),
            tick,
            goal: goal.clone(),
            selected_plan: selected_plan.plan_id.clone(),
            operator_chain,
            drive_pressures,
            rejected_alternatives,
            contextual_constraints: Vec::new(),
            planning_mode: mode,
        };

        self.reason_envelopes.push(envelope);
    }

    // -----------------------------------------------------------------------
    // 18.1 — Deterministic replay infrastructure
    // -----------------------------------------------------------------------

    /// Compute a replay hash over the event log for quick comparison.
    ///
    /// Two simulation runs with the same seed and initial configuration MUST
    /// produce identical replay hashes.  The hash covers every event's tick,
    /// sequence number, event type, location, actors, and details — i.e. all
    /// fields that are deterministically produced by the simulation loop.
    ///
    /// Fields that are *not* included: `created_at` (always empty in the
    /// agent-world engine), `schema_version`, `run_id` (configuration, not
    /// simulation output).
    pub fn replay_hash(&self) -> u64 {
        replay_hash_of_events(&self.event_log)
    }

    /// Traverse the causal chain for an event, producing a root-to-leaf trace.
    pub fn traverse_causal_chain(&self, event_id: &str) -> Vec<String> {
        let mut chain = Vec::new();
        let mut current_id = event_id.to_string();

        // Walk backwards through caused_by references.
        loop {
            chain.push(current_id.clone());
            let event = self.event_log.iter().find(|e| e.event_id == current_id);
            match event {
                Some(e) if !e.caused_by.is_empty() => {
                    current_id = e.caused_by[0].clone();
                }
                _ => break,
            }
            // Safety: prevent infinite loops.
            if chain.len() > 1000 {
                break;
            }
        }

        chain.reverse(); // root-to-leaf order
        chain
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Process an agent's action: commit mutations, broadcast observations,
    /// handle reaction chains.
    fn process_action(
        &mut self,
        agent_id: &str,
        tick: u64,
        tick_result: &TickResult,
        reaction_depth: u32,
    ) {
        let max_depth = self.config.agency.max_reaction_depth;
        if reaction_depth > max_depth {
            return; // Prevent runaway escalation.
        }

        match &tick_result.action {
            AgentAction::Execute(bound_op) => {
                // Build a WorldMutation from the bound operator.
                let mutation = WorldMutation {
                    agent_id: agent_id.to_string(),
                    operator_id: bound_op.operator_id.clone(),
                    deltas: bound_op.effects.clone(),
                    resource_transfers: Vec::new(), // Resource transfers handled by economy
                };

                let priority = self.scheduler.priority_for(agent_id, tick);
                let commit_result = self.conflict_resolver.try_commit(
                    &mut self.world.economy,
                    &mutation,
                    priority,
                    tick,
                );

                match commit_result {
                    Ok(commit) => {
                        // Emit plan step event.
                        let caused_by = commit.event_id.clone();
                        self.emit_event_with_cause(
                            tick,
                            EventType::NpcActionCommitted,
                            agent_id,
                            &self.agent_location_owned(agent_id),
                            Some(json!({
                                "agency_event": "plan_step_completed",
                                "operator": bound_op.operator_id,
                            })),
                            vec![caused_by.clone()],
                        );

                        // Broadcast observation for nearby agents.
                        let location = self.agent_location_owned(agent_id);
                        self.observation_bus.broadcast(Observation {
                            event_id: format!("obs-{}-{}", agent_id, tick),
                            tick,
                            location_id: location,
                            event_type: bound_op.operator_id.clone(),
                            actors: vec![agent_id.to_string()],
                            visibility: 50,
                            details: json!({"operator": bound_op.operator_id}),
                        });

                        // Apply drive effects from the operator.
                        if let Some(op_def) = self.operator_catalog.get(&bound_op.operator_id) {
                            if let Some(agent) = self.agents.get_mut(agent_id) {
                                for &(kind, effect) in &op_def.drive_effects {
                                    agent.drives.apply_effect(kind, effect);
                                }
                            }
                        }

                        // Mark occupancy.
                        self.scheduler.mark_occupied(agent_id);
                    }
                    Err(conflict) => {
                        // Emit conflict event.
                        self.emit_event(
                            tick,
                            EventType::NpcActionCommitted,
                            agent_id,
                            &self.agent_location_owned(agent_id),
                            Some(json!({
                                "agency_event": "conflict_detected",
                                "conflicting_agent": conflict.conflicting_agent_id,
                                "resource": conflict.conflicting_resource,
                                "reason": conflict.reason,
                            })),
                        );

                        // Agent needs to replan.
                        if let Some(agent) = self.agents.get_mut(agent_id) {
                            agent.active_plan = None;
                            agent.occupancy.clear();
                        }
                    }
                }
            }
            AgentAction::Continue => {
                // Agent is still occupied — no action needed.
            }
            AgentAction::Idle(reason) => {
                self.emit_event(
                    tick,
                    EventType::NpcActionCommitted,
                    agent_id,
                    &self.agent_location_owned(agent_id),
                    Some(json!({
                        "agency_event": "agent_idle",
                        "reason": reason,
                    })),
                );
                self.scheduler.clear_occupied(agent_id);
            }
            AgentAction::Replan => {
                self.emit_event(
                    tick,
                    EventType::NpcActionCommitted,
                    agent_id,
                    &self.agent_location_owned(agent_id),
                    Some(json!({
                        "agency_event": "plan_interrupted",
                    })),
                );
                self.scheduler.clear_occupied(agent_id);
            }
        }
    }

    /// Emit an event into the event log.
    fn emit_event(
        &mut self,
        tick: u64,
        event_type: EventType,
        actor_id: &str,
        location_id: &str,
        details: Option<Value>,
    ) {
        self.emit_event_with_cause(tick, event_type, actor_id, location_id, details, Vec::new());
    }

    /// Emit an event with causal parent references.
    fn emit_event_with_cause(
        &mut self,
        tick: u64,
        event_type: EventType,
        actor_id: &str,
        location_id: &str,
        details: Option<Value>,
        caused_by: Vec<String>,
    ) {
        let seq = self.next_event_sequence;
        self.next_event_sequence += 1;

        let event = Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.config.run_id.clone(),
            tick,
            created_at: String::new(),
            event_id: format!("evt-{}-{}", tick, seq),
            sequence_in_tick: seq,
            event_type,
            location_id: location_id.to_string(),
            actors: vec![contracts::ActorRef {
                actor_kind: "npc".into(),
                actor_id: actor_id.to_string(),
            }],
            reason_packet_id: None,
            caused_by,
            targets: Vec::new(),
            tags: Vec::new(),
            visibility: Some("public".into()),
            details,
        };

        self.event_log.push(event);
    }

    /// Get an agent's location (borrowed).
    fn agent_location(&self, agent_id: &str) -> String {
        self.agents
            .get(agent_id)
            .map(|a| a.location_id.clone())
            .unwrap_or_else(|| "unknown".into())
    }

    /// Get an agent's location (owned, for use when self is mutably borrowed).
    fn agent_location_owned(&self, agent_id: &str) -> String {
        self.agent_location(agent_id)
    }
}

// ---------------------------------------------------------------------------
// Deterministic RNG seed derivation
// ---------------------------------------------------------------------------

/// Derive a deterministic seed for an agent at a given tick and phase.
fn deterministic_agent_seed(seed: u64, agent_id: &str, tick: u64, phase: u64) -> u64 {
    let mut h = seed;
    h = h.wrapping_add(tick.wrapping_mul(0x9e3779b97f4a7c15));
    h = h.wrapping_add(phase.wrapping_mul(0xbf58476d1ce4e5b9));
    for b in agent_id.bytes() {
        h = h.wrapping_add(b as u64);
        h = h.wrapping_mul(0x94d049bb133111eb);
    }
    h ^ (h >> 31)
}

// ---------------------------------------------------------------------------
// 18.1 — Replay hash: deterministic hash of the event log
// ---------------------------------------------------------------------------

/// Fold a u64 value into a running FNV-1a hash.
fn fnv1a_fold(hash: u64, value: u64) -> u64 {
    let bytes = value.to_le_bytes();
    let mut h = hash;
    for &b in &bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Fold a string into a running FNV-1a hash.
fn fnv1a_fold_str(hash: u64, s: &str) -> u64 {
    let mut h = hash;
    for &b in s.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // Separator byte to avoid collisions between adjacent strings.
    h ^= 0xff;
    h = h.wrapping_mul(0x100000001b3);
    h
}

/// Compute a replay hash over a slice of events.
///
/// The hash covers deterministic fields only: tick, sequence_in_tick,
/// event_type (via Debug format), location_id, actors, and details
/// (serialized to JSON).  This allows quick comparison of two simulation
/// runs without diffing the full event log.
pub fn replay_hash_of_events(events: &[Event]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325; // FNV offset basis

    for event in events {
        h = fnv1a_fold(h, event.tick);
        h = fnv1a_fold(h, event.sequence_in_tick);
        h = fnv1a_fold_str(h, &format!("{:?}", event.event_type));
        h = fnv1a_fold_str(h, &event.location_id);
        for actor in &event.actors {
            h = fnv1a_fold_str(h, &actor.actor_id);
        }
        if let Some(ref details) = event.details {
            h = fnv1a_fold_str(h, &details.to_string());
        }
    }

    h
}

// ---------------------------------------------------------------------------
// Default identity generation
// ---------------------------------------------------------------------------

fn default_identity(seed: u64, index: usize) -> contracts::agency::IdentityProfile {
    use contracts::agency::*;

    let professions = [
        "farmer", "merchant", "guard", "healer", "artisan",
        "laborer", "scholar", "priest", "innkeeper", "hunter",
    ];
    let profession = professions[index % professions.len()].to_string();

    // Derive personality from seed + index for variety.
    let mut h = seed.wrapping_add(index as u64 * 7919);
    let trait_val = |h: &mut u64| -> i64 {
        *h = h.wrapping_mul(0xbf58476d1ce4e5b9).wrapping_add(1);
        ((*h >> 32) as i64 % 201) - 100 // [-100, 100]
    };

    IdentityProfile {
        profession,
        capabilities: CapabilitySet {
            physical: 30 + (index as i64 * 7) % 40,
            social: 30 + (index as i64 * 11) % 40,
            trade: 20 + (index as i64 * 13) % 40,
            combat: 10 + (index as i64 * 17) % 40,
            literacy: 10 + (index as i64 * 19) % 40,
            influence: 10 + (index as i64 * 23) % 40,
            stealth: 10 + (index as i64 * 29) % 40,
            care: 20 + (index as i64 * 31) % 40,
            law: 5 + (index as i64 * 37) % 30,
        },
        personality: PersonalityTraits {
            bravery: trait_val(&mut h),
            morality: trait_val(&mut h),
            impulsiveness: trait_val(&mut h),
            sociability: trait_val(&mut h),
            ambition: trait_val(&mut h),
            empathy: trait_val(&mut h),
            patience: trait_val(&mut h),
            curiosity: trait_val(&mut h),
            jealousy: trait_val(&mut h),
            pride: trait_val(&mut h),
            vindictiveness: trait_val(&mut h),
            greed: trait_val(&mut h),
            loyalty: trait_val(&mut h),
            honesty: trait_val(&mut h),
            piety: trait_val(&mut h),
            vanity: trait_val(&mut h),
            humor: trait_val(&mut h),
        },
        temperament: Temperament::Phlegmatic,
        values: vec![NpcValue {
            name: "stability".into(),
            weight: 50,
        }],
        likes: vec!["peace".into()],
        dislikes: vec!["chaos".into()],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RunConfig {
        RunConfig {
            seed: 42,
            duration_days: 1, // 24 ticks
            npc_count_min: 3,
            npc_count_max: 3,
            ..RunConfig::default()
        }
    }

    // -- 17.1 WorldState tests --

    #[test]
    fn world_state_composes_all_subsystems() {
        let ws = WorldState::new();
        // Spatial
        assert!(ws.spatial.locations().is_empty());
        assert!(ws.spatial.routes().is_empty());
        // Economy
        assert!(ws.economy.accounts().is_empty());
        // Social
        assert!(ws.social_graph.edges().is_empty());
        // Institutions
        assert!(ws.institutions.is_empty());
        // Groups
        assert!(ws.groups.is_empty());
        // Items, production, markets
        assert!(ws.items.is_empty());
        assert!(ws.production_nodes.is_empty());
        assert!(ws.markets.is_empty());
    }

    #[test]
    fn world_state_to_world_view() {
        let mut ws = WorldState::new();
        ws.spatial.add_location(Location {
            location_id: "town".into(),
            name: "Town".into(),
            tags: vec![],
        });
        ws.economy
            .source("npc_0", contracts::agency::ResourceKind::Money, 50, "init", 0);

        let mut agents = BTreeMap::new();
        agents.insert(
            "npc_0".into(),
            NpcAgent::new(
                "npc_0".into(),
                default_identity(42, 0),
                Aspiration {
                    current: "test".into(),
                    previous: None,
                    changed_at_tick: None,
                    change_cause: None,
                },
                "town".into(),
            ),
        );

        let view = ws.to_world_view(&agents);
        assert!(view.location_ids.contains(&"town".to_string()));
        assert!(view.npcs_by_location.get("town").unwrap().contains(&"npc_0".to_string()));
        assert_eq!(*view.facts.get("money:npc_0").unwrap(), 50);
    }

    // -- 17.2 AgentWorld tests --

    #[test]
    fn agent_world_new_creates_agents_and_scheduler() {
        let config = test_config();
        let world = AgentWorld::new(config);
        assert_eq!(world.agents.len(), 3);
        assert!(world.scheduler.has_pending());
        assert_eq!(world.status.mode, RunMode::Paused);
    }

    #[test]
    fn agent_world_step_advances_simulation() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        world.start();
        let continued = world.step();
        assert!(continued);
        assert!(world.current_tick() > 0);
        assert!(!world.events().is_empty());
    }

    #[test]
    fn agent_world_step_n_advances_multiple() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        world.start();
        let count = world.step_n(5);
        assert!(count > 0);
    }

    #[test]
    fn agent_world_run_to_tick() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        world.start();
        let final_tick = world.run_to_tick(3);
        assert!(final_tick >= 3);
    }

    #[test]
    fn agent_world_paused_does_not_step() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        // Don't start — should remain paused.
        let continued = world.step();
        assert!(!continued);
    }

    #[test]
    fn deterministic_replay_same_seed() {
        let config1 = test_config();
        let config2 = test_config();

        let mut world1 = AgentWorld::new(config1);
        world1.start();
        world1.step_n(10);

        let mut world2 = AgentWorld::new(config2);
        world2.start();
        world2.step_n(10);

        // Same seed should produce identical event counts and ticks.
        assert_eq!(world1.events().len(), world2.events().len());
        assert_eq!(world1.current_tick(), world2.current_tick());

        // Event IDs should match.
        for (e1, e2) in world1.events().iter().zip(world2.events().iter()) {
            assert_eq!(e1.event_id, e2.event_id);
            assert_eq!(e1.tick, e2.tick);
        }

        // Replay hashes must be identical.
        assert_eq!(world1.replay_hash(), world2.replay_hash());
    }

    // -- 18.1 Replay hash tests --

    #[test]
    fn replay_hash_is_nonzero_after_stepping() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        world.start();
        world.step_n(5);
        // After producing events, the hash should not be the bare FNV offset basis.
        assert_ne!(world.replay_hash(), 0);
        assert!(!world.events().is_empty());
    }

    #[test]
    fn replay_hash_differs_for_different_seeds() {
        let mut config1 = test_config();
        config1.seed = 42;
        let mut config2 = test_config();
        config2.seed = 999;

        let mut world1 = AgentWorld::new(config1);
        world1.start();
        world1.step_n(10);

        let mut world2 = AgentWorld::new(config2);
        world2.start();
        world2.step_n(10);

        // Different seeds should (with overwhelming probability) produce
        // different event logs and therefore different replay hashes.
        assert_ne!(world1.replay_hash(), world2.replay_hash());
    }

    #[test]
    fn replay_hash_empty_event_log_is_fnv_offset_basis() {
        // An empty event log should produce the FNV-1a offset basis.
        let hash = replay_hash_of_events(&[]);
        assert_eq!(hash, 0xcbf29ce484222325_u64);
    }

    #[test]
    fn deterministic_agent_seed_is_deterministic() {
        let s1 = deterministic_agent_seed(42, "npc_0", 10, 0);
        let s2 = deterministic_agent_seed(42, "npc_0", 10, 0);
        assert_eq!(s1, s2);

        // Different agent → different seed.
        let s3 = deterministic_agent_seed(42, "npc_1", 10, 0);
        assert_ne!(s1, s3);

        // Different tick → different seed.
        let s4 = deterministic_agent_seed(42, "npc_0", 11, 0);
        assert_ne!(s1, s4);

        // Different phase → different seed.
        let s5 = deterministic_agent_seed(42, "npc_0", 10, 1);
        assert_ne!(s1, s5);
    }

    // -- 17.3 Snapshot and inspection tests --

    #[test]
    fn snapshot_captures_state() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        world.start();
        world.step_n(3);
        let snap = world.snapshot();
        assert_eq!(snap.tick, world.current_tick());
        assert!(!snap.run_id.is_empty());
    }

    #[test]
    fn inspect_npc_returns_data() {
        let config = test_config();
        let world = AgentWorld::new(config);
        let inspection = world.inspect_npc("npc_0");
        assert!(inspection.is_some());
        let insp = inspection.unwrap();
        assert_eq!(insp.agent_id, "npc_0");
        assert_eq!(insp.drives.len(), 7);
    }

    #[test]
    fn inspect_npc_missing_returns_none() {
        let config = test_config();
        let world = AgentWorld::new(config);
        assert!(world.inspect_npc("nonexistent").is_none());
    }

    #[test]
    fn inspect_settlement_returns_data() {
        let config = test_config();
        let world = AgentWorld::new(config);
        let inspection = world.inspect_settlement("crownvale");
        assert!(inspection.is_some());
        let insp = inspection.unwrap();
        assert_eq!(insp.settlement_id, "crownvale");
        assert_eq!(insp.local_npc_ids.len(), 3);
    }

    #[test]
    fn inject_command_creates_observation() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        let cmd = Command::new(
            "cmd_1",
            &world.config.run_id,
            0,
            contracts::CommandType::InjectRumor,
            CommandPayload::InjectRumor {
                location_id: "crownvale".into(),
                rumor_text: "the mayor is corrupt".into(),
            },
        );
        world.inject_command(cmd);
        assert!(!world.observation_bus.is_empty());
    }

    // -- 17.4 ReasonEnvelope and causal chain tests --

    #[test]
    fn traverse_causal_chain_finds_root() {
        let config = test_config();
        let mut world = AgentWorld::new(config);

        // Manually add events with causal links.
        world.emit_event(1, EventType::NpcActionCommitted, "npc_0", "crownvale", None);
        let root_id = world.event_log.last().unwrap().event_id.clone();

        world.emit_event_with_cause(
            2,
            EventType::NpcActionCommitted,
            "npc_1",
            "crownvale",
            None,
            vec![root_id.clone()],
        );
        let child_id = world.event_log.last().unwrap().event_id.clone();

        world.emit_event_with_cause(
            3,
            EventType::NpcActionCommitted,
            "npc_2",
            "crownvale",
            None,
            vec![child_id.clone()],
        );
        let grandchild_id = world.event_log.last().unwrap().event_id.clone();

        let chain = world.traverse_causal_chain(&grandchild_id);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], root_id);
        assert_eq!(chain[2], grandchild_id);
    }

    #[test]
    fn events_record_caused_by() {
        let config = test_config();
        let mut world = AgentWorld::new(config);
        world.start();
        world.step_n(5);

        // After stepping, we should have events in the log.
        assert!(!world.events().is_empty());
    }
}
