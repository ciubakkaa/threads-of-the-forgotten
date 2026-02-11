use std::collections::{BTreeMap, BTreeSet};

use contracts::{
    AccountBalance, ActorRef, AgentAction, AgentSnapshot, Aspiration, CapabilitySet, Command,
    CommandPayload, DriveSystem, DriveValue, EconomyLedgerSnapshot, Event, EventType,
    IdentityProfile, PersonalityTraits, ReasonEnvelope, ReasonPacket, ResourceKind, RumorPayload,
    RunConfig, RunMode, RunStatus, SchedulerSnapshot, Snapshot, Temperament, WorldFactDelta,
    WorldMutation, WorldStateSnapshot, SCHEMA_VERSION_V1,
};
use serde_json::{json, Value};

use crate::agent::NpcAgent;
use crate::economy::EconomyLedger;
use crate::institution::{InstitutionState, ServiceRequest};
use crate::operator::{OperatorCatalog, PlanningWorldView};
use crate::planner::PlannerConfig;
use crate::scheduler::{AgentScheduler, ConflictResolver};
use crate::social::{EdgeUpdate, RumorNetwork, SocialGraph};
use crate::spatial::{Location, Route, SpatialModel, Weather};

#[derive(Debug, Clone)]
struct QueuedCommand {
    effective_tick: u64,
    insertion_sequence: u64,
    command: Command,
}

#[derive(Debug)]
pub struct AgentWorld {
    config: RunConfig,
    status: RunStatus,
    queued_commands: Vec<QueuedCommand>,
    event_log: Vec<Event>,
    reason_packet_log: Vec<ReasonPacket>,
    scheduler: AgentScheduler,
    planner_config: PlannerConfig,
    agents: BTreeMap<String, NpcAgent>,
    reason_envelopes: Vec<ReasonEnvelope>,
    operator_catalog: OperatorCatalog,
    conflict_resolver: ConflictResolver,
    next_command_sequence: u64,
    weather: String,
    state_hash: u64,
    replay_hash: u64,
    economy_ledger: EconomyLedger,
    social_graph: SocialGraph,
    rumor_network: RumorNetwork,
    spatial_model: SpatialModel,
    institutions: BTreeMap<String, InstitutionState>,
    reaction_depth_by_event: BTreeMap<String, u8>,
    groups: BTreeMap<String, Vec<String>>,
    stock_staples: i64,
    stock_fuel: i64,
    stock_medicine: i64,
    production_backlog: i64,
    market_price_index: i64,
    emitted_transfer_count: usize,
}

impl AgentWorld {
    pub fn new(config: RunConfig) -> Self {
        let status = RunStatus {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: config.run_id.clone(),
            current_tick: 0,
            max_ticks: config.max_ticks(),
            mode: RunMode::Paused,
            queue_depth: 0,
        };

        let mut scheduler = AgentScheduler::new(config.seed);
        let (min, max) = config.normalized_npc_bounds();
        let count = usize::from((min + max) / 2);
        let mut agents = BTreeMap::new();
        let mut economy_ledger = EconomyLedger::default();
        let mut social_graph = SocialGraph::default();
        let rumor_network = RumorNetwork::with_distortion_bps(config.rumor_distortion_bps);
        let mut spatial_model = SpatialModel::new();
        let mut institutions = BTreeMap::new();

        spatial_model.locations.insert(
            "settlement:greywall".to_string(),
            Location {
                location_id: "settlement:greywall".to_string(),
            },
        );
        spatial_model.locations.insert(
            "settlement:rivergate".to_string(),
            Location {
                location_id: "settlement:rivergate".to_string(),
            },
        );
        spatial_model.routes.insert(
            "route:greywall-rivergate".to_string(),
            Route {
                route_id: "route:greywall-rivergate".to_string(),
                origin: "settlement:greywall".to_string(),
                destination: "settlement:rivergate".to_string(),
                travel_time: 2,
                risk_level: 20,
                weather_modifier: 1,
            },
        );
        institutions.insert(
            "institution:market".to_string(),
            InstitutionState::new("institution:market".to_string(), 8, 1, 75, 20),
        );
        institutions.insert(
            "institution:court".to_string(),
            InstitutionState::new("institution:court".to_string(), 4, 2, 70, 25),
        );
        economy_ledger.accounts.insert(
            "wallet:market".to_string(),
            AccountBalance {
                money: 500,
                food: 200,
                fuel: 80,
                medicine: 30,
            },
        );
        economy_ledger.accounts.insert(
            "wallet:landlord".to_string(),
            AccountBalance {
                money: 200,
                food: 20,
                fuel: 10,
                medicine: 5,
            },
        );
        economy_ledger.accounts.insert(
            "wallet:employer".to_string(),
            AccountBalance {
                money: 1000,
                food: 0,
                fuel: 0,
                medicine: 0,
            },
        );

        for idx in 0..count {
            let id = format!("npc_{idx:03}");
            scheduler.schedule_next(&id, 1, contracts::WakeReason::Idle);
            agents.insert(
                id.clone(),
                NpcAgent::from_identity(
                    id.clone(),
                    default_identity(),
                    Aspiration {
                        aspiration_id: "asp:survive".to_string(),
                        label: "survive".to_string(),
                        updated_tick: 0,
                        cause: "init".to_string(),
                    },
                    "settlement:greywall".to_string(),
                    default_drives(&config),
                ),
            );

            economy_ledger.accounts.insert(
                format!("wallet:{id}"),
                AccountBalance {
                    money: 10,
                    food: 2,
                    fuel: 1,
                    medicine: 0,
                },
            );
        }

        let agent_ids = agents.keys().cloned().collect::<Vec<_>>();
        for from in &agent_ids {
            for to in &agent_ids {
                if from == to {
                    continue;
                }
                social_graph.update_edge(
                    from,
                    to,
                    EdgeUpdate {
                        trust_delta: 35,
                        reputation_delta: 10,
                        ..EdgeUpdate::default()
                    },
                );
            }
        }

        Self {
            planner_config: PlannerConfig {
                beam_width: usize::from(config.planning_beam_width.max(1)),
                horizon: usize::from(config.planning_horizon.max(1)),
                idle_threshold: config.idle_threshold,
            },
            config,
            status,
            queued_commands: Vec::new(),
            event_log: Vec::new(),
            reason_packet_log: Vec::new(),
            scheduler,
            agents,
            reason_envelopes: Vec::new(),
            operator_catalog: OperatorCatalog::default_catalog(),
            conflict_resolver: ConflictResolver::default(),
            next_command_sequence: 0,
            weather: "clear".to_string(),
            state_hash: 0,
            replay_hash: 0,
            economy_ledger,
            social_graph,
            rumor_network,
            spatial_model,
            institutions,
            reaction_depth_by_event: BTreeMap::new(),
            groups: BTreeMap::new(),
            stock_staples: 120,
            stock_fuel: 45,
            stock_medicine: 20,
            production_backlog: 0,
            market_price_index: 100,
            emitted_transfer_count: 0,
        }
    }

    pub fn start(&mut self) {
        if !self.status.is_complete() {
            self.status.mode = RunMode::Running;
        }
    }

    pub fn pause(&mut self) {
        self.status.mode = RunMode::Paused;
    }

    pub fn run_id(&self) -> &str {
        &self.status.run_id
    }

    pub fn config(&self) -> &RunConfig {
        &self.config
    }

    pub fn status(&self) -> &RunStatus {
        &self.status
    }

    pub fn enqueue_command(&mut self, command: Command, effective_tick: u64) {
        self.queued_commands.push(QueuedCommand {
            effective_tick,
            insertion_sequence: self.next_command_sequence,
            command,
        });
        self.next_command_sequence = self.next_command_sequence.saturating_add(1);
        self.sync_queue_depth();
    }

    pub fn events(&self) -> &[Event] {
        &self.event_log
    }

    pub fn reason_packets(&self) -> &[ReasonPacket] {
        &self.reason_packet_log
    }

    pub fn reason_envelopes(&self) -> &[ReasonEnvelope] {
        &self.reason_envelopes
    }

    pub fn replay_hash(&self) -> u64 {
        self.replay_hash
    }

    pub fn step(&mut self) -> bool {
        if self.status.is_complete() {
            self.status.mode = RunMode::Paused;
            return false;
        }

        self.status.current_tick = self.status.current_tick.saturating_add(1);
        let tick = self.status.current_tick;
        let mut sequence_in_tick = 0_u64;

        self.process_due_commands(tick, &mut sequence_in_tick);
        self.process_cadence_updates(tick, &mut sequence_in_tick);
        self.process_institutions(tick, &mut sequence_in_tick);
        self.emit_pressure_event(tick, &mut sequence_in_tick);

        self.conflict_resolver.clear_window();
        let windows = self.config.scheduling_window_size.max(1);
        for _ in 0..windows {
            let Some(next_tick) = self.scheduler.peek_next_tick() else {
                break;
            };
            if next_tick > tick {
                break;
            }

            let Some(window) = self.scheduler.pop_next() else {
                break;
            };
            self.execute_agent_window(window.agent_id, tick, &mut sequence_in_tick);
        }

        self.emit_new_accounting_transfer_events(tick, &mut sequence_in_tick);

        self.state_hash = mix_state_hash(self.state_hash, tick, sequence_in_tick);

        if self.status.current_tick >= self.status.max_ticks {
            self.status.mode = RunMode::Paused;
        }

        true
    }

    pub fn step_n(&mut self, n: u64) -> u64 {
        let mut committed = 0_u64;
        for _ in 0..n {
            if !self.step() {
                break;
            }
            committed += 1;
        }
        committed
    }

    pub fn run_to_tick(&mut self, tick: u64) -> u64 {
        let mut committed = 0_u64;
        while self.status.current_tick < tick {
            if !self.step() {
                break;
            }
            committed += 1;
        }
        committed
    }

    pub fn inject_command(&mut self, command: Command) {
        let effective_tick = self.status.current_tick + 1;
        self.enqueue_command(command, effective_tick);
    }

    pub fn snapshot_for_current_tick(&self) -> Snapshot {
        let drives = self
            .agents
            .values()
            .map(|agent| {
                json!({
                    "npc_id": agent.id,
                    "tick": self.status.current_tick,
                    "need_food": agent.drives.drives.food.current,
                    "need_shelter": agent.drives.drives.shelter.current,
                    "need_income": agent.drives.drives.income.current,
                    "need_safety": agent.drives.drives.safety.current,
                    "need_belonging": agent.drives.drives.belonging.current,
                    "need_status": agent.drives.drives.status.current,
                    "need_recovery": agent.drives.drives.health.current,
                    "stress": (agent.drives.drives.safety.current + agent.drives.drives.health.current) / 2,
                })
            })
            .collect::<Vec<_>>();

        let occupancy = self
            .agents
            .values()
            .map(|agent| serde_json::to_value(&agent.occupancy).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();

        let active_plans = self
            .agents
            .values()
            .filter_map(|agent| agent.active_plan.as_ref())
            .map(|plan| serde_json::to_value(plan).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();

        let beliefs = self
            .agents
            .values()
            .flat_map(|agent| agent.beliefs.claims.iter())
            .map(|belief| serde_json::to_value(belief).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();

        let npc_ledgers = self
            .agents
            .values()
            .map(|agent| {
                json!({
                    "npc_id": agent.id,
                    "household_id": format!("household:{}", agent.id),
                    "wallet": self
                        .economy_ledger
                        .accounts
                        .get(&format!("wallet:{}", agent.id))
                        .map(|acc| acc.money)
                        .unwrap_or(0),
                    "debt_balance": 0,
                    "food_reserve_days": 2,
                    "shelter_status": "stable",
                    "dependents_count": 0,
                    "profession": agent.identity.profession,
                    "aspiration": agent.aspiration.label,
                    "health": 100 - agent.drives.drives.health.current,
                    "illness_ticks": 0,
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
                "pressure_index": self.agents.values().map(|agent| agent.drives.drives.food.current).sum::<i64>(),
                "winter_severity": 40,
                "social_cohesion": 0,
                "drives": drives,
                "occupancy": occupancy,
                "active_plans": active_plans,
                "beliefs": beliefs,
                "relationships": self
                    .social_graph
                    .edges
                    .values()
                    .map(|edge| serde_json::to_value(edge).unwrap_or_else(|_| json!({})))
                    .collect::<Vec<_>>(),
                "opportunities": [],
                "commitments": [],
                "time_budgets": [],
                "narrative_summaries": [],
                "operator_effect_trace": [],
                "institutions": self
                    .institutions
                    .values()
                    .map(|institution| json!({
                        "settlement_id": "settlement:greywall",
                        "institution_id": institution.institution_id,
                        "enforcement_capacity": institution.capacity,
                        "corruption_level": institution.corruption_bias,
                        "bias_level": institution.corruption_bias / 2,
                        "response_latency_ticks": institution.processing_latency_ticks,
                    }))
                    .collect::<Vec<_>>(),
                "groups": [],
                "group_entities": self
                    .groups
                    .iter()
                    .map(|(group_id, members)| json!({
                        "group_id": group_id,
                        "settlement_id": "settlement:greywall",
                        "leader_npc_id": members.first().cloned().unwrap_or_default(),
                        "member_npc_ids": members,
                        "norm_tags": ["mutual_aid"],
                        "cohesion_score": self.config.group_cohesion_threshold,
                    }))
                    .collect::<Vec<_>>(),
                "mobility": self
                    .spatial_model
                    .routes
                    .values()
                    .map(|route| json!({
                        "route_id": route.route_id,
                        "origin_settlement_id": route.origin,
                        "destination_settlement_id": route.destination,
                        "travel_time_ticks": route.travel_time,
                        "hazard_score": route.risk_level,
                        "weather_window_open": true,
                    }))
                    .collect::<Vec<_>>(),
                "institution_queue": self
                    .institutions
                    .values()
                    .map(|institution| json!({
                        "settlement_id": "settlement:greywall",
                        "pending_cases": institution.queue.len() as i64,
                        "processed_cases": 0,
                        "dropped_cases": 0,
                        "avg_response_latency": institution.processing_latency_ticks as i64,
                    }))
                    .collect::<Vec<_>>(),
                "accounting_transfers": self
                    .economy_ledger
                    .transfers
                    .iter()
                    .map(|transfer| json!({
                        "transfer_id": transfer.transfer_id,
                        "tick": transfer.tick,
                        "settlement_id": "settlement:greywall",
                        "from_account": transfer.from_account,
                        "to_account": transfer.to_account,
                        "resource_kind": format!("{:?}", transfer.resource_kind).to_lowercase(),
                        "amount": transfer.amount,
                        "cause_event_id": transfer.cause_event_id,
                    }))
                    .collect::<Vec<_>>(),
                "market_clearing": [json!({
                    "settlement_id": "settlement:greywall",
                    "staples_price_index": self.market_price_index,
                    "fuel_price_index": self.market_price_index + (100 - self.stock_fuel).max(0) / 5,
                    "medicine_price_index": self.market_price_index + (100 - self.stock_medicine).max(0) / 4,
                    "wage_pressure": 0,
                    "shortage_score": (100 - self.stock_staples).max(0) / 2,
                    "unmet_demand": (80 - self.stock_staples).max(0),
                    "cleared_tick": self.status.current_tick,
                    "market_cleared": true,
                })],
                "labor": {"contracts": [], "markets": []},
                "production": {
                    "stocks": [json!({
                        "settlement_id": "settlement:greywall",
                        "staples": self.stock_staples,
                        "fuel": self.stock_fuel,
                        "medicine": self.stock_medicine,
                        "craft_inputs": self.production_backlog.max(0),
                        "local_price_pressure": (self.market_price_index - 100),
                        "coin_reserve": self
                            .economy_ledger
                            .accounts
                            .get("wallet:market")
                            .map(|acc| acc.money)
                            .unwrap_or(0),
                    })],
                    "nodes": [json!({
                        "node_id": "node:mill",
                        "settlement_id": "settlement:greywall",
                        "node_kind": "grain_mill",
                        "input_backlog": self.production_backlog.max(0),
                        "output_backlog": self.stock_staples.max(0),
                        "spoilage_timer": self.config.spoilage_rate_ticks,
                    })]
                }
            }),
            settlement_states: json!([]),
            npc_state_refs: json!({
                "npc_ledgers": npc_ledgers,
                "households": []
            }),
            diff_from_prev_snapshot: None,
            perf_stats: Some(json!({
                "event_count": self.event_log.len(),
                "reason_packet_count": self.reason_packet_log.len(),
                "scheduler_pending": self.scheduler.pending_events(),
            })),
            agents: self.agent_snapshots(),
            scheduler_state: Some(SchedulerSnapshot {
                current_tick: self.scheduler.current_tick(),
                pending: self.scheduler.pending_snapshot(),
            }),
            world_state_v2: Some(WorldStateSnapshot {
                tick: self.status.current_tick,
                weather: self.weather.clone(),
                payload: json!({
                    "agent_count": self.agents.len(),
                    "reason_envelope_count": self.reason_envelopes.len(),
                }),
            }),
            economy_ledger_v2: Some(EconomyLedgerSnapshot {
                accounts: self.economy_ledger.accounts.clone(),
                transfers: self.economy_ledger.transfers.clone(),
            }),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_for_current_tick()
    }

    pub fn inspect_npc(&self, npc_id: &str) -> Option<Value> {
        self.agents.get(npc_id).map(|agent| {
            let latest_reason = self
                .reason_envelopes
                .iter()
                .rev()
                .find(|reason| reason.agent_id == npc_id)
                .cloned();
            let relationships = self
                .social_graph
                .edges
                .values()
                .filter(|edge| edge.source_npc_id == npc_id || edge.target_npc_id == npc_id)
                .cloned()
                .collect::<Vec<_>>();

            json!({
                "npc_id": npc_id,
                "current_location": agent.location_id,
                "drives": agent.drives.drives,
                "active_plan": agent.active_plan,
                "occupancy": agent.occupancy,
                "beliefs": agent.beliefs.claims,
                "relationships": relationships,
                "recent_reason_envelope": latest_reason,
                "recent_actions": self
                    .event_log
                    .iter()
                    .rev()
                    .filter(|event| {
                        event.event_type == EventType::NpcActionCommitted
                            && event.actors.iter().any(|actor| actor.actor_id == npc_id)
                    })
                    .take(8)
                    .cloned()
                    .collect::<Vec<_>>(),
            })
        })
    }

    pub fn inspect_settlement(&self, settlement_id: &str) -> Option<Value> {
        if settlement_id.is_empty() {
            return None;
        }

        let recent_events = self
            .event_log
            .iter()
            .rev()
            .filter(|event| event.location_id == settlement_id)
            .take(96)
            .cloned()
            .collect::<Vec<_>>();
        let action_summary = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .fold(BTreeMap::<String, i64>::new(), |mut acc, event| {
                let key = event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("operator_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                *acc.entry(key).or_insert(0) += 1;
                acc
            });

        Some(json!({
            "settlement_id": settlement_id,
            "current_tick": self.status.current_tick,
            "local_npc_count": self
                .agents
                .values()
                .filter(|agent| agent.location_id == settlement_id)
                .count(),
            "recent_events": recent_events,
            "institution_queues": self
                .institutions
                .values()
                .map(|institution| json!({
                    "institution_id": institution.institution_id,
                    "pending": institution.queue.len(),
                    "capacity": institution.capacity,
                    "quality_rating": institution.quality_rating,
                    "corruption_bias": institution.corruption_bias,
                }))
                .collect::<Vec<_>>(),
            "market_state": {
                "price_index": self.market_price_index,
                "stock_staples": self.stock_staples,
                "stock_fuel": self.stock_fuel,
                "stock_medicine": self.stock_medicine,
                "clearing": if self.stock_staples < 40 { "failed" } else { "stable" },
            },
            "economic_state": {
                "accounts": self.economy_ledger.accounts.len(),
                "transfers": self.economy_ledger.transfers.len(),
            },
            "local_activity_summary": action_summary,
        }))
    }

    pub fn traverse_causal_chain(&self, event_id: &str) -> Vec<Event> {
        let mut chain = Vec::new();
        let mut cursor = self
            .event_log
            .iter()
            .find(|event| event.event_id == event_id)
            .cloned();
        let mut guard = 0_usize;

        while let Some(event) = cursor {
            if guard > self.event_log.len() + 1 {
                break;
            }
            guard += 1;
            let parent = event.caused_by.first().cloned();
            chain.push(event.clone());
            cursor = parent.and_then(|parent_id| {
                self.event_log
                    .iter()
                    .find(|candidate| candidate.event_id == parent_id)
                    .cloned()
            });
        }

        chain
    }

    fn process_due_commands(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        self.queued_commands.sort_by(|a, b| {
            a.effective_tick
                .cmp(&b.effective_tick)
                .then(a.insertion_sequence.cmp(&b.insertion_sequence))
        });

        let mut future = Vec::new();
        let mut due = Vec::new();
        for queued in self.queued_commands.drain(..) {
            if queued.effective_tick <= tick {
                due.push(queued);
            } else {
                future.push(queued);
            }
        }
        self.queued_commands = future;
        self.sync_queue_depth();

        for queued in due {
            self.apply_command(queued.command, tick, sequence_in_tick);
        }
    }

    fn emit_new_accounting_transfer_events(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let pending = self.economy_ledger.transfers[self.emitted_transfer_count..].to_vec();
        self.emitted_transfer_count = self.economy_ledger.transfers.len();
        for transfer in pending {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::AccountingTransferRecorded,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                transfer.cause_event_id.iter().cloned().collect::<Vec<_>>(),
                Some(json!({
                    "transfer_id": transfer.transfer_id,
                    "from": transfer.from_account,
                    "to": transfer.to_account,
                    "amount": transfer.amount,
                    "resource_kind": format!("{:?}", transfer.resource_kind).to_lowercase(),
                })),
            );
        }
    }

    fn apply_command(&mut self, command: Command, tick: u64, sequence_in_tick: &mut u64) {
        let command_ref = format!("cmd:{}", command.command_id);
        match &command.payload {
            CommandPayload::SimStart => self.start(),
            CommandPayload::SimPause => self.pause(),
            CommandPayload::SimStepTick { .. } | CommandPayload::SimRunToTick { .. } => {}
            CommandPayload::InjectRumor {
                location_id,
                rumor_text,
            } => {
                let root_event_id = self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::RumorInjected,
                    location_id.clone(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    Some(json!({ "rumor_text": rumor_text })),
                );
                let rumor = RumorPayload {
                    rumor_id: format!("rumor:{tick}:{}", self.event_log.len()),
                    source_npc_id: "system".to_string(),
                    core_claim: rumor_text.clone(),
                    details: rumor_text.clone(),
                    hop_count: 0,
                };
                self.propagate_rumor(
                    &rumor,
                    "system",
                    location_id,
                    tick,
                    sequence_in_tick,
                    &root_event_id,
                );
            }
            CommandPayload::InjectSpawnCaravan {
                origin_settlement_id,
                destination_settlement_id,
            } => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::CaravanSpawned,
                    origin_settlement_id.clone(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    Some(json!({
                        "destination_settlement_id": destination_settlement_id,
                    })),
                );
            }
            CommandPayload::InjectRemoveNpc { npc_id } => {
                if self.agents.remove(npc_id).is_some() {
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::NpcRemoved,
                        "settlement:greywall".to_string(),
                        vec![ActorRef {
                            actor_id: npc_id.clone(),
                            actor_kind: "removed".to_string(),
                        }],
                        None,
                        vec![command_ref.clone()],
                        None,
                    );
                }
            }
            CommandPayload::InjectForceBadHarvest { settlement_id } => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::BadHarvestForced,
                    settlement_id.clone(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    None,
                );
            }
            CommandPayload::InjectSetWinterSeverity { severity } => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::WinterSeveritySet,
                    "region:crownvale".to_string(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    Some(json!({ "severity": severity })),
                );
            }
        }

        self.push_event(
            tick,
            sequence_in_tick,
            EventType::CommandApplied,
            "system".to_string(),
            Vec::new(),
            None,
            vec![command_ref],
            Some(json!({ "command_type": command.command_type })),
        );
    }

    fn emit_pressure_event(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let pressure = self
            .agents
            .values()
            .map(|agent| agent.drives.drives.food.current + agent.drives.drives.income.current)
            .sum::<i64>();

        self.push_event(
            tick,
            sequence_in_tick,
            EventType::PressureEconomyUpdated,
            "region:crownvale".to_string(),
            Vec::new(),
            None,
            Vec::new(),
            Some(json!({ "aggregate_pressure": pressure })),
        );
    }

    fn process_cadence_updates(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let agent_ids = self.agents.keys().cloned().collect::<Vec<_>>();
        if tick % self.config.rent_period_ticks.max(1) == 0 {
            for agent_id in &agent_ids {
                let tenant = format!("wallet:{agent_id}");
                let outcome = self
                    .economy_ledger
                    .process_rent(&tenant, "wallet:landlord", 2, tick);
                let event_type = if outcome.is_ok() {
                    EventType::RentPaid
                } else {
                    EventType::RentUnpaid
                };
                self.push_event(
                    tick,
                    sequence_in_tick,
                    event_type,
                    "settlement:greywall".to_string(),
                    vec![ActorRef {
                        actor_id: agent_id.to_string(),
                        actor_kind: "tenant".to_string(),
                    }],
                    None,
                    Vec::new(),
                    None,
                );
            }
        }

        if tick % self.config.wage_period_ticks_daily.max(1) == 0 {
            for agent_id in &agent_ids {
                let worker = format!("wallet:{agent_id}");
                let outcome = self
                    .economy_ledger
                    .process_wage("wallet:employer", &worker, 1, tick);
                self.push_event(
                    tick,
                    sequence_in_tick,
                    if outcome.is_ok() {
                        EventType::WagePaid
                    } else {
                        EventType::WageDelayed
                    },
                    "settlement:greywall".to_string(),
                    vec![ActorRef {
                        actor_id: agent_id.to_string(),
                        actor_kind: "worker".to_string(),
                    }],
                    None,
                    Vec::new(),
                    None,
                );
            }
        }

        if tick % self.config.production_rate_ticks.max(1) == 0 {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::ProductionStarted,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "node_id": "node:mill" })),
            );
            if self.stock_fuel > 0 {
                self.stock_fuel -= 1;
                self.stock_staples += 3;
                self.production_backlog = (self.production_backlog - 2).max(0);
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::ProductionCompleted,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "staples_added": 3 })),
                );
            } else {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::StockShortage,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "resource": "fuel" })),
                );
            }
        }

        if tick % self.config.spoilage_rate_ticks.max(1) == 0 {
            let spoiled = (self.stock_staples / 20).max(1);
            self.stock_staples = (self.stock_staples - spoiled).max(0);
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::SpoilageOccurred,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "staples_spoiled": spoiled })),
            );
        }

        if tick % (self.config.market_clearing_frequency_ticks.max(1)) == 0 {
            self.market_price_index = if self.stock_staples < 60 {
                (self.market_price_index + 2).min(180)
            } else if self.stock_staples > 140 {
                (self.market_price_index - 2).max(60)
            } else {
                self.market_price_index
            };
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::MarketCleared,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "price_index": self.market_price_index })),
            );
            if self.stock_staples < 40 {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::MarketFailed,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "reason": "insufficient_supply" })),
                );
            } else {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::StockRecovered,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "staples": self.stock_staples })),
                );
            }
        }

        if tick % 24 == 0 {
            let weather = match (tick / 24) % 4 {
                0 => Weather::Clear,
                1 => Weather::Rain,
                2 => Weather::Storm,
                _ => Weather::Snow,
            };
            self.spatial_model.update_weather(weather);
            self.weather = format!("{weather:?}").to_lowercase();
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::RouteRiskUpdated,
                "region:crownvale".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "weather": self.weather })),
            );

            for edge in self.social_graph.edges.values_mut() {
                if edge.trust > 0 {
                    edge.trust = (edge.trust - self.config.trust_decay_rate.max(0)).max(0);
                } else if edge.trust < 0 {
                    edge.trust = (edge.trust + self.config.trust_decay_rate.max(0)).min(0);
                }
            }

            self.update_groups_from_social_graph(tick, sequence_in_tick);
        }
    }

    fn update_groups_from_social_graph(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let cohesion = self.config.group_cohesion_threshold;
        let edge_view = self
            .social_graph
            .edges
            .iter()
            .map(|((from, to), edge)| (from.clone(), to.clone(), edge.trust))
            .collect::<Vec<_>>();
        for (from, to, trust) in edge_view {
            let group_id = format!("group:{}:{}", from, to);
            if trust >= cohesion {
                if !self.groups.contains_key(&group_id) {
                    self.groups
                        .insert(group_id.clone(), vec![from.clone(), to.clone()]);
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::GroupFormed,
                        "settlement:greywall".to_string(),
                        vec![ActorRef {
                            actor_id: from.clone(),
                            actor_kind: "founder".to_string(),
                        }],
                        None,
                        Vec::new(),
                        Some(json!({ "group_id": group_id, "member": to })),
                    );
                }
            } else if trust <= cohesion / 2 && self.groups.remove(&group_id).is_some() {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::GroupDissolved,
                    "settlement:greywall".to_string(),
                    vec![ActorRef {
                        actor_id: from.clone(),
                        actor_kind: "former_member".to_string(),
                    }],
                    None,
                    Vec::new(),
                    Some(json!({ "group_id": group_id, "member": to })),
                );
            }
        }
    }

    fn process_institutions(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let mut pending_events = Vec::new();
        for institution in self.institutions.values_mut() {
            institution.degrade_quality(1);
            let institution_id = institution.institution_id.clone();
            let outcomes = institution.process_cycle();
            for outcome in outcomes {
                pending_events.push((
                    EventType::InstitutionCaseResolved,
                    vec![ActorRef {
                        actor_id: outcome.npc_id,
                        actor_kind: "requester".to_string(),
                    }],
                    Some(json!({
                        "institution_id": institution_id,
                        "quality_score": outcome.quality_score,
                    })),
                ));
            }
            if institution.queue.len() >= institution.capacity {
                pending_events.push((
                    EventType::InstitutionQueueFull,
                    Vec::new(),
                    Some(json!({ "institution_id": institution_id })),
                ));
            }
            if institution.corruption_bias > 60 {
                pending_events.push((
                    EventType::InstitutionCorruptionEvent,
                    Vec::new(),
                    Some(json!({ "institution_id": institution_id, "corruption": institution.corruption_bias })),
                ));
            }
        }
        for (event_type, actors, details) in pending_events {
            self.push_event(
                tick,
                sequence_in_tick,
                event_type,
                "settlement:greywall".to_string(),
                actors,
                None,
                Vec::new(),
                details,
            );
        }
    }

    fn propagate_rumor(
        &mut self,
        rumor: &RumorPayload,
        source_id: &str,
        location_id: &str,
        tick: u64,
        sequence_in_tick: &mut u64,
        root_event_id: &str,
    ) {
        let mut delivered = Vec::new();
        if source_id == "system" {
            delivered.extend(
                self.agents
                    .values()
                    .filter(|agent| agent.location_id == location_id)
                    .map(|agent| {
                        (
                            agent.id.clone(),
                            RumorPayload {
                                hop_count: 1,
                                source_npc_id: source_id.to_string(),
                                ..rumor.clone()
                            },
                        )
                    }),
            );
        } else {
            delivered.extend(
                self.rumor_network
                    .propagate(rumor, source_id, &self.social_graph),
            );
        }

        for (target_npc, payload) in delivered {
            let mut target_location = None;
            if let Some(agent) = self.agents.get_mut(&target_npc) {
                let source_trust = self
                    .social_graph
                    .get_edge(source_id, &target_npc)
                    .map(|edge| edge.trust)
                    .unwrap_or(30);
                agent
                    .beliefs
                    .update_from_rumor(&payload, &target_npc, tick, source_trust);
                target_location = Some(agent.location_id.clone());
            }
            if let Some(location) = target_location {
                let event_type = if payload.details.contains("distorted") {
                    EventType::RumorDistorted
                } else {
                    EventType::RumorPropagated
                };
                self.push_event(
                    tick,
                    sequence_in_tick,
                    event_type,
                    location,
                    vec![ActorRef {
                        actor_id: target_npc.clone(),
                        actor_kind: "receiver".to_string(),
                    }],
                    None,
                    vec![root_event_id.to_string()],
                    Some(json!({
                        "rumor_id": payload.rumor_id,
                        "source": source_id,
                        "hop_count": payload.hop_count,
                    })),
                );
                self.scheduler.schedule_next(
                    &target_npc,
                    tick + 1,
                    contracts::WakeReason::Reactive,
                );
            }
        }
    }

    fn execute_agent_window(&mut self, agent_id: String, tick: u64, sequence_in_tick: &mut u64) {
        let Some(agent_ro) = self.agents.get(&agent_id) else {
            return;
        };
        let actor_id = agent_ro.id.clone();
        let location_id = agent_ro.location_id.clone();
        let food_urgent =
            agent_ro.drives.drives.food.current >= agent_ro.drives.drives.food.urgency_threshold;
        let morality = agent_ro.identity.personality.morality;

        let observations = self
            .event_log
            .iter()
            .rev()
            .filter(|event| event.location_id == location_id)
            .take(6)
            .map(|event| contracts::Observation {
                event_id: event.event_id.clone(),
                tick: event.tick,
                location_id: event.location_id.clone(),
                event_type: event.event_type,
                actors: event
                    .actors
                    .iter()
                    .map(|actor| actor.actor_id.clone())
                    .collect(),
                visibility: 70,
                details: event.details.clone().unwrap_or_else(|| json!({})),
            })
            .collect::<Vec<_>>();

        let npc_ids = self
            .agents
            .keys()
            .filter(|id| *id != &actor_id)
            .cloned()
            .collect::<Vec<_>>();
        let location_ids = self
            .agents
            .values()
            .map(|other| other.location_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut facts = BTreeMap::from([
            ("agent.food_urgent".to_string(), food_urgent.to_string()),
            ("agent.morality".to_string(), morality.to_string()),
            ("agent.location".to_string(), location_id.clone()),
        ]);
        for other in &npc_ids {
            let trust = self
                .social_graph
                .get_edge(&actor_id, other)
                .map(|edge| edge.trust)
                .unwrap_or(0);
            facts.insert(
                format!("social.trust:{}:{}", actor_id, other),
                trust.to_string(),
            );
        }
        let world_view = PlanningWorldView {
            npc_ids,
            location_ids,
            object_ids: vec!["item:coin_purse".to_string(), "item:bread".to_string()],
            institution_ids: vec![
                "institution:market".to_string(),
                "institution:court".to_string(),
            ],
            resource_types: vec!["money".to_string(), "food".to_string()],
            facts,
        };

        let result = {
            let Some(agent_mut) = self.agents.get_mut(&agent_id) else {
                return;
            };
            agent_mut.tick(
                tick,
                &world_view,
                &observations,
                &self.operator_catalog,
                &self.planner_config,
            )
        };

        let mut reason_packet_id = None;
        if let Some(envelope) = result.reason.clone() {
            reason_packet_id = Some(self.push_reason_packet(tick, &actor_id, &envelope));
            self.reason_envelopes.push(envelope);
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::PlanCreated,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: actor_id.clone(),
                    actor_kind: "planner".to_string(),
                }],
                reason_packet_id.clone(),
                Vec::new(),
                None,
            );
        }
        if let Some(change) = result.aspiration_change {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::AspirationChanged,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: actor_id.clone(),
                    actor_kind: "aspiring".to_string(),
                }],
                reason_packet_id.clone(),
                Vec::new(),
                Some(json!({
                    "old": change.old_aspiration.map(|asp| asp.label),
                    "new": change.new_aspiration.label,
                    "cause": change.cause,
                })),
            );
        }
        for drive in result.drive_threshold_crossings {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::DriveThresholdCrossed,
                location_id.clone(),
                vec![ActorRef {
                    actor_id: actor_id.clone(),
                    actor_kind: "drive".to_string(),
                }],
                reason_packet_id.clone(),
                Vec::new(),
                Some(json!({ "drive": format!("{drive:?}").to_lowercase() })),
            );
        }

        match result.action {
            AgentAction::Execute(bound) => {
                if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                    let target_location = self
                        .agents
                        .get(target_npc)
                        .map(|npc| npc.location_id.clone())
                        .unwrap_or_else(|| location_id.clone());
                    if !self
                        .spatial_model
                        .can_interact_same_location(&location_id, &target_location)
                    {
                        self.mark_agent_for_replan(&actor_id, tick);
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::PlanInterrupted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "replan".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "reason": "location_mismatch",
                                "target_npc": target_npc,
                            })),
                        );
                        self.scheduler.schedule_next(
                            &actor_id,
                            tick + 1,
                            contracts::WakeReason::Reactive,
                        );
                        return;
                    }
                }

                let priority = self.scheduler.priority_for(tick, &actor_id);
                let mutation = WorldMutation {
                    agent_id: actor_id.clone(),
                    operator_id: bound.operator_id.clone(),
                    deltas: vec![WorldFactDelta {
                        fact_key: format!("operator:{}", bound.operator_id),
                        delta: 1,
                        from_value: 0,
                        to_value: 1,
                        location_id: location_id.clone(),
                    }],
                    resource_transfers: Vec::new(),
                };

                let commit = self.conflict_resolver.try_commit(&mutation, priority);
                match commit {
                    Ok(_) => {
                        let operator_id = bound.operator_id.clone();
                        let parameters = bound.parameters.clone();
                        let event_id = self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::NpcActionCommitted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "actor".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "operator_id": operator_id,
                                "parameters": parameters,
                            })),
                        );
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::PlanStepStarted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "executor".to_string(),
                            }],
                            reason_packet_id.clone(),
                            vec![event_id.clone()],
                            None,
                        );

                        self.apply_operator_side_effects(
                            &actor_id,
                            &location_id,
                            &bound,
                            tick,
                            sequence_in_tick,
                            &event_id,
                            reason_packet_id.clone(),
                        );
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::PlanStepCompleted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "executor".to_string(),
                            }],
                            reason_packet_id.clone(),
                            vec![event_id.clone()],
                            None,
                        );
                        self.schedule_observer_reactions(
                            &location_id,
                            &actor_id,
                            &event_id,
                            tick,
                            sequence_in_tick,
                        );

                        self.scheduler.schedule_next(
                            &actor_id,
                            tick + 1,
                            contracts::WakeReason::PlanStepComplete,
                        );
                    }
                    Err(conflict) => {
                        self.mark_agent_for_replan(&actor_id, tick);
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::ConflictDetected,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "contender".to_string(),
                            }],
                            reason_packet_id.clone(),
                            Vec::new(),
                            Some(json!({
                                "conflicting_agent_id": conflict.conflicting_agent_id,
                                "resource": conflict.conflicting_resource,
                            })),
                        );
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::PlanInterrupted,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "replan".to_string(),
                            }],
                            None,
                            Vec::new(),
                            Some(json!({ "reason": "conflict_rejected" })),
                        );
                        self.push_event(
                            tick,
                            sequence_in_tick,
                            EventType::ConflictResolved,
                            location_id.clone(),
                            vec![ActorRef {
                                actor_id: actor_id.clone(),
                                actor_kind: "contender".to_string(),
                            }],
                            reason_packet_id,
                            Vec::new(),
                            Some(json!({
                                "conflicting_agent_id": conflict.conflicting_agent_id,
                                "resource": conflict.conflicting_resource,
                            })),
                        );
                        self.scheduler.schedule_next(
                            &actor_id,
                            tick + 1,
                            contracts::WakeReason::Interrupted,
                        );
                    }
                }
            }
            AgentAction::Idle(reason) => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::AgentIdle,
                    location_id.clone(),
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "idle".to_string(),
                    }],
                    reason_packet_id,
                    Vec::new(),
                    Some(json!({ "reason": reason })),
                );
                self.scheduler
                    .schedule_next(&actor_id, tick + 1, contracts::WakeReason::Idle);
            }
            AgentAction::Continue => {
                self.scheduler.schedule_next(
                    &actor_id,
                    tick + 1,
                    contracts::WakeReason::PlanStepComplete,
                );
            }
            AgentAction::Replan => {
                self.mark_agent_for_replan(&actor_id, tick);
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::PlanInterrupted,
                    location_id,
                    vec![ActorRef {
                        actor_id: actor_id.clone(),
                        actor_kind: "replan".to_string(),
                    }],
                    reason_packet_id,
                    Vec::new(),
                    None,
                );
                self.scheduler
                    .schedule_next(&actor_id, tick + 1, contracts::WakeReason::Reactive);
            }
        }
    }

    fn mark_agent_for_replan(&mut self, actor_id: &str, tick: u64) {
        if let Some(agent) = self.agents.get_mut(actor_id) {
            agent.active_plan = None;
            agent.occupancy = contracts::NpcOccupancyState {
                npc_id: actor_id.to_string(),
                tick,
                occupancy: contracts::NpcOccupancyKind::Idle,
                state_tag: "replan".to_string(),
                until_tick: tick + 1,
                interruptible: true,
                location_id: agent.location_id.clone(),
                active_plan_id: None,
                active_operator_id: None,
            };
        }
    }

    fn apply_operator_side_effects(
        &mut self,
        actor_id: &str,
        location_id: &str,
        bound: &contracts::BoundOperator,
        tick: u64,
        sequence_in_tick: &mut u64,
        source_event_id: &str,
        reason_packet_id: Option<String>,
    ) {
        if bound.operator_id.contains("steal") {
            let mut victim = "wallet:market".to_string();
            if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                victim = format!("wallet:{target_npc}");
            }
            let thief_wallet = format!("wallet:{actor_id}");
            let transfer = self.economy_ledger.transfer(
                &victim,
                &thief_wallet,
                ResourceKind::Money,
                1,
                source_event_id,
                tick,
            );
            if transfer.is_ok() {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::TheftCommitted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    Some(json!({ "victim_wallet": victim })),
                );
                if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                    self.social_graph.update_edge(
                        actor_id,
                        target_npc,
                        EdgeUpdate {
                            trust_delta: -self.config.relationship_negative_trust_delta,
                            grievance_delta: self.config.relationship_grievance_delta,
                            fear_delta: 10,
                            ..EdgeUpdate::default()
                        },
                    );
                }
            }
        }

        if bound.operator_id.contains("social") {
            if let Some(target_npc) = bound.parameters.target_npc.as_ref() {
                self.social_graph.update_edge(
                    actor_id,
                    target_npc,
                    EdgeUpdate {
                        trust_delta: self.config.relationship_positive_trust_delta,
                        respect_delta: 5,
                        obligation_delta: 3,
                        ..EdgeUpdate::default()
                    },
                );
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::RelationshipShifted,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "actor".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    Some(json!({ "target_npc": target_npc })),
                );

                if bound.operator_id.contains("gossip") {
                    let rumor = RumorPayload {
                        rumor_id: format!("rumor:{tick}:{}", self.event_log.len() + 1),
                        source_npc_id: actor_id.to_string(),
                        core_claim: "street_whispers".to_string(),
                        details: format!("{actor_id} gossiped about {target_npc}"),
                        hop_count: 0,
                    };
                    self.propagate_rumor(
                        &rumor,
                        actor_id,
                        location_id,
                        tick,
                        sequence_in_tick,
                        source_event_id,
                    );
                }
            }
        }

        if bound.operator_id.contains("work") {
            let worker = format!("wallet:{actor_id}");
            if self
                .economy_ledger
                .process_wage("wallet:employer", &worker, 1, tick)
                .is_ok()
            {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::WagePaid,
                    location_id.to_string(),
                    vec![ActorRef {
                        actor_id: actor_id.to_string(),
                        actor_kind: "worker".to_string(),
                    }],
                    reason_packet_id.clone(),
                    vec![source_event_id.to_string()],
                    None,
                );
            }
        }

        if bound.operator_id.contains("eat") {
            let eater = format!("wallet:{actor_id}");
            let _ = self.economy_ledger.transfer(
                &eater,
                "wallet:market",
                ResourceKind::Food,
                1,
                source_event_id,
                tick,
            );
            if let Some(agent) = self.agents.get_mut(actor_id) {
                agent.drives.apply_effect(contracts::DriveKind::Food, 20);
            }
        }

        if bound.operator_id.contains("travel") {
            if let Some(target_location) = bound.parameters.target_location.as_ref() {
                let can_reach = self.spatial_model.locations.contains_key(target_location);
                if can_reach {
                    if let Some(agent) = self.agents.get_mut(actor_id) {
                        agent.location_id = target_location.clone();
                        agent.occupancy.location_id = target_location.clone();
                    }
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::TravelWindowShifted,
                        target_location.clone(),
                        vec![ActorRef {
                            actor_id: actor_id.to_string(),
                            actor_kind: "traveler".to_string(),
                        }],
                        reason_packet_id,
                        vec![source_event_id.to_string()],
                        None,
                    );
                }
            }
        }

        if bound.operator_id.contains("petition") || bound.operator_id.contains("enforce_law") {
            if let Some(institution) = self.institutions.get_mut("institution:court") {
                let queued = institution.enqueue(ServiceRequest {
                    request_id: format!("svc:{tick}:{}", institution.queue.len() + 1),
                    npc_id: actor_id.to_string(),
                    submitted_tick: tick,
                    status_score: 40,
                    connected_score: 30,
                });
                if !queued {
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::InstitutionQueueFull,
                        location_id.to_string(),
                        vec![ActorRef {
                            actor_id: actor_id.to_string(),
                            actor_kind: "requester".to_string(),
                        }],
                        None,
                        vec![source_event_id.to_string()],
                        Some(json!({ "institution_id": "institution:court" })),
                    );
                }
            }
        }
    }

    fn schedule_observer_reactions(
        &mut self,
        location_id: &str,
        actor_id: &str,
        source_event_id: &str,
        tick: u64,
        sequence_in_tick: &mut u64,
    ) {
        let depth = self
            .reaction_depth_by_event
            .get(source_event_id)
            .copied()
            .unwrap_or(0);
        if depth >= self.config.max_reaction_depth {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::ReactionDepthExceeded,
                location_id.to_string(),
                Vec::new(),
                None,
                vec![source_event_id.to_string()],
                Some(json!({ "depth": depth })),
            );
            return;
        }

        for observer in self
            .agents
            .values()
            .filter(|agent| agent.location_id == location_id && agent.id != actor_id)
            .map(|agent| agent.id.clone())
            .collect::<Vec<_>>()
        {
            self.scheduler
                .schedule_next(&observer, tick + 1, contracts::WakeReason::Reactive);
        }
    }

    fn push_reason_packet(
        &mut self,
        tick: u64,
        actor_id: &str,
        envelope: &ReasonEnvelope,
    ) -> String {
        let id = format!("reason_{tick}_{:04}", self.reason_packet_log.len() + 1);
        self.reason_packet_log.push(ReasonPacket {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, 0),
            reason_packet_id: id.clone(),
            actor_id: actor_id.to_string(),
            chosen_action: envelope
                .operator_chain
                .first()
                .cloned()
                .unwrap_or_else(|| "idle".to_string()),
            top_intents: vec![envelope.goal.label.clone()],
            top_beliefs: Vec::new(),
            top_pressures: envelope
                .drive_pressures
                .iter()
                .map(|(kind, pressure)| format!("{kind:?}:{pressure}"))
                .collect(),
            alternatives_considered: envelope
                .rejected_alternatives
                .iter()
                .map(|rejected| rejected.plan_id.clone())
                .collect(),
            motive_families: Vec::new(),
            feasibility_checks: Vec::new(),
            chosen_verb: None,
            context_constraints: envelope.contextual_constraints.clone(),
            why_chain: vec![
                format!("goal={}", envelope.goal.label),
                format!("planning_mode={:?}", envelope.planning_mode),
            ],
            expected_consequences: Vec::new(),
            goal_id: Some(envelope.goal.goal_id.clone()),
            plan_id: Some(envelope.selected_plan.plan_id.clone()),
            operator_chain_ids: envelope.operator_chain.clone(),
            blocked_plan_ids: envelope
                .rejected_alternatives
                .iter()
                .map(|rejected| rejected.plan_id.clone())
                .collect(),
            selection_rationale: "utility_maximization".to_string(),
        });
        id
    }

    fn push_event(
        &mut self,
        tick: u64,
        sequence_in_tick: &mut u64,
        event_type: EventType,
        location_id: String,
        actors: Vec<ActorRef>,
        reason_packet_id: Option<String>,
        caused_by: Vec<String>,
        details: Option<Value>,
    ) -> String {
        *sequence_in_tick = sequence_in_tick.saturating_add(1);
        let event_id = format!("evt_{tick:06}_{:04}", *sequence_in_tick);
        let depth = caused_by
            .first()
            .and_then(|parent| self.reaction_depth_by_event.get(parent))
            .copied()
            .unwrap_or(0);
        self.event_log.push(Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: event_id.clone(),
            sequence_in_tick: *sequence_in_tick,
            event_type,
            location_id,
            actors,
            reason_packet_id,
            caused_by,
            targets: Vec::new(),
            tags: Vec::new(),
            visibility: Some("public".to_string()),
            details,
        });
        self.replay_hash = mix_replay_hash(self.replay_hash, &event_id, tick, *sequence_in_tick);
        self.reaction_depth_by_event
            .insert(event_id.clone(), depth.saturating_add(1));
        event_id
    }

    fn sync_queue_depth(&mut self) {
        self.status.queue_depth = self.queued_commands.len();
    }

    fn agent_snapshots(&self) -> Vec<AgentSnapshot> {
        self.agents
            .values()
            .map(|agent| AgentSnapshot {
                agent_id: agent.id.clone(),
                identity: agent.identity.clone(),
                drives: agent.drives.drives.clone(),
                beliefs: agent.beliefs.claims.clone(),
                memories: agent.memory.memories.clone(),
                active_plan: agent.active_plan.clone(),
                occupancy: agent.occupancy.clone(),
                aspiration: agent.aspiration.clone(),
                location_id: agent.location_id.clone(),
                next_wake_tick: self
                    .scheduler
                    .peek_next_tick()
                    .unwrap_or(self.status.current_tick),
            })
            .collect()
    }
}

fn default_identity() -> IdentityProfile {
    IdentityProfile {
        profession: "laborer".to_string(),
        capabilities: CapabilitySet {
            physical: 50,
            social: 40,
            trade: 40,
            combat: 30,
            literacy: 30,
            influence: 20,
            stealth: 30,
            care: 20,
            law: 10,
        },
        personality: PersonalityTraits {
            bravery: 10,
            morality: -20,
            impulsiveness: 25,
            sociability: 20,
            ambition: 40,
            empathy: 20,
            patience: 10,
            curiosity: 20,
            jealousy: 10,
            pride: 10,
            vindictiveness: 0,
            greed: 20,
            loyalty: 10,
            honesty: 0,
            piety: 0,
            vanity: 0,
            humor: 10,
        },
        temperament: Temperament::Stoic,
        values: vec!["survival".to_string()],
        likes: vec!["warm_meals".to_string()],
        dislikes: vec!["hunger".to_string()],
    }
}

fn default_drives(config: &RunConfig) -> DriveSystem {
    let decay = |key: &str, fallback: i64| {
        config
            .drive_decay_rates
            .get(key)
            .copied()
            .unwrap_or(fallback)
            .max(0)
    };
    let threshold = |key: &str, fallback: i64| {
        config
            .drive_urgency_thresholds
            .get(key)
            .copied()
            .unwrap_or(fallback)
            .clamp(1, 100)
    };
    DriveSystem {
        food: DriveValue {
            current: 40,
            decay_rate: decay("food", 2),
            urgency_threshold: threshold("food", 70),
        },
        shelter: DriveValue {
            current: 20,
            decay_rate: decay("shelter", 1),
            urgency_threshold: threshold("shelter", 70),
        },
        income: DriveValue {
            current: 35,
            decay_rate: decay("income", 2),
            urgency_threshold: threshold("income", 70),
        },
        safety: DriveValue {
            current: 20,
            decay_rate: decay("safety", 1),
            urgency_threshold: threshold("safety", 70),
        },
        belonging: DriveValue {
            current: 20,
            decay_rate: decay("belonging", 1),
            urgency_threshold: threshold("belonging", 65),
        },
        status: DriveValue {
            current: 20,
            decay_rate: decay("status", 1),
            urgency_threshold: threshold("status", 65),
        },
        health: DriveValue {
            current: 20,
            decay_rate: decay("health", 2),
            urgency_threshold: threshold("health", 70),
        },
    }
}

fn synthetic_timestamp(tick: u64, seq: u64) -> String {
    format!(
        "1970-01-01T{:02}:{:02}:{:02}Z",
        (tick / 3600) % 24,
        (tick / 60) % 60,
        (tick + seq) % 60
    )
}

fn mix_state_hash(state_hash: u64, tick: u64, sequence_in_tick: u64) -> u64 {
    let mut hash = state_hash ^ tick.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    hash ^= sequence_in_tick.wrapping_mul(0x517C_C1B7_2722_0A95);
    hash.rotate_left(17)
}

fn mix_replay_hash(current: u64, event_id: &str, tick: u64, sequence: u64) -> u64 {
    let mut hash = current ^ tick.wrapping_mul(0xA24B_1C62_5B93_2D47);
    hash ^= sequence.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in event_id.as_bytes() {
        hash = hash.rotate_left(7) ^ u64::from(*byte);
        hash = hash.wrapping_mul(0x517C_C1B7_2722_0A95);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_advances_and_populates_v2_snapshot_blocks() {
        let mut world = AgentWorld::new(RunConfig::default());
        world.start();
        assert!(world.step());
        let snapshot = world.snapshot_for_current_tick();
        assert!(snapshot.scheduler_state.is_some());
        assert!(snapshot.world_state_v2.is_some());
        assert!(!snapshot.agents.is_empty());
    }

    #[test]
    fn inspect_endpoints_return_values() {
        let world = AgentWorld::new(RunConfig::default());
        let npc_id = world.agents.keys().next().expect("agent exists").clone();
        let npc = world.inspect_npc(&npc_id).expect("npc inspect present");
        let relationships = npc
            .get("relationships")
            .and_then(Value::as_array)
            .expect("relationships present");
        assert!(!relationships.is_empty());

        let settlement = world
            .inspect_settlement("settlement:greywall")
            .expect("settlement inspect present");
        assert!(settlement.get("market_state").is_some());
        assert!(settlement.get("local_activity_summary").is_some());
    }

    #[test]
    fn command_injection_emits_rumor_event() {
        let mut world = AgentWorld::new(RunConfig::default());
        let command = Command::new(
            "cmd_1",
            world.run_id().to_string(),
            1,
            contracts::CommandType::InjectRumor,
            CommandPayload::InjectRumor {
                location_id: "settlement:greywall".to_string(),
                rumor_text: "watch the alley".to_string(),
            },
        );
        world.enqueue_command(command, 1);
        world.step();

        assert!(world
            .events()
            .iter()
            .any(|event| event.event_type == EventType::RumorInjected));
    }

    #[test]
    fn step_emits_pressure_event_for_trace_compatibility() {
        let mut world = AgentWorld::new(RunConfig::default());
        world.step();
        assert!(world
            .events()
            .iter()
            .any(|event| event.event_type == EventType::PressureEconomyUpdated));
    }

    #[test]
    fn step_three_advances_tick_three() {
        let mut world = AgentWorld::new(RunConfig::default());
        let committed = world.step_n(3);
        assert_eq!(committed, 3);
        assert_eq!(world.status().current_tick, 3);
    }

    #[test]
    fn reaction_depth_limit_emits_suppression_event() {
        let mut config = RunConfig::default();
        config.max_reaction_depth = 1;
        let mut world = AgentWorld::new(config);
        world.start();

        let injected = Command::new(
            "cmd_react",
            world.run_id().to_string(),
            1,
            contracts::CommandType::InjectRumor,
            CommandPayload::InjectRumor {
                location_id: "settlement:greywall".to_string(),
                rumor_text: "panic in market".to_string(),
            },
        );
        world.enqueue_command(injected, 1);
        world.step_n(3);

        assert!(world
            .events()
            .iter()
            .any(|event| event.event_type == EventType::ReactionDepthExceeded));
    }

    #[test]
    fn causal_chain_traversal_terminates() {
        let mut world = AgentWorld::new(RunConfig::default());
        world.step_n(2);
        let tail = world
            .events()
            .last()
            .expect("event exists")
            .event_id
            .clone();
        let chain = world.traverse_causal_chain(&tail);
        assert!(!chain.is_empty());
        assert!(chain.len() <= world.events().len());
    }

    #[test]
    fn snapshots_include_relationships_and_transfers() {
        let mut world = AgentWorld::new(RunConfig::default());
        world.step_n(3);
        let snapshot = world.snapshot_for_current_tick();
        let relationships = snapshot
            .region_state
            .get("relationships")
            .and_then(Value::as_array)
            .expect("relationships present");
        assert!(!relationships.is_empty());
        assert!(snapshot.economy_ledger_v2.is_some());
    }

    #[test]
    fn plan_interruption_on_location_mismatch_enters_replanning_state() {
        let mut config = RunConfig::default();
        config.npc_count_min = 2;
        config.npc_count_max = 2;
        let mut world = AgentWorld::new(config);
        world.planner_config.idle_threshold = -100;
        world.operator_catalog = OperatorCatalog::new(vec![contracts::OperatorDef {
            operator_id: "social:greet".to_string(),
            family: "social".to_string(),
            display_name: "greet".to_string(),
            param_schema: contracts::ParamSchema {
                required: vec![contracts::ParamDef {
                    name: "target_npc".to_string(),
                    param_type: contracts::ParamType::NpcId,
                }],
                optional: Vec::new(),
            },
            duration_ticks: 1,
            risk: 10,
            visibility: 40,
            preconditions: Vec::new(),
            effects: Vec::new(),
            drive_effects: Vec::new(),
            capability_requirements: Vec::new(),
        }]);

        let mut ids = world.agents.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        let actor_id = ids[0].clone();
        let target_id = ids[1].clone();
        if let Some(target) = world.agents.get_mut(&target_id) {
            target.location_id = "settlement:rivergate".to_string();
            target.occupancy.location_id = "settlement:rivergate".to_string();
        }

        let mut seq = 0_u64;
        world.execute_agent_window(actor_id.clone(), 1, &mut seq);

        assert!(world.events().iter().any(|event| {
            event.event_type == EventType::PlanInterrupted
                && event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("reason"))
                    .and_then(Value::as_str)
                    == Some("location_mismatch")
        }));

        let actor = world.agents.get(&actor_id).expect("actor exists");
        assert!(actor.active_plan.is_none());
        assert_eq!(actor.occupancy.state_tag, "replan");
    }

    #[test]
    fn rejected_mutation_triggers_replanning_state() {
        let mut config = RunConfig::default();
        config.npc_count_min = 2;
        config.npc_count_max = 2;
        let mut world = AgentWorld::new(config);
        world.planner_config.idle_threshold = -100;
        world.operator_catalog = OperatorCatalog::new(vec![contracts::OperatorDef {
            operator_id: "work:work".to_string(),
            family: "work".to_string(),
            display_name: "work".to_string(),
            param_schema: contracts::ParamSchema {
                required: Vec::new(),
                optional: Vec::new(),
            },
            duration_ticks: 1,
            risk: 10,
            visibility: 40,
            preconditions: Vec::new(),
            effects: Vec::new(),
            drive_effects: Vec::new(),
            capability_requirements: Vec::new(),
        }]);

        let mut ids = world.agents.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        let first_priority = world.scheduler.priority_for(1, &ids[0]);
        let second_priority = world.scheduler.priority_for(1, &ids[1]);
        let (winner, loser) = if first_priority >= second_priority {
            (ids[0].clone(), ids[1].clone())
        } else {
            (ids[1].clone(), ids[0].clone())
        };

        let mut seq = 0_u64;
        world.execute_agent_window(winner, 1, &mut seq);
        world.execute_agent_window(loser, 1, &mut seq);

        assert!(world
            .events()
            .iter()
            .any(|event| event.event_type == EventType::ConflictResolved));
        assert!(world.events().iter().any(|event| {
            event.event_type == EventType::PlanInterrupted
                && event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("reason"))
                    .and_then(Value::as_str)
                    == Some("conflict_rejected")
        }));

        let replanning_agents = world
            .agents
            .values()
            .filter(|agent| agent.occupancy.state_tag == "replan")
            .collect::<Vec<_>>();
        assert!(!replanning_agents.is_empty());
    }
}
