use std::collections::{BTreeMap, BTreeSet, VecDeque};
mod commands;
mod commit;
mod events;
mod helpers;
mod init;
mod inspect;
mod reactions;
mod snapshot;
mod step;

use contracts::{
    AccountBalance, ActorRef, AgentAction, AgentSnapshot, Aspiration, CapabilitySet, Command,
    CommandPayload, CommitmentState, DriveSystem, DriveValue, EconomyLedgerSnapshot, Event,
    EventType, IdentityProfile, Observation, PersonalityTraits, ReasonEnvelope, ReasonPacket,
    ResourceKind, RumorPayload, RunConfig, RunMode, RunStatus, ScheduledEvent, SchedulerSnapshot,
    Snapshot, Temperament, WakeReason, WorldFactDelta, WorldMutation, WorldStateSnapshot,
    SCHEMA_VERSION_V1,
};
use serde_json::{json, Value};

use crate::agent::NpcAgent;
use crate::economy::EconomyLedger;
use crate::institution::{InstitutionState, ServiceRequest};
use crate::operator::{OperatorCatalog, PlanningWorldView};
use crate::planner::PlannerConfig;
use crate::social::{EdgeUpdate, RumorNetwork, SocialGraph};
use crate::spatial::{Location, Route, SpatialModel, Weather};

#[derive(Debug, Clone)]
struct QueuedCommand {
    effective_tick: u64,
    insertion_sequence: u64,
    command: Command,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StepMetrics {
    pub advanced_ticks: u64,
    pub processed_batch_tick: u64,
    pub processed_agents: u64,
}

#[derive(Debug, Clone)]
struct AgentEvaluation {
    agent_id: String,
    location_id: String,
    updated_agent: NpcAgent,
    result: crate::agent::TickResult,
}

#[derive(Debug, Default)]
struct ObservationBus {
    inbox_by_agent: BTreeMap<String, VecDeque<Observation>>,
    agents_by_location: BTreeMap<String, BTreeSet<String>>,
}

impl ObservationBus {
    fn register_agent(&mut self, agent_id: &str, location_id: &str) {
        self.agents_by_location
            .entry(location_id.to_string())
            .or_default()
            .insert(agent_id.to_string());
        self.inbox_by_agent.entry(agent_id.to_string()).or_default();
    }

    fn remove_agent(&mut self, agent_id: &str, location_id: &str) {
        if let Some(agents) = self.agents_by_location.get_mut(location_id) {
            agents.remove(agent_id);
            if agents.is_empty() {
                self.agents_by_location.remove(location_id);
            }
        }
        self.inbox_by_agent.remove(agent_id);
    }

    fn move_agent(&mut self, agent_id: &str, from: &str, to: &str) {
        if from != to {
            self.remove_agent(agent_id, from);
            self.register_agent(agent_id, to);
        }
    }

    fn publish_to_agent(&mut self, agent_id: &str, observation: Observation) {
        self.inbox_by_agent
            .entry(agent_id.to_string())
            .or_default()
            .push_back(observation);
    }

    fn local_agents(&self, location_id: &str) -> Vec<String> {
        self.agents_by_location
            .get(location_id)
            .map(|entries| entries.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    fn drain(&mut self, agent_id: &str) -> Vec<Observation> {
        let mut drained = Vec::new();
        if let Some(inbox) = self.inbox_by_agent.get_mut(agent_id) {
            while let Some(entry) = inbox.pop_front() {
                drained.push(entry);
            }
        }
        drained
    }

    fn pending_count(&self, agent_id: &str) -> usize {
        self.inbox_by_agent
            .get(agent_id)
            .map(VecDeque::len)
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
struct SharedArcState {
    arc_id: String,
    participants: Vec<String>,
    arc_kind: String,
    stage_index: usize,
    stage_label: String,
    progress: i64,
    status: String,
    joint_plan_status: Option<String>,
    joint_plan_id: Option<String>,
    joint_plan_proposer: Option<String>,
    joint_plan_last_actor: Option<String>,
    joint_plan_updated_tick: Option<u64>,
    started_tick: u64,
    last_updated_tick: u64,
    root_event_id: Option<String>,
}

#[derive(Debug, Clone)]
struct JointContribution {
    money_paid: i64,
    actions_done: u16,
}

#[derive(Debug, Clone)]
struct JointContract {
    contract_id: String,
    arc_id: String,
    participants: Vec<String>,
    arc_kind: String,
    status: String,
    proposed_tick: u64,
    due_tick: u64,
    required_money_each: i64,
    required_actions_each: u16,
    contributions: BTreeMap<String, JointContribution>,
    last_updated_tick: u64,
}

#[derive(Debug)]
pub struct AgentWorld {
    config: RunConfig,
    status: RunStatus,
    queued_commands: Vec<QueuedCommand>,
    event_log: Vec<Event>,
    reason_packet_log: Vec<ReasonPacket>,
    planner_config: PlannerConfig,
    agents: BTreeMap<String, NpcAgent>,
    reason_envelopes: Vec<ReasonEnvelope>,
    operator_catalog: OperatorCatalog,
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
    last_processed_tick: Option<u64>,
    action_count_this_tick: BTreeMap<String, u16>,
    channel_reservations: BTreeMap<String, BTreeMap<String, u64>>,
    max_actions_per_tick: u16,
    last_operator_by_agent: BTreeMap<String, String>,
    operator_history_by_agent: BTreeMap<String, VecDeque<String>>,
    last_family_by_agent: BTreeMap<String, String>,
    family_streak_by_agent: BTreeMap<String, u16>,
    planner_worker_threads: usize,
    planner_pool: Option<rayon::ThreadPool>,
    observation_bus: ObservationBus,
    recent_events_by_location: BTreeMap<String, VecDeque<String>>,
    event_index_by_id: BTreeMap<String, usize>,
    next_wake_tick_by_agent: BTreeMap<String, u64>,
    wake_reason_by_agent: BTreeMap<String, WakeReason>,
    claimed_resources_this_tick: BTreeMap<String, Vec<String>>,
    shared_arcs: BTreeMap<String, SharedArcState>,
    joint_contracts: BTreeMap<String, JointContract>,
    last_step_metrics: StepMetrics,
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

fn inferred_operator_channels(
    operator_def: Option<&contracts::OperatorDef>,
    operator_id: &str,
    params: &contracts::OperatorParams,
) -> Vec<String> {
    let mut channels = Vec::<String>::new();
    push_unique_channel(&mut channels, "attention");

    let family = operator_def
        .map(|operator| operator.family.as_str())
        .or_else(|| operator_id.split(':').next())
        .unwrap_or("household");
    match family {
        "mobility" => push_unique_channel(&mut channels, "locomotion"),
        "social" | "leisure" | "governance" => push_unique_channel(&mut channels, "speech"),
        "livelihood" | "household" | "health" | "security" | "illicit" => {
            push_unique_channel(&mut channels, "hands")
        }
        _ => {}
    }

    if let Some(operator) = operator_def {
        for param in operator
            .param_schema
            .required
            .iter()
            .chain(operator.param_schema.optional.iter())
        {
            match &param.param_type {
                contracts::ParamType::LocationId => {
                    push_unique_channel(&mut channels, "locomotion")
                }
                contracts::ParamType::NpcId | contracts::ParamType::InstitutionId => {
                    push_unique_channel(&mut channels, "speech")
                }
                contracts::ParamType::ObjectId
                | contracts::ParamType::ResourceType
                | contracts::ParamType::Quantity => push_unique_channel(&mut channels, "hands"),
                contracts::ParamType::Method(_) | contracts::ParamType::FreeText => {}
            }
        }

        for (capability, _) in &operator.capability_requirements {
            match capability.as_str() {
                "physical" | "trade" | "combat" | "stealth" | "care" => {
                    push_unique_channel(&mut channels, "hands")
                }
                "social" | "influence" | "law" => push_unique_channel(&mut channels, "speech"),
                _ => {}
            }
        }
    }

    if params.target_location.is_some() {
        push_unique_channel(&mut channels, "locomotion");
    }
    if params.target_npc.is_some() || params.target_institution.is_some() {
        push_unique_channel(&mut channels, "speech");
    }
    if params.target_object.is_some() || params.resource_type.is_some() || params.quantity.is_some()
    {
        push_unique_channel(&mut channels, "hands");
    }

    channels
}

fn push_unique_channel(channels: &mut Vec<String>, channel: &str) {
    if channels.iter().any(|existing| existing == channel) {
        return;
    }
    channels.push(channel.to_string());
}

fn mix_seed(seed: u64, salt: u64) -> u64 {
    let mut value = seed ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    value ^= value.rotate_left(29);
    value = value.wrapping_mul(0x517C_C1B7_2722_0A95);
    value ^ (value >> 31)
}

fn sample_range_i64(seed: u64, stream: u64, min: i64, max: i64) -> i64 {
    if max <= min {
        return min;
    }
    let span = (max - min + 1) as u64;
    let mixed = mix_seed(seed, stream);
    min + (mixed % span) as i64
}

fn stable_pair_hash(a: &str, b: &str) -> u64 {
    let mut hash = 0_u64;
    for byte in a.as_bytes().iter().chain(b.as_bytes()) {
        hash = hash.rotate_left(5) ^ u64::from(*byte);
        hash = hash.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    hash
}

fn generated_identity(seed: u64) -> IdentityProfile {
    let professions = ["laborer", "farmer", "carter", "fisher", "guard", "artisan"];
    let profession = professions[(mix_seed(seed, 100) % professions.len() as u64) as usize];
    let temperaments = [
        Temperament::Stoic,
        Temperament::Choleric,
        Temperament::Melancholic,
        Temperament::Sanguine,
        Temperament::Volatile,
        Temperament::Calm,
    ];
    let temperament = temperaments[(mix_seed(seed, 101) % temperaments.len() as u64) as usize];

    IdentityProfile {
        profession: profession.to_string(),
        capabilities: CapabilitySet {
            physical: sample_range_i64(seed, 10, 20, 90),
            social: sample_range_i64(seed, 11, 10, 85),
            trade: sample_range_i64(seed, 12, 10, 80),
            combat: sample_range_i64(seed, 13, 5, 80),
            literacy: sample_range_i64(seed, 14, 5, 75),
            influence: sample_range_i64(seed, 15, 5, 80),
            stealth: sample_range_i64(seed, 16, 5, 80),
            care: sample_range_i64(seed, 17, 5, 80),
            law: sample_range_i64(seed, 18, 0, 70),
        },
        personality: PersonalityTraits {
            bravery: sample_range_i64(seed, 20, -30, 90),
            morality: sample_range_i64(seed, 21, -40, 80),
            impulsiveness: sample_range_i64(seed, 22, -20, 90),
            sociability: sample_range_i64(seed, 23, -20, 90),
            ambition: sample_range_i64(seed, 24, -20, 90),
            empathy: sample_range_i64(seed, 25, -30, 90),
            patience: sample_range_i64(seed, 26, -30, 90),
            curiosity: sample_range_i64(seed, 27, -30, 90),
            jealousy: sample_range_i64(seed, 28, -40, 80),
            pride: sample_range_i64(seed, 29, -30, 90),
            vindictiveness: sample_range_i64(seed, 30, -40, 80),
            greed: sample_range_i64(seed, 31, -40, 90),
            loyalty: sample_range_i64(seed, 32, -30, 90),
            honesty: sample_range_i64(seed, 33, -50, 90),
            piety: sample_range_i64(seed, 34, -40, 90),
            vanity: sample_range_i64(seed, 35, -40, 90),
            humor: sample_range_i64(seed, 36, -40, 90),
        },
        temperament,
        values: vec![
            "survival".to_string(),
            if sample_range_i64(seed, 40, 0, 1) == 0 {
                "family".to_string()
            } else {
                "wealth".to_string()
            },
        ],
        likes: vec![if sample_range_i64(seed, 41, 0, 1) == 0 {
            "warm_meals".to_string()
        } else {
            "quiet_nights".to_string()
        }],
        dislikes: vec![if sample_range_i64(seed, 42, 0, 1) == 0 {
            "hunger".to_string()
        } else {
            "crowds".to_string()
        }],
    }
}

fn generated_drives(config: &RunConfig, seed: u64) -> DriveSystem {
    let mut drives = default_drives(config);
    drives.food.current = sample_range_i64(seed, 50, 25, 88);
    drives.shelter.current = sample_range_i64(seed, 51, 15, 82);
    drives.income.current = sample_range_i64(seed, 52, 20, 88);
    drives.safety.current = sample_range_i64(seed, 53, 10, 80);
    drives.belonging.current = sample_range_i64(seed, 54, 5, 78);
    drives.status.current = sample_range_i64(seed, 55, 5, 78);
    drives.health.current = sample_range_i64(seed, 56, 10, 82);
    drives.food.decay_rate = sample_range_i64(seed, 57, 1, 4);
    drives.income.decay_rate = sample_range_i64(seed, 58, 1, 4);
    drives.health.decay_rate = sample_range_i64(seed, 59, 1, 4);
    drives
}

#[cfg(test)]
mod tests;
