//! NPC Agency System contract types.
//!
//! This module contains all data models for the agent-based NPC architecture:
//! identity, drives, memory, beliefs, operators, planning, scheduling,
//! conflict resolution, economy, social, spatial, institutions, observability,
//! and snapshot/configuration extensions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// 1.1 — Identity, Drive, and Memory contract types
// ---------------------------------------------------------------------------

/// Static and slowly-evolving NPC attributes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdentityProfile {
    pub profession: String,
    pub capabilities: CapabilitySet,
    pub personality: PersonalityTraits,
    pub temperament: Temperament,
    pub values: Vec<NpcValue>,
    pub likes: Vec<String>,
    pub dislikes: Vec<String>,
}

/// Big-Five-inspired personality traits scaled to [-100, 100].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalityTraits {
    pub bravery: i64,
    pub morality: i64,
    pub impulsiveness: i64,
    pub sociability: i64,
    pub ambition: i64,
    pub empathy: i64,
    pub patience: i64,
    pub curiosity: i64,
    pub jealousy: i64,
    pub pride: i64,
    pub vindictiveness: i64,
    pub greed: i64,
    pub loyalty: i64,
    pub honesty: i64,
    pub piety: i64,
    pub vanity: i64,
    pub humor: i64,
}

/// Capability scores for an NPC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilitySet {
    pub physical: i64,
    pub social: i64,
    pub trade: i64,
    pub combat: i64,
    pub literacy: i64,
    pub influence: i64,
    pub stealth: i64,
    pub care: i64,
    pub law: i64,
}

/// Emotional temperament archetype.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Temperament {
    Choleric,
    Sanguine,
    Melancholic,
    Phlegmatic,
}

/// A value an NPC holds dear (e.g. family, honour, wealth).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NpcValue {
    pub name: String,
    pub weight: i64,
}

/// A single drive with current pressure, decay rate, and urgency threshold.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriveValue {
    /// 0 = fully satisfied, 100 = critical.
    pub current: i64,
    /// Per-tick base decay (context-modified at runtime).
    pub decay_rate: i64,
    /// Pressure above this flags the drive as urgent.
    pub urgency_threshold: i64,
}

/// The seven fundamental NPC drives.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DriveKind {
    Food,
    Shelter,
    Income,
    Safety,
    Belonging,
    Status,
    Health,
}

/// Contextual modifiers that affect drive decay rates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriveContext {
    /// True when the NPC is performing physical labour.
    pub physical_labor: bool,
    /// True when the NPC is outdoors / unsheltered.
    pub exposed: bool,
    /// True when the NPC is in a dangerous area.
    pub in_danger: bool,
    /// True when the NPC is socially isolated.
    pub isolated: bool,
}

/// A single memory entry stored by the memory system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryEntry {
    pub memory_id: String,
    pub tick: u64,
    pub topic: String,
    pub details: MemoryDetails,
    /// Decays over time; 0–100.
    pub confidence: i64,
    /// Importance weight for eviction ordering.
    pub salience: i64,
    pub source: MemorySource,
}

/// Flexible detail payload for a memory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryDetails {
    pub description: String,
    pub actors: Vec<String>,
    pub location_id: String,
    #[serde(default)]
    pub extra: Option<Value>,
}

/// How a memory was acquired.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemorySource {
    DirectObservation,
    Rumor,
    Deduction,
    Teaching,
}

/// Extended belief claim with confidence decay and source tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BeliefClaim {
    pub claim_id: String,
    pub topic: String,
    pub content: String,
    pub confidence: i64,
    pub source: AgencyBeliefSource,
    pub last_updated_tick: u64,
}

/// Source attribution for a belief in the agency system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AgencyBeliefSource {
    DirectPerception { observer_id: String },
    Rumor { source_npc_id: String, trust: i64 },
    Deduction,
    OfficialNotice,
}

/// NPC long-term aspiration with change tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Aspiration {
    pub current: String,
    pub previous: Option<String>,
    pub changed_at_tick: Option<u64>,
    pub change_cause: Option<String>,
}

// ---------------------------------------------------------------------------
// 1.2 — Operator, Planner, and Plan contract types
// ---------------------------------------------------------------------------

/// Schema describing the parameters an operator accepts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParamSchema {
    pub required: Vec<ParamDef>,
    #[serde(default)]
    pub optional: Vec<ParamDef>,
}

/// A single parameter definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParamDef {
    pub name: String,
    pub param_type: ParamType,
}

/// The type of a parameter binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    NpcId,
    LocationId,
    ObjectId,
    InstitutionId,
    ResourceType,
    Quantity,
    Method(Vec<String>),
    FreeText,
}

/// Concrete parameter bindings for an operator invocation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct OperatorParams {
    pub target_npc: Option<String>,
    pub target_location: Option<String>,
    pub target_object: Option<String>,
    pub target_institution: Option<String>,
    pub resource_type: Option<String>,
    pub quantity: Option<i64>,
    pub method: Option<String>,
    pub context: Option<String>,
}

/// An operator definition in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorDef {
    pub operator_id: String,
    pub family: String,
    pub display_name: String,
    pub param_schema: ParamSchema,
    pub duration_ticks: u64,
    pub risk: i64,
    pub visibility: i64,
    pub preconditions: Vec<FactPredicate>,
    pub effects: Vec<EffectTemplate>,
    pub drive_effects: Vec<(DriveKind, i64)>,
    pub capability_requirements: Vec<(String, i64)>,
}

/// An operator bound with concrete parameters, ready for execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundOperator {
    pub operator_id: String,
    pub parameters: OperatorParams,
    pub duration_ticks: u64,
    pub preconditions: Vec<FactPredicate>,
    pub effects: Vec<AgencyWorldFactDelta>,
}

/// A candidate plan produced by the planner.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidatePlan {
    pub plan_id: String,
    pub goal: Goal,
    /// 1–6 bound operators.
    pub steps: Vec<BoundOperator>,
    pub planning_mode: PlanningMode,
}

/// Reactive vs deliberate planning.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlanningMode {
    Reactive,
    Deliberate,
}

/// A high-level goal that motivates a plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Goal {
    pub goal_id: String,
    pub description: String,
    pub target_drive: Option<DriveKind>,
}

/// Numeric score for a candidate plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanScore {
    pub plan_id: String,
    pub total: i64,
    pub drive_urgency_component: i64,
    pub personality_component: i64,
    pub risk_component: i64,
    pub opportunity_cost_component: i64,
    pub social_component: i64,
}

/// The result of plan selection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanSelection {
    Selected { plan: CandidatePlan, score: PlanScore },
    Idle { reason: String },
}

/// An active plan being executed by an agent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivePlan {
    pub plan_id: String,
    pub goal: Goal,
    pub steps: Vec<BoundOperator>,
    pub current_step_index: usize,
    pub planning_mode: PlanningMode,
    pub started_tick: u64,
}

/// A predicate over world facts used as a precondition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FactPredicate {
    pub fact_key: String,
    pub operator: PredicateOp,
    pub value: i64,
}

/// Comparison operator for fact predicates.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// A template for an effect that an operator produces.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectTemplate {
    pub fact_key: String,
    pub delta_expr: String,
}

/// A concrete world-fact delta produced by a bound operator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgencyWorldFactDelta {
    pub fact_key: String,
    pub delta: i64,
    pub location_id: String,
}

// ---------------------------------------------------------------------------
// 1.3 — Scheduler, Conflict, Economy, and Observability contract types
// ---------------------------------------------------------------------------

/// An event in the agent scheduler's priority queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledEvent {
    pub wake_tick: u64,
    pub priority: u64,
    pub agent_id: String,
    pub reason: WakeReason,
}

/// Why an agent is being woken.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WakeReason {
    Idle,
    PlanStepComplete,
    Interrupted,
    Reactive,
}

/// The output of an agent tick.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentAction {
    Execute(BoundOperator),
    Continue,
    Idle(String),
    Replan,
}

/// A mutation an agent wants to apply to the world.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorldMutation {
    pub agent_id: String,
    pub operator_id: String,
    pub deltas: Vec<AgencyWorldFactDelta>,
    pub resource_transfers: Vec<ResourceTransfer>,
}

/// A single resource transfer within a mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceTransfer {
    pub from_account: String,
    pub to_account: String,
    pub kind: ResourceKind,
    pub amount: i64,
}

/// Result of a successfully committed mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitResult {
    pub event_id: String,
    pub tick: u64,
}

/// Result when a mutation is rejected due to conflict.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConflictResult {
    pub conflicting_agent_id: String,
    pub conflicting_resource: String,
    pub reason: String,
}

/// Kinds of trackable resources in the economy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Money,
    Food,
    Fuel,
    Medicine,
    CraftInputs,
    CraftOutputs,
}

/// An account balance in the economy ledger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AccountBalance {
    pub money: i64,
    pub food: i64,
    pub fuel: i64,
    pub medicine: i64,
}

/// Extended accounting transfer with typed resource kind.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountingTransfer {
    pub transfer_id: String,
    pub tick: u64,
    pub from_account: String,
    pub to_account: String,
    pub resource_kind: ResourceKind,
    pub amount: i64,
    pub cause_event_id: String,
}

/// A perceivable world event distributed via the observation bus.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Observation {
    pub event_id: String,
    pub tick: u64,
    pub location_id: String,
    pub event_type: String,
    pub actors: Vec<String>,
    pub visibility: i64,
    pub details: Value,
}

/// Extended relationship edge with obligation, grievance, fear, respect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipEdge {
    pub trust: i64,
    pub reputation: i64,
    pub obligation: i64,
    pub grievance: i64,
    pub fear: i64,
    pub respect: i64,
}

impl Default for RelationshipEdge {
    fn default() -> Self {
        Self {
            trust: 0,
            reputation: 0,
            obligation: 0,
            grievance: 0,
            fear: 0,
            respect: 0,
        }
    }
}

/// A rumor payload propagated through the social graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RumorPayload {
    pub rumor_id: String,
    pub origin_npc_id: String,
    pub topic: String,
    pub content: String,
    pub hops: u32,
    pub distorted: bool,
}

/// Full explainability record for an NPC action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReasonEnvelope {
    pub agent_id: String,
    pub tick: u64,
    pub goal: Goal,
    pub selected_plan: String,
    pub operator_chain: Vec<String>,
    pub drive_pressures: Vec<(DriveKind, i64)>,
    pub rejected_alternatives: Vec<RejectedPlan>,
    pub contextual_constraints: Vec<String>,
    pub planning_mode: PlanningMode,
}

/// A plan that was considered but not selected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RejectedPlan {
    pub plan_id: String,
    pub goal: Goal,
    pub score: i64,
    pub rejection_reason: String,
}

// ---------------------------------------------------------------------------
// 1.4 — Snapshot, Configuration, and Event Type extensions
// ---------------------------------------------------------------------------

/// Per-agent snapshot for save/restore.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentSnapshot {
    pub agent_id: String,
    pub identity: IdentityProfile,
    pub drives: AgentDrivesSnapshot,
    pub beliefs: Vec<BeliefClaim>,
    pub memories: Vec<MemoryEntry>,
    pub active_plan: Option<ActivePlan>,
    pub occupancy: AgentOccupancySnapshot,
    pub aspiration: Aspiration,
    pub location_id: String,
    pub next_wake_tick: u64,
}

/// Snapshot of all 7 drives for an agent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentDrivesSnapshot {
    pub food: DriveValue,
    pub shelter: DriveValue,
    pub income: DriveValue,
    pub safety: DriveValue,
    pub belonging: DriveValue,
    pub status: DriveValue,
    pub health: DriveValue,
}

/// Snapshot of agent occupancy state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentOccupancySnapshot {
    pub kind: String,
    pub until_tick: u64,
    pub interruptible: bool,
}

/// Scheduler state for deterministic restore.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerSnapshot {
    pub current_tick: u64,
    pub events: Vec<ScheduledEvent>,
}

/// World state snapshot (high-level; subsystem snapshots nested).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorldStateSnapshot {
    pub locations: BTreeMap<String, Value>,
    pub routes: BTreeMap<String, Value>,
    pub social_edges: Vec<(String, String, RelationshipEdge)>,
    pub groups: BTreeMap<String, Value>,
    pub institutions: BTreeMap<String, Value>,
    pub weather: Value,
}

/// Economy ledger snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EconomyLedgerSnapshot {
    pub accounts: BTreeMap<String, AccountBalance>,
    pub transfers: Vec<AccountingTransfer>,
}

/// New event types for the agency system.
///
/// These extend the existing `EventType` enum in the contracts crate.
/// They are kept in a separate enum to avoid breaking existing code.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AgencyEventType {
    AgentWake,
    AgentIdle,
    PlanCreated,
    PlanStepStarted,
    PlanStepCompleted,
    PlanInterrupted,
    PlanAbandoned,
    ConflictDetected,
    ConflictResolved,
    DriveThresholdCrossed,
    AspirationChanged,
    AgencyBeliefUpdated,
    BeliefReconciled,
    RumorPropagated,
    RumorDistorted,
    AgencyGroupFormed,
    AgencyGroupDissolved,
    InstitutionQueueFull,
    InstitutionCorruptionEvent,
}

/// Extended run configuration for the agency system.
///
/// Layered on top of the base `RunConfig`. All fields have documented defaults.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgencyRunConfig {
    // --- Planner ---
    /// Beam search width for candidate plan generation.
    #[serde(default = "default_planning_beam_width")]
    pub planning_beam_width: u32,
    /// Maximum planning horizon in operator steps.
    #[serde(default = "default_planning_horizon")]
    pub planning_horizon: u32,
    /// Plans scoring below this are replaced with idle.
    #[serde(default = "default_idle_threshold")]
    pub idle_threshold: i64,

    // --- Scheduler ---
    /// Maximum reaction chain depth per originating event.
    #[serde(default = "default_max_reaction_depth")]
    pub max_reaction_depth: u32,
    /// Number of ticks in one scheduling window.
    #[serde(default = "default_scheduling_window_size")]
    pub scheduling_window_size: u64,

    // --- Social ---
    /// Per-tick trust decay rate.
    #[serde(default = "default_trust_decay_rate")]
    pub trust_decay_rate: i64,
    /// Probability (0–100) of rumor distortion per hop.
    #[serde(default = "default_rumor_distortion_probability")]
    pub rumor_distortion_probability: u32,
    /// Cohesion score below which a group dissolves.
    #[serde(default = "default_group_cohesion_threshold")]
    pub group_cohesion_threshold: i64,

    // --- Economy cadences (in ticks) ---
    /// Rent processing period in ticks.
    #[serde(default = "default_rent_period")]
    pub rent_period: u64,
    /// Wage processing period in ticks.
    #[serde(default = "default_wage_period")]
    pub wage_period: u64,

    // --- Production ---
    /// Base production rate multiplier (percent, 100 = normal).
    #[serde(default = "default_production_rate")]
    pub production_rate: u32,
    /// Spoilage rate for perishables (percent per tick).
    #[serde(default = "default_spoilage_rate")]
    pub spoilage_rate: u32,

    // --- Market ---
    /// How often the market clears, in ticks.
    #[serde(default = "default_market_clearing_frequency")]
    pub market_clearing_frequency: u64,
}

fn default_planning_beam_width() -> u32 { 24 }
fn default_planning_horizon() -> u32 { 4 }
fn default_idle_threshold() -> i64 { 10 }
fn default_max_reaction_depth() -> u32 { 5 }
fn default_scheduling_window_size() -> u64 { 1 }
fn default_trust_decay_rate() -> i64 { 1 }
fn default_rumor_distortion_probability() -> u32 { 15 }
fn default_group_cohesion_threshold() -> i64 { 20 }
fn default_rent_period() -> u64 { 720 }   // 30 days * 24 ticks
fn default_wage_period() -> u64 { 24 }    // daily
fn default_production_rate() -> u32 { 100 }
fn default_spoilage_rate() -> u32 { 2 }
fn default_market_clearing_frequency() -> u64 { 24 } // daily

impl Default for AgencyRunConfig {
    fn default() -> Self {
        Self {
            planning_beam_width: default_planning_beam_width(),
            planning_horizon: default_planning_horizon(),
            idle_threshold: default_idle_threshold(),
            max_reaction_depth: default_max_reaction_depth(),
            scheduling_window_size: default_scheduling_window_size(),
            trust_decay_rate: default_trust_decay_rate(),
            rumor_distortion_probability: default_rumor_distortion_probability(),
            group_cohesion_threshold: default_group_cohesion_threshold(),
            rent_period: default_rent_period(),
            wage_period: default_wage_period(),
            production_rate: default_production_rate(),
            spoilage_rate: default_spoilage_rate(),
            market_clearing_frequency: default_market_clearing_frequency(),
        }
    }
}
