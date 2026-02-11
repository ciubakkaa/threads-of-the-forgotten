//! v1 cross-boundary contracts for kernel, API, persistence, and observatory.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA_VERSION_V1: &str = "1.0";
pub const TICKS_PER_DAY: u64 = 24;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RegionId {
    Crownvale,
    IronreachMarch,
    SaltmereCoast,
    Sunsteppe,
    Fenreach,
    AshenWilds,
    SkylarkRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunConfig {
    pub schema_version: String,
    pub run_id: String,
    #[serde(with = "serde_u64_string")]
    pub seed: u64,
    pub duration_days: u32,
    pub region_id: RegionId,
    pub snapshot_every_ticks: u64,
    #[serde(default = "default_npc_count_min")]
    pub npc_count_min: u16,
    #[serde(default = "default_npc_count_max")]
    pub npc_count_max: u16,
    #[serde(default)]
    pub enabled_systems: BTreeMap<String, bool>,
    #[serde(default)]
    pub scenario_flags: BTreeMap<String, bool>,
    pub notes: Option<String>,
}

impl RunConfig {
    pub fn max_ticks(&self) -> u64 {
        u64::from(self.duration_days) * TICKS_PER_DAY
    }

    pub fn normalized_npc_bounds(&self) -> (u16, u16) {
        let min = self.npc_count_min.max(1);
        let max = self.npc_count_max.max(min);
        (min, max)
    }
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: "run_local_001".to_string(),
            seed: 1337,
            duration_days: 30,
            region_id: RegionId::Crownvale,
            snapshot_every_ticks: TICKS_PER_DAY,
            npc_count_min: default_npc_count_min(),
            npc_count_max: default_npc_count_max(),
            enabled_systems: BTreeMap::new(),
            scenario_flags: BTreeMap::new(),
            notes: None,
        }
    }
}

fn default_npc_count_min() -> u16 {
    5
}

fn default_npc_count_max() -> u16 {
    10
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Running,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunStatus {
    pub schema_version: String,
    pub run_id: String,
    pub current_tick: u64,
    pub max_ticks: u64,
    pub mode: RunMode,
    pub queue_depth: usize,
}

impl RunStatus {
    pub fn is_complete(&self) -> bool {
        self.current_tick >= self.max_ticks
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "run_id={} tick={}/{} mode={:?} queue_depth={}",
            self.run_id, self.current_tick, self.max_ticks, self.mode, self.queue_depth
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandType {
    SimStart,
    SimPause,
    SimStepTick,
    SimRunToTick,
    InjectRumor,
    InjectSpawnCaravan,
    InjectRemoveNpc,
    InjectForceBadHarvest,
    InjectSetWinterSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandPayload {
    SimStart,
    SimPause,
    SimStepTick {
        steps: u64,
    },
    SimRunToTick {
        target_tick: u64,
    },
    InjectRumor {
        location_id: String,
        rumor_text: String,
    },
    InjectSpawnCaravan {
        origin_settlement_id: String,
        destination_settlement_id: String,
    },
    InjectRemoveNpc {
        npc_id: String,
    },
    InjectForceBadHarvest {
        settlement_id: String,
    },
    InjectSetWinterSeverity {
        severity: u8,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Command {
    pub schema_version: String,
    pub command_id: String,
    pub run_id: String,
    pub issued_at_tick: u64,
    pub command_type: CommandType,
    pub payload: CommandPayload,
}

impl Command {
    pub fn new(
        command_id: impl Into<String>,
        run_id: impl Into<String>,
        issued_at_tick: u64,
        command_type: CommandType,
        payload: CommandPayload,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            command_id: command_id.into(),
            run_id: run_id.into(),
            issued_at_tick,
            command_type,
            payload,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    RunNotFound,
    InvalidCommand,
    InvalidQuery,
    TickOutOfRange,
    ContractVersionUnsupported,
    RunStateConflict,
    InternalError,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiError {
    pub schema_version: String,
    pub error_code: ErrorCode,
    pub message: String,
    pub details: Option<String>,
}

impl ApiError {
    pub fn new(error_code: ErrorCode, message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            error_code,
            message: message.into(),
            details,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandResult {
    pub schema_version: String,
    pub command_id: String,
    pub run_id: String,
    pub accepted: bool,
    pub error: Option<ApiError>,
}

impl CommandResult {
    pub fn accepted(command: &Command) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            command_id: command.command_id.clone(),
            run_id: command.run_id.clone(),
            accepted: true,
            error: None,
        }
    }

    pub fn rejected(command: &Command, error: ApiError) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            command_id: command.command_id.clone(),
            run_id: command.run_id.clone(),
            accepted: false,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorRef {
    pub actor_id: String,
    pub actor_kind: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MotiveFamily {
    Survival,
    Security,
    Dignity,
    Belonging,
    Ambition,
    JusticeGrievance,
    Ideology,
    Attachment,
    Coercion,
    CuriosityMeaning,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FeasibilityCheck {
    PressureIndexHigh,
    PressureIndexLow,
    RumorHeatHigh,
    HarvestShockHigh,
    WinterSeverityHigh,
    LawCaseLoadHigh,
    IsWanted,
    HasStolenItem,
    LivelihoodAvailable,
    MarketAccessible,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AffordanceVerb {
    WorkForFood,
    WorkForCoin,
    PayRent,
    SeekShelter,
    TendFields,
    CraftGoods,
    ShareMeal,
    FormMutualAidGroup,
    DefendPatron,
    SpreadAccusation,
    MediateDispute,
    TrainApprentice,
    ShareRumor,
    PatrolRoad,
    AssistNeighbor,
    ConverseNeighbor,
    LendCoin,
    CourtRomance,
    SeekTreatment,
    ObserveNotableEvent,
    Forage,
    StealSupplies,
    InvestigateRumor,
    GatherFirewood,
    CoverAbsentNeighbor,
    OrganizeWatch,
    FenceGoods,
    AvoidPatrols,
    QuestionTravelers,
    RationGrain,
    RepairHearth,
    CollectTestimony,
    TradeVisit,
}

impl AffordanceVerb {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkForFood => "work_for_food",
            Self::WorkForCoin => "work_for_coin",
            Self::PayRent => "pay_rent",
            Self::SeekShelter => "seek_shelter",
            Self::TendFields => "tend_fields",
            Self::CraftGoods => "craft_goods",
            Self::ShareMeal => "share_meal",
            Self::FormMutualAidGroup => "form_mutual_aid_group",
            Self::DefendPatron => "defend_patron",
            Self::SpreadAccusation => "spread_accusation",
            Self::MediateDispute => "mediate_dispute",
            Self::TrainApprentice => "train_apprentice",
            Self::ShareRumor => "share_rumor",
            Self::PatrolRoad => "patrol_road",
            Self::AssistNeighbor => "assist_neighbor",
            Self::ConverseNeighbor => "converse_neighbor",
            Self::LendCoin => "lend_coin",
            Self::CourtRomance => "court_romance",
            Self::SeekTreatment => "seek_treatment",
            Self::ObserveNotableEvent => "observe_notable_event",
            Self::Forage => "forage",
            Self::StealSupplies => "steal_supplies",
            Self::InvestigateRumor => "investigate_rumor",
            Self::GatherFirewood => "gather_firewood",
            Self::CoverAbsentNeighbor => "cover_absent_neighbor",
            Self::OrganizeWatch => "organize_watch",
            Self::FenceGoods => "fence_goods",
            Self::AvoidPatrols => "avoid_patrols",
            Self::QuestionTravelers => "question_travelers",
            Self::RationGrain => "ration_grain",
            Self::RepairHearth => "repair_hearth",
            Self::CollectTestimony => "collect_testimony",
            Self::TradeVisit => "trade_visit",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ShelterStatus {
    Stable,
    Precarious,
    Unsheltered,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    Hostile,
    Distrusting,
    Cautious,
    Familiar,
    Trusted,
    Bonded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ContractCompensationType {
    Coin,
    Board,
    Mixed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ContractCadence {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BeliefSource {
    Witnessed,
    TrustedRumor,
    UntrustedRumor,
    OfficialNotice,
    Hearsay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NpcHouseholdLedgerSnapshot {
    pub npc_id: String,
    pub household_id: String,
    pub wallet: i64,
    pub debt_balance: i64,
    pub food_reserve_days: i64,
    pub shelter_status: ShelterStatus,
    pub dependents_count: u8,
    #[serde(default)]
    pub profession: String,
    #[serde(default)]
    pub aspiration: String,
    #[serde(default)]
    pub health: i64,
    #[serde(default)]
    pub illness_ticks: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HouseholdLedgerSnapshot {
    pub household_id: String,
    pub member_npc_ids: Vec<String>,
    pub shared_pantry_stock: i64,
    pub fuel_stock: i64,
    pub rent_due_tick: u64,
    #[serde(default)]
    pub rent_cadence_ticks: u64,
    #[serde(default)]
    pub rent_amount: i64,
    #[serde(default)]
    pub rent_reserve_coin: i64,
    #[serde(default)]
    pub landlord_balance: i64,
    pub eviction_risk_score: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmploymentContractRecord {
    pub contract_id: String,
    pub employer_id: String,
    pub worker_id: String,
    pub settlement_id: String,
    pub compensation_type: ContractCompensationType,
    pub cadence: ContractCadence,
    pub wage_amount: i64,
    pub reliability_score: u8,
    pub active: bool,
    pub breached: bool,
    pub next_payment_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementLaborMarketSnapshot {
    pub settlement_id: String,
    pub open_roles: u32,
    pub wage_band_low: i64,
    pub wage_band_high: i64,
    pub underemployment_index: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementStockLedger {
    pub settlement_id: String,
    pub staples: i64,
    pub fuel: i64,
    pub medicine: i64,
    pub craft_inputs: i64,
    pub local_price_pressure: i64,
    #[serde(default)]
    pub coin_reserve: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionNodeState {
    pub node_id: String,
    pub settlement_id: String,
    pub node_kind: String,
    pub input_backlog: i64,
    pub output_backlog: i64,
    pub spoilage_timer: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipEdgeState {
    pub source_npc_id: String,
    pub target_npc_id: String,
    pub trust: i64,
    pub attachment: i64,
    pub obligation: i64,
    pub grievance: i64,
    pub fear: i64,
    pub respect: i64,
    pub jealousy: i64,
    pub trust_level: TrustLevel,
    pub recent_interaction_tick: u64,
    #[serde(default)]
    pub compatibility_score: i64,
    #[serde(default)]
    pub shared_context_tags: Vec<String>,
    #[serde(default)]
    pub relation_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BeliefClaimState {
    pub claim_id: String,
    pub npc_id: String,
    pub settlement_id: String,
    pub confidence: f32,
    pub source: BeliefSource,
    pub distortion_score: f32,
    pub willingness_to_share: f32,
    pub truth_link: Option<String>,
    pub claim_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstitutionProfileState {
    pub settlement_id: String,
    pub enforcement_capacity: i64,
    pub corruption_level: i64,
    pub bias_level: i64,
    pub response_latency_ticks: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupEntityState {
    pub group_id: String,
    pub settlement_id: String,
    pub leader_npc_id: String,
    #[serde(default)]
    pub member_npc_ids: Vec<String>,
    #[serde(default)]
    pub norm_tags: Vec<String>,
    pub cohesion_score: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MobilityRouteState {
    pub route_id: String,
    pub origin_settlement_id: String,
    pub destination_settlement_id: String,
    pub travel_time_ticks: u64,
    pub hazard_score: i64,
    pub weather_window_open: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NarrativeWhyChainSummary {
    pub summary_id: String,
    pub tick: u64,
    pub location_id: String,
    pub actor_id: String,
    pub motive_chain: Vec<String>,
    pub failed_alternatives: Vec<String>,
    pub social_consequence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpportunityState {
    pub opportunity_id: String,
    pub npc_id: String,
    pub location_id: String,
    pub action: String,
    pub source: String,
    pub opened_tick: u64,
    pub expires_tick: u64,
    pub utility_hint: i64,
    #[serde(default)]
    pub constraints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitmentState {
    pub commitment_id: String,
    pub npc_id: String,
    pub action_family: String,
    pub started_tick: u64,
    pub due_tick: u64,
    pub cadence_ticks: u64,
    pub progress_ticks: u64,
    pub inertia_score: i64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeBudgetState {
    pub npc_id: String,
    pub tick: u64,
    pub sleep_hours: i64,
    pub work_hours: i64,
    pub care_hours: i64,
    pub travel_hours: i64,
    pub social_hours: i64,
    pub recovery_hours: i64,
    pub free_hours: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NpcOccupancyKind {
    Idle,
    Resting,
    Loitering,
    Traveling,
    ExecutingPlanStep,
    Recovering,
    SocialPresence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NpcDriveState {
    pub npc_id: String,
    pub tick: u64,
    pub need_food: i64,
    pub need_shelter: i64,
    pub need_income: i64,
    pub need_safety: i64,
    pub need_belonging: i64,
    pub need_status: i64,
    pub need_recovery: i64,
    pub stress: i64,
    #[serde(default)]
    pub active_obligations: Vec<String>,
    #[serde(default)]
    pub active_aspirations: Vec<String>,
    #[serde(default)]
    pub moral_bounds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NpcOccupancyState {
    pub npc_id: String,
    pub tick: u64,
    pub occupancy: NpcOccupancyKind,
    pub state_tag: String,
    pub until_tick: u64,
    pub interruptible: bool,
    pub location_id: String,
    pub active_plan_id: Option<String>,
    pub active_operator_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorDefinition {
    pub operator_id: String,
    pub family: String,
    pub action: String,
    pub duration_ticks: u64,
    pub risk: i64,
    pub visibility: i64,
    #[serde(default)]
    pub preconditions: Vec<String>,
    #[serde(default)]
    pub effects: Vec<String>,
    #[serde(default)]
    pub resource_delta: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanCandidateState {
    pub plan_id: String,
    pub npc_id: String,
    pub tick: u64,
    pub goal_id: String,
    pub action: String,
    pub utility_score: i64,
    pub risk_score: i64,
    pub temporal_fit: i64,
    #[serde(default)]
    pub operator_chain_ids: Vec<String>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionExecutionState {
    pub execution_id: String,
    pub npc_id: String,
    pub tick: u64,
    pub plan_id: String,
    pub goal_id: String,
    pub action: String,
    pub active_step_index: usize,
    pub remaining_ticks: u64,
    pub interruption_policy: String,
    #[serde(default)]
    pub operator_chain_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorldFactDelta {
    pub fact_key: String,
    pub delta: i64,
    pub from_value: i64,
    pub to_value: i64,
    pub location_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventSynthesisTrace {
    pub trace_id: String,
    pub tick: u64,
    pub run_id: String,
    pub source_plan_id: String,
    pub source_operator_id: String,
    pub source_event_id: String,
    pub event_type: EventType,
    #[serde(default)]
    pub trigger_facts: Vec<String>,
    #[serde(default)]
    pub rejected_alternatives: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AtomicOpKind {
    Perceive,
    Observe,
    Stealth,
    Approach,
    Greet,
    Converse,
    AskQuestion,
    ShareInfo,
    ShareRumor,
    Compliment,
    Flirt,
    GiftOffer,
    Promise,
    Negotiate,
    AgreeTerms,
    Disagree,
    Apologize,
    Insult,
    Threaten,
    Challenge,
    Strike,
    Defend,
    Flee,
    CallGuards,
    ReportIncident,
    Arrest,
    Buy,
    Sell,
    Barter,
    LendCoin,
    BorrowCoin,
    PayRent,
    ReceiveWage,
    WorkShift,
    Craft,
    Farm,
    Forage,
    Eat,
    Drink,
    Rest,
    SeekShelter,
    SeekTreatment,
    Travel,
    Escort,
    Patrol,
    Mediate,
    InviteToGroup,
    JoinGroup,
    LeaveGroup,
    Marry,
    Breakup,
    CareForChild,
    Train,
    Teach,
    TradeVisit,
    Celebrate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AtomicOp {
    pub kind: AtomicOpKind,
    pub target_id: Option<String>,
    pub item_kind: Option<String>,
    pub location_id: Option<String>,
    pub tone: Option<String>,
    pub intensity: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionTemplateDefinition {
    pub template_id: String,
    pub base_action: String,
    pub goal_family: String,
    pub op_sequence: Vec<AtomicOpKind>,
    pub required_capability: Option<String>,
    #[serde(default)]
    pub optional_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NpcComposablePlanState {
    pub npc_id: String,
    pub tick: u64,
    pub goal_family: String,
    pub template_id: String,
    pub base_action: String,
    pub composed_action: String,
    pub ops: Vec<AtomicOp>,
    #[serde(default)]
    pub rejected_alternatives: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProcessKind {
    Romance,
    BusinessPartnership,
    ConflictSpiral,
    HouseholdFormation,
    Apprenticeship,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStatus {
    Active,
    Dormant,
    Resolved,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessInstanceState {
    pub process_id: String,
    pub kind: ProcessKind,
    pub status: ProcessStatus,
    pub stage: String,
    pub participants: Vec<String>,
    pub started_tick: u64,
    pub last_updated_tick: u64,
    pub last_event_id: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NpcCapabilityProfile {
    pub npc_id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PerceptionRecord {
    pub observer_npc_id: String,
    pub tick: u64,
    pub location_id: String,
    pub observed_event_id: String,
    pub confidence: i64,
    pub bias: i64,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryRecord {
    pub npc_id: String,
    pub memory_id: String,
    pub tick: u64,
    pub topic: String,
    pub salience: i64,
    pub valence: i64,
    pub source_npc_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketClearingState {
    pub settlement_id: String,
    pub staples_price_index: i64,
    pub fuel_price_index: i64,
    pub medicine_price_index: i64,
    pub wage_pressure: i64,
    pub shortage_score: i64,
    pub unmet_demand: i64,
    pub cleared_tick: u64,
    pub market_cleared: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountingTransferState {
    pub transfer_id: String,
    pub tick: u64,
    pub settlement_id: String,
    pub from_account: String,
    pub to_account: String,
    pub resource_kind: String,
    pub amount: i64,
    pub cause_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstitutionQueueState {
    pub settlement_id: String,
    pub pending_cases: i64,
    pub processed_cases: i64,
    pub dropped_cases: i64,
    pub avg_response_latency: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    SystemTick,
    NpcActionCommitted,
    CommandApplied,
    RumorInjected,
    CaravanSpawned,
    NpcRemoved,
    BadHarvestForced,
    WinterSeveritySet,
    TheftCommitted,
    ItemTransferred,
    InvestigationProgressed,
    ArrestMade,
    SiteDiscovered,
    LeverageGained,
    RelationshipShifted,
    PressureEconomyUpdated,
    HouseholdConsumptionApplied,
    RentDue,
    RentPaid,
    RentUnpaid,
    EvictionRiskChanged,
    HouseholdBufferExhausted,
    JobSought,
    ContractSigned,
    WagePaid,
    WageDelayed,
    ContractBreached,
    EmploymentTerminated,
    ProductionStarted,
    ProductionCompleted,
    SpoilageOccurred,
    StockShortage,
    StockRecovered,
    TrustChanged,
    ObligationCreated,
    ObligationCalled,
    GrievanceRecorded,
    RelationshipStatusChanged,
    BeliefFormed,
    BeliefUpdated,
    BeliefDisputed,
    RumorMutated,
    BeliefForgotten,
    InstitutionProfileUpdated,
    InstitutionCaseResolved,
    InstitutionalErrorRecorded,
    GroupFormed,
    GroupMembershipChanged,
    GroupSplit,
    GroupDissolved,
    ConversationHeld,
    LoanExtended,
    RomanceAdvanced,
    IllnessContracted,
    IllnessRecovered,
    ObservationLogged,
    InsultExchanged,
    PunchThrown,
    BrawlStarted,
    BrawlStopped,
    GuardsDispatched,
    ApprenticeshipProgressed,
    SuccessionTransferred,
    RouteRiskUpdated,
    TravelWindowShifted,
    NarrativeWhySummary,
    OpportunityOpened,
    OpportunityExpired,
    OpportunityAccepted,
    OpportunityRejected,
    CommitmentStarted,
    CommitmentContinued,
    CommitmentCompleted,
    CommitmentBroken,
    MarketCleared,
    MarketFailed,
    AccountingTransferRecorded,
    InstitutionQueueUpdated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub schema_version: String,
    pub run_id: String,
    pub tick: u64,
    pub created_at: String,
    pub event_id: String,
    pub sequence_in_tick: u64,
    pub event_type: EventType,
    pub location_id: String,
    pub actors: Vec<ActorRef>,
    pub reason_packet_id: Option<String>,
    pub caused_by: Vec<String>,
    #[serde(default)]
    pub targets: Vec<ActorRef>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub visibility: Option<String>,
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReasonPacket {
    pub schema_version: String,
    pub run_id: String,
    pub tick: u64,
    pub created_at: String,
    pub reason_packet_id: String,
    pub actor_id: String,
    pub chosen_action: String,
    pub top_intents: Vec<String>,
    pub top_beliefs: Vec<String>,
    pub top_pressures: Vec<String>,
    pub alternatives_considered: Vec<String>,
    #[serde(default)]
    pub motive_families: Vec<MotiveFamily>,
    #[serde(default)]
    pub feasibility_checks: Vec<FeasibilityCheck>,
    #[serde(default)]
    pub chosen_verb: Option<AffordanceVerb>,
    #[serde(default)]
    pub context_constraints: Vec<String>,
    #[serde(default)]
    pub why_chain: Vec<String>,
    #[serde(default)]
    pub expected_consequences: Vec<String>,
    #[serde(default)]
    pub goal_id: Option<String>,
    #[serde(default)]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub operator_chain_ids: Vec<String>,
    #[serde(default)]
    pub blocked_plan_ids: Vec<String>,
    pub selection_rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub schema_version: String,
    pub run_id: String,
    pub tick: u64,
    pub created_at: String,
    pub snapshot_id: String,
    pub world_state_hash: String,
    pub region_state: Value,
    pub settlement_states: Value,
    pub npc_state_refs: Value,
    pub diff_from_prev_snapshot: Option<Value>,
    pub perf_stats: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResponse {
    pub schema_version: String,
    pub query_type: String,
    pub run_id: String,
    pub generated_at_tick: u64,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AiProposal {
    pub schema_version: String,
    pub proposal_id: String,
    pub run_id: String,
    pub tick: u64,
    pub subject_id: String,
    pub proposal_type: String,
    pub proposal_payload: Value,
    pub confidence: f32,
}

pub mod serde_u64_string {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse::<u64>().map_err(D::Error::custom)
    }
}
