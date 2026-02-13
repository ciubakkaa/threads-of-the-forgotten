//! v1 cross-boundary contracts for kernel, API, persistence, and observatory.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod api;
pub mod commands;
pub mod config;
pub mod economy;
pub mod events;
pub mod planning;
pub mod scheduler;
pub mod serde_u64_string;
pub mod snapshot;

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
    #[serde(default = "default_planning_beam_width")]
    pub planning_beam_width: u16,
    #[serde(default = "default_planning_horizon")]
    pub planning_horizon: u8,
    #[serde(default = "default_planner_worker_threads")]
    pub planner_worker_threads: u16,
    #[serde(default = "default_idle_threshold")]
    pub idle_threshold: i64,
    #[serde(default = "default_max_reaction_depth")]
    pub max_reaction_depth: u8,
    #[serde(default = "default_max_same_tick_rounds")]
    pub max_same_tick_rounds: u8,
    #[serde(default = "default_scheduling_window_size")]
    pub scheduling_window_size: u64,
    #[serde(default = "default_drive_decay_rates")]
    pub drive_decay_rates: BTreeMap<String, i64>,
    #[serde(default = "default_drive_urgency_thresholds")]
    pub drive_urgency_thresholds: BTreeMap<String, i64>,
    #[serde(default = "default_rent_period_ticks")]
    pub rent_period_ticks: u64,
    #[serde(default = "default_wage_period_ticks_daily")]
    pub wage_period_ticks_daily: u64,
    #[serde(default = "default_wage_period_ticks_weekly")]
    pub wage_period_ticks_weekly: u64,
    #[serde(default = "default_production_rate_ticks")]
    pub production_rate_ticks: u64,
    #[serde(default = "default_spoilage_rate_ticks")]
    pub spoilage_rate_ticks: u64,
    #[serde(default = "default_market_clearing_frequency_ticks")]
    pub market_clearing_frequency_ticks: u64,
    #[serde(default = "default_trust_decay_rate")]
    pub trust_decay_rate: i64,
    #[serde(default = "default_rumor_distortion_bps")]
    pub rumor_distortion_bps: u16,
    #[serde(default = "default_group_cohesion_threshold")]
    pub group_cohesion_threshold: i64,
    #[serde(default = "default_relationship_positive_trust_delta")]
    pub relationship_positive_trust_delta: i64,
    #[serde(default = "default_relationship_negative_trust_delta")]
    pub relationship_negative_trust_delta: i64,
    #[serde(default = "default_relationship_grievance_delta")]
    pub relationship_grievance_delta: i64,
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

    pub fn rumor_distortion_probability(&self) -> f32 {
        f32::from(self.rumor_distortion_bps.min(10_000)) / 10_000.0
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
            planning_beam_width: default_planning_beam_width(),
            planning_horizon: default_planning_horizon(),
            planner_worker_threads: default_planner_worker_threads(),
            idle_threshold: default_idle_threshold(),
            max_reaction_depth: default_max_reaction_depth(),
            max_same_tick_rounds: default_max_same_tick_rounds(),
            scheduling_window_size: default_scheduling_window_size(),
            drive_decay_rates: default_drive_decay_rates(),
            drive_urgency_thresholds: default_drive_urgency_thresholds(),
            rent_period_ticks: default_rent_period_ticks(),
            wage_period_ticks_daily: default_wage_period_ticks_daily(),
            wage_period_ticks_weekly: default_wage_period_ticks_weekly(),
            production_rate_ticks: default_production_rate_ticks(),
            spoilage_rate_ticks: default_spoilage_rate_ticks(),
            market_clearing_frequency_ticks: default_market_clearing_frequency_ticks(),
            trust_decay_rate: default_trust_decay_rate(),
            rumor_distortion_bps: default_rumor_distortion_bps(),
            group_cohesion_threshold: default_group_cohesion_threshold(),
            relationship_positive_trust_delta: default_relationship_positive_trust_delta(),
            relationship_negative_trust_delta: default_relationship_negative_trust_delta(),
            relationship_grievance_delta: default_relationship_grievance_delta(),
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

fn default_planning_beam_width() -> u16 {
    24
}

fn default_planning_horizon() -> u8 {
    4
}

fn default_planner_worker_threads() -> u16 {
    let available = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let workers = available.saturating_sub(1).max(1);
    workers.min(u16::MAX as usize) as u16
}

fn default_idle_threshold() -> i64 {
    10
}

fn default_max_reaction_depth() -> u8 {
    4
}

fn default_max_same_tick_rounds() -> u8 {
    8
}

fn default_scheduling_window_size() -> u64 {
    1
}

fn default_drive_decay_rates() -> BTreeMap<String, i64> {
    let mut rates = BTreeMap::new();
    rates.insert("food".to_string(), 3);
    rates.insert("shelter".to_string(), 1);
    rates.insert("income".to_string(), 2);
    rates.insert("safety".to_string(), 1);
    rates.insert("belonging".to_string(), 1);
    rates.insert("status".to_string(), 1);
    rates.insert("health".to_string(), 2);
    rates
}

fn default_drive_urgency_thresholds() -> BTreeMap<String, i64> {
    let mut thresholds = BTreeMap::new();
    thresholds.insert("food".to_string(), 70);
    thresholds.insert("shelter".to_string(), 75);
    thresholds.insert("income".to_string(), 70);
    thresholds.insert("safety".to_string(), 75);
    thresholds.insert("belonging".to_string(), 65);
    thresholds.insert("status".to_string(), 65);
    thresholds.insert("health".to_string(), 70);
    thresholds
}

fn default_rent_period_ticks() -> u64 {
    30 * TICKS_PER_DAY
}

fn default_wage_period_ticks_daily() -> u64 {
    TICKS_PER_DAY
}

fn default_wage_period_ticks_weekly() -> u64 {
    7 * TICKS_PER_DAY
}

fn default_production_rate_ticks() -> u64 {
    TICKS_PER_DAY
}

fn default_spoilage_rate_ticks() -> u64 {
    3 * TICKS_PER_DAY
}

fn default_market_clearing_frequency_ticks() -> u64 {
    TICKS_PER_DAY
}

fn default_trust_decay_rate() -> i64 {
    1
}

fn default_rumor_distortion_bps() -> u16 {
    1_200
}

fn default_group_cohesion_threshold() -> i64 {
    60
}

fn default_relationship_positive_trust_delta() -> i64 {
    8
}

fn default_relationship_negative_trust_delta() -> i64 {
    20
}

fn default_relationship_grievance_delta() -> i64 {
    25
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
    BeliefReconciled,
    RumorPropagated,
    RumorDistorted,
    InstitutionQueueFull,
    InstitutionCorruptionEvent,
    MarketClearingFailed,
    ReactionDepthExceeded,
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
    #[serde(default)]
    pub agents: Vec<AgentSnapshot>,
    #[serde(default)]
    pub scheduler_state: Option<SchedulerSnapshot>,
    #[serde(default)]
    pub world_state_v2: Option<WorldStateSnapshot>,
    #[serde(default)]
    pub economy_ledger_v2: Option<EconomyLedgerSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriveValue {
    pub current: i64,
    pub decay_rate: i64,
    pub urgency_threshold: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DriveContext {
    pub physical_labor: bool,
    pub exposed_to_weather: bool,
    pub social_stress: i64,
    pub illness_level: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriveSystem {
    pub food: DriveValue,
    pub shelter: DriveValue,
    pub income: DriveValue,
    pub safety: DriveValue,
    pub belonging: DriveValue,
    pub status: DriveValue,
    pub health: DriveValue,
}

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Temperament {
    Stoic,
    Choleric,
    Melancholic,
    Sanguine,
    Volatile,
    Calm,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdentityProfile {
    pub profession: String,
    pub capabilities: CapabilitySet,
    pub personality: PersonalityTraits,
    pub temperament: Temperament,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub likes: Vec<String>,
    #[serde(default)]
    pub dislikes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Aspiration {
    pub aspiration_id: String,
    pub label: String,
    pub updated_tick: u64,
    pub cause: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AspirationChange {
    pub npc_id: String,
    pub old_aspiration: Option<Aspiration>,
    pub new_aspiration: Aspiration,
    pub cause: String,
    pub tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryEntry {
    pub memory_id: String,
    pub tick: u64,
    pub topic: String,
    pub details: Value,
    pub confidence: i64,
    pub salience: i64,
    pub source: BeliefSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BeliefClaim {
    pub claim_id: String,
    pub npc_id: String,
    pub topic: String,
    pub content: String,
    pub confidence: i64,
    pub source: BeliefSource,
    pub last_updated_tick: u64,
    pub uncertain: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Goal {
    pub goal_id: String,
    pub label: String,
    pub priority: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlanningMode {
    Reactive,
    Deliberate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParamSchema {
    #[serde(default)]
    pub required: Vec<ParamDef>,
    #[serde(default)]
    pub optional: Vec<ParamDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParamDef {
    pub name: String,
    pub param_type: ParamType,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FactPredicate {
    pub fact_key: String,
    pub operator: String,
    pub expected: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectTemplate {
    pub fact_key: String,
    pub delta: i64,
    pub location_selector: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorDef {
    pub operator_id: String,
    pub family: String,
    pub display_name: String,
    pub param_schema: ParamSchema,
    pub duration_ticks: u64,
    pub risk: i64,
    pub visibility: i64,
    #[serde(default)]
    pub preconditions: Vec<FactPredicate>,
    #[serde(default)]
    pub effects: Vec<EffectTemplate>,
    #[serde(default)]
    pub drive_effects: Vec<(DriveKind, i64)>,
    #[serde(default)]
    pub capability_requirements: Vec<(String, i64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundOperator {
    pub operator_id: String,
    pub parameters: OperatorParams,
    pub duration_ticks: u64,
    #[serde(default)]
    pub preconditions: Vec<FactPredicate>,
    #[serde(default)]
    pub effects: Vec<WorldFactDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidatePlan {
    pub plan_id: String,
    pub goal: Goal,
    #[serde(default)]
    pub steps: Vec<BoundOperator>,
    pub planning_mode: PlanningMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanScore {
    pub plan_id: String,
    pub score: i64,
    #[serde(default)]
    pub factors: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivePlan {
    pub npc_id: String,
    pub plan_id: String,
    pub goal: Goal,
    pub planning_mode: PlanningMode,
    #[serde(default)]
    pub steps: Vec<BoundOperator>,
    pub next_step_index: usize,
    pub created_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanSelection {
    pub selected_plan_id: Option<String>,
    pub idle_reason: Option<String>,
    #[serde(default)]
    pub scores: Vec<PlanScore>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WakeReason {
    Idle,
    PlanStepComplete,
    Interrupted,
    Reactive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduledEvent {
    pub wake_tick: u64,
    pub priority: u64,
    pub agent_id: String,
    pub reason: WakeReason,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AgentAction {
    Execute(BoundOperator),
    Continue,
    Idle(String),
    Replan,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Money,
    Food,
    Fuel,
    Medicine,
    Item,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceTransfer {
    pub from_account: String,
    pub to_account: String,
    pub resource_kind: ResourceKind,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorldMutation {
    pub agent_id: String,
    pub operator_id: String,
    #[serde(default)]
    pub deltas: Vec<WorldFactDelta>,
    #[serde(default)]
    pub resource_transfers: Vec<ResourceTransfer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitResult {
    pub committed: bool,
    pub event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConflictResult {
    pub conflicting_agent_id: String,
    pub conflicting_resource: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Observation {
    pub event_id: String,
    pub tick: u64,
    pub location_id: String,
    pub event_type: EventType,
    #[serde(default)]
    pub actors: Vec<String>,
    pub visibility: i64,
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RumorPayload {
    pub rumor_id: String,
    pub source_npc_id: String,
    pub core_claim: String,
    pub details: String,
    pub hop_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipEdge {
    pub source_npc_id: String,
    pub target_npc_id: String,
    pub trust: i64,
    pub reputation: i64,
    pub obligation: i64,
    pub grievance: i64,
    pub fear: i64,
    pub respect: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountingTransfer {
    pub transfer_id: String,
    pub tick: u64,
    pub from_account: String,
    pub to_account: String,
    pub resource_kind: ResourceKind,
    pub amount: i64,
    pub cause_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AccountBalance {
    pub money: i64,
    pub food: i64,
    pub fuel: i64,
    pub medicine: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RejectedPlan {
    pub plan_id: String,
    pub goal: Goal,
    pub score: i64,
    pub rejection_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanSummary {
    pub plan_id: String,
    pub goal: Goal,
    pub utility_score: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReasonEnvelope {
    pub agent_id: String,
    pub tick: u64,
    pub goal: Goal,
    pub selected_plan: PlanSummary,
    #[serde(default)]
    pub operator_chain: Vec<String>,
    #[serde(default)]
    pub drive_pressures: Vec<(DriveKind, i64)>,
    #[serde(default)]
    pub rejected_alternatives: Vec<RejectedPlan>,
    #[serde(default)]
    pub contextual_constraints: Vec<String>,
    pub planning_mode: PlanningMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSnapshot {
    pub agent_id: String,
    pub identity: IdentityProfile,
    pub drives: DriveSystem,
    #[serde(default)]
    pub beliefs: Vec<BeliefClaim>,
    #[serde(default)]
    pub memories: Vec<MemoryEntry>,
    pub active_plan: Option<ActivePlan>,
    pub occupancy: NpcOccupancyState,
    pub aspiration: Aspiration,
    pub location_id: String,
    pub next_wake_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerSnapshot {
    pub current_tick: u64,
    #[serde(default)]
    pub pending: Vec<ScheduledEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorldStateSnapshot {
    pub tick: u64,
    pub weather: String,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EconomyLedgerSnapshot {
    #[serde(default)]
    pub accounts: BTreeMap<String, AccountBalance>,
    #[serde(default)]
    pub transfers: Vec<AccountingTransfer>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_config_round_trip_serialization_preserves_values() {
        for seed in [1_u64, 7, 1337, 42_4242] {
            let mut cfg = RunConfig::default();
            cfg.seed = seed;
            cfg.run_id = format!("run_{seed}");
            cfg.idle_threshold += (seed % 9) as i64;
            cfg.max_reaction_depth = (seed % 7) as u8 + 1;
            cfg.rumor_distortion_bps = 900 + (seed % 700) as u16;
            cfg.enabled_systems
                .insert("agency".to_string(), seed % 2 == 0);
            cfg.scenario_flags
                .insert("famine".to_string(), seed % 3 == 0);

            let json = serde_json::to_string(&cfg).expect("serialize config");
            let decoded: RunConfig = serde_json::from_str(&json).expect("deserialize config");
            assert_eq!(cfg, decoded);
        }
    }

    #[test]
    fn default_configuration_has_non_zero_core_parameters() {
        let cfg = RunConfig::default();

        assert!(cfg.planning_beam_width > 0);
        assert!(cfg.planning_horizon > 0);
        assert!(cfg.planner_worker_threads > 0);
        assert!(cfg.scheduling_window_size > 0);
        assert!(cfg.max_same_tick_rounds > 0);
        assert!(cfg.rent_period_ticks > 0);
        assert!(cfg.wage_period_ticks_daily > 0);
        assert!(cfg.wage_period_ticks_weekly > 0);
        assert!(cfg.production_rate_ticks > 0);
        assert!(cfg.spoilage_rate_ticks > 0);
        assert!(cfg.market_clearing_frequency_ticks > 0);
        assert!(cfg.group_cohesion_threshold > 0);
        assert!(cfg.rumor_distortion_bps > 0);
        assert!(cfg.relationship_positive_trust_delta > 0);
        assert!(cfg.relationship_negative_trust_delta > 0);
        assert!(cfg.relationship_grievance_delta > 0);
        assert!(!cfg.drive_decay_rates.is_empty());
        assert!(!cfg.drive_urgency_thresholds.is_empty());
        assert!(cfg.drive_decay_rates.values().all(|value| *value > 0));
        assert!(cfg
            .drive_urgency_thresholds
            .values()
            .all(|value| *value > 0));
    }
}
