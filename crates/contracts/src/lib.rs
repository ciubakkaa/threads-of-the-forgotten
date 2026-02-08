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
            enabled_systems: BTreeMap::new(),
            scenario_flags: BTreeMap::new(),
            notes: None,
        }
    }
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
    ApprenticeshipProgressed,
    SuccessionTransferred,
    RouteRiskUpdated,
    TravelWindowShifted,
    NarrativeWhySummary,
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
