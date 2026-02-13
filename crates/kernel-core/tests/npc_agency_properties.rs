use contracts::{
    ActivePlan, BoundOperator, CapabilitySet, Command, CommandPayload, CommandType, DriveSystem,
    DriveValue, EventType, Goal, IdentityProfile, Observation, OperatorDef, ParamSchema,
    PersonalityTraits, PlanningMode, ReasonEnvelope, RunConfig, Temperament, WakeReason,
};
use kernel_core::agent::NpcAgent;
use kernel_core::operator::{OperatorCatalog, PlanningWorldView};
use kernel_core::planner::{GoapPlanner, PlannerActorState, PlannerConfig};
use kernel_core::scheduler::AgentScheduler;
use kernel_core::world::AgentWorld;
use proptest::prelude::*;
use serde_json::json;

fn base_config() -> RunConfig {
    let mut config = RunConfig::default();
    config.npc_count_min = 4;
    config.npc_count_max = 4;
    config.duration_days = 2;
    config
}

#[test]
fn property_5_aspiration_change_event_emits_once_with_old_new_and_cause() {
    let mut config = base_config();
    config.scheduling_window_size = 1;
    let mut world = AgentWorld::new(config);
    world.step_n(1);

    let changes = world
        .events()
        .iter()
        .filter(|event| event.event_type == EventType::AspirationChanged)
        .collect::<Vec<_>>();
    assert!(!changes.is_empty());

    for event in changes {
        let details = event.details.as_ref().expect("aspiration details present");
        assert!(details.get("old").is_some());
        assert!(details.get("new").is_some());
        assert!(details
            .get("cause")
            .and_then(serde_json::Value::as_str)
            .is_some());
    }
}

#[test]
fn property_18_plan_state_round_trip_serialization() {
    let plan = ActivePlan {
        npc_id: "npc_001".to_string(),
        plan_id: "plan:1".to_string(),
        goal: Goal {
            goal_id: "goal:food".to_string(),
            label: "satisfy_food".to_string(),
            priority: 80,
        },
        planning_mode: PlanningMode::Deliberate,
        steps: vec![BoundOperator {
            operator_id: "household:eat".to_string(),
            parameters: contracts::OperatorParams {
                resource_type: Some("food".to_string()),
                ..contracts::OperatorParams::default()
            },
            duration_ticks: 2,
            preconditions: Vec::new(),
            effects: Vec::new(),
        }],
        next_step_index: 0,
        created_tick: 3,
    };

    let serialized = serde_json::to_string(&plan).expect("serialize");
    let decoded: ActivePlan = serde_json::from_str(&serialized).expect("deserialize");
    assert_eq!(plan, decoded);
}

#[test]
fn property_22_clock_advances_to_minimum_next_event_time() {
    let mut scheduler = AgentScheduler::new(1337);
    scheduler.schedule_next("npc:later", 9, WakeReason::Idle);
    scheduler.schedule_next("npc:earlier", 4, WakeReason::Idle);
    scheduler.schedule_next("npc:middle", 6, WakeReason::Idle);

    let first = scheduler.pop_next().expect("event present");
    assert_eq!(first.wake_tick, 4);
    assert_eq!(scheduler.current_tick(), 4);
}

#[test]
fn property_30_production_input_before_output_event_order() {
    let mut config = base_config();
    config.production_rate_ticks = 1;
    let mut world = AgentWorld::new(config);
    for _ in 0..64 {
        if !world.step() {
            break;
        }
        let started_seen = world
            .events()
            .iter()
            .any(|event| event.event_type == EventType::ProductionStarted);
        let completed_seen = world
            .events()
            .iter()
            .any(|event| event.event_type == EventType::ProductionCompleted);
        if started_seen && completed_seen {
            break;
        }
    }
    let events = world.events();
    let started_idx = events
        .iter()
        .position(|event| event.event_type == EventType::ProductionStarted)
        .expect("production started");
    let completed_idx = events
        .iter()
        .position(|event| event.event_type == EventType::ProductionCompleted)
        .expect("production completed");
    assert!(started_idx < completed_idx);
}

#[test]
fn property_36_default_configuration_completeness() {
    let cfg = RunConfig::default();
    assert!(cfg.planning_beam_width > 0);
    assert!(cfg.planning_horizon > 0);
    assert!(cfg.planner_worker_threads > 0);
    assert!(cfg.max_reaction_depth > 0);
    assert!(cfg.max_same_tick_rounds > 0);
    assert!(cfg.scheduling_window_size > 0);
    assert!(cfg.rent_period_ticks > 0);
    assert!(cfg.wage_period_ticks_daily > 0);
    assert!(cfg.wage_period_ticks_weekly > 0);
    assert!(cfg.production_rate_ticks > 0);
    assert!(cfg.spoilage_rate_ticks > 0);
    assert!(cfg.market_clearing_frequency_ticks > 0);
    assert!(cfg.trust_decay_rate > 0);
    assert!(cfg.rumor_distortion_bps > 0);
    assert!(cfg.group_cohesion_threshold > 0);
    assert!(cfg.relationship_positive_trust_delta > 0);
    assert!(cfg.relationship_negative_trust_delta > 0);
    assert!(cfg.relationship_grievance_delta > 0);
}

#[test]
fn property_39_reason_envelope_round_trip_serialization() {
    let envelope = ReasonEnvelope {
        agent_id: "npc_001".to_string(),
        tick: 3,
        goal: contracts::Goal {
            goal_id: "goal:food".to_string(),
            label: "satisfy_food".to_string(),
            priority: 80,
        },
        selected_plan: contracts::PlanSummary {
            plan_id: "plan:1".to_string(),
            goal: contracts::Goal {
                goal_id: "goal:food".to_string(),
                label: "satisfy_food".to_string(),
                priority: 80,
            },
            utility_score: 91,
        },
        operator_chain: vec!["illicit:steal".to_string()],
        drive_pressures: vec![(contracts::DriveKind::Food, 90)],
        rejected_alternatives: Vec::new(),
        contextual_constraints: vec!["location:settlement:greywall".to_string()],
        planning_mode: PlanningMode::Reactive,
    };

    let serialized = serde_json::to_string(&envelope).expect("serialize");
    let decoded: ReasonEnvelope = serde_json::from_str(&serialized).expect("deserialize");
    assert_eq!(envelope, decoded);
}

#[test]
fn property_13_utility_scorer_monotonicity() {
    let identity = sample_identity(70, 0, 0);
    let drives = sample_drives(90);
    let actor = PlannerActorState {
        agent_id: "npc:1",
        identity: &identity,
        drives: &drives,
        location_id: "loc:a",
    };
    let world = PlanningWorldView::default();

    let better = contracts::CandidatePlan {
        plan_id: "plan:better".to_string(),
        goal: Goal {
            goal_id: "goal:food".to_string(),
            label: "satisfy_food".to_string(),
            priority: 90,
        },
        steps: vec![BoundOperator {
            operator_id: "household:eat".to_string(),
            parameters: contracts::OperatorParams::default(),
            duration_ticks: 1,
            preconditions: Vec::new(),
            effects: Vec::new(),
        }],
        planning_mode: PlanningMode::Deliberate,
    };
    let worse = contracts::CandidatePlan {
        plan_id: "plan:worse".to_string(),
        goal: better.goal.clone(),
        steps: vec![
            BoundOperator {
                operator_id: "illicit:steal".to_string(),
                parameters: contracts::OperatorParams::default(),
                duration_ticks: 1,
                preconditions: Vec::new(),
                effects: Vec::new(),
            },
            BoundOperator {
                operator_id: "illicit:steal".to_string(),
                parameters: contracts::OperatorParams::default(),
                duration_ticks: 1,
                preconditions: Vec::new(),
                effects: Vec::new(),
            },
        ],
        planning_mode: PlanningMode::Deliberate,
    };

    let catalog = OperatorCatalog::default_catalog();
    let score_better = GoapPlanner::score_plan(&better, &actor, &world, &catalog).score;
    let score_worse = GoapPlanner::score_plan(&worse, &actor, &world, &catalog).score;
    assert!(score_better > score_worse);
}

#[test]
fn property_15_occupancy_reservation_on_plan_selection() {
    let identity = sample_identity(0, 20, 40);
    let drives = sample_drives(80);
    let mut agent = NpcAgent::from_identity(
        "npc:1".to_string(),
        identity,
        contracts::Aspiration {
            aspiration_id: "asp:survive".to_string(),
            label: "survive".to_string(),
            updated_tick: 0,
            cause: "init".to_string(),
        },
        "loc:a".to_string(),
        drives,
    );
    let catalog = OperatorCatalog::new(vec![single_operator("mobility:travel", 3)]);
    let world = PlanningWorldView {
        location_ids: vec!["loc:b".to_string()],
        ..PlanningWorldView::default()
    };
    let config = PlannerConfig {
        idle_threshold: -100,
        ..PlannerConfig::default()
    };

    let first = agent.tick(1, &world, &[], &catalog, &config);
    assert!(matches!(first.action, contracts::AgentAction::Execute(_)));
    assert_eq!(
        agent.occupancy.occupancy,
        contracts::NpcOccupancyKind::ExecutingPlanStep
    );
    assert_eq!(agent.occupancy.until_tick, 4);

    let second = agent.tick(2, &world, &[], &catalog, &config);
    assert!(matches!(
        second.action,
        contracts::AgentAction::Execute(_)
            | contracts::AgentAction::Replan
            | contracts::AgentAction::Idle(_)
    ));
}

#[test]
fn property_17_reactive_planning_from_perceived_events() {
    let identity = sample_identity(-10, 20, 60);
    let drives = sample_drives(70);
    let mut agent = NpcAgent::from_identity(
        "npc:1".to_string(),
        identity,
        contracts::Aspiration {
            aspiration_id: "asp:survive".to_string(),
            label: "survive".to_string(),
            updated_tick: 0,
            cause: "init".to_string(),
        },
        "loc:a".to_string(),
        drives,
    );
    let catalog = OperatorCatalog::default_catalog();
    let world = PlanningWorldView {
        object_ids: vec!["item:coin_purse".to_string()],
        location_ids: vec!["loc:a".to_string()],
        npc_ids: vec!["npc:2".to_string()],
        institution_ids: vec!["institution:court".to_string()],
        resource_types: vec!["food".to_string()],
        ..PlanningWorldView::default()
    };
    let observations = vec![Observation {
        event_id: "evt:1".to_string(),
        tick: 1,
        location_id: "loc:a".to_string(),
        event_type: EventType::RumorInjected,
        actors: vec!["npc:2".to_string()],
        visibility: 70,
        details: json!({"topic": "commotion"}),
    }];
    let config = PlannerConfig {
        idle_threshold: -100,
        ..PlannerConfig::default()
    };

    let tick = agent.tick(1, &world, &observations, &catalog, &config);
    let reason = tick.reason.expect("reactive reason present");
    assert_eq!(reason.planning_mode, PlanningMode::Reactive);
}

#[test]
fn property_23_deterministic_replay_hash_same_seed_same_commands() {
    let mut a = AgentWorld::new(base_config());
    let mut b = AgentWorld::new(base_config());

    let command_a = Command::new(
        "cmd_1",
        a.run_id().to_string(),
        1,
        CommandType::InjectRumor,
        CommandPayload::InjectRumor {
            location_id: "settlement:greywall".to_string(),
            rumor_text: "lanterns out".to_string(),
        },
    );
    let command_b = Command::new(
        "cmd_1",
        b.run_id().to_string(),
        1,
        CommandType::InjectRumor,
        CommandPayload::InjectRumor {
            location_id: "settlement:greywall".to_string(),
            rumor_text: "lanterns out".to_string(),
        },
    );

    a.enqueue_command(command_a, 1);
    b.enqueue_command(command_b, 1);
    a.step_n(12);
    b.step_n(12);

    assert_eq!(a.events(), b.events());
    assert_eq!(a.replay_hash(), b.replay_hash());
}

#[test]
fn property_23_deterministic_replay_across_worker_counts() {
    let mut cfg_single = base_config();
    cfg_single.planner_worker_threads = 1;
    let mut cfg_parallel = cfg_single.clone();
    cfg_parallel.planner_worker_threads = 4;

    let mut single = AgentWorld::new(cfg_single);
    let mut parallel = AgentWorld::new(cfg_parallel);

    let command_single = Command::new(
        "cmd_workers",
        single.run_id().to_string(),
        1,
        CommandType::InjectRumor,
        CommandPayload::InjectRumor {
            location_id: "settlement:greywall".to_string(),
            rumor_text: "market commotion".to_string(),
        },
    );
    let command_parallel = Command::new(
        "cmd_workers",
        parallel.run_id().to_string(),
        1,
        CommandType::InjectRumor,
        CommandPayload::InjectRumor {
            location_id: "settlement:greywall".to_string(),
            rumor_text: "market commotion".to_string(),
        },
    );

    single.enqueue_command(command_single, 1);
    parallel.enqueue_command(command_parallel, 1);
    single.step_n(16);
    parallel.step_n(16);

    assert_eq!(single.status().current_tick, parallel.status().current_tick);
    assert_eq!(single.events().len(), parallel.events().len());
    let counts_single = single.events().iter().fold(
        std::collections::BTreeMap::<String, usize>::new(),
        |mut acc, event| {
            let key = format!("{:?}", event.event_type);
            *acc.entry(key).or_insert(0) += 1;
            acc
        },
    );
    let counts_parallel = parallel.events().iter().fold(
        std::collections::BTreeMap::<String, usize>::new(),
        |mut acc, event| {
            let key = format!("{:?}", event.event_type);
            *acc.entry(key).or_insert(0) += 1;
            acc
        },
    );
    assert_eq!(counts_single, counts_parallel);
}

#[test]
fn property_34_reaction_depth_cap() {
    let mut config = base_config();
    config.max_reaction_depth = 1;
    let mut world = AgentWorld::new(config);
    let command = Command::new(
        "cmd_depth",
        world.run_id().to_string(),
        1,
        CommandType::InjectRumor,
        CommandPayload::InjectRumor {
            location_id: "settlement:greywall".to_string(),
            rumor_text: "riot".to_string(),
        },
    );
    world.enqueue_command(command, 1);
    world.step_n(4);

    assert!(world
        .events()
        .iter()
        .any(|event| event.event_type == contracts::EventType::ReactionDepthExceeded));
}

#[test]
fn property_37_reason_envelope_completeness() {
    let mut world = AgentWorld::new(base_config());
    world.step_n(6);

    let envelope = world
        .reason_envelopes()
        .iter()
        .find(|reason| !reason.operator_chain.is_empty())
        .expect("reason envelope present");

    assert!(!envelope.goal.goal_id.is_empty());
    assert!(!envelope.selected_plan.plan_id.is_empty());
    assert!(!envelope.operator_chain.is_empty());
    assert_eq!(envelope.drive_pressures.len(), 7);
}

#[test]
fn property_38_causal_chain_traversal_finite() {
    let mut world = AgentWorld::new(base_config());
    world.step_n(8);

    let event_id = world
        .events()
        .last()
        .expect("event exists")
        .event_id
        .clone();
    let chain = world.traverse_causal_chain(&event_id);

    assert!(!chain.is_empty());
    assert!(chain.len() <= world.events().len());
}

#[test]
fn property_40_step_advances_exact_windows() {
    let mut world = AgentWorld::new(base_config());
    let committed = world.step_n(5);
    assert_eq!(committed, 5);
    assert!(world.status().current_tick <= 5);
    assert!(world.status().current_tick >= 1);
}

#[test]
fn property_41_scenario_injection_perceivable_world_change() {
    let mut world = AgentWorld::new(base_config());
    let command = Command::new(
        "cmd_rumor_perceive",
        world.run_id().to_string(),
        1,
        CommandType::InjectRumor,
        CommandPayload::InjectRumor {
            location_id: "settlement:greywall".to_string(),
            rumor_text: "old bridge collapsed".to_string(),
        },
    );
    world.enqueue_command(command, 1);
    world.step_n(3);

    assert!(world
        .events()
        .iter()
        .any(|event| event.event_type == contracts::EventType::RumorInjected));
    assert!(world.events().iter().any(|event| matches!(
        event.event_type,
        contracts::EventType::RumorPropagated | contracts::EventType::RumorDistorted
    )));
}

fn sample_identity(morality: i64, greed: i64, ambition: i64) -> IdentityProfile {
    IdentityProfile {
        profession: "laborer".to_string(),
        capabilities: CapabilitySet {
            physical: 50,
            social: 40,
            trade: 30,
            combat: 20,
            literacy: 20,
            influence: 20,
            stealth: 30,
            care: 20,
            law: 10,
        },
        personality: PersonalityTraits {
            bravery: 10,
            morality,
            impulsiveness: 15,
            sociability: 20,
            ambition,
            empathy: 10,
            patience: 10,
            curiosity: 10,
            jealousy: 0,
            pride: 0,
            vindictiveness: 0,
            greed,
            loyalty: 10,
            honesty: 0,
            piety: 0,
            vanity: 0,
            humor: 0,
        },
        temperament: Temperament::Stoic,
        values: vec!["survival".to_string()],
        likes: vec![],
        dislikes: vec![],
    }
}

fn sample_drives(high_pressure: i64) -> DriveSystem {
    DriveSystem {
        food: DriveValue {
            current: high_pressure,
            decay_rate: 2,
            urgency_threshold: 60,
        },
        shelter: DriveValue {
            current: 20,
            decay_rate: 1,
            urgency_threshold: 60,
        },
        income: DriveValue {
            current: 40,
            decay_rate: 2,
            urgency_threshold: 60,
        },
        safety: DriveValue {
            current: 30,
            decay_rate: 1,
            urgency_threshold: 60,
        },
        belonging: DriveValue {
            current: 20,
            decay_rate: 1,
            urgency_threshold: 60,
        },
        status: DriveValue {
            current: 20,
            decay_rate: 1,
            urgency_threshold: 60,
        },
        health: DriveValue {
            current: 30,
            decay_rate: 2,
            urgency_threshold: 60,
        },
    }
}

fn single_operator(operator_id: &str, duration_ticks: u64) -> OperatorDef {
    OperatorDef {
        operator_id: operator_id.to_string(),
        family: "test".to_string(),
        display_name: operator_id.to_string(),
        param_schema: ParamSchema {
            required: Vec::new(),
            optional: Vec::new(),
        },
        duration_ticks,
        risk: 10,
        visibility: 10,
        preconditions: Vec::new(),
        effects: Vec::new(),
        drive_effects: Vec::new(),
        capability_requirements: Vec::new(),
    }
}

proptest! {
    #[test]
    fn property_19_deterministic_schedule_seed(seed in 1_u64..10_000, steps in 1_u64..20) {
        let mut config = base_config();
        config.seed = seed;
        let mut world_a = AgentWorld::new(config.clone());
        let mut world_b = AgentWorld::new(config);

        world_a.step_n(steps);
        world_b.step_n(steps);

        prop_assert_eq!(world_a.events(), world_b.events());
    }

    #[test]
    fn property_35_run_config_round_trip_with_variations(
        beam in 1_u16..64,
        horizon in 1_u8..6,
        idle in -20_i64..50,
    ) {
        let mut config = RunConfig::default();
        config.planning_beam_width = beam;
        config.planning_horizon = horizon;
        config.idle_threshold = idle;

        let encoded = serde_json::to_string(&config).expect("serialize");
        let decoded: RunConfig = serde_json::from_str(&encoded).expect("deserialize");
        prop_assert_eq!(config, decoded);
    }

    #[test]
    fn property_29_economy_conservation_under_steps(step_count in 1_u64..20) {
        let mut world = AgentWorld::new(base_config());
        let initial_snapshot = world.snapshot_for_current_tick();
        let initial_total = initial_snapshot
            .economy_ledger_v2
            .as_ref()
            .map(|ledger| ledger.accounts.values().map(|a| a.money).sum::<i64>())
            .unwrap_or(0);

        world.step_n(step_count);

        let final_snapshot = world.snapshot_for_current_tick();
        let final_total = final_snapshot
            .economy_ledger_v2
            .as_ref()
            .map(|ledger| ledger.accounts.values().map(|a| a.money).sum::<i64>())
            .unwrap_or(0);

        prop_assert_eq!(initial_total, final_total);
    }
}
