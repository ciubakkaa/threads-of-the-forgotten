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

    let arc_summary = world
        .inspect_npc_arc_summary(&npc_id)
        .expect("arc summary present");
    assert!(arc_summary.get("arc_tracks").is_some());
    assert!(arc_summary.get("arc_summary").is_some());
    assert!(arc_summary.get("storyline").is_some());
    let storyline = world
        .inspect_npc_storyline(&npc_id)
        .expect("storyline present");
    assert!(storyline.get("chapters").is_some());
}

#[test]
fn life_domain_progresses_over_time() {
    let mut config = RunConfig::default();
    config.npc_count_min = 6;
    config.npc_count_max = 6;
    let mut world = AgentWorld::new(config);
    world.step_n(72);

    assert!(world
        .agents
        .values()
        .any(|agent| agent.life_domain.career_xp > 0));
    assert!(world
        .agents
        .values()
        .any(|agent| agent.life_domain.home_quality != 35));
}

#[test]
fn social_courtship_creates_joint_contract_flow() {
    let mut config = RunConfig::default();
    config.npc_count_min = 2;
    config.npc_count_max = 2;
    let mut world = AgentWorld::new(config);
    let mut ids = world.agents.keys().cloned().collect::<Vec<_>>();
    ids.sort();
    let actor_id = ids[0].clone();
    let target_id = ids[1].clone();
    let location_id = world
        .agents
        .get(&actor_id)
        .map(|agent| agent.location_id.clone())
        .expect("location");

    let bound = contracts::BoundOperator {
        operator_id: "social:court".to_string(),
        parameters: contracts::OperatorParams {
            target_npc: Some(target_id.clone()),
            ..contracts::OperatorParams::default()
        },
        duration_ticks: 1,
        preconditions: Vec::new(),
        effects: Vec::new(),
    };
    let mut seq = 0_u64;
    let source = world.push_event(
        1,
        &mut seq,
        EventType::NpcActionCommitted,
        location_id.clone(),
        vec![ActorRef {
            actor_id: actor_id.clone(),
            actor_kind: "actor".to_string(),
        }],
        None,
        Vec::new(),
        Some(serde_json::json!({"operator_id":"social:court"})),
    );
    for tick in 1..=8 {
        let actor = if tick % 2 == 0 { &target_id } else { &actor_id };
        let target = if tick % 2 == 0 { &actor_id } else { &target_id };
        let mut step_bound = bound.clone();
        step_bound.parameters.target_npc = Some(target.clone());
        world.apply_operator_side_effects(
            actor,
            &location_id,
            &step_bound,
            tick,
            &mut seq,
            &source,
            None,
        );
    }

    assert!(!world.shared_arcs.is_empty());
    assert!(!world.joint_contracts.is_empty());
    assert!(world
        .shared_arcs
        .values()
        .any(|arc| arc.joint_plan_status.is_some()));
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
fn step_three_commits_three_windows() {
    let mut world = AgentWorld::new(RunConfig::default());
    let committed = world.step_n(3);
    assert_eq!(committed, 3);
    assert!(world.status().current_tick >= 1);
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
fn planner_prefilters_location_mismatch_targets() {
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

    world.step_n(1);

    assert!(!world.events().iter().any(|event| {
        event.event_type == EventType::PlanInterrupted
            && event
                .details
                .as_ref()
                .and_then(|details| details.get("reason"))
                .and_then(Value::as_str)
                == Some("location_mismatch")
    }));

    let actor = world.agents.get(&actor_id).expect("actor exists");
    assert_ne!(actor.occupancy.state_tag, "replan");
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
    let winner = ids[0].clone();
    let loser = ids[1].clone();
    let winner_eval = world
        .evaluate_single_agent(&winner, 1, Vec::new())
        .expect("winner eval");
    let loser_eval = world
        .evaluate_single_agent(&loser, 1, Vec::new())
        .expect("loser eval");

    let mut seq = 0_u64;
    world.claimed_resources_this_tick.clear();
    world.commit_agent_evaluation(winner_eval, 1, &mut seq);
    world.commit_agent_evaluation(loser_eval, 1, &mut seq);

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

#[test]
fn channel_inference_uses_operator_metadata_and_params() {
    let operator = contracts::OperatorDef {
        operator_id: "custom:serve".to_string(),
        family: "social".to_string(),
        display_name: "serve".to_string(),
        param_schema: contracts::ParamSchema {
            required: vec![contracts::ParamDef {
                name: "target_npc".to_string(),
                param_type: contracts::ParamType::NpcId,
            }],
            optional: vec![contracts::ParamDef {
                name: "resource_type".to_string(),
                param_type: contracts::ParamType::ResourceType,
            }],
        },
        duration_ticks: 1,
        risk: 10,
        visibility: 40,
        preconditions: Vec::new(),
        effects: Vec::new(),
        drive_effects: Vec::new(),
        capability_requirements: vec![("social".to_string(), 20)],
    };
    let params = contracts::OperatorParams {
        target_npc: Some("npc:2".to_string()),
        resource_type: Some("food".to_string()),
        ..contracts::OperatorParams::default()
    };
    let channels = inferred_operator_channels(Some(&operator), &operator.operator_id, &params);
    assert!(channels.iter().any(|channel| channel == "attention"));
    assert!(channels.iter().any(|channel| channel == "speech"));
    assert!(channels.iter().any(|channel| channel == "hands"));
}

#[test]
fn channel_inference_falls_back_to_operator_family_when_metadata_absent() {
    let channels = inferred_operator_channels(
        None,
        "mobility:travel",
        &contracts::OperatorParams::default(),
    );
    assert!(channels.iter().any(|channel| channel == "attention"));
    assert!(channels.iter().any(|channel| channel == "locomotion"));
}

#[test]
fn directed_targets_react_same_tick_locals_react_next_tick() {
    let mut config = RunConfig::default();
    config.npc_count_min = 3;
    config.npc_count_max = 3;
    let mut world = AgentWorld::new(config);

    let mut ids = world.agents.keys().cloned().collect::<Vec<_>>();
    ids.sort();
    let actor_id = ids[0].clone();
    let target_id = ids[1].clone();
    let bystander_id = ids[2].clone();
    let location = world
        .agents
        .get(&actor_id)
        .map(|agent| agent.location_id.clone())
        .expect("actor location");

    for npc_id in &ids {
        world.next_wake_tick_by_agent.insert(npc_id.clone(), 10);
    }

    let mut seq = 0_u64;
    let event_id = world.push_event(
        1,
        &mut seq,
        EventType::NpcActionCommitted,
        location.clone(),
        vec![ActorRef {
            actor_id: actor_id.clone(),
            actor_kind: "actor".to_string(),
        }],
        None,
        Vec::new(),
        Some(serde_json::json!({
            "operator_id": "social:greet",
            "parameters": {"target_npc": target_id}
        })),
    );

    world.schedule_observer_reactions(
        &location,
        &actor_id,
        &event_id,
        Some(&target_id),
        1,
        &mut seq,
    );

    assert_eq!(
        world.next_wake_tick_by_agent.get(&target_id).copied(),
        Some(1)
    );
    assert_eq!(
        world.next_wake_tick_by_agent.get(&bystander_id).copied(),
        Some(2)
    );
}

#[test]
fn npcs_spawn_with_distinct_identity_and_drive_profiles() {
    let mut config = RunConfig::default();
    config.npc_count_min = 12;
    config.npc_count_max = 12;
    let world = AgentWorld::new(config);

    let unique_personality = world
        .agents
        .values()
        .map(|agent| {
            (
                agent.identity.personality.morality,
                agent.identity.personality.greed,
                agent.identity.personality.sociability,
                agent.identity.personality.impulsiveness,
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let unique_food_pressure = world
        .agents
        .values()
        .map(|agent| agent.drives.drives.food.current)
        .collect::<std::collections::BTreeSet<_>>();

    assert!(
        unique_personality.len() > 1,
        "expected at least 2 distinct personality profiles"
    );
    assert!(
        unique_food_pressure.len() > 1,
        "expected at least 2 distinct drive profiles"
    );
}

#[test]
fn directed_service_action_can_trigger_same_tick_target_response() {
    let mut config = RunConfig::default();
    config.npc_count_min = 2;
    config.npc_count_max = 2;
    config.max_same_tick_rounds = 8;
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
        visibility: 50,
        preconditions: Vec::new(),
        effects: Vec::new(),
        drive_effects: Vec::new(),
        capability_requirements: vec![("social".to_string(), 10)],
    }]);

    let mut ids = world.agents.keys().cloned().collect::<Vec<_>>();
    ids.sort();
    let actor_id = ids[0].clone();
    let shared_location = "settlement:greywall".to_string();
    for npc_id in &ids {
        if let Some(agent) = world.agents.get_mut(npc_id) {
            agent.location_id = shared_location.clone();
            agent.occupancy.location_id = shared_location.clone();
        }
    }
    world.observation_bus = ObservationBus::default();
    for npc_id in &ids {
        world
            .observation_bus
            .register_agent(npc_id, &shared_location);
    }

    for npc_id in &ids {
        world.next_wake_tick_by_agent.insert(npc_id.clone(), 10);
    }
    world.next_wake_tick_by_agent.insert(actor_id, 1);

    world.start();
    assert!(world.step(), "expected first batch to commit");

    let source_event = world.events().iter().find(|event| {
        event.event_type == EventType::NpcActionCommitted
            && event
                .details
                .as_ref()
                .and_then(|details| details.get("directed_target"))
                .and_then(Value::as_str)
                .is_some()
    });
    let source_event = source_event.expect("directed social action should exist");
    let directed_target = source_event
        .details
        .as_ref()
        .and_then(|details| details.get("directed_target"))
        .and_then(Value::as_str)
        .expect("directed target")
        .to_string();

    let target_follow_up = world.events().iter().any(|event| {
        event.event_type == EventType::NpcActionCommitted
            && event.tick == source_event.tick
            && event.sequence_in_tick > source_event.sequence_in_tick
            && event
                .actors
                .iter()
                .any(|actor| actor.actor_id == directed_target)
    });

    assert!(
        target_follow_up,
        "expected directed target to act later in the same tick"
    );
}

#[test]
fn multiple_agents_commit_actions_without_waiting_for_turns() {
    let mut config = RunConfig::default();
    config.npc_count_min = 6;
    config.npc_count_max = 6;
    let mut world = AgentWorld::new(config);
    world.start();
    world.step_n(2);

    let action_events = world
        .events()
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
        .collect::<Vec<_>>();
    assert!(!action_events.is_empty(), "expected action commits");

    let mut actors_same_tick =
        std::collections::BTreeMap::<u64, std::collections::BTreeSet<String>>::new();
    for event in action_events {
        let actor = event
            .actors
            .first()
            .map(|actor| actor.actor_id.clone())
            .unwrap_or_default();
        actors_same_tick
            .entry(event.tick)
            .or_default()
            .insert(actor);
    }

    let concurrent_tick_found = actors_same_tick.values().any(|actors| actors.len() >= 2);
    assert!(
        concurrent_tick_found,
        "expected at least one tick where 2+ NPCs committed actions"
    );
}
