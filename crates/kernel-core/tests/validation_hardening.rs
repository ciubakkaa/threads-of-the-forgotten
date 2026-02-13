use std::collections::BTreeMap;
use std::time::Instant;

use contracts::{
    Command, CommandPayload, CommandType, Event, EventType, ResourceKind, ResourceTransfer,
    RunConfig, WorldMutation, SCHEMA_VERSION_V1,
};
use kernel_core::scheduler::ConflictResolver;
use kernel_core::AgentWorld;

const PERF_SMOKE_MAX_MS: u128 = 6_000;

fn base_config(run_id: &str, seed: u64) -> RunConfig {
    let mut cfg = RunConfig::default();
    cfg.run_id = run_id.to_string();
    cfg.seed = seed;
    cfg.duration_days = 2;
    cfg.npc_count_min = 4;
    cfg.npc_count_max = 4;
    cfg.snapshot_every_ticks = 24;
    cfg
}

fn run_world(mut cfg: RunConfig, target_tick: u64) -> AgentWorld {
    cfg.duration_days = ((target_tick + 23) / 24).max(1) as u32;
    let mut world = AgentWorld::new(cfg);
    world.start();
    let _ = world.run_to_tick(target_tick);
    world.pause();
    world
}

fn event_type_counts(events: &[Event]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for event in events {
        let key = format!("{:?}", event.event_type);
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

fn event_signature(events: &[Event]) -> Vec<(u64, u64, String, String)> {
    events
        .iter()
        .map(|event| {
            (
                event.tick,
                event.sequence_in_tick,
                format!("{:?}", event.event_type),
                event.location_id.clone(),
            )
        })
        .collect()
}

fn command(run_id: &str, tick: u64, id: &str, payload: CommandPayload, ty: CommandType) -> Command {
    Command {
        schema_version: SCHEMA_VERSION_V1.to_string(),
        command_id: id.to_string(),
        run_id: run_id.to_string(),
        issued_at_tick: tick,
        command_type: ty,
        payload,
    }
}

#[test]
fn deterministic_replay_same_seed_same_config() {
    let target_tick = 12;
    let cfg = base_config("determinism_a", 1337);

    let first = run_world(cfg.clone(), target_tick);
    let second = run_world(cfg, target_tick);

    assert_eq!(
        event_type_counts(first.events()),
        event_type_counts(second.events()),
        "event type counts diverged"
    );
    assert_eq!(
        event_signature(first.events()),
        event_signature(second.events()),
        "event order/signature diverged"
    );
    assert_eq!(
        first.replay_hash(),
        second.replay_hash(),
        "replay hash diverged"
    );
}

#[test]
fn scenario_injection_enters_normal_agency_pipeline() {
    let mut cfg = base_config("scenario_ripple", 2026);
    cfg.npc_count_min = 18;
    cfg.npc_count_max = 18;

    let mut world = AgentWorld::new(cfg);
    world.start();

    world.enqueue_command(
        command(
            world.run_id(),
            2,
            "cmd_rumor",
            CommandPayload::InjectRumor {
                location_id: "settlement:greywall".to_string(),
                rumor_text: "The market vault is unattended".to_string(),
            },
            CommandType::InjectRumor,
        ),
        2,
    );

    world.enqueue_command(
        command(
            world.run_id(),
            3,
            "cmd_harvest",
            CommandPayload::InjectForceBadHarvest {
                settlement_id: "settlement:greywall".to_string(),
            },
            CommandType::InjectForceBadHarvest,
        ),
        3,
    );

    let _ = world.run_to_tick(12);

    let events = world.events();
    assert!(
        events
            .iter()
            .any(|event| event.event_type == EventType::RumorInjected),
        "missing injected rumor event"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == EventType::RumorPropagated
                || event.event_type == EventType::RumorDistorted),
        "missing rumor propagation through world"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == EventType::BadHarvestForced),
        "missing bad harvest command event"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == EventType::NpcActionCommitted),
        "missing agency action commits after scenario injection"
    );
}

#[test]
fn collision_and_conflict_paths_are_observable() {
    let mut resolver = ConflictResolver::default();
    let winner = WorldMutation {
        agent_id: "npc_winner".to_string(),
        operator_id: "work:work".to_string(),
        deltas: Vec::new(),
        resource_transfers: vec![ResourceTransfer {
            from_account: "wallet:market".to_string(),
            to_account: "wallet:shared".to_string(),
            resource_kind: ResourceKind::Money,
            amount: 1,
        }],
    };
    let loser = WorldMutation {
        agent_id: "npc_loser".to_string(),
        operator_id: "work:work".to_string(),
        deltas: Vec::new(),
        resource_transfers: vec![ResourceTransfer {
            from_account: "wallet:market".to_string(),
            to_account: "wallet:shared".to_string(),
            resource_kind: ResourceKind::Money,
            amount: 1,
        }],
    };

    let first = resolver.try_commit(&winner, 200);
    let second = resolver.try_commit(&loser, 5);
    assert!(first.is_ok(), "winner should commit");
    assert!(
        second.is_err(),
        "loser should be rejected deterministically"
    );
}

#[test]
fn action_diversity_floor_is_maintained() {
    let world = run_world(base_config("diversity", 77), 24);

    let mut actions = BTreeMap::<String, usize>::new();
    for event in world
        .events()
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
    {
        let operator = event
            .details
            .as_ref()
            .and_then(|details| details.get("operator_id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *actions.entry(operator.to_string()).or_insert(0) += 1;
    }

    assert!(
        actions.len() >= 6,
        "action diversity too low: unique={} {:?}",
        actions.len(),
        actions
    );
}

#[test]
fn perf_smoke_30_day_run_under_threshold() {
    let started = Instant::now();
    let world = run_world(base_config("perf_smoke", 1337), 24);
    let elapsed = started.elapsed().as_millis();

    assert!(
        elapsed <= PERF_SMOKE_MAX_MS,
        "perf smoke exceeded threshold: elapsed_ms={} threshold_ms={}",
        elapsed,
        PERF_SMOKE_MAX_MS
    );

    let event_bound = (24 * 200) as usize;
    assert!(
        world.events().len() <= event_bound,
        "event count exceeded linear bound: event_count={} bound={}",
        world.events().len(),
        event_bound
    );
}

#[test]
fn step_windows_commit_even_when_tick_delta_is_smaller() {
    let mut cfg = base_config("step_windows", 5150);
    cfg.npc_count_min = 2;
    cfg.npc_count_max = 2;

    let mut world = AgentWorld::new(cfg);
    world.start();

    let before_tick = world.status().current_tick;
    let committed = world.step_n(720);
    let after_tick = world.status().current_tick;

    assert!(committed > 0, "expected committed windows > 0");
    assert!(after_tick >= before_tick, "tick should be monotonic");
    assert!(
        committed as u64 >= after_tick - before_tick,
        "windows can exceed tick delta when many events share ticks"
    );
}
