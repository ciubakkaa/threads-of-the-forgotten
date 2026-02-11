use std::collections::{HashMap, HashSet};
use std::time::Instant;

use contracts::{
    Command, CommandPayload, CommandType, Event, EventType, ReasonPacket, RunConfig, Snapshot,
};
use kernel_core::Kernel;
use serde_json::Value;

const WARMUP_TICKS: u64 = 24;
const MAX_INACTIVITY_WINDOW: u64 = 72;
const TRACE_DEPTH_BOUND: usize = 16;
const DEFAULT_PERF_SMOKE_MAX_MS: u128 = 5_500;
const MIN_UNIQUE_ACTIONS: usize = 7;
const MIN_UNIQUE_ACTIONS_REDUCED_POPULATION: usize = 6;
const MAX_DOMINANT_ACTION_SHARE: f64 = 0.62;
const MIN_NPC_ACTION_ENTROPY: f64 = 1.05;
const MAX_COMMITMENT_BREAK_START_RATIO: f64 = 1.0;
const STABLE_MIN_LIVELIHOOD_SHARE: f64 = 0.35;
const CRISIS_MIN_LIVELIHOOD_SHARE: f64 = 0.20;
const STABLE_MAX_THEFT_SHARE: f64 = 0.30;
const CRISIS_MAX_THEFT_SHARE: f64 = 0.55;
const HORIZON_30D_MIN_PER_NPC_UNIQUE: usize = 5;
const HORIZON_90D_MIN_PER_NPC_UNIQUE: usize = 6;
const HORIZON_360D_MIN_PER_NPC_UNIQUE: usize = 7;
const HORIZON_30D_MIN_PER_NPC_ENTROPY: f64 = 0.95;
const HORIZON_90D_MIN_PER_NPC_ENTROPY: f64 = 1.00;
const HORIZON_360D_MIN_PER_NPC_ENTROPY: f64 = 1.05;
const HORIZON_30D_MAX_PER_NPC_DOMINANT_SHARE: f64 = 0.78;
const HORIZON_90D_MAX_PER_NPC_DOMINANT_SHARE: f64 = 0.75;
const HORIZON_360D_MAX_PER_NPC_DOMINANT_SHARE: f64 = 0.72;
const MAX_BASELINE_THEFT_SHARE_90D: f64 = 0.01;
const MIN_STRESS_RUNS_WITH_THEFT_90D: usize = 2;
const MAX_STRESS_THEFT_SHARE_90D: f64 = 0.10;
const MAX_NON_COMMIT_SHARE: f64 = 0.90;

#[derive(Clone, Copy, Debug)]
enum Scenario {
    BadHarvest,
    RumorSpike,
    KeyNpcRemoved,
    CaravanFlowDisruption,
    WinterHardening,
}

impl Scenario {
    fn all() -> [Self; 5] {
        [
            Self::BadHarvest,
            Self::RumorSpike,
            Self::KeyNpcRemoved,
            Self::CaravanFlowDisruption,
            Self::WinterHardening,
        ]
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::BadHarvest => "bad_harvest",
            Self::RumorSpike => "rumor_spike",
            Self::KeyNpcRemoved => "key_npc_removed",
            Self::CaravanFlowDisruption => "caravan_flow_disruption",
            Self::WinterHardening => "winter_hardening",
        }
    }
}

#[derive(Debug)]
struct RunArtifact {
    scenario: Scenario,
    seed: u64,
    config: RunConfig,
    events: Vec<Event>,
    reason_packets: Vec<ReasonPacket>,
    snapshots: Vec<Snapshot>,
    final_state_hash: u64,
}

#[test]
fn deterministic_replay_suite_matches_event_and_snapshot_fingerprints() {
    let seeds = [1337_u64, 2026_u64, 9001_u64];
    let scenarios = [
        Scenario::BadHarvest,
        Scenario::RumorSpike,
        Scenario::WinterHardening,
    ];

    for scenario in scenarios {
        for seed in seeds {
            let first = run_scenario(seed, scenario);
            let second = run_scenario(seed, scenario);

            assert_eq!(
                event_counts_by_type(&first.events),
                event_counts_by_type(&second.events),
                "event type count divergence: scenario={} seed={}",
                scenario.as_str(),
                seed
            );

            assert_eq!(
                event_order_signature(&first.events),
                event_order_signature(&second.events),
                "event order divergence: scenario={} seed={}",
                scenario.as_str(),
                seed
            );

            assert_eq!(
                snapshot_signature(&first.snapshots),
                snapshot_signature(&second.snapshots),
                "snapshot sequence divergence: scenario={} seed={}",
                scenario.as_str(),
                seed
            );

            assert_eq!(
                first.final_state_hash,
                second.final_state_hash,
                "state hash divergence: scenario={} seed={}",
                scenario.as_str(),
                seed
            );
        }
    }
}

#[test]
fn seed_sweep_suite_satisfies_activity_and_ripple_gates() {
    let seeds = [101_u64, 202_u64, 303_u64, 404_u64, 505_u64, 606_u64];

    for scenario in Scenario::all() {
        for seed in seeds {
            let artifact = run_scenario(seed, scenario);
            assert_seed_floor_gates(&artifact);
            assert_durable_ripple(&artifact);
        }
    }
}

#[test]
fn trace_integrity_checks_pass_on_scenario_run() {
    let artifact = run_scenario(4242, Scenario::RumorSpike);

    assert_event_ordering(&artifact.events, artifact.scenario, artifact.seed);
    assert_causal_links_resolve(&artifact.events, artifact.scenario, artifact.seed);
    assert_reason_packet_integrity(
        &artifact.events,
        &artifact.reason_packets,
        artifact.scenario,
        artifact.seed,
    );
    assert_trace_depth_resolves(&artifact.events, artifact.scenario, artifact.seed);
}

#[test]
fn perf_smoke_suite_default_30_day_run_under_threshold() {
    let started = Instant::now();
    let artifact = run_scenario(1337, Scenario::WinterHardening);
    let elapsed = started.elapsed();

    let threshold_ms = perf_smoke_threshold_ms();
    assert!(
        elapsed.as_millis() <= threshold_ms,
        "perf smoke exceeded threshold: elapsed_ms={} threshold_ms={}",
        elapsed.as_millis(),
        threshold_ms
    );

    // Simple bounded-growth smoke guard: event output should remain linearly bounded by ticks.
    let max_expected_events = artifact.config.max_ticks() as usize * 28;
    assert!(
        artifact.events.len() <= max_expected_events,
        "event count exceeded smoke bound: event_count={} bound={}",
        artifact.events.len(),
        max_expected_events
    );
}

#[test]
fn multi_horizon_per_npc_diversity_regression_gate() {
    let seed = 1337_u64;
    let scenario = Scenario::RumorSpike;
    let horizons = [30_u32, 90_u32, 360_u32];

    let mut previous_avg_entropy = 0.0_f64;
    for duration_days in horizons {
        let artifact = run_scenario_for_days(seed, scenario, duration_days);
        let stats = per_npc_action_stats(&artifact.events);
        assert!(
            !stats.is_empty(),
            "horizon diversity gate failed: no npc stats: scenario={} seed={} days={}",
            scenario.as_str(),
            seed,
            duration_days
        );
        let avg_entropy = stats.iter().map(|entry| entry.entropy).sum::<f64>() / stats.len() as f64;
        let min_unique = stats
            .iter()
            .map(|entry| entry.unique_actions)
            .min()
            .unwrap_or_default();
        let max_dominant = stats
            .iter()
            .map(|entry| entry.dominant_share)
            .fold(0.0_f64, f64::max);

        let (entropy_floor, unique_floor, dominant_cap) = match duration_days {
            30 => (
                HORIZON_30D_MIN_PER_NPC_ENTROPY,
                HORIZON_30D_MIN_PER_NPC_UNIQUE,
                HORIZON_30D_MAX_PER_NPC_DOMINANT_SHARE,
            ),
            90 => (
                HORIZON_90D_MIN_PER_NPC_ENTROPY,
                HORIZON_90D_MIN_PER_NPC_UNIQUE,
                HORIZON_90D_MAX_PER_NPC_DOMINANT_SHARE,
            ),
            _ => (
                HORIZON_360D_MIN_PER_NPC_ENTROPY,
                HORIZON_360D_MIN_PER_NPC_UNIQUE,
                HORIZON_360D_MAX_PER_NPC_DOMINANT_SHARE,
            ),
        };

        assert!(
            avg_entropy >= entropy_floor,
            "horizon diversity entropy gate failed: scenario={} seed={} days={} avg_entropy={:.3} min={:.3}",
            scenario.as_str(),
            seed,
            duration_days,
            avg_entropy,
            entropy_floor
        );
        assert!(
            min_unique >= unique_floor,
            "horizon diversity unique gate failed: scenario={} seed={} days={} min_unique={} min={}",
            scenario.as_str(),
            seed,
            duration_days,
            min_unique,
            unique_floor
        );
        assert!(
            max_dominant <= dominant_cap,
            "horizon diversity dominance gate failed: scenario={} seed={} days={} max_dominant={:.3} cap={:.3}",
            scenario.as_str(),
            seed,
            duration_days,
            max_dominant,
            dominant_cap
        );
        if duration_days > 30 {
            assert!(
                avg_entropy + 0.08 >= previous_avg_entropy,
                "horizon diversity entropy collapse gate failed: scenario={} seed={} days={} avg_entropy={:.3} prev={:.3}",
                scenario.as_str(),
                seed,
                duration_days,
                avg_entropy,
                previous_avg_entropy
            );
        }
        previous_avg_entropy = avg_entropy;
    }
}

#[test]
fn theft_activation_under_stress_without_baseline_inflation() {
    let baseline_runs = [
        run_scenario_for_days(1337, Scenario::RumorSpike, 90),
        run_scenario_for_days(2026, Scenario::CaravanFlowDisruption, 90),
    ];
    let baseline_actions = baseline_runs
        .iter()
        .map(|artifact| count_actions(artifact.events.as_slice()))
        .sum::<usize>();
    let baseline_theft = baseline_runs
        .iter()
        .map(|artifact| count_theft_actions(artifact.events.as_slice()))
        .sum::<usize>();
    let baseline_share = ratio(baseline_theft, baseline_actions);
    assert!(
        baseline_share <= MAX_BASELINE_THEFT_SHARE_90D,
        "theft baseline inflation gate failed: share={:.4} max={:.4}",
        baseline_share,
        MAX_BASELINE_THEFT_SHARE_90D
    );

    let stress_runs = [
        run_scenario_for_days(101, Scenario::WinterHardening, 90),
        run_scenario_for_days(202, Scenario::WinterHardening, 90),
        run_scenario_for_days(303, Scenario::WinterHardening, 90),
        run_scenario_for_days(404, Scenario::BadHarvest, 90),
        run_scenario_for_days(505, Scenario::BadHarvest, 90),
        run_scenario_for_days(606, Scenario::BadHarvest, 90),
    ];
    let stress_breakdown = stress_runs
        .iter()
        .map(|artifact| {
            let theft = count_theft_actions(artifact.events.as_slice());
            let actions = count_actions(artifact.events.as_slice());
            let min_wallet = min_wallet_in_snapshots(artifact.snapshots.as_slice());
            let max_eviction = max_household_eviction_risk(artifact.snapshots.as_slice());
            format!(
                "{}:{} theft={} share={:.5} min_wallet={} max_eviction={}",
                artifact.scenario.as_str(),
                artifact.seed,
                theft,
                ratio(theft, actions),
                min_wallet,
                max_eviction
            )
        })
        .collect::<Vec<_>>();

    let runs_with_theft = stress_runs
        .iter()
        .filter(|artifact| count_theft_actions(artifact.events.as_slice()) > 0)
        .count();
    assert!(
        runs_with_theft >= MIN_STRESS_RUNS_WITH_THEFT_90D,
        "theft stress activation gate failed: runs_with_theft={} min={} breakdown={:?}",
        runs_with_theft,
        MIN_STRESS_RUNS_WITH_THEFT_90D,
        stress_breakdown
    );

    let stress_actions = stress_runs
        .iter()
        .map(|artifact| count_actions(artifact.events.as_slice()))
        .sum::<usize>();
    let stress_theft = stress_runs
        .iter()
        .map(|artifact| count_theft_actions(artifact.events.as_slice()))
        .sum::<usize>();
    let stress_share = ratio(stress_theft, stress_actions);
    assert!(
        stress_share <= MAX_STRESS_THEFT_SHARE_90D,
        "theft stress runaway gate failed: share={:.4} max={:.4}",
        stress_share,
        MAX_STRESS_THEFT_SHARE_90D
    );
}

fn run_scenario(seed: u64, scenario: Scenario) -> RunArtifact {
    run_scenario_for_days(seed, scenario, 30)
}

fn run_scenario_for_days(seed: u64, scenario: Scenario, duration_days: u32) -> RunArtifact {
    let mut config = RunConfig::default();
    config.seed = seed;
    config.run_id = format!(
        "run_validation_{}_{}_{}d",
        scenario.as_str(),
        seed,
        duration_days
    );
    config.duration_days = duration_days;
    config.snapshot_every_ticks = 24;
    config.npc_count_min = 3;
    config.npc_count_max = 3;

    let mut kernel = Kernel::new(config.clone());
    for command in scenario_commands(&config.run_id, scenario) {
        kernel.enqueue_command(command.clone(), command.issued_at_tick);
    }

    let mut snapshots = Vec::new();
    let mut last_snapshot_tick = None;

    while kernel.step_tick() {
        maybe_capture_snapshot(&kernel, &config, &mut snapshots, &mut last_snapshot_tick);
    }

    assert_eq!(
        kernel.status().current_tick,
        config.max_ticks(),
        "run did not reach full duration: scenario={} seed={}",
        scenario.as_str(),
        seed
    );

    RunArtifact {
        scenario,
        seed,
        config,
        events: kernel.events().to_vec(),
        reason_packets: kernel.reason_packets().to_vec(),
        snapshots,
        final_state_hash: kernel.state_hash(),
    }
}

fn maybe_capture_snapshot(
    kernel: &Kernel,
    config: &RunConfig,
    snapshots: &mut Vec<Snapshot>,
    last_snapshot_tick: &mut Option<u64>,
) {
    let current_tick = kernel.status().current_tick;
    let cadence = config.snapshot_every_ticks.max(1);
    let due = current_tick > 0
        && ((current_tick % cadence == 0) || kernel.status().is_complete())
        && *last_snapshot_tick != Some(current_tick);

    if due {
        snapshots.push(kernel.snapshot_for_current_tick());
        *last_snapshot_tick = Some(current_tick);
    }
}

fn scenario_commands(run_id: &str, scenario: Scenario) -> Vec<Command> {
    match scenario {
        Scenario::BadHarvest => vec![
            command(
                "cmd_bad_harvest_1",
                run_id,
                25,
                CommandType::InjectForceBadHarvest,
                CommandPayload::InjectForceBadHarvest {
                    settlement_id: "settlement:oakham".to_string(),
                },
            ),
            command(
                "cmd_bad_harvest_2",
                run_id,
                72,
                CommandType::InjectForceBadHarvest,
                CommandPayload::InjectForceBadHarvest {
                    settlement_id: "settlement:millford".to_string(),
                },
            ),
        ],
        Scenario::RumorSpike => vec![
            command(
                "cmd_rumor_1",
                run_id,
                25,
                CommandType::InjectRumor,
                CommandPayload::InjectRumor {
                    location_id: "settlement:greywall".to_string(),
                    rumor_text: "Bridge lights dance at dusk".to_string(),
                },
            ),
            command(
                "cmd_rumor_2",
                run_id,
                28,
                CommandType::InjectRumor,
                CommandPayload::InjectRumor {
                    location_id: "settlement:oakham".to_string(),
                    rumor_text: "A caravan vanished near the marsh".to_string(),
                },
            ),
            command(
                "cmd_rumor_3",
                run_id,
                36,
                CommandType::InjectRumor,
                CommandPayload::InjectRumor {
                    location_id: "settlement:millford".to_string(),
                    rumor_text: "Watchmen whisper about hidden tolls".to_string(),
                },
            ),
        ],
        Scenario::KeyNpcRemoved => vec![command(
            "cmd_remove_npc",
            run_id,
            25,
            CommandType::InjectRemoveNpc,
            CommandPayload::InjectRemoveNpc {
                npc_id: "npc_002".to_string(),
            },
        )],
        Scenario::CaravanFlowDisruption => vec![
            command(
                "cmd_caravan_1",
                run_id,
                25,
                CommandType::InjectSpawnCaravan,
                CommandPayload::InjectSpawnCaravan {
                    origin_settlement_id: "settlement:oakham".to_string(),
                    destination_settlement_id: "settlement:millford".to_string(),
                },
            ),
            command(
                "cmd_caravan_2",
                run_id,
                49,
                CommandType::InjectSpawnCaravan,
                CommandPayload::InjectSpawnCaravan {
                    origin_settlement_id: "settlement:greywall".to_string(),
                    destination_settlement_id: "settlement:oakham".to_string(),
                },
            ),
            command(
                "cmd_disruption_rumor",
                run_id,
                50,
                CommandType::InjectRumor,
                CommandPayload::InjectRumor {
                    location_id: "settlement:greywall".to_string(),
                    rumor_text: "Bandits demand road tolls by the pass".to_string(),
                },
            ),
        ],
        Scenario::WinterHardening => vec![
            command(
                "cmd_winter",
                run_id,
                25,
                CommandType::InjectSetWinterSeverity,
                CommandPayload::InjectSetWinterSeverity { severity: 82 },
            ),
            command(
                "cmd_winter_harvest",
                run_id,
                40,
                CommandType::InjectForceBadHarvest,
                CommandPayload::InjectForceBadHarvest {
                    settlement_id: "settlement:oakham".to_string(),
                },
            ),
        ],
    }
}

fn command(
    command_id: &str,
    run_id: &str,
    issued_at_tick: u64,
    command_type: CommandType,
    payload: CommandPayload,
) -> Command {
    Command::new(command_id, run_id, issued_at_tick, command_type, payload)
}

fn assert_seed_floor_gates(artifact: &RunArtifact) {
    let meaningful_ticks = collect_meaningful_ticks(&artifact.events);
    assert!(
        !meaningful_ticks.is_empty(),
        "dead run: no meaningful activity: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );

    let longest_inactivity =
        longest_inactivity_window(&meaningful_ticks, WARMUP_TICKS, artifact.config.max_ticks());
    assert!(
        longest_inactivity <= MAX_INACTIVITY_WINDOW,
        "inactivity gate failed: scenario={} seed={} longest_window={}",
        artifact.scenario.as_str(),
        artifact.seed,
        longest_inactivity
    );

    assert!(
        has_traceable_causal_chain(&artifact.events),
        "traceable causal chain gate failed: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );

    assert_action_diversity_and_adaptive_livelihood(artifact);
    assert_reason_envelope_completeness(artifact);
    assert_trust_and_belief_signals(artifact);
    assert_adaptive_causal_conditionals(artifact);
    assert_opportunity_feasibility(artifact);
    assert_cadence_realism(artifact);
    assert_accounting_and_commitment_signals(artifact);
    assert_institution_queue_signal(artifact);
}

fn assert_durable_ripple(artifact: &RunArtifact) {
    let ripple_key = match artifact.scenario {
        Scenario::BadHarvest => "harvest_pressure",
        Scenario::RumorSpike => "rumor_pressure",
        Scenario::KeyNpcRemoved => "removed_npc_count",
        Scenario::CaravanFlowDisruption => "caravan_relief",
        Scenario::WinterHardening => "winter_pressure",
    };

    let nonzero_ticks = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::PressureEconomyUpdated)
        .filter_map(|event| {
            let value = detail_i64(event.details.as_ref(), ripple_key);
            if value > 0 {
                Some(event.tick)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let unique_ticks = nonzero_ticks
        .into_iter()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    assert!(
        unique_ticks.len() >= 2,
        "durable ripple gate failed: scenario={} seed={} key={}",
        artifact.scenario.as_str(),
        artifact.seed,
        ripple_key
    );
}

fn assert_event_ordering(events: &[Event], scenario: Scenario, seed: u64) {
    let mut expected_sequence_by_tick = HashMap::<u64, u64>::new();

    for event in events {
        let expected = expected_sequence_by_tick.entry(event.tick).or_insert(0);
        assert_eq!(
            event.sequence_in_tick,
            *expected,
            "event sequence gap: scenario={} seed={} tick={} expected={} actual={} event_id={}",
            scenario.as_str(),
            seed,
            event.tick,
            *expected,
            event.sequence_in_tick,
            event.event_id
        );
        *expected += 1;
    }
}

fn assert_causal_links_resolve(events: &[Event], scenario: Scenario, seed: u64) {
    let index = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();

    for event in events {
        for parent_id in &event.caused_by {
            let parent = index.get(parent_id.as_str()).unwrap_or_else(|| {
                panic!(
                    "missing causal parent: scenario={} seed={} event_id={} parent_id={}",
                    scenario.as_str(),
                    seed,
                    event.event_id,
                    parent_id
                )
            });

            assert!(
                parent.tick < event.tick
                    || (parent.tick == event.tick
                        && parent.sequence_in_tick < event.sequence_in_tick),
                "causal parent must be earlier: scenario={} seed={} parent={} child={}",
                scenario.as_str(),
                seed,
                parent.event_id,
                event.event_id
            );
        }
    }
}

fn assert_reason_packet_integrity(
    events: &[Event],
    reason_packets: &[ReasonPacket],
    scenario: Scenario,
    seed: u64,
) {
    let packet_ids = reason_packets
        .iter()
        .map(|packet| packet.reason_packet_id.as_str())
        .collect::<HashSet<_>>();

    for event in events {
        if event.event_type == EventType::NpcActionCommitted {
            let reason_id = event.reason_packet_id.as_ref().unwrap_or_else(|| {
                panic!(
                    "missing reason packet ref: scenario={} seed={} event_id={}",
                    scenario.as_str(),
                    seed,
                    event.event_id
                )
            });

            assert!(
                packet_ids.contains(reason_id.as_str()),
                "reason packet ref not found: scenario={} seed={} event_id={} reason_id={}",
                scenario.as_str(),
                seed,
                event.event_id,
                reason_id
            );
        }
    }
}

fn assert_trace_depth_resolves(events: &[Event], scenario: Scenario, seed: u64) {
    let index = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();

    let sampled = events
        .iter()
        .filter(|event| is_meaningful_event(event))
        .take(24)
        .collect::<Vec<_>>();

    for event in sampled {
        let depth = ancestry_depth(event, &index, TRACE_DEPTH_BOUND);
        if !event.caused_by.is_empty() {
            assert!(
                depth >= 1,
                "trace traversal failed: scenario={} seed={} event_id={}",
                scenario.as_str(),
                seed,
                event.event_id
            );
        }
    }
}

fn collect_meaningful_ticks(events: &[Event]) -> HashSet<u64> {
    events
        .iter()
        .filter(|event| is_meaningful_event(event))
        .map(|event| event.tick)
        .collect::<HashSet<_>>()
}

fn assert_action_diversity_and_adaptive_livelihood(artifact: &RunArtifact) {
    let actions = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
        .filter_map(|event| detail_str(event.details.as_ref(), "chosen_action"))
        .collect::<Vec<_>>();

    assert!(
        !actions.is_empty(),
        "no npc actions found: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );

    let unique = actions.iter().cloned().collect::<HashSet<_>>();
    let minimum_unique = if matches!(artifact.scenario, Scenario::KeyNpcRemoved) {
        MIN_UNIQUE_ACTIONS_REDUCED_POPULATION
    } else {
        MIN_UNIQUE_ACTIONS
    };

    assert!(
        unique.len() >= minimum_unique,
        "action diversity gate failed: scenario={} seed={} unique_actions={} minimum={}",
        artifact.scenario.as_str(),
        artifact.seed,
        unique.len(),
        minimum_unique
    );

    let mut action_counts = HashMap::<String, usize>::new();
    for action in &actions {
        *action_counts.entry(action.clone()).or_default() += 1;
    }
    let dominant_share = action_counts
        .values()
        .max()
        .map(|count| *count as f64 / actions.len() as f64)
        .unwrap_or(0.0);
    let dominant_limit = if matches!(artifact.scenario, Scenario::KeyNpcRemoved) {
        (MAX_DOMINANT_ACTION_SHARE + 0.06).min(0.75)
    } else {
        MAX_DOMINANT_ACTION_SHARE
    };
    assert!(
        dominant_share <= dominant_limit,
        "dominant action share gate failed: scenario={} seed={} dominant_share={:.3} limit={:.3}",
        artifact.scenario.as_str(),
        artifact.seed,
        dominant_share,
        dominant_limit
    );

    let mut per_npc_counts = HashMap::<String, HashMap<String, usize>>::new();
    for event in artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
    {
        let npc_id = event
            .actors
            .first()
            .map(|actor| actor.actor_id.clone())
            .unwrap_or_default();
        let action = detail_str(event.details.as_ref(), "chosen_action").unwrap_or_default();
        let per_action = per_npc_counts.entry(npc_id).or_default();
        *per_action.entry(action).or_default() += 1;
    }
    let entropy_values = per_npc_counts
        .values()
        .map(shannon_entropy)
        .collect::<Vec<_>>();
    let avg_entropy = if entropy_values.is_empty() {
        0.0
    } else {
        entropy_values.iter().sum::<f64>() / entropy_values.len() as f64
    };
    assert!(
        avg_entropy >= MIN_NPC_ACTION_ENTROPY,
        "per-npc entropy gate failed: scenario={} seed={} avg_entropy={:.3} minimum={:.3}",
        artifact.scenario.as_str(),
        artifact.seed,
        avg_entropy,
        MIN_NPC_ACTION_ENTROPY
    );

    let livelihood_count = actions
        .iter()
        .filter(|action| is_livelihood_action(action))
        .count();
    let theft_count = actions
        .iter()
        .filter(|action| action.as_str() == "steal_supplies")
        .count();

    let livelihood_share = livelihood_count as f64 / actions.len() as f64;
    let theft_share = theft_count as f64 / actions.len() as f64;

    let (min_livelihood, max_theft) = scenario_envelope(artifact.scenario);
    assert!(
        livelihood_share >= min_livelihood,
        "adaptive livelihood gate failed: scenario={} seed={} livelihood_share={:.3} minimum={:.3}",
        artifact.scenario.as_str(),
        artifact.seed,
        livelihood_share,
        min_livelihood
    );
    assert!(
        theft_share <= max_theft,
        "adaptive theft gate failed: scenario={} seed={} theft_share={:.3} maximum={:.3}",
        artifact.scenario.as_str(),
        artifact.seed,
        theft_share,
        max_theft
    );
}

fn assert_reason_envelope_completeness(artifact: &RunArtifact) {
    let packets = artifact
        .reason_packets
        .iter()
        .filter(|packet| !packet.chosen_action.is_empty())
        .collect::<Vec<_>>();
    assert!(
        !packets.is_empty(),
        "reason envelope gate failed: scenario={} seed={} no packets",
        artifact.scenario.as_str(),
        artifact.seed,
    );
    assert!(
        packets
            .iter()
            .all(|packet| !packet.motive_families.is_empty()
                && !packet.feasibility_checks.is_empty()),
        "reason envelope gate failed: scenario={} seed={} missing motive/check data",
        artifact.scenario.as_str(),
        artifact.seed
    );
    assert!(
        packets
            .iter()
            .all(|packet| !packet.why_chain.is_empty() && !packet.context_constraints.is_empty()),
        "reason envelope gate failed: scenario={} seed={} missing why/context chain",
        artifact.scenario.as_str(),
        artifact.seed
    );
    assert!(
        packets.iter().all(|packet| packet
            .goal_id
            .as_ref()
            .map(|value| !value.is_empty())
            .unwrap_or(false)
            && packet
                .plan_id
                .as_ref()
                .map(|value| !value.is_empty())
                .unwrap_or(false)
            && !packet.operator_chain_ids.is_empty()),
        "reason envelope gate failed: scenario={} seed={} missing goal/plan/operator linkage",
        artifact.scenario.as_str(),
        artifact.seed
    );
}

fn assert_trust_and_belief_signals(artifact: &RunArtifact) {
    let trust_events = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::TrustChanged)
        .count();
    let belief_events = artifact
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                EventType::BeliefFormed
                    | EventType::BeliefUpdated
                    | EventType::BeliefDisputed
                    | EventType::RumorMutated
                    | EventType::BeliefForgotten
            )
        })
        .count();
    assert!(
        trust_events > 0,
        "trust signal gate failed: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );
    assert!(
        belief_events > 0,
        "belief signal gate failed: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );
}

fn assert_adaptive_causal_conditionals(artifact: &RunArtifact) {
    let wage_delays = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::WageDelayed)
        .count();
    let rent_unpaid = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::RentUnpaid)
        .count();
    let thefts = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::TheftCommitted)
        .count();
    if wage_delays > 0 {
        assert!(
            rent_unpaid > 0 || thefts > 0,
            "causal conditional gate failed: scenario={} seed={} wage_delays={} rent_unpaid={} thefts={}",
            artifact.scenario.as_str(),
            artifact.seed,
            wage_delays,
            rent_unpaid,
            thefts
        );
    }
}

fn assert_opportunity_feasibility(artifact: &RunArtifact) {
    let opened = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::OpportunityOpened)
        .filter_map(|event| detail_str(event.details.as_ref(), "opportunity_id"))
        .collect::<HashSet<_>>();

    let accepted = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::OpportunityAccepted)
        .filter_map(|event| detail_str(event.details.as_ref(), "opportunity_id"))
        .collect::<Vec<_>>();

    if !accepted.is_empty() {
        assert!(
            accepted.iter().all(|id| opened.contains(id)),
            "opportunity feasibility gate failed: scenario={} seed={} accepted_without_open={:?}",
            artifact.scenario.as_str(),
            artifact.seed,
            accepted
                .iter()
                .filter(|id| !opened.contains(*id))
                .cloned()
                .collect::<Vec<_>>()
        );
    }
}

fn assert_cadence_realism(artifact: &RunArtifact) {
    let mut last_pay_rent_tick_by_npc = HashMap::<String, u64>::new();
    let mut last_theft_tick_by_npc = HashMap::<String, u64>::new();

    for event in artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
    {
        let action = detail_str(event.details.as_ref(), "chosen_action").unwrap_or_default();
        let npc_id = event
            .actors
            .first()
            .map(|actor| actor.actor_id.clone())
            .unwrap_or_default();
        if action == "pay_rent" {
            if let Some(previous) = last_pay_rent_tick_by_npc.insert(npc_id.clone(), event.tick) {
                assert!(
                    event.tick.saturating_sub(previous) >= 6,
                    "cadence gate failed (pay_rent): scenario={} seed={} npc_id={} previous_tick={} current_tick={}",
                    artifact.scenario.as_str(),
                    artifact.seed,
                    npc_id,
                    previous,
                    event.tick
                );
            }
        }
        if action == "steal_supplies" {
            if let Some(previous) = last_theft_tick_by_npc.insert(npc_id.clone(), event.tick) {
                assert!(
                    event.tick.saturating_sub(previous) >= 12,
                    "cadence gate failed (steal_supplies): scenario={} seed={} npc_id={} previous_tick={} current_tick={}",
                    artifact.scenario.as_str(),
                    artifact.seed,
                    npc_id,
                    previous,
                    event.tick
                );
            }
        }
    }

    let npc_count = snapshot_npc_count(artifact.snapshots.as_slice()).max(1);
    let max_possible_commits = artifact.config.max_ticks() as usize * npc_count;
    let commit_count = count_actions(artifact.events.as_slice());
    let non_commit_share = if max_possible_commits == 0 {
        0.0
    } else {
        1.0 - (commit_count as f64 / max_possible_commits as f64)
    };
    let min_non_commit_share = match artifact.scenario {
        Scenario::BadHarvest | Scenario::WinterHardening => 0.01,
        _ => 0.04,
    };
    assert!(
        non_commit_share >= min_non_commit_share && non_commit_share <= MAX_NON_COMMIT_SHARE,
        "cadence gate failed (non-commit share): scenario={} seed={} non_commit_share={:.3} bounds={:.2}..{:.2}",
        artifact.scenario.as_str(),
        artifact.seed,
        non_commit_share,
        min_non_commit_share,
        MAX_NON_COMMIT_SHARE
    );
}

fn assert_accounting_and_commitment_signals(artifact: &RunArtifact) {
    let accounting_events = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::AccountingTransferRecorded)
        .collect::<Vec<_>>();
    assert!(
        !accounting_events.is_empty(),
        "accounting signal gate failed: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );
    assert!(accounting_events.iter().all(|event| {
        detail_i64(event.details.as_ref(), "transfer_count") >= 0
            && detail_i64(event.details.as_ref(), "total_amount") >= 0
    }));

    let commitment_started = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::CommitmentStarted)
        .count();
    let commitment_broken = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::CommitmentBroken)
        .count();
    let commitment_continued = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::CommitmentContinued)
        .count();
    let commitment_completed = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::CommitmentCompleted)
        .count();
    assert!(
        commitment_started > 0,
        "commitment signal gate failed: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );
    let break_ratio = if commitment_started == 0 {
        0.0
    } else {
        commitment_broken as f64 / commitment_started as f64
    };
    let break_ratio_limit = MAX_COMMITMENT_BREAK_START_RATIO;
    assert!(
        break_ratio <= break_ratio_limit,
        "commitment churn gate failed: scenario={} seed={} break_ratio={:.3} limit={:.3}",
        artifact.scenario.as_str(),
        artifact.seed,
        break_ratio,
        break_ratio_limit
    );
    assert!(
        commitment_continued > 0 || commitment_completed > 0,
        "commitment continuity gate failed: scenario={} seed={} continued={} completed={}",
        artifact.scenario.as_str(),
        artifact.seed,
        commitment_continued,
        commitment_completed
    );
}

fn assert_institution_queue_signal(artifact: &RunArtifact) {
    let queue_events = artifact
        .events
        .iter()
        .filter(|event| event.event_type == EventType::InstitutionQueueUpdated)
        .collect::<Vec<_>>();
    assert!(
        !queue_events.is_empty(),
        "institution queue signal gate failed: scenario={} seed={}",
        artifact.scenario.as_str(),
        artifact.seed
    );
    assert!(queue_events.iter().all(|event| {
        detail_i64(event.details.as_ref(), "avg_response_latency") >= 1
            && detail_i64(event.details.as_ref(), "pending_cases") >= 0
    }));
}

fn scenario_envelope(scenario: Scenario) -> (f64, f64) {
    match scenario {
        Scenario::BadHarvest | Scenario::WinterHardening | Scenario::KeyNpcRemoved => {
            (CRISIS_MIN_LIVELIHOOD_SHARE, CRISIS_MAX_THEFT_SHARE)
        }
        Scenario::RumorSpike | Scenario::CaravanFlowDisruption => {
            (STABLE_MIN_LIVELIHOOD_SHARE, STABLE_MAX_THEFT_SHARE)
        }
    }
}

fn is_livelihood_action(action: &str) -> bool {
    matches!(
        action,
        "work_for_food"
            | "work_for_coin"
            | "pay_rent"
            | "seek_shelter"
            | "tend_fields"
            | "craft_goods"
            | "share_meal"
            | "converse_neighbor"
            | "lend_coin"
            | "court_romance"
            | "seek_treatment"
            | "observe_notable_event"
            | "form_mutual_aid_group"
            | "defend_patron"
            | "mediate_dispute"
            | "train_apprentice"
            | "forage"
            | "patrol_road"
            | "organize_watch"
            | "investigate_rumor"
            | "gather_firewood"
            | "cover_absent_neighbor"
            | "question_travelers"
            | "ration_grain"
            | "repair_hearth"
            | "collect_testimony"
            | "trade_visit"
    )
}

fn is_meaningful_event(event: &Event) -> bool {
    matches!(
        event.event_type,
        EventType::NpcActionCommitted
            | EventType::RumorInjected
            | EventType::CaravanSpawned
            | EventType::NpcRemoved
            | EventType::BadHarvestForced
            | EventType::WinterSeveritySet
            | EventType::TheftCommitted
            | EventType::ItemTransferred
            | EventType::InvestigationProgressed
            | EventType::ArrestMade
            | EventType::SiteDiscovered
            | EventType::LeverageGained
            | EventType::RelationshipShifted
            | EventType::PressureEconomyUpdated
            | EventType::HouseholdConsumptionApplied
            | EventType::RentDue
            | EventType::RentUnpaid
            | EventType::EvictionRiskChanged
            | EventType::HouseholdBufferExhausted
            | EventType::JobSought
            | EventType::ContractSigned
            | EventType::WagePaid
            | EventType::WageDelayed
            | EventType::ContractBreached
            | EventType::EmploymentTerminated
            | EventType::ProductionStarted
            | EventType::ProductionCompleted
            | EventType::SpoilageOccurred
            | EventType::StockShortage
            | EventType::StockRecovered
            | EventType::TrustChanged
            | EventType::ObligationCreated
            | EventType::ObligationCalled
            | EventType::GrievanceRecorded
            | EventType::RelationshipStatusChanged
            | EventType::BeliefFormed
            | EventType::BeliefUpdated
            | EventType::BeliefDisputed
            | EventType::RumorMutated
            | EventType::BeliefForgotten
            | EventType::InstitutionProfileUpdated
            | EventType::InstitutionCaseResolved
            | EventType::InstitutionalErrorRecorded
            | EventType::GroupFormed
            | EventType::GroupMembershipChanged
            | EventType::GroupSplit
            | EventType::GroupDissolved
            | EventType::ConversationHeld
            | EventType::LoanExtended
            | EventType::RomanceAdvanced
            | EventType::IllnessContracted
            | EventType::IllnessRecovered
            | EventType::ObservationLogged
            | EventType::InsultExchanged
            | EventType::PunchThrown
            | EventType::BrawlStarted
            | EventType::BrawlStopped
            | EventType::GuardsDispatched
            | EventType::ApprenticeshipProgressed
            | EventType::SuccessionTransferred
            | EventType::RouteRiskUpdated
            | EventType::TravelWindowShifted
            | EventType::NarrativeWhySummary
            | EventType::OpportunityOpened
            | EventType::OpportunityExpired
            | EventType::OpportunityAccepted
            | EventType::OpportunityRejected
            | EventType::CommitmentStarted
            | EventType::CommitmentContinued
            | EventType::CommitmentCompleted
            | EventType::CommitmentBroken
            | EventType::MarketCleared
            | EventType::MarketFailed
            | EventType::AccountingTransferRecorded
            | EventType::InstitutionQueueUpdated
    )
}

fn longest_inactivity_window(meaningful_ticks: &HashSet<u64>, warmup: u64, max_tick: u64) -> u64 {
    let mut longest = 0_u64;
    let mut current = 0_u64;

    for tick in (warmup + 1)..=max_tick {
        if meaningful_ticks.contains(&tick) {
            longest = longest.max(current);
            current = 0;
        } else {
            current += 1;
        }
    }

    longest.max(current)
}

fn has_traceable_causal_chain(events: &[Event]) -> bool {
    let index = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();

    events
        .iter()
        .filter(|event| is_meaningful_event(event))
        .any(|event| ancestry_depth(event, &index, TRACE_DEPTH_BOUND) >= 2)
}

fn ancestry_depth<'a>(
    event: &Event,
    index: &HashMap<&'a str, &'a Event>,
    max_depth: usize,
) -> usize {
    let mut depth = 0_usize;
    let mut frontier = event.caused_by.clone();
    let mut visited = HashSet::new();

    while !frontier.is_empty() && depth < max_depth {
        let mut next = Vec::new();
        for parent_id in frontier {
            if !visited.insert(parent_id.clone()) {
                continue;
            }

            if let Some(parent) = index.get(parent_id.as_str()) {
                next.extend(parent.caused_by.iter().cloned());
            }
        }

        depth += 1;
        frontier = next;
    }

    depth
}

fn event_counts_by_type(events: &[Event]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for event in events {
        *counts.entry(format!("{:?}", event.event_type)).or_insert(0) += 1;
    }
    counts
}

fn count_actions(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
        .count()
}

fn count_theft_actions(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
        .filter_map(|event| detail_str(event.details.as_ref(), "chosen_action"))
        .filter(|action| action == "steal_supplies")
        .count()
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn min_wallet_in_snapshots(snapshots: &[Snapshot]) -> i64 {
    snapshots
        .iter()
        .flat_map(|snapshot| {
            snapshot
                .npc_state_refs
                .get("npc_ledgers")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|ledger| ledger.get("wallet").and_then(Value::as_i64))
        .min()
        .unwrap_or_default()
}

fn max_household_eviction_risk(snapshots: &[Snapshot]) -> i64 {
    snapshots
        .iter()
        .flat_map(|snapshot| {
            snapshot
                .region_state
                .get("households")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|household| household.get("eviction_risk_score").and_then(Value::as_i64))
        .max()
        .unwrap_or_default()
}

fn snapshot_npc_count(snapshots: &[Snapshot]) -> usize {
    snapshots
        .iter()
        .find_map(|snapshot| {
            snapshot
                .region_state
                .get("npc_profiles")
                .and_then(Value::as_array)
                .map(Vec::len)
        })
        .unwrap_or(0)
}

#[derive(Debug)]
struct PerNpcActionStat {
    unique_actions: usize,
    dominant_share: f64,
    entropy: f64,
}

fn per_npc_action_stats(events: &[Event]) -> Vec<PerNpcActionStat> {
    let mut per_npc_counts = HashMap::<String, HashMap<String, usize>>::new();
    for event in events
        .iter()
        .filter(|event| event.event_type == EventType::NpcActionCommitted)
    {
        let Some(npc_id) = event.actors.first().map(|actor| actor.actor_id.clone()) else {
            continue;
        };
        let Some(action) = detail_str(event.details.as_ref(), "chosen_action") else {
            continue;
        };
        let per_action = per_npc_counts.entry(npc_id).or_default();
        *per_action.entry(action).or_default() += 1;
    }

    per_npc_counts
        .values()
        .map(|counts| {
            let total = counts.values().sum::<usize>().max(1);
            let dominant_share = counts
                .values()
                .max()
                .map(|value| *value as f64 / total as f64)
                .unwrap_or(0.0);
            PerNpcActionStat {
                unique_actions: counts.len(),
                dominant_share,
                entropy: shannon_entropy(counts),
            }
        })
        .collect::<Vec<_>>()
}

fn shannon_entropy(counts: &HashMap<String, usize>) -> f64 {
    let total = counts.values().sum::<usize>() as f64;
    if total <= 0.0 {
        return 0.0;
    }
    counts
        .values()
        .map(|count| *count as f64 / total)
        .filter(|p| *p > 0.0)
        .map(|p| -(p * p.log2()))
        .sum::<f64>()
}

fn event_order_signature(events: &[Event]) -> Vec<(u64, u64, String, String)> {
    events
        .iter()
        .map(|event| {
            (
                event.tick,
                event.sequence_in_tick,
                format!("{:?}", event.event_type),
                event.event_id.clone(),
            )
        })
        .collect::<Vec<_>>()
}

fn snapshot_signature(snapshots: &[Snapshot]) -> Vec<(u64, String)> {
    snapshots
        .iter()
        .map(|snapshot| (snapshot.tick, snapshot.world_state_hash.clone()))
        .collect::<Vec<_>>()
}

fn detail_i64(details: Option<&Value>, key: &str) -> i64 {
    details
        .and_then(|value| value.get(key))
        .and_then(Value::as_i64)
        .unwrap_or_default()
}

fn detail_str(details: Option<&Value>, key: &str) -> Option<String> {
    details
        .and_then(|value| value.get(key))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn perf_smoke_threshold_ms() -> u128 {
    std::env::var("THREADS_PERF_SMOKE_MAX_MS")
        .ok()
        .and_then(|raw| raw.parse::<u128>().ok())
        .unwrap_or(DEFAULT_PERF_SMOKE_MAX_MS)
}
