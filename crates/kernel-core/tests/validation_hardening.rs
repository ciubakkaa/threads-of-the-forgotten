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
const DEFAULT_PERF_SMOKE_MAX_MS: u128 = 2_000;
const MIN_UNIQUE_ACTIONS: usize = 7;
const STABLE_MIN_LIVELIHOOD_SHARE: f64 = 0.35;
const CRISIS_MIN_LIVELIHOOD_SHARE: f64 = 0.20;
const STABLE_MAX_THEFT_SHARE: f64 = 0.30;
const CRISIS_MAX_THEFT_SHARE: f64 = 0.55;

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
                first.final_state_hash, second.final_state_hash,
                "state hash divergence: scenario={} seed={}",
                scenario.as_str(), seed
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

fn run_scenario(seed: u64, scenario: Scenario) -> RunArtifact {
    let mut config = RunConfig::default();
    config.seed = seed;
    config.run_id = format!("run_validation_{}_{}", scenario.as_str(), seed);
    config.duration_days = 30;
    config.snapshot_every_ticks = 24;

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

    let longest_inactivity = longest_inactivity_window(
        &meaningful_ticks,
        WARMUP_TICKS,
        artifact.config.max_ticks(),
    );
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
            if value > 0 { Some(event.tick) } else { None }
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
            event.sequence_in_tick, *expected,
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
                    || (parent.tick == event.tick && parent.sequence_in_tick < event.sequence_in_tick),
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
    assert!(
        unique.len() >= MIN_UNIQUE_ACTIONS,
        "action diversity gate failed: scenario={} seed={} unique_actions={} minimum={}",
        artifact.scenario.as_str(),
        artifact.seed,
        unique.len(),
        MIN_UNIQUE_ACTIONS
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
            .all(|packet| !packet.motive_families.is_empty() && !packet.feasibility_checks.is_empty()),
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
            | "form_mutual_aid_group"
            | "defend_patron"
            | "mediate_dispute"
            | "train_apprentice"
            | "forage"
            | "assist_neighbor"
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
            | EventType::ApprenticeshipProgressed
            | EventType::SuccessionTransferred
            | EventType::RouteRiskUpdated
            | EventType::TravelWindowShifted
            | EventType::NarrativeWhySummary
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
