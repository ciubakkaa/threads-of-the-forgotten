//! GOAP Planner: generates candidate plans from the operator catalog,
//! scores them using a utility function, and selects the best plan or idle.
//!
//! Supports two planning modes:
//! - **Reactive**: fewer steps, rough goal, step-by-step binding
//! - **Deliberate**: more steps, higher optimization, stored goal with
//!   per-step precondition re-evaluation

use contracts::agency::{
    AgencyWorldFactDelta, BoundOperator, CandidatePlan, DriveKind, Goal, OperatorDef,
    OperatorParams, PlanScore, PlanSelection, PlanningMode,
};

use crate::drive::DriveSystem;
use crate::operator::{AgentView, OperatorCatalog, WorldView};
use crate::social::SocialGraph;

// ---------------------------------------------------------------------------
// Planner configuration
// ---------------------------------------------------------------------------

/// Configuration knobs for the planner, typically sourced from `AgencyRunConfig`.
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Maximum number of candidate plans to keep during beam search.
    pub beam_width: usize,
    /// Maximum number of operator steps per plan.
    pub planning_horizon: usize,
    /// Plans scoring below this threshold result in idle.
    pub idle_threshold: i64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            beam_width: 24,
            planning_horizon: 4,
            idle_threshold: 10,
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic RNG (minimal, seedable)
// ---------------------------------------------------------------------------

/// A simple deterministic RNG for the planner. Produces repeatable sequences
/// given the same seed, ensuring deterministic replay.
#[derive(Debug, Clone)]
pub struct AgentRng {
    state: u64,
}

impl AgentRng {
    /// Create a new RNG from a seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed.wrapping_add(1) }
    }

    /// Produce the next pseudo-random u64.
    pub fn next_u64(&mut self) -> u64 {
        // SplitMix64
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    /// Produce a random i64 in [lo, hi] inclusive.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        if lo >= hi {
            return lo;
        }
        let span = (hi - lo + 1) as u64;
        lo + (self.next_u64() % span) as i64
    }
}

// ---------------------------------------------------------------------------
// GoapPlanner — candidate generation (Task 11.1)
// ---------------------------------------------------------------------------

/// Stateless GOAP planner. All methods are associated functions.
pub struct GoapPlanner;

impl GoapPlanner {
    /// Generate candidate plans for an agent given current world state.
    ///
    /// Uses beam search: for each applicable operator, enumerate bindings,
    /// then iteratively extend partial plans up to `config.planning_horizon`
    /// steps, keeping the top `config.beam_width` candidates at each level.
    ///
    /// The `mode` parameter controls reactive vs deliberate planning:
    /// - Reactive: max 2 steps, rough goal derived from top drive pressure
    /// - Deliberate: up to `planning_horizon` steps, goal from top drive
    pub fn generate_candidates(
        drives: &DriveSystem,
        agent: &AgentView,
        world: &WorldView,
        catalog: &OperatorCatalog,
        config: &PlannerConfig,
        mode: PlanningMode,
        rng: &mut AgentRng,
    ) -> Vec<CandidatePlan> {
        let horizon = match mode {
            PlanningMode::Reactive => 2.min(config.planning_horizon),
            PlanningMode::Deliberate => config.planning_horizon,
        };

        // Derive a goal from the agent's top drive pressure.
        let top = drives.top_pressures(1);
        let goal = if let Some(&(kind, _)) = top.first() {
            Goal {
                goal_id: format!("goal_{}", drive_kind_str(kind)),
                description: format!("satisfy {}", drive_kind_str(kind)),
                target_drive: Some(kind),
            }
        } else {
            Goal {
                goal_id: "goal_idle".into(),
                description: "no pressing need".into(),
                target_drive: None,
            }
        };

        // --- Seed generation: single-step plans from all applicable operators ---
        let mut seeds: Vec<PartialPlan> = Vec::new();
        let plan_counter = rng.next_u64();

        for (op_idx, op_def) in catalog.operators().iter().enumerate() {
            if !catalog.check_preconditions(op_def, &OperatorParams::default(), world) {
                continue;
            }
            let bindings = catalog.enumerate_bindings(op_def, agent, world);
            for (bind_idx, params) in bindings.into_iter().enumerate() {
                let bound = bind_operator(op_def, &params);
                let drive_score = drive_relevance(drives, op_def);
                seeds.push(PartialPlan {
                    steps: vec![bound],
                    cumulative_drive_score: drive_score,
                    id_seed: plan_counter
                        .wrapping_add(op_idx as u64)
                        .wrapping_mul(1000)
                        .wrapping_add(bind_idx as u64),
                });
            }
        }

        // Sort by drive relevance descending, keep top beam_width.
        seeds.sort_by(|a, b| b.cumulative_drive_score.cmp(&a.cumulative_drive_score));
        seeds.truncate(config.beam_width);

        // --- Iterative extension up to horizon ---
        let mut beam = seeds;
        for _depth in 1..horizon {
            let mut next_beam: Vec<PartialPlan> = Vec::new();
            for partial in &beam {
                // Try extending with each operator.
                for op_def in catalog.operators() {
                    if !catalog.check_preconditions(op_def, &OperatorParams::default(), world) {
                        continue;
                    }
                    let bindings = catalog.enumerate_bindings(op_def, agent, world);
                    if let Some(params) = bindings.into_iter().next() {
                        // Take first valid binding to limit combinatorial explosion.
                        let bound = bind_operator(op_def, &params);
                        let extra_score = drive_relevance(drives, op_def);
                        let mut extended = partial.clone();
                        extended.steps.push(bound);
                        extended.cumulative_drive_score += extra_score;
                        next_beam.push(extended);
                    }
                }
                // Also keep the partial as-is (shorter plan).
                next_beam.push(partial.clone());
            }
            next_beam.sort_by(|a, b| b.cumulative_drive_score.cmp(&a.cumulative_drive_score));
            next_beam.truncate(config.beam_width);
            beam = next_beam;
        }

        // Deduplicate by step count + first operator to avoid near-identical plans.
        beam.dedup_by(|a, b| {
            a.steps.len() == b.steps.len()
                && a.steps.first().map(|s| &s.operator_id)
                    == b.steps.first().map(|s| &s.operator_id)
        });

        // Enforce max 6 steps per plan.
        for p in &mut beam {
            p.steps.truncate(6);
        }

        // Convert to CandidatePlan.
        beam.into_iter()
            .enumerate()
            .map(|(i, partial)| CandidatePlan {
                plan_id: format!("plan_{}", partial.id_seed.wrapping_add(i as u64)),
                goal: goal.clone(),
                steps: partial.steps,
                planning_mode: mode,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Intermediate representation during beam search.
#[derive(Debug, Clone)]
struct PartialPlan {
    steps: Vec<BoundOperator>,
    cumulative_drive_score: i64,
    id_seed: u64,
}

/// Bind an operator definition with concrete parameters into a `BoundOperator`.
fn bind_operator(op_def: &OperatorDef, params: &OperatorParams) -> BoundOperator {
    let effects: Vec<AgencyWorldFactDelta> = op_def
        .effects
        .iter()
        .map(|eff| AgencyWorldFactDelta {
            fact_key: eff.fact_key.clone(),
            delta: parse_delta_expr(&eff.delta_expr),
            location_id: params
                .target_location
                .clone()
                .unwrap_or_default(),
        })
        .collect();

    BoundOperator {
        operator_id: op_def.operator_id.clone(),
        parameters: params.clone(),
        duration_ticks: op_def.duration_ticks,
        preconditions: op_def.preconditions.clone(),
        effects,
    }
}

/// Rough numeric extraction from a delta expression string.
/// Handles "+N", "-N", and named expressions (returns 1 as default).
fn parse_delta_expr(expr: &str) -> i64 {
    let trimmed = expr.trim();
    if let Ok(v) = trimmed.parse::<i64>() {
        return v;
    }
    if trimmed.starts_with('+') {
        if let Ok(v) = trimmed[1..].parse::<i64>() {
            return v;
        }
    }
    if trimmed.starts_with('-') {
        if let Ok(v) = trimmed.parse::<i64>() {
            return v;
        }
    }
    // Named expression like "+harvest_yield" — default to 1.
    if trimmed.starts_with('+') {
        1
    } else if trimmed.starts_with('-') {
        -1
    } else {
        0
    }
}

/// Score how relevant an operator is to the agent's current drive pressures.
/// Higher score = operator addresses more urgent drives.
fn drive_relevance(drives: &DriveSystem, op_def: &OperatorDef) -> i64 {
    let mut score: i64 = 0;
    for &(kind, effect) in &op_def.drive_effects {
        let pressure = drives.get(kind).current;
        // Positive effect on a high-pressure drive is very relevant.
        if effect > 0 {
            score += pressure * effect / 10;
        } else {
            // Negative drive effects (costs) reduce relevance.
            score += effect;
        }
    }
    score
}

fn drive_kind_str(kind: DriveKind) -> &'static str {
    match kind {
        DriveKind::Food => "food",
        DriveKind::Shelter => "shelter",
        DriveKind::Income => "income",
        DriveKind::Safety => "safety",
        DriveKind::Belonging => "belonging",
        DriveKind::Status => "status",
        DriveKind::Health => "health",
    }
}

// ---------------------------------------------------------------------------
// UtilityScorer — plan scoring and selection (Task 11.2)
// ---------------------------------------------------------------------------

/// Stateless utility scorer. Evaluates candidate plans and selects the best.
pub struct UtilityScorer;

/// Personality-derived weights used during scoring.
pub struct PersonalityWeights {
    pub risk_tolerance: i64,   // higher = less penalty for risky plans
    pub social_affinity: i64,  // higher = bonus for social operators
    pub ambition_bonus: i64,   // higher = bonus for status/income plans
    pub morality_penalty: i64, // higher = penalty for illicit operators
}

impl UtilityScorer {
    /// Score a candidate plan by combining:
    /// - Drive urgency satisfaction
    /// - Personality bias from IdentityProfile
    /// - Risk tolerance
    /// - Opportunity cost (plan duration)
    /// - Social graph factors (trust toward target NPCs)
    pub fn score_plan(
        candidate: &CandidatePlan,
        drives: &DriveSystem,
        personality: &PersonalityWeights,
        social: Option<&SocialGraph>,
        agent_id: &str,
    ) -> PlanScore {
        let mut drive_urgency_component: i64 = 0;
        let mut risk_component: i64 = 0;
        let mut opportunity_cost_component: i64 = 0;
        let mut personality_component: i64 = 0;
        let mut social_component: i64 = 0;

        for step in &candidate.steps {
            // --- Drive urgency ---
            // Look up the operator in the catalog isn't possible here (stateless),
            // so we estimate from the bound operator's effects.
            // A positive delta on a high-pressure drive is valuable.
            for eff in &step.effects {
                if eff.delta > 0 {
                    // We don't know which drive this maps to from fact_key alone,
                    // so we give a flat bonus for positive effects.
                    drive_urgency_component += eff.delta * 5;
                }
            }

            // --- Risk ---
            // We don't have risk on BoundOperator directly, but we can infer
            // from operator_id patterns. For now, use duration as a proxy.
            risk_component -= (step.duration_ticks as i64) * 2;

            // --- Opportunity cost ---
            opportunity_cost_component -= step.duration_ticks as i64;

            // --- Social factors ---
            if let (Some(graph), Some(target)) = (social, &step.parameters.target_npc) {
                if let Some(edge) = graph.get_edge(agent_id, target) {
                    social_component += edge.trust / 5;
                    social_component += edge.respect / 10;
                    social_component -= edge.grievance / 10;
                }
            }
        }

        // --- Personality bias ---
        // Adjust risk penalty by risk tolerance.
        risk_component = risk_component * (100 - personality.risk_tolerance) / 100;
        // Social operators get a bonus from sociability.
        personality_component += social_component * personality.social_affinity / 100;
        // Ambition bonus for plans targeting income/status.
        if candidate.goal.target_drive == Some(DriveKind::Income)
            || candidate.goal.target_drive == Some(DriveKind::Status)
        {
            personality_component += personality.ambition_bonus;
        }

        // Also add drive urgency from the goal's target drive.
        if let Some(target_drive) = candidate.goal.target_drive {
            let pressure = drives.get(target_drive).current;
            drive_urgency_component += pressure;
        }

        // Morality penalty for illicit operators.
        for step in &candidate.steps {
            if step.operator_id == "steal"
                || step.operator_id == "smuggle"
                || step.operator_id == "bribe"
                || step.operator_id == "extort"
                || step.operator_id == "fence"
            {
                personality_component -= personality.morality_penalty;
            }
        }

        let total = drive_urgency_component
            + personality_component
            + risk_component
            + opportunity_cost_component
            + social_component;

        PlanScore {
            plan_id: candidate.plan_id.clone(),
            total,
            drive_urgency_component,
            personality_component,
            risk_component,
            opportunity_cost_component,
            social_component,
        }
    }

    /// Select the best plan, or idle if all plans score below the threshold.
    pub fn select(
        candidates: &[CandidatePlan],
        scores: &[PlanScore],
        idle_threshold: i64,
    ) -> PlanSelection {
        if candidates.is_empty() || scores.is_empty() {
            return PlanSelection::Idle {
                reason: "no candidates".into(),
            };
        }

        let best_idx = scores
            .iter()
            .enumerate()
            .max_by_key(|(_, s)| s.total)
            .map(|(i, _)| i)
            .unwrap();

        let best_score = &scores[best_idx];
        if best_score.total < idle_threshold {
            return PlanSelection::Idle {
                reason: format!(
                    "best score {} below threshold {}",
                    best_score.total, idle_threshold
                ),
            };
        }

        PlanSelection::Selected {
            plan: candidates[best_idx].clone(),
            score: best_score.clone(),
        }
    }

    /// Score and select in one call, using personality derived from the
    /// identity profile fields.
    pub fn score_and_select(
        candidates: &[CandidatePlan],
        drives: &DriveSystem,
        personality: &contracts::agency::PersonalityTraits,
        social: Option<&SocialGraph>,
        agent_id: &str,
        idle_threshold: i64,
    ) -> PlanSelection {
        let weights = personality_to_weights(personality);
        let scores: Vec<PlanScore> = candidates
            .iter()
            .map(|c| Self::score_plan(c, drives, &weights, social, agent_id))
            .collect();
        Self::select(candidates, &scores, idle_threshold)
    }
}

/// Convert personality traits to scoring weights.
pub fn personality_to_weights(p: &contracts::agency::PersonalityTraits) -> PersonalityWeights {
    PersonalityWeights {
        // Bravery maps to risk tolerance: brave NPCs tolerate more risk.
        risk_tolerance: (p.bravery + 100) / 2, // map [-100,100] to [0,100]
        // Sociability maps to social affinity.
        social_affinity: (p.sociability + 100) / 2,
        // Ambition maps to bonus for income/status goals.
        ambition_bonus: p.ambition.max(0) / 5,
        // Morality maps to penalty for illicit actions.
        morality_penalty: p.morality.max(0) / 5,
    }
}

// ---------------------------------------------------------------------------
// Occupancy reservation and plan interruption
// ---------------------------------------------------------------------------

/// Occupancy state for an agent — tracks whether the agent is busy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupancyState {
    /// The operator currently occupying the agent, if any.
    pub active_operator_id: Option<String>,
    /// Tick at which the current occupancy ends.
    pub occupied_until: u64,
    /// Whether the current occupancy can be interrupted.
    pub interruptible: bool,
}

impl OccupancyState {
    pub fn idle() -> Self {
        Self {
            active_operator_id: None,
            occupied_until: 0,
            interruptible: true,
        }
    }

    /// Reserve occupancy for the first step of a selected plan.
    pub fn reserve(&mut self, plan: &CandidatePlan, current_tick: u64) {
        if let Some(first_step) = plan.steps.first() {
            self.active_operator_id = Some(first_step.operator_id.clone());
            self.occupied_until = current_tick + first_step.duration_ticks;
            self.interruptible = true; // all plans are interruptible by default
        }
    }

    /// Whether the agent is currently occupied.
    pub fn is_occupied(&self, current_tick: u64) -> bool {
        self.active_operator_id.is_some() && current_tick < self.occupied_until
    }

    /// Clear occupancy (e.g. on interruption or completion).
    pub fn clear(&mut self) {
        self.active_operator_id = None;
        self.occupied_until = 0;
        self.interruptible = true;
    }
}

impl Default for OccupancyState {
    fn default() -> Self {
        Self::idle()
    }
}

/// Check whether a plan's current step preconditions are still satisfied.
/// Returns `true` if the plan should be interrupted (preconditions failed).
pub fn should_interrupt_plan(
    plan: &contracts::agency::ActivePlan,
    world: &WorldView,
) -> bool {
    if plan.current_step_index >= plan.steps.len() {
        return false; // plan already complete
    }
    let step = &plan.steps[plan.current_step_index];
    // Check each precondition against world facts.
    for pred in &step.preconditions {
        let actual = world.facts.get(&pred.fact_key).copied().unwrap_or(0);
        let satisfied = match pred.operator {
            contracts::agency::PredicateOp::Eq => actual == pred.value,
            contracts::agency::PredicateOp::Neq => actual != pred.value,
            contracts::agency::PredicateOp::Gt => actual > pred.value,
            contracts::agency::PredicateOp::Gte => actual >= pred.value,
            contracts::agency::PredicateOp::Lt => actual < pred.value,
            contracts::agency::PredicateOp::Lte => actual <= pred.value,
        };
        if !satisfied {
            return true; // precondition failed → interrupt
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::default_catalog;
    use contracts::agency::{ActivePlan, FactPredicate, PersonalityTraits, PredicateOp};
    use std::collections::BTreeMap;

    fn test_world() -> WorldView {
        let mut npcs_by_location = BTreeMap::new();
        npcs_by_location.insert(
            "town_square".into(),
            vec!["npc_1".into(), "npc_2".into(), "npc_3".into()],
        );
        let mut objects_by_location = BTreeMap::new();
        objects_by_location.insert(
            "town_square".into(),
            vec!["door_1".into(), "chest_1".into()],
        );
        let mut institutions_by_location = BTreeMap::new();
        institutions_by_location.insert(
            "town_square".into(),
            vec!["market".into(), "court".into()],
        );
        let mut resources_by_location = BTreeMap::new();
        resources_by_location.insert(
            "town_square".into(),
            vec!["food".into(), "fuel".into()],
        );
        let mut facts = BTreeMap::new();
        facts.insert("food_available".into(), 5);
        facts.insert("food_raw".into(), 3);
        facts.insert("money".into(), 10);
        WorldView {
            npcs_by_location,
            location_ids: vec!["town_square".into(), "forest".into()],
            objects_by_location,
            institutions_by_location,
            resources_by_location,
            facts,
        }
    }

    fn test_agent() -> AgentView {
        AgentView {
            agent_id: "npc_1".into(),
            location_id: "town_square".into(),
        }
    }

    fn hungry_drives() -> DriveSystem {
        let mut ds = DriveSystem::default();
        ds.food.current = 85; // urgent
        ds.income.current = 40;
        ds
    }

    fn neutral_personality() -> PersonalityTraits {
        PersonalityTraits {
            bravery: 0, morality: 0, impulsiveness: 0, sociability: 0,
            ambition: 0, empathy: 0, patience: 0, curiosity: 0,
            jealousy: 0, pride: 0, vindictiveness: 0, greed: 0,
            loyalty: 0, honesty: 0, piety: 0, vanity: 0, humor: 0,
        }
    }

    // -- GoapPlanner tests (11.1) --

    #[test]
    fn generate_candidates_produces_plans() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = hungry_drives();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        assert!(!candidates.is_empty(), "should produce at least one candidate");
    }

    #[test]
    fn candidates_have_1_to_6_steps() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = hungry_drives();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        for c in &candidates {
            assert!(c.steps.len() >= 1 && c.steps.len() <= 6,
                "plan {} has {} steps", c.plan_id, c.steps.len());
        }
    }

    #[test]
    fn candidates_within_beam_width() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = hungry_drives();
        let config = PlannerConfig { beam_width: 5, ..Default::default() };
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        assert!(candidates.len() <= config.beam_width,
            "got {} candidates, beam_width={}", candidates.len(), config.beam_width);
    }

    #[test]
    fn reactive_mode_limits_steps() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = hungry_drives();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Reactive, &mut rng,
        );
        for c in &candidates {
            assert!(c.steps.len() <= 2,
                "reactive plan {} has {} steps (max 2)", c.plan_id, c.steps.len());
            assert_eq!(c.planning_mode, PlanningMode::Reactive);
        }
    }

    #[test]
    fn deliberate_mode_tags_plans() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = hungry_drives();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        for c in &candidates {
            assert_eq!(c.planning_mode, PlanningMode::Deliberate);
        }
    }

    // -- UtilityScorer tests (11.2) --

    #[test]
    fn idle_when_all_below_threshold() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = DriveSystem::default(); // all pressures at 0
        let config = PlannerConfig { idle_threshold: 9999, ..Default::default() };
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        let personality = neutral_personality();
        let selection = UtilityScorer::score_and_select(
            &candidates, &drives, &personality, None, "npc_1", 9999,
        );
        match selection {
            PlanSelection::Idle { .. } => {} // expected
            PlanSelection::Selected { .. } => panic!("should be idle when threshold is very high"),
        }
    }

    #[test]
    fn selects_plan_when_above_threshold() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let drives = hungry_drives();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let candidates = GoapPlanner::generate_candidates(
            &drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        let personality = neutral_personality();
        let selection = UtilityScorer::score_and_select(
            &candidates, &drives, &personality, None, "npc_1", 0,
        );
        match selection {
            PlanSelection::Selected { plan, score } => {
                assert!(!plan.steps.is_empty());
                assert!(score.total >= 0);
            }
            PlanSelection::Idle { reason } => {
                panic!("should select a plan with hungry drives, got idle: {reason}");
            }
        }
    }

    #[test]
    fn higher_drive_pressure_produces_higher_score() {
        let catalog = default_catalog();
        let world = test_world();
        let agent = test_agent();
        let config = PlannerConfig::default();
        let personality = neutral_personality();

        // Low pressure
        let low_drives = DriveSystem::default();
        let mut rng = AgentRng::new(42);
        let low_candidates = GoapPlanner::generate_candidates(
            &low_drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng,
        );
        let weights = personality_to_weights(&personality);
        let low_scores: Vec<PlanScore> = low_candidates
            .iter()
            .map(|c| UtilityScorer::score_plan(c, &low_drives, &weights, None, "npc_1"))
            .collect();
        let low_best = low_scores.iter().map(|s| s.total).max().unwrap_or(0);

        // High pressure
        let high_drives = hungry_drives();
        let mut rng2 = AgentRng::new(42);
        let high_candidates = GoapPlanner::generate_candidates(
            &high_drives, &agent, &world, &catalog, &config,
            PlanningMode::Deliberate, &mut rng2,
        );
        let high_scores: Vec<PlanScore> = high_candidates
            .iter()
            .map(|c| UtilityScorer::score_plan(c, &high_drives, &weights, None, "npc_1"))
            .collect();
        let high_best = high_scores.iter().map(|s| s.total).max().unwrap_or(0);

        assert!(high_best > low_best,
            "high pressure best={high_best} should exceed low pressure best={low_best}");
    }

    // -- Occupancy tests --

    #[test]
    fn occupancy_reserve_and_check() {
        let mut occ = OccupancyState::idle();
        assert!(!occ.is_occupied(0));

        let plan = CandidatePlan {
            plan_id: "p1".into(),
            goal: Goal { goal_id: "g1".into(), description: "eat".into(), target_drive: Some(DriveKind::Food) },
            steps: vec![BoundOperator {
                operator_id: "eat".into(),
                parameters: OperatorParams::default(),
                duration_ticks: 3,
                preconditions: vec![],
                effects: vec![],
            }],
            planning_mode: PlanningMode::Deliberate,
        };
        occ.reserve(&plan, 10);
        assert!(occ.is_occupied(10));
        assert!(occ.is_occupied(12));
        assert!(!occ.is_occupied(13)); // duration 3: ticks 10,11,12
    }

    #[test]
    fn occupancy_clear() {
        let mut occ = OccupancyState::idle();
        let plan = CandidatePlan {
            plan_id: "p1".into(),
            goal: Goal { goal_id: "g1".into(), description: "eat".into(), target_drive: None },
            steps: vec![BoundOperator {
                operator_id: "eat".into(),
                parameters: OperatorParams::default(),
                duration_ticks: 5,
                preconditions: vec![],
                effects: vec![],
            }],
            planning_mode: PlanningMode::Deliberate,
        };
        occ.reserve(&plan, 0);
        assert!(occ.is_occupied(0));
        occ.clear();
        assert!(!occ.is_occupied(0));
    }

    // -- Plan interruption tests --

    #[test]
    fn interrupt_when_precondition_fails() {
        let plan = ActivePlan {
            plan_id: "p1".into(),
            goal: Goal { goal_id: "g1".into(), description: "eat".into(), target_drive: Some(DriveKind::Food) },
            steps: vec![BoundOperator {
                operator_id: "eat".into(),
                parameters: OperatorParams::default(),
                duration_ticks: 1,
                preconditions: vec![FactPredicate {
                    fact_key: "food_available".into(),
                    operator: PredicateOp::Gt,
                    value: 0,
                }],
                effects: vec![],
            }],
            current_step_index: 0,
            planning_mode: PlanningMode::Deliberate,
            started_tick: 0,
        };

        // World with food available — no interrupt.
        let mut world = test_world();
        assert!(!should_interrupt_plan(&plan, &world));

        // Remove food — should interrupt.
        world.facts.insert("food_available".into(), 0);
        assert!(should_interrupt_plan(&plan, &world));
    }

    #[test]
    fn no_interrupt_when_preconditions_satisfied() {
        let plan = ActivePlan {
            plan_id: "p1".into(),
            goal: Goal { goal_id: "g1".into(), description: "eat".into(), target_drive: None },
            steps: vec![BoundOperator {
                operator_id: "eat".into(),
                parameters: OperatorParams::default(),
                duration_ticks: 1,
                preconditions: vec![FactPredicate {
                    fact_key: "food_available".into(),
                    operator: PredicateOp::Gt,
                    value: 0,
                }],
                effects: vec![],
            }],
            current_step_index: 0,
            planning_mode: PlanningMode::Deliberate,
            started_tick: 0,
        };
        let world = test_world(); // food_available = 5
        assert!(!should_interrupt_plan(&plan, &world));
    }

    // -- Social scoring test --

    #[test]
    fn social_graph_affects_score() {
        let _world = test_world();
        let _agent = test_agent();
        let drives = hungry_drives();
        let personality = neutral_personality();
        let weights = personality_to_weights(&personality);

        // Build a plan targeting npc_2.
        let plan = CandidatePlan {
            plan_id: "social_plan".into(),
            goal: Goal { goal_id: "g1".into(), description: "socialize".into(), target_drive: Some(DriveKind::Belonging) },
            steps: vec![BoundOperator {
                operator_id: "greet".into(),
                parameters: OperatorParams { target_npc: Some("npc_2".into()), ..Default::default() },
                duration_ticks: 1,
                preconditions: vec![],
                effects: vec![],
            }],
            planning_mode: PlanningMode::Deliberate,
        };

        // Score without social graph.
        let score_no_social = UtilityScorer::score_plan(
            &plan, &drives, &weights, None, "npc_1",
        );

        // Score with positive trust.
        let mut graph = SocialGraph::new();
        use crate::social::EdgeUpdate;
        graph.update_edge("npc_1", "npc_2", &EdgeUpdate { trust_delta: 80, ..Default::default() }, &neutral_personality());
        let score_with_trust = UtilityScorer::score_plan(
            &plan, &drives, &weights, Some(&graph), "npc_1",
        );

        assert!(score_with_trust.social_component > score_no_social.social_component,
            "trust should increase social component: {} vs {}",
            score_with_trust.social_component, score_no_social.social_component);
    }

    // -- Empty candidates --

    #[test]
    fn select_idle_on_empty_candidates() {
        let selection = UtilityScorer::select(&[], &[], 10);
        match selection {
            PlanSelection::Idle { .. } => {}
            _ => panic!("should be idle on empty candidates"),
        }
    }
}
