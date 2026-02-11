//! NPC Agent: autonomous agent composing identity, drives, memory, beliefs,
//! planning, and occupancy subsystems into a single `tick` loop.
//!
//! The agent loop is: perceive → update memory/beliefs → decay drives → plan → select action.

use contracts::agency::{
    ActivePlan, AgentAction, Aspiration, BoundOperator, CandidatePlan, DriveContext, DriveKind,
    IdentityProfile, Observation, PlanSelection, PlanningMode,
};

use crate::drive::DriveSystem;
use crate::memory::{BeliefModel, MemorySystem};
use crate::operator::{AgentView, OperatorCatalog, WorldView};
use crate::perception::PerceptionSystem;
use crate::planner::{AgentRng, GoapPlanner, OccupancyState, PlannerConfig, UtilityScorer};
use crate::social::SocialGraph;

// ---------------------------------------------------------------------------
// Aspiration change event
// ---------------------------------------------------------------------------

/// Emitted when an NPC's aspiration changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AspirationChangeEvent {
    pub agent_id: String,
    pub old_aspiration: String,
    pub new_aspiration: String,
    pub cause: String,
    pub tick: u64,
}

// ---------------------------------------------------------------------------
// NpcAgent
// ---------------------------------------------------------------------------

/// An autonomous NPC agent that composes all subsystems.
///
/// Each tick the agent: perceives observations → updates memory and beliefs →
/// decays drives → checks plan validity → plans if needed → selects an action.
#[derive(Debug, Clone)]
pub struct NpcAgent {
    pub id: String,
    pub identity: IdentityProfile,
    pub drives: DriveSystem,
    pub memory: MemorySystem,
    pub beliefs: BeliefModel,
    pub active_plan: Option<ActivePlan>,
    pub occupancy: OccupancyState,
    pub aspiration: Aspiration,
    pub location_id: String,
}

/// Result of a single agent tick, including the action and any aspiration change.
#[derive(Debug, Clone)]
pub struct TickResult {
    pub action: AgentAction,
    pub aspiration_change: Option<AspirationChangeEvent>,
}

impl NpcAgent {
    /// Create a new agent with the given identity and starting location.
    pub fn new(
        id: String,
        identity: IdentityProfile,
        aspiration: Aspiration,
        location_id: String,
    ) -> Self {
        Self {
            id,
            identity,
            drives: DriveSystem::default(),
            memory: MemorySystem::new(256),
            beliefs: BeliefModel::new(),
            active_plan: None,
            occupancy: OccupancyState::idle(),
            aspiration,
            location_id,
        }
    }

    /// Main agent loop: perceive → update memory/beliefs → decay drives →
    /// check plan → plan if needed → select action.
    ///
    /// Returns a `TickResult` containing the chosen action and an optional
    /// aspiration change event.
    pub fn tick(
        &mut self,
        world: &WorldView,
        observations: &[Observation],
        catalog: &OperatorCatalog,
        social: Option<&SocialGraph>,
        config: &PlannerConfig,
        current_tick: u64,
        rng: &mut AgentRng,
    ) -> TickResult {
        // 1. Perceive — filter observations and store in memory/beliefs.
        let perceived = PerceptionSystem::perceive_and_remember(
            observations,
            &self.identity.capabilities,
            &mut self.memory,
        );

        // Update beliefs from perceived observations.
        for obs in &perceived {
            self.beliefs.update_from_perception(obs, 100);
        }

        // 2. Decay memory confidence and belief confidence.
        self.memory.decay(current_tick);
        self.beliefs.decay(current_tick);

        // 3. Decay drives based on current context.
        let context = self.build_drive_context();
        self.drives.tick_decay(&context);

        // 4. Detect aspiration changes from drive pressure.
        let aspiration_change = self.check_aspiration_change(current_tick);

        // 5. If occupied, continue current action.
        if self.occupancy.is_occupied(current_tick) {
            return TickResult {
                action: AgentAction::Continue,
                aspiration_change,
            };
        }

        // 6. If we have an active plan, check if it should be interrupted.
        if let Some(ref plan) = self.active_plan {
            if crate::planner::should_interrupt_plan(plan, world) {
                self.active_plan = None;
                self.occupancy.clear();
                return TickResult {
                    action: AgentAction::Replan,
                    aspiration_change,
                };
            }

            // Advance to next step if current step is complete.
            let mut plan = self.active_plan.take().unwrap();
            if plan.current_step_index < plan.steps.len() {
                let step = plan.steps[plan.current_step_index].clone();
                plan.current_step_index += 1;
                self.active_plan = Some(plan.clone());
                self.occupancy.reserve_from_bound(&step, current_tick);
                return TickResult {
                    action: AgentAction::Execute(step),
                    aspiration_change,
                };
            } else {
                // Plan complete.
                self.active_plan = None;
                self.occupancy.clear();
            }
        }

        // 7. Plan — choose mode based on urgency.
        let mode = if self.drives.any_urgent() || !observations.is_empty() {
            PlanningMode::Reactive
        } else {
            PlanningMode::Deliberate
        };

        let agent_view = AgentView {
            agent_id: self.id.clone(),
            location_id: self.location_id.clone(),
        };

        let candidates = GoapPlanner::generate_candidates(
            &self.drives,
            &agent_view,
            world,
            catalog,
            config,
            mode,
            rng,
        );

        // 8. Score and select.
        let selection = UtilityScorer::score_and_select(
            &candidates,
            &self.drives,
            &self.identity.personality,
            social,
            &self.id,
            config.idle_threshold,
        );

        let action = match selection {
            PlanSelection::Selected { plan, .. } => {
                self.activate_plan(&plan, current_tick)
            }
            PlanSelection::Idle { reason } => AgentAction::Idle(reason),
        };

        TickResult {
            action,
            aspiration_change,
        }
    }

    /// Activate a selected plan: store it, reserve occupancy, return the first step.
    fn activate_plan(
        &mut self,
        candidate: &CandidatePlan,
        current_tick: u64,
    ) -> AgentAction {
        if candidate.steps.is_empty() {
            return AgentAction::Idle("empty plan".into());
        }

        let first_step = candidate.steps[0].clone();

        let active = ActivePlan {
            plan_id: candidate.plan_id.clone(),
            goal: candidate.goal.clone(),
            steps: candidate.steps.clone(),
            current_step_index: 1, // first step is being executed now
            planning_mode: candidate.planning_mode,
            started_tick: current_tick,
        };

        self.active_plan = Some(active);
        self.occupancy.reserve_from_bound(&first_step, current_tick);

        AgentAction::Execute(first_step)
    }

    /// Build a drive context from the agent's current state.
    fn build_drive_context(&self) -> DriveContext {
        // In a full implementation this would inspect the world state.
        // For now, derive from location heuristics.
        DriveContext {
            physical_labor: false,
            exposed: false,
            in_danger: false,
            isolated: false,
        }
    }

    /// Check if the agent's aspiration should change based on sustained drive pressure.
    ///
    /// If the top drive has been critical (>= 90) for a while, the aspiration
    /// shifts to address that drive. Returns an event if a change occurred.
    fn check_aspiration_change(&mut self, current_tick: u64) -> Option<AspirationChangeEvent> {
        let top = self.drives.top_pressures(1);
        let (kind, pressure) = match top.first() {
            Some(&(k, p)) => (k, p),
            None => return None,
        };

        // Only shift aspiration when pressure is critical.
        if pressure < 90 {
            return None;
        }

        let new_aspiration = aspiration_for_drive(kind);
        if new_aspiration == self.aspiration.current {
            return None;
        }

        let old = self.aspiration.current.clone();
        let cause = format!("{:?} pressure critical ({})", kind, pressure);

        self.aspiration.previous = Some(old.clone());
        self.aspiration.current = new_aspiration.clone();
        self.aspiration.changed_at_tick = Some(current_tick);
        self.aspiration.change_cause = Some(cause.clone());

        Some(AspirationChangeEvent {
            agent_id: self.id.clone(),
            old_aspiration: old,
            new_aspiration,
            cause,
            tick: current_tick,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extension trait for OccupancyState to reserve from a BoundOperator directly.
trait OccupancyReserveExt {
    fn reserve_from_bound(&mut self, step: &BoundOperator, current_tick: u64);
}

impl OccupancyReserveExt for OccupancyState {
    fn reserve_from_bound(&mut self, step: &BoundOperator, current_tick: u64) {
        self.active_operator_id = Some(step.operator_id.clone());
        self.occupied_until = current_tick + step.duration_ticks;
        self.interruptible = true;
    }
}

/// Map a critical drive to a new aspiration string.
fn aspiration_for_drive(kind: DriveKind) -> String {
    match kind {
        DriveKind::Food => "secure reliable food supply".into(),
        DriveKind::Shelter => "find safe shelter".into(),
        DriveKind::Income => "earn steady income".into(),
        DriveKind::Safety => "ensure personal safety".into(),
        DriveKind::Belonging => "build meaningful relationships".into(),
        DriveKind::Status => "gain social standing".into(),
        DriveKind::Health => "restore health".into(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::default_catalog;
    use contracts::agency::{
        CapabilitySet, NpcValue, PersonalityTraits, Temperament,
    };
    use std::collections::BTreeMap;

    fn test_identity() -> IdentityProfile {
        IdentityProfile {
            profession: "farmer".into(),
            capabilities: CapabilitySet {
                physical: 60,
                social: 40,
                trade: 30,
                combat: 20,
                literacy: 10,
                influence: 10,
                stealth: 10,
                care: 30,
                law: 5,
            },
            personality: PersonalityTraits {
                bravery: 20,
                morality: 50,
                impulsiveness: -10,
                sociability: 30,
                ambition: 10,
                empathy: 40,
                patience: 50,
                curiosity: 20,
                jealousy: -20,
                pride: 10,
                vindictiveness: -30,
                greed: -10,
                loyalty: 40,
                honesty: 60,
                piety: 20,
                vanity: -10,
                humor: 30,
            },
            temperament: Temperament::Phlegmatic,
            values: vec![NpcValue {
                name: "family".into(),
                weight: 80,
            }],
            likes: vec!["gardening".into()],
            dislikes: vec!["violence".into()],
        }
    }

    fn test_aspiration() -> Aspiration {
        Aspiration {
            current: "live peacefully".into(),
            previous: None,
            changed_at_tick: None,
            change_cause: None,
        }
    }

    fn test_world() -> WorldView {
        let mut npcs_by_location = BTreeMap::new();
        npcs_by_location.insert("village".into(), vec!["npc_1".into(), "npc_2".into()]);
        let mut facts = BTreeMap::new();
        facts.insert("food:npc_1".into(), 10);
        facts.insert("money:npc_1".into(), 50);
        WorldView {
            npcs_by_location,
            location_ids: vec!["village".into()],
            objects_by_location: BTreeMap::new(),
            institutions_by_location: BTreeMap::new(),
            resources_by_location: BTreeMap::new(),
            facts,
        }
    }

    #[test]
    fn agent_has_all_identity_fields_and_drives() {
        let agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );

        // Identity fields present.
        assert_eq!(agent.identity.profession, "farmer");
        assert!(!agent.identity.values.is_empty());
        assert!(!agent.identity.likes.is_empty());
        assert!(!agent.identity.dislikes.is_empty());

        // All 7 drives present with valid pressure values.
        assert!(agent.drives.food.current >= 0 && agent.drives.food.current <= 100);
        assert!(agent.drives.shelter.current >= 0 && agent.drives.shelter.current <= 100);
        assert!(agent.drives.income.current >= 0 && agent.drives.income.current <= 100);
        assert!(agent.drives.safety.current >= 0 && agent.drives.safety.current <= 100);
        assert!(agent.drives.belonging.current >= 0 && agent.drives.belonging.current <= 100);
        assert!(agent.drives.status.current >= 0 && agent.drives.status.current <= 100);
        assert!(agent.drives.health.current >= 0 && agent.drives.health.current <= 100);
    }

    #[test]
    fn tick_returns_action_when_not_occupied() {
        let mut agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );
        let world = test_world();
        let catalog = default_catalog();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let result = agent.tick(&world, &[], &catalog, None, &config, 0, &mut rng);

        // Should produce some action (Execute or Idle), not Continue.
        match &result.action {
            AgentAction::Execute(_) | AgentAction::Idle(_) => {}
            other => panic!("expected Execute or Idle, got {:?}", other),
        }
    }

    #[test]
    fn occupied_agent_returns_continue() {
        let mut agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );
        // Manually set occupancy.
        agent.occupancy.active_operator_id = Some("work".into());
        agent.occupancy.occupied_until = 10;

        let world = test_world();
        let catalog = default_catalog();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let result = agent.tick(&world, &[], &catalog, None, &config, 5, &mut rng);
        assert_eq!(result.action, AgentAction::Continue);
    }

    #[test]
    fn aspiration_changes_on_critical_drive_pressure() {
        let mut agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );
        // Push food pressure to critical.
        agent.drives.food.current = 95;

        let world = test_world();
        let catalog = default_catalog();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let result = agent.tick(&world, &[], &catalog, None, &config, 10, &mut rng);

        // Aspiration should have changed.
        let change = result.aspiration_change.expect("expected aspiration change");
        assert_eq!(change.old_aspiration, "live peacefully");
        assert_eq!(change.new_aspiration, "secure reliable food supply");
        assert_eq!(change.tick, 10);
        assert!(change.cause.contains("Food"));
    }

    #[test]
    fn no_aspiration_change_when_pressure_low() {
        let mut agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );
        // All drives at default (0).
        let world = test_world();
        let catalog = default_catalog();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let result = agent.tick(&world, &[], &catalog, None, &config, 0, &mut rng);
        assert!(result.aspiration_change.is_none());
    }

    #[test]
    fn tick_perceives_observations_into_memory() {
        let mut agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );
        let world = test_world();
        let catalog = default_catalog();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let obs = Observation {
            event_id: "evt_1".into(),
            tick: 5,
            location_id: "village".into(),
            event_type: "trade".into(),
            actors: vec!["npc_2".into()],
            visibility: 50,
            details: serde_json::json!({"item": "bread"}),
        };

        agent.tick(&world, &[obs], &catalog, None, &config, 5, &mut rng);

        // Memory should have at least one entry from the observation.
        assert!(!agent.memory.is_empty());
    }

    #[test]
    fn plan_interruption_on_precondition_failure() {
        use contracts::agency::{ActivePlan, BoundOperator, FactPredicate, Goal, PlanningMode, PredicateOp, OperatorParams};

        let mut agent = NpcAgent::new(
            "npc_1".into(),
            test_identity(),
            test_aspiration(),
            "village".into(),
        );

        // Set up an active plan with a precondition that won't be satisfied.
        agent.active_plan = Some(ActivePlan {
            plan_id: "plan_test".into(),
            goal: Goal {
                goal_id: "goal_food".into(),
                description: "eat".into(),
                target_drive: Some(DriveKind::Food),
            },
            steps: vec![BoundOperator {
                operator_id: "eat".into(),
                parameters: OperatorParams::default(),
                duration_ticks: 1,
                preconditions: vec![FactPredicate {
                    fact_key: "food:npc_1".into(),
                    operator: PredicateOp::Gte,
                    value: 999, // impossible to satisfy
                }],
                effects: vec![],
            }],
            current_step_index: 0,
            planning_mode: PlanningMode::Reactive,
            started_tick: 0,
        });

        let world = test_world();
        // food:npc_1 is 10, precondition requires >= 999
        let catalog = default_catalog();
        let config = PlannerConfig::default();
        let mut rng = AgentRng::new(42);

        let result = agent.tick(&world, &[], &catalog, None, &config, 1, &mut rng);
        assert_eq!(result.action, AgentAction::Replan);
        assert!(agent.active_plan.is_none());
    }
}
