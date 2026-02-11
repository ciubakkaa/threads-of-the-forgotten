use contracts::{
    ActivePlan, AgentAction, Aspiration, AspirationChange, DriveContext, DriveKind, DriveSystem,
    Goal, IdentityProfile, NpcOccupancyKind, NpcOccupancyState, PlanSummary, PlanningMode,
    ReasonEnvelope, RejectedPlan,
};

use crate::drive::DriveSubsystem;
use crate::memory::{BeliefModel, MemorySystem};
use crate::operator::{OperatorCatalog, PlanningWorldView};
use crate::perception::{PerceptionContext, PerceptionSystem};
use crate::planner::{GoapPlanner, PlannerActorState, PlannerConfig};

#[derive(Debug, Clone)]
pub struct NpcAgent {
    pub id: String,
    pub identity: IdentityProfile,
    pub drives: DriveSubsystem,
    pub memory: MemorySystem,
    pub beliefs: BeliefModel,
    pub active_plan: Option<ActivePlan>,
    pub occupancy: NpcOccupancyState,
    pub aspiration: Aspiration,
    pub location_id: String,
}

#[derive(Debug, Clone)]
pub struct TickResult {
    pub action: AgentAction,
    pub reason: Option<ReasonEnvelope>,
    pub aspiration_change: Option<AspirationChange>,
    pub drive_threshold_crossings: Vec<DriveKind>,
}

impl NpcAgent {
    pub fn tick(
        &mut self,
        tick: u64,
        world: &PlanningWorldView,
        observations: &[contracts::Observation],
        catalog: &OperatorCatalog,
        planner_config: &PlannerConfig,
    ) -> TickResult {
        if self.occupancy.until_tick > tick
            && matches!(
                self.occupancy.occupancy,
                NpcOccupancyKind::ExecutingPlanStep
            )
        {
            return TickResult {
                action: AgentAction::Continue,
                reason: None,
                aspiration_change: None,
                drive_threshold_crossings: Vec::new(),
            };
        }

        let perception = PerceptionSystem;
        for observation in observations {
            let filtered = perception.ingest(
                &mut self.memory,
                observation,
                &PerceptionContext {
                    attention: 70,
                    proximity: 80,
                    sensory: 75,
                },
            );
            self.beliefs.update_from_perception(&filtered, &self.id, 80);
        }

        let drive_urgent_before = [
            (
                DriveKind::Food,
                self.drives.drives.food.current,
                self.drives.drives.food.urgency_threshold,
            ),
            (
                DriveKind::Shelter,
                self.drives.drives.shelter.current,
                self.drives.drives.shelter.urgency_threshold,
            ),
            (
                DriveKind::Income,
                self.drives.drives.income.current,
                self.drives.drives.income.urgency_threshold,
            ),
            (
                DriveKind::Safety,
                self.drives.drives.safety.current,
                self.drives.drives.safety.urgency_threshold,
            ),
            (
                DriveKind::Belonging,
                self.drives.drives.belonging.current,
                self.drives.drives.belonging.urgency_threshold,
            ),
            (
                DriveKind::Status,
                self.drives.drives.status.current,
                self.drives.drives.status.urgency_threshold,
            ),
            (
                DriveKind::Health,
                self.drives.drives.health.current,
                self.drives.drives.health.urgency_threshold,
            ),
        ]
        .into_iter()
        .filter(|(_, current, threshold)| *current >= *threshold)
        .map(|(kind, _, _)| kind)
        .collect::<Vec<_>>();

        self.memory.decay(tick);
        self.beliefs.decay(tick);
        self.drives.tick_decay(&DriveContext::default());

        let previous_aspiration = self.aspiration.label.clone();
        let top_pressure = self
            .drives
            .top_pressures(1)
            .first()
            .map(|(kind, _)| format!("satisfy_{kind:?}").to_lowercase())
            .unwrap_or_else(|| "survive".to_string());
        let aspiration_change = if previous_aspiration != top_pressure {
            let old = self.aspiration.clone();
            self.aspiration.label = top_pressure;
            self.aspiration.updated_tick = tick;
            self.aspiration.cause = "dominant_drive_shift".to_string();
            Some(AspirationChange {
                npc_id: self.id.clone(),
                old_aspiration: Some(old),
                new_aspiration: self.aspiration.clone(),
                cause: self.aspiration.cause.clone(),
                tick,
            })
        } else {
            None
        };

        let drive_urgent_after = [
            (
                DriveKind::Food,
                self.drives.drives.food.current,
                self.drives.drives.food.urgency_threshold,
            ),
            (
                DriveKind::Shelter,
                self.drives.drives.shelter.current,
                self.drives.drives.shelter.urgency_threshold,
            ),
            (
                DriveKind::Income,
                self.drives.drives.income.current,
                self.drives.drives.income.urgency_threshold,
            ),
            (
                DriveKind::Safety,
                self.drives.drives.safety.current,
                self.drives.drives.safety.urgency_threshold,
            ),
            (
                DriveKind::Belonging,
                self.drives.drives.belonging.current,
                self.drives.drives.belonging.urgency_threshold,
            ),
            (
                DriveKind::Status,
                self.drives.drives.status.current,
                self.drives.drives.status.urgency_threshold,
            ),
            (
                DriveKind::Health,
                self.drives.drives.health.current,
                self.drives.drives.health.urgency_threshold,
            ),
        ]
        .into_iter()
        .filter(|(_, current, threshold)| *current >= *threshold)
        .map(|(kind, _, _)| kind)
        .collect::<Vec<_>>();
        let drive_threshold_crossings = drive_urgent_after
            .iter()
            .filter(|kind| !drive_urgent_before.contains(kind))
            .copied()
            .collect::<Vec<_>>();

        let planning_mode = if observations.is_empty() {
            PlanningMode::Deliberate
        } else {
            PlanningMode::Reactive
        };

        let planner_state = PlannerActorState {
            agent_id: &self.id,
            identity: &self.identity,
            drives: &self.drives.drives,
            location_id: &self.location_id,
        };

        let candidates = GoapPlanner::generate_candidates(
            &planner_state,
            world,
            catalog,
            planner_config,
            planning_mode,
        );
        let scores = candidates
            .iter()
            .map(|candidate| GoapPlanner::score_plan(candidate, &planner_state, world))
            .collect::<Vec<_>>();
        let selection = GoapPlanner::select(&candidates, &scores, planner_config.idle_threshold);

        let Some(selected_plan_id) = selection.selected_plan_id else {
            self.occupancy = NpcOccupancyState {
                npc_id: self.id.clone(),
                tick,
                occupancy: NpcOccupancyKind::Idle,
                state_tag: "idle".to_string(),
                until_tick: tick + 1,
                interruptible: true,
                location_id: self.location_id.clone(),
                active_plan_id: None,
                active_operator_id: None,
            };
            return TickResult {
                action: AgentAction::Idle(
                    selection
                        .idle_reason
                        .unwrap_or_else(|| "planner_idle".to_string()),
                ),
                reason: None,
                aspiration_change,
                drive_threshold_crossings,
            };
        };

        let Some(selected) = candidates
            .iter()
            .find(|candidate| candidate.plan_id == selected_plan_id)
            .cloned()
        else {
            return TickResult {
                action: AgentAction::Replan,
                reason: None,
                aspiration_change,
                drive_threshold_crossings,
            };
        };

        let first_step = selected.steps.first().cloned();
        self.active_plan = Some(ActivePlan {
            npc_id: self.id.clone(),
            plan_id: selected.plan_id.clone(),
            goal: selected.goal.clone(),
            planning_mode: selected.planning_mode,
            steps: selected.steps.clone(),
            next_step_index: 0,
            created_tick: tick,
        });

        self.occupancy = NpcOccupancyState {
            npc_id: self.id.clone(),
            tick,
            occupancy: NpcOccupancyKind::ExecutingPlanStep,
            state_tag: "executing".to_string(),
            until_tick: tick
                + first_step
                    .as_ref()
                    .map(|step| step.duration_ticks)
                    .unwrap_or(1),
            interruptible: true,
            location_id: self.location_id.clone(),
            active_plan_id: Some(selected.plan_id.clone()),
            active_operator_id: first_step.as_ref().map(|step| step.operator_id.clone()),
        };

        let reason = Some(ReasonEnvelope {
            agent_id: self.id.clone(),
            tick,
            goal: selected.goal.clone(),
            selected_plan: PlanSummary {
                plan_id: selected.plan_id.clone(),
                goal: selected.goal.clone(),
                utility_score: scores.iter().map(|entry| entry.score).max().unwrap_or(0),
            },
            operator_chain: first_step
                .iter()
                .map(|step| step.operator_id.clone())
                .collect::<Vec<_>>(),
            drive_pressures: self.drives.top_pressures(7),
            rejected_alternatives: scores
                .iter()
                .filter(|score| score.plan_id != selected.plan_id)
                .map(|score| RejectedPlan {
                    plan_id: score.plan_id.clone(),
                    goal: Goal {
                        goal_id: format!("goal:{}", score.plan_id),
                        label: "alternative".to_string(),
                        priority: score.score,
                    },
                    score: score.score,
                    rejection_reason: "lower_utility".to_string(),
                })
                .collect(),
            contextual_constraints: vec![format!("location:{}", self.location_id)],
            planning_mode,
        });

        TickResult {
            action: first_step
                .map(AgentAction::Execute)
                .unwrap_or_else(|| AgentAction::Idle("no_step".to_string())),
            reason,
            aspiration_change,
            drive_threshold_crossings,
        }
    }

    pub fn from_identity(
        id: String,
        identity: IdentityProfile,
        aspiration: Aspiration,
        location_id: String,
        drives: DriveSystem,
    ) -> Self {
        Self {
            occupancy: NpcOccupancyState {
                npc_id: id.clone(),
                tick: 0,
                occupancy: NpcOccupancyKind::Idle,
                state_tag: "idle".to_string(),
                until_tick: 0,
                interruptible: true,
                location_id: location_id.clone(),
                active_plan_id: None,
                active_operator_id: None,
            },
            id,
            identity,
            drives: DriveSubsystem::new(drives),
            memory: MemorySystem::new(128),
            beliefs: BeliefModel::new(25),
            active_plan: None,
            aspiration,
            location_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{CapabilitySet, DriveValue, PersonalityTraits, Temperament};

    use crate::operator::{OperatorCatalog, PlanningWorldView};
    use crate::planner::PlannerConfig;

    #[test]
    fn agent_has_identity_and_drive_completeness() {
        let identity = IdentityProfile {
            profession: "worker".to_string(),
            capabilities: CapabilitySet {
                physical: 1,
                social: 1,
                trade: 1,
                combat: 1,
                literacy: 1,
                influence: 1,
                stealth: 1,
                care: 1,
                law: 1,
            },
            personality: PersonalityTraits {
                bravery: 0,
                morality: 0,
                impulsiveness: 0,
                sociability: 0,
                ambition: 0,
                empathy: 0,
                patience: 0,
                curiosity: 0,
                jealousy: 0,
                pride: 0,
                vindictiveness: 0,
                greed: 0,
                loyalty: 0,
                honesty: 0,
                piety: 0,
                vanity: 0,
                humor: 0,
            },
            temperament: Temperament::Calm,
            values: vec![],
            likes: vec![],
            dislikes: vec![],
        };

        let drives = DriveSystem {
            food: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
            shelter: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
            income: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
            safety: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
            belonging: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
            status: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
            health: DriveValue {
                current: 1,
                decay_rate: 1,
                urgency_threshold: 5,
            },
        };

        let mut agent = NpcAgent::from_identity(
            "npc:1".to_string(),
            identity,
            Aspiration {
                aspiration_id: "asp:1".to_string(),
                label: "survive".to_string(),
                updated_tick: 0,
                cause: "init".to_string(),
            },
            "loc:1".to_string(),
            drives,
        );

        let world = PlanningWorldView {
            object_ids: vec!["obj:1".to_string()],
            location_ids: vec!["loc:2".to_string()],
            ..PlanningWorldView::default()
        };
        let catalog = OperatorCatalog::default_catalog();
        let result = agent.tick(1, &world, &[], &catalog, &PlannerConfig::default());

        assert!(matches!(
            result.action,
            AgentAction::Execute(_)
                | AgentAction::Idle(_)
                | AgentAction::Replan
                | AgentAction::Continue
        ));
        assert_eq!(agent.occupancy.npc_id, "npc:1");
    }
}
