use contracts::{
    ActivePlan, AgentAction, Aspiration, AspirationChange, CommitmentState, DriveContext,
    DriveKind, DriveSystem, Goal, IdentityProfile, NpcOccupancyKind, NpcOccupancyState,
    PlanSummary, PlanningMode, ReasonEnvelope, RejectedPlan,
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
    pub plan_backlog: Vec<ActivePlan>,
    pub commitments: Vec<CommitmentState>,
    pub goal_tracks: Vec<GoalTrack>,
    pub arc_tracks: Vec<ArcTrack>,
    pub life_domain: LifeDomainState,
    pub fatigue: i64,
    pub recreation_need: i64,
    pub time_budget: TimeBudgetAllocation,
    pub occupancy: NpcOccupancyState,
    pub aspiration: Aspiration,
    pub location_id: String,
    pub reactive_streak: u16,
    pub last_deliberate_tick: u64,
}

#[derive(Debug, Clone)]
pub struct TickResult {
    pub action: AgentAction,
    pub reason: Option<ReasonEnvelope>,
    pub aspiration_change: Option<AspirationChange>,
    pub drive_threshold_crossings: Vec<DriveKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalTrackStatus {
    Active,
    Paused,
    Blocked,
    Derailed,
    Abandoned,
    Completed,
}

#[derive(Debug, Clone)]
pub struct GoalTrack {
    pub track_id: String,
    pub plan_id: String,
    pub goal_family: String,
    pub stage: String,
    pub status: GoalTrackStatus,
    pub progress_ticks: u64,
    pub inertia_score: i64,
    pub utility_hint: i64,
    pub blockers: Vec<String>,
    pub last_advanced_tick: u64,
    pub last_break_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArcTrackStatus {
    Active,
    Paused,
    Blocked,
    Derailed,
    Completed,
}

#[derive(Debug, Clone)]
pub struct ArcTrack {
    pub arc_id: String,
    pub title: String,
    pub family: String,
    pub milestone_index: usize,
    pub milestones: Vec<String>,
    pub progress: i64,
    pub status: ArcTrackStatus,
    pub blockers: Vec<String>,
    pub last_advanced_tick: u64,
    pub last_break_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TimeBudgetAllocation {
    pub work: i64,
    pub social: i64,
    pub recovery: i64,
    pub household: i64,
    pub exploration: i64,
}

#[derive(Debug, Clone)]
pub struct LifeDomainState {
    pub career_tier: u8,
    pub career_xp: i64,
    pub marital_status: String,
    pub spouse_npc_id: Option<String>,
    pub children_count: u8,
    pub home_quality: i64,
    pub property_value: i64,
    pub social_reputation: i64,
    pub contract_successes: u32,
    pub contract_failures: u32,
    pub last_domain_event_tick: u64,
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
        self.update_budget_pressures(observations);
        self.rebalance_time_budget(world);

        let top_pressure = self
            .drives
            .top_pressures(1)
            .first()
            .cloned()
            .unwrap_or((DriveKind::Food, 0));
        let dominant_aspiration = format!("satisfy_{:?}", top_pressure.0).to_lowercase();
        let determination = self.determination_score();
        let laziness = self.laziness_score();
        let disruption = Self::observation_disruption(observations);
        self.refresh_arc_tracks(world, tick, determination, laziness);
        let anchored_aspiration = self
            .active_plan
            .as_ref()
            .map(|plan| plan.goal.label.clone())
            .or_else(|| {
                self.goal_tracks
                    .iter()
                    .filter(|track| {
                        matches!(
                            track.status,
                            GoalTrackStatus::Active
                                | GoalTrackStatus::Paused
                                | GoalTrackStatus::Blocked
                        )
                    })
                    .max_by_key(|track| track.utility_hint + track.inertia_score)
                    .map(|track| format!("satisfy_{}", track.goal_family))
            })
            .or_else(|| {
                self.plan_backlog
                    .first()
                    .map(|plan| plan.goal.label.clone())
            });
        let next_aspiration = if determination >= laziness + 12 && disruption < 65 {
            anchored_aspiration.unwrap_or_else(|| dominant_aspiration.clone())
        } else {
            dominant_aspiration.clone()
        };
        let previous_aspiration = self.aspiration.label.clone();
        let aspiration_change = if previous_aspiration != next_aspiration {
            let old = self.aspiration.clone();
            self.aspiration.label = next_aspiration;
            self.aspiration.updated_tick = tick;
            self.aspiration.cause = if determination >= laziness + 12 {
                "commitment_anchor".to_string()
            } else {
                "dominant_drive_shift".to_string()
            };
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

        let urgent_observation = Self::has_urgent_observation(observations);
        let planning_mode = if observations.is_empty() {
            PlanningMode::Deliberate
        } else if urgent_observation || disruption >= 70 {
            PlanningMode::Reactive
        } else if self.reactive_streak >= 1 {
            PlanningMode::Deliberate
        } else {
            PlanningMode::Reactive
        };
        if matches!(planning_mode, PlanningMode::Reactive) {
            self.reactive_streak = self.reactive_streak.saturating_add(1);
        } else {
            self.reactive_streak = 0;
            self.last_deliberate_tick = tick;
        }

        let should_derail_active_plan = self.active_plan.is_some()
            && (disruption >= 85
                || (urgent_observation && disruption >= 70)
                || (urgent_observation
                    && ((top_pressure.0 == DriveKind::Safety && top_pressure.1 >= 88)
                        || (top_pressure.0 == DriveKind::Health && top_pressure.1 >= 90))));
        if should_derail_active_plan {
            if let Some(active) = self.active_plan.take() {
                if let Some(step) = active.steps.get(
                    active
                        .next_step_index
                        .min(active.steps.len().saturating_sub(1)),
                ) {
                    self.set_arc_status_for_operator(
                        &step.operator_id,
                        ArcTrackStatus::Derailed,
                        Some("high_priority_disruption".to_string()),
                        tick,
                    );
                }
                let commitment_id = Self::commitment_id_for(&self.id, &active.plan_id);
                let active_plan_id = active.plan_id.clone();
                if determination >= laziness + 20 && disruption < 90 {
                    self.set_commitment_status(&commitment_id, "delayed", tick);
                    self.queue_plan(active);
                } else {
                    self.set_commitment_status(&commitment_id, "broken", tick);
                }
                self.set_track_status(
                    &active_plan_id,
                    GoalTrackStatus::Derailed,
                    Some("high_priority_disruption".to_string()),
                    tick,
                );
            }
            self.occupancy = NpcOccupancyState {
                npc_id: self.id.clone(),
                tick,
                occupancy: NpcOccupancyKind::Idle,
                state_tag: "derailed".to_string(),
                until_tick: tick + 1,
                interruptible: true,
                location_id: self.location_id.clone(),
                active_plan_id: None,
                active_operator_id: None,
            };
        }

        if let Some(mut active) = self.active_plan.take() {
            if active.next_step_index < active.steps.len() {
                let idx = active.next_step_index;
                let step = active.steps[idx].clone();
                active.next_step_index += 1;
                let commitment_id = Self::commitment_id_for(&self.id, &active.plan_id);
                self.bump_commitment_progress(&commitment_id, step.duration_ticks, tick);
                self.apply_step_budget_effect(&step.operator_id, step.duration_ticks);
                self.note_track_progress(&active.plan_id, &step.operator_id, tick);
                self.note_arc_progress(&step.operator_id, step.duration_ticks, tick, world);
                let finished = active.next_step_index >= active.steps.len();
                let interruptible = step.duration_ticks == 0;
                if finished {
                    self.set_commitment_status(&commitment_id, "completed", tick);
                } else {
                    self.set_commitment_status(&commitment_id, "active", tick);
                    self.active_plan = Some(active.clone());
                }
                self.occupancy = NpcOccupancyState {
                    npc_id: self.id.clone(),
                    tick,
                    occupancy: NpcOccupancyKind::ExecutingPlanStep,
                    state_tag: if finished {
                        "executing_final_step".to_string()
                    } else {
                        "executing_commitment".to_string()
                    },
                    until_tick: tick + step.duration_ticks.max(1),
                    interruptible,
                    location_id: self.location_id.clone(),
                    active_plan_id: Some(active.plan_id.clone()),
                    active_operator_id: Some(step.operator_id.clone()),
                };
                return TickResult {
                    action: AgentAction::Execute(step.clone()),
                    reason: Some(ReasonEnvelope {
                        agent_id: self.id.clone(),
                        tick,
                        goal: active.goal.clone(),
                        selected_plan: PlanSummary {
                            plan_id: active.plan_id.clone(),
                            goal: active.goal.clone(),
                            utility_score: active.goal.priority,
                        },
                        operator_chain: active
                            .steps
                            .iter()
                            .skip(idx)
                            .map(|bound| bound.operator_id.clone())
                            .collect::<Vec<_>>(),
                        drive_pressures: self.drives.top_pressures(7),
                        rejected_alternatives: Vec::new(),
                        contextual_constraints: vec![
                            format!("location:{}", self.location_id),
                            format!("commitment:{commitment_id}"),
                        ],
                        planning_mode,
                    }),
                    aspiration_change,
                    drive_threshold_crossings,
                };
            }
            let commitment_id = Self::commitment_id_for(&self.id, &active.plan_id);
            self.set_commitment_status(&commitment_id, "completed", tick);
        }

        if self.active_plan.is_none() && determination >= laziness + 8 && disruption < 60 {
            if let Some(mut resumed) =
                self.pop_backlog_plan_with_policy(determination, laziness, disruption)
            {
                if resumed.next_step_index < resumed.steps.len() {
                    let idx = resumed.next_step_index;
                    let step = resumed.steps[idx].clone();
                    resumed.next_step_index += 1;
                    let commitment_id = Self::commitment_id_for(&self.id, &resumed.plan_id);
                    self.bump_commitment_progress(&commitment_id, step.duration_ticks, tick);
                    self.apply_step_budget_effect(&step.operator_id, step.duration_ticks);
                    self.note_track_progress(&resumed.plan_id, &step.operator_id, tick);
                    self.note_arc_progress(&step.operator_id, step.duration_ticks, tick, world);
                    let finished = resumed.next_step_index >= resumed.steps.len();
                    if finished {
                        self.set_commitment_status(&commitment_id, "completed", tick);
                    } else {
                        self.set_commitment_status(&commitment_id, "active", tick);
                        self.active_plan = Some(resumed.clone());
                    }
                    self.occupancy = NpcOccupancyState {
                        npc_id: self.id.clone(),
                        tick,
                        occupancy: NpcOccupancyKind::ExecutingPlanStep,
                        state_tag: "resumed_commitment".to_string(),
                        until_tick: tick + step.duration_ticks.max(1),
                        interruptible: true,
                        location_id: self.location_id.clone(),
                        active_plan_id: Some(resumed.plan_id.clone()),
                        active_operator_id: Some(step.operator_id.clone()),
                    };
                    return TickResult {
                        action: AgentAction::Execute(step),
                        reason: Some(ReasonEnvelope {
                            agent_id: self.id.clone(),
                            tick,
                            goal: resumed.goal.clone(),
                            selected_plan: PlanSummary {
                                plan_id: resumed.plan_id.clone(),
                                goal: resumed.goal.clone(),
                                utility_score: resumed.goal.priority,
                            },
                            operator_chain: resumed
                                .steps
                                .iter()
                                .skip(idx)
                                .map(|bound| bound.operator_id.clone())
                                .collect::<Vec<_>>(),
                            drive_pressures: self.drives.top_pressures(7),
                            rejected_alternatives: Vec::new(),
                            contextual_constraints: vec![
                                format!("location:{}", self.location_id),
                                format!("commitment:{commitment_id}"),
                            ],
                            planning_mode,
                        }),
                        aspiration_change,
                        drive_threshold_crossings,
                    };
                }
            }
        }

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
            .map(|candidate| GoapPlanner::score_plan(candidate, &planner_state, world, catalog))
            .collect::<Vec<_>>();
        self.refresh_goal_tracks(&candidates, &scores, tick, determination - laziness);
        let default_selection =
            GoapPlanner::select(&candidates, &scores, planner_config.idle_threshold);
        let selected_plan_id = self.select_plan_id_from_portfolio(
            default_selection.selected_plan_id.clone(),
            &candidates,
            &scores,
            determination,
            laziness,
            disruption,
        );

        let Some(selected_plan_id) = selected_plan_id else {
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
                    default_selection
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
                action: AgentAction::Idle("selection_miss".to_string()),
                reason: None,
                aspiration_change,
                drive_threshold_crossings,
            };
        };

        let mut ranked = scores.clone();
        ranked.sort_by(|a, b| b.score.cmp(&a.score).then(a.plan_id.cmp(&b.plan_id)));
        for scored in ranked.iter().take(4) {
            if scored.plan_id == selected.plan_id {
                continue;
            }
            if let Some(candidate) = candidates
                .iter()
                .find(|candidate| candidate.plan_id == scored.plan_id)
            {
                self.queue_plan(ActivePlan {
                    npc_id: self.id.clone(),
                    plan_id: candidate.plan_id.clone(),
                    goal: candidate.goal.clone(),
                    planning_mode: candidate.planning_mode,
                    steps: candidate.steps.clone(),
                    next_step_index: 0,
                    created_tick: tick,
                });
            }
        }

        let mut selected_active = ActivePlan {
            npc_id: self.id.clone(),
            plan_id: selected.plan_id.clone(),
            goal: selected.goal.clone(),
            planning_mode: selected.planning_mode,
            steps: selected.steps.clone(),
            next_step_index: 0,
            created_tick: tick,
        };
        let first_step = selected_active.steps.first().cloned();
        if first_step.is_some() {
            selected_active.next_step_index = 1;
        }
        self.upsert_track_for_plan(
            &selected_active,
            scores
                .iter()
                .find(|entry| entry.plan_id == selected_active.plan_id)
                .map(|entry| entry.score)
                .unwrap_or(0),
            tick,
            determination - laziness,
        );
        let commitment_id = Self::commitment_id_for(&self.id, &selected_active.plan_id);
        self.start_or_refresh_commitment(
            &selected_active,
            tick,
            determination - laziness,
            first_step
                .as_ref()
                .map(|step| step.duration_ticks)
                .unwrap_or(0),
        );
        if let Some(step) = first_step.as_ref() {
            self.apply_step_budget_effect(&step.operator_id, step.duration_ticks);
            self.note_track_progress(&selected_active.plan_id, &step.operator_id, tick);
            self.note_arc_progress(&step.operator_id, step.duration_ticks, tick, world);
        }
        if selected_active.next_step_index < selected_active.steps.len() {
            self.active_plan = Some(selected_active.clone());
        } else {
            self.set_commitment_status(&commitment_id, "completed", tick);
            self.active_plan = None;
        }

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
            interruptible: first_step
                .as_ref()
                .map(|step| step.duration_ticks == 0)
                .unwrap_or(true),
            location_id: self.location_id.clone(),
            active_plan_id: Some(selected_active.plan_id.clone()),
            active_operator_id: first_step.as_ref().map(|step| step.operator_id.clone()),
        };

        let reason = Some(ReasonEnvelope {
            agent_id: self.id.clone(),
            tick,
            goal: selected.goal.clone(),
            selected_plan: PlanSummary {
                plan_id: selected.plan_id.clone(),
                goal: selected.goal.clone(),
                utility_score: scores
                    .iter()
                    .find(|entry| entry.plan_id == selected.plan_id)
                    .map(|entry| entry.score)
                    .unwrap_or(0),
            },
            operator_chain: selected
                .steps
                .iter()
                .map(|bound| bound.operator_id.clone())
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
            contextual_constraints: vec![
                format!("location:{}", self.location_id),
                format!("commitment:{commitment_id}"),
            ],
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

    fn determination_score(&self) -> i64 {
        let personality = &self.identity.personality;
        ((personality.ambition * 2 + personality.patience + personality.loyalty)
            - personality.impulsiveness / 2)
            .clamp(0, 100)
    }

    fn laziness_score(&self) -> i64 {
        let personality = &self.identity.personality;
        let drive_drag =
            (self.drives.drives.health.current + self.drives.drives.shelter.current) / 2;
        (55 - personality.ambition / 3 - personality.curiosity / 4 + drive_drag / 5).clamp(0, 100)
    }

    fn observation_disruption(observations: &[contracts::Observation]) -> i64 {
        observations.iter().fold(0_i64, |acc, observation| {
            let severity = match observation.event_type {
                contracts::EventType::BrawlStarted
                | contracts::EventType::PunchThrown
                | contracts::EventType::GuardsDispatched
                | contracts::EventType::ConflictDetected
                | contracts::EventType::PlanInterrupted
                | contracts::EventType::TheftCommitted => 85,
                contracts::EventType::IllnessContracted
                | contracts::EventType::InstitutionQueueFull
                | contracts::EventType::ReactionDepthExceeded => 70,
                contracts::EventType::RumorPropagated
                | contracts::EventType::RumorDistorted
                | contracts::EventType::ObservationLogged => 35,
                _ => 20,
            };
            acc.max(severity + observation.visibility / 5)
        })
    }

    fn has_urgent_observation(observations: &[contracts::Observation]) -> bool {
        observations.iter().any(|observation| {
            matches!(
                observation.event_type,
                contracts::EventType::TheftCommitted
                    | contracts::EventType::BrawlStarted
                    | contracts::EventType::PunchThrown
                    | contracts::EventType::GuardsDispatched
                    | contracts::EventType::ConflictDetected
                    | contracts::EventType::ConflictResolved
                    | contracts::EventType::PlanInterrupted
                    | contracts::EventType::InstitutionQueueFull
            )
        })
    }

    fn update_budget_pressures(&mut self, observations: &[contracts::Observation]) {
        let disruption = Self::observation_disruption(observations);
        let stress = (self.drives.drives.health.current + self.drives.drives.safety.current) / 2;
        self.fatigue = (self.fatigue + 1 + stress / 30 + disruption / 60).clamp(0, 100);
        self.recreation_need =
            (self.recreation_need + 1 + self.fatigue / 25 + stress / 35).clamp(0, 100);
    }

    fn rebalance_time_budget(&mut self, world: &PlanningWorldView) {
        let money = world
            .facts
            .get("agent.money")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let belonging_pressure = self.drives.drives.belonging.current;
        let status_pressure = self.drives.drives.status.current;

        let mut work = 32;
        let mut social = 20;
        let mut recovery = 20;
        let mut household = 18;
        let mut exploration = 10;

        if money <= 3 {
            work += 20;
            household -= 4;
        } else if money >= 20 {
            social += 8;
            exploration += 6;
        }

        if self.fatigue >= 65 {
            recovery += 18;
            work -= 8;
            exploration -= 5;
        }
        if self.recreation_need >= 65 {
            social += 12;
            recovery += 10;
            work -= 8;
        }
        if belonging_pressure >= 65 {
            social += 10;
        }
        if status_pressure >= 65 {
            work += 8;
            exploration += 4;
        }

        work = work.clamp(5, 70);
        social = social.clamp(5, 60);
        recovery = recovery.clamp(5, 60);
        household = household.clamp(5, 50);
        exploration = exploration.clamp(5, 40);
        let sum = work + social + recovery + household + exploration;
        self.time_budget = TimeBudgetAllocation {
            work: (work * 100) / sum,
            social: (social * 100) / sum,
            recovery: (recovery * 100) / sum,
            household: (household * 100) / sum,
            exploration: (exploration * 100) / sum,
        };
    }

    fn apply_step_budget_effect(&mut self, operator_id: &str, duration_ticks: u64) {
        let family = operator_id.split(':').next().unwrap_or("household");
        let duration = i64::try_from(duration_ticks.max(1)).unwrap_or(1);
        match family {
            "livelihood" | "security" | "governance" | "information" => {
                self.fatigue = (self.fatigue + 4 * duration).clamp(0, 100);
                self.recreation_need = (self.recreation_need + 3 * duration).clamp(0, 100);
            }
            "mobility" => {
                self.fatigue = (self.fatigue + 3 * duration).clamp(0, 100);
                self.recreation_need = (self.recreation_need + 2 * duration).clamp(0, 100);
            }
            "social" => {
                self.fatigue = (self.fatigue + duration).clamp(0, 100);
                self.recreation_need = (self.recreation_need - 2 * duration).clamp(0, 100);
            }
            "leisure" => {
                self.fatigue = (self.fatigue - 5 * duration).clamp(0, 100);
                self.recreation_need = (self.recreation_need - 8 * duration).clamp(0, 100);
                if operator_id.ends_with(":drink") {
                    // Relief now, mild fatigue rebound.
                    self.fatigue = (self.fatigue + duration).clamp(0, 100);
                }
            }
            "health" => {
                self.fatigue = (self.fatigue - 4 * duration).clamp(0, 100);
                self.recreation_need = (self.recreation_need - duration).clamp(0, 100);
            }
            "household" => {
                if operator_id.ends_with(":sleep") {
                    self.fatigue = (self.fatigue - 6 * duration).clamp(0, 100);
                    self.recreation_need = (self.recreation_need - 3 * duration).clamp(0, 100);
                } else {
                    self.fatigue = (self.fatigue + duration).clamp(0, 100);
                }
            }
            _ => {}
        }
    }

    fn canonical_family(family: &str) -> &str {
        match family {
            "health" => "recovery",
            "information" => "exploration",
            "security" | "governance" => "civic",
            other => other,
        }
    }

    fn arc_blueprint_for(family: &str) -> (String, Vec<String>) {
        match Self::canonical_family(family) {
            "livelihood" => (
                "economic_uplift".to_string(),
                vec![
                    "stabilize_income".to_string(),
                    "build_buffer".to_string(),
                    "specialize_skill".to_string(),
                    "secure_assets".to_string(),
                    "mentor_or_lead".to_string(),
                ],
            ),
            "social" => (
                "bond_network".to_string(),
                vec![
                    "meet_people".to_string(),
                    "grow_trust".to_string(),
                    "form_strong_bond".to_string(),
                    "mutual_commitment".to_string(),
                    "family_or_circle".to_string(),
                ],
            ),
            "household" => (
                "home_stability".to_string(),
                vec![
                    "maintain_shelter".to_string(),
                    "improve_routines".to_string(),
                    "expand_capacity".to_string(),
                    "resilient_household".to_string(),
                ],
            ),
            "recovery" => (
                "wellbeing".to_string(),
                vec![
                    "recover_balance".to_string(),
                    "build_habits".to_string(),
                    "stabilize_health".to_string(),
                    "sustain_energy".to_string(),
                ],
            ),
            "leisure" => (
                "joy_and_release".to_string(),
                vec![
                    "find_relief".to_string(),
                    "build_hobby".to_string(),
                    "social_fun".to_string(),
                    "community_presence".to_string(),
                ],
            ),
            "civic" => (
                "public_standing".to_string(),
                vec![
                    "be_visible".to_string(),
                    "earn_recognition".to_string(),
                    "shape_outcomes".to_string(),
                    "hold_influence".to_string(),
                ],
            ),
            _ => (
                "curiosity_path".to_string(),
                vec![
                    "observe".to_string(),
                    "experiment".to_string(),
                    "integrate_lessons".to_string(),
                    "mastery".to_string(),
                ],
            ),
        }
    }

    fn ensure_arc_track_for_family(&mut self, family: &str, tick: u64) {
        let canonical = Self::canonical_family(family);
        if self
            .arc_tracks
            .iter()
            .any(|track| track.family == canonical)
        {
            return;
        }
        let (title, milestones) = Self::arc_blueprint_for(canonical);
        self.arc_tracks.push(ArcTrack {
            arc_id: format!("arc:{}:{}", self.id, canonical),
            title,
            family: canonical.to_string(),
            milestone_index: 0,
            milestones,
            progress: 0,
            status: ArcTrackStatus::Active,
            blockers: Vec::new(),
            last_advanced_tick: tick,
            last_break_reason: None,
        });
    }

    fn refresh_arc_tracks(
        &mut self,
        world: &PlanningWorldView,
        tick: u64,
        determination: i64,
        laziness: i64,
    ) {
        let money = world
            .facts
            .get("agent.money")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let food = world
            .facts
            .get("agent.food")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        for track in &mut self.arc_tracks {
            if matches!(track.status, ArcTrackStatus::Completed) {
                continue;
            }
            let mut blockers = Vec::new();
            match track.family.as_str() {
                "livelihood" => {
                    if self.fatigue >= 85 {
                        blockers.push("exhausted".to_string());
                    }
                }
                "social" => {
                    if self.fatigue >= 80 {
                        blockers.push("too_tired_to_socialize".to_string());
                    }
                    if self.identity.personality.sociability < -30 {
                        blockers.push("low_sociability".to_string());
                    }
                }
                "household" => {
                    if money <= 0 && food <= 0 {
                        blockers.push("no_household_resources".to_string());
                    }
                }
                "recovery" => {
                    if self.fatigue <= 20 && self.recreation_need <= 20 {
                        blockers.push("already_recovered".to_string());
                    }
                }
                "civic" => {
                    if self.identity.personality.bravery < -30 {
                        blockers.push("risk_averse".to_string());
                    }
                }
                _ => {}
            }

            if !blockers.is_empty() {
                let hard_block = blockers.iter().any(|blocker| Self::blocker_is_hard(blocker));
                track.status = if hard_block {
                    ArcTrackStatus::Blocked
                } else {
                    ArcTrackStatus::Paused
                };
                track.blockers = blockers;
                if track.last_break_reason.is_none() {
                    track.last_break_reason = Some("gating_blocker".to_string());
                }
                continue;
            }

            if matches!(
                track.status,
                ArcTrackStatus::Blocked | ArcTrackStatus::Paused
            ) {
                let recovery_roll = determination - laziness;
                if recovery_roll >= -10 {
                    track.status = ArcTrackStatus::Active;
                    track.blockers.clear();
                    track.last_break_reason = None;
                }
            }

            if matches!(track.status, ArcTrackStatus::Active)
                && tick.saturating_sub(track.last_advanced_tick) > 96
            {
                track.status = ArcTrackStatus::Paused;
                track.last_break_reason = Some("inactive_too_long".to_string());
            }
        }
    }

    fn note_arc_progress(
        &mut self,
        operator_id: &str,
        duration_ticks: u64,
        tick: u64,
        world: &PlanningWorldView,
    ) {
        let family = operator_id.split(':').next().unwrap_or("household");
        let canonical = Self::canonical_family(family).to_string();
        self.ensure_arc_track_for_family(&canonical, tick);
        self.apply_life_domain_step(operator_id, duration_ticks, tick);
        let momentum = ((self.determination_score() - self.laziness_score()) / 10).clamp(-4, 8);

        let money = world
            .facts
            .get("agent.money")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let food = world
            .facts
            .get("agent.food")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let strong_relations = self.strong_relation_count(world, 30);
        let sociability = self.identity.personality.sociability;
        let loyalty = self.identity.personality.loyalty;
        let humor = self.identity.personality.humor;
        let bravery = self.identity.personality.bravery;
        let curiosity = self.identity.personality.curiosity;
        let capabilities = self.identity.capabilities.clone();
        let shelter_pressure = self.drives.drives.shelter.current;
        let health_pressure = self.drives.drives.health.current;
        let duration = i64::try_from(duration_ticks.max(1)).unwrap_or(1);
        if let Some(track) = self
            .arc_tracks
            .iter_mut()
            .find(|track| track.family == canonical)
        {
            if matches!(
                track.status,
                ArcTrackStatus::Blocked | ArcTrackStatus::Derailed
            ) {
                return;
            }
            track.status = ArcTrackStatus::Active;
            let mut delta = (duration * 8) + momentum;
            if track.family == "livelihood" && money <= 2 {
                delta += 4;
            }
            if track.family == "recovery" && self.fatigue >= 60 {
                delta += 6;
            }
            if track.family == "social" && self.identity.personality.sociability > 40 {
                delta += 4;
            }
            track.progress = (track.progress + delta).clamp(0, 1000);
            track.last_advanced_tick = tick;
            track.blockers.clear();

            while track.progress >= 100 {
                if let Some(blocker) = Self::arc_milestone_blocker(
                    &track.family,
                    track.milestone_index,
                    money,
                    food,
                    strong_relations,
                    sociability,
                    loyalty,
                    humor,
                    bravery,
                    curiosity,
                    capabilities.trade,
                    capabilities.physical,
                    capabilities.influence,
                    capabilities.law,
                    shelter_pressure,
                    health_pressure,
                    self.fatigue,
                    self.recreation_need,
                ) {
                    track.status = if Self::blocker_is_hard(&blocker) {
                        ArcTrackStatus::Blocked
                    } else {
                        ArcTrackStatus::Paused
                    };
                    track.blockers = vec![blocker];
                    track.last_break_reason = Some("milestone_gate".to_string());
                    track.progress = 99;
                    break;
                }
                track.progress -= 100;
                track.milestone_index = track.milestone_index.saturating_add(1);
                if track.milestone_index >= track.milestones.len() {
                    track.milestone_index = track.milestones.len().saturating_sub(1);
                    track.status = ArcTrackStatus::Completed;
                    track.progress = 100;
                    track.last_break_reason = None;
                    break;
                }
                track.last_break_reason = None;
            }
        }
    }

    fn apply_life_domain_step(&mut self, operator_id: &str, duration_ticks: u64, tick: u64) {
        let family = operator_id.split(':').next().unwrap_or("household");
        let duration = i64::try_from(duration_ticks.max(1)).unwrap_or(1);
        match family {
            "livelihood" => {
                self.life_domain.career_xp =
                    self.life_domain.career_xp.saturating_add(3 * duration);
                self.life_domain.social_reputation =
                    (self.life_domain.social_reputation + duration).clamp(-100, 100);
            }
            "social" => {
                self.life_domain.social_reputation =
                    (self.life_domain.social_reputation + 2 * duration).clamp(-100, 100);
                if operator_id.ends_with(":court") && self.life_domain.marital_status == "single" {
                    self.life_domain.marital_status = "courting".to_string();
                }
            }
            "household" => {
                self.life_domain.home_quality =
                    (self.life_domain.home_quality + 2 * duration).clamp(0, 100);
                self.life_domain.property_value = self
                    .life_domain
                    .property_value
                    .saturating_add(self.life_domain.home_quality / 20);
            }
            "governance" | "security" => {
                self.life_domain.social_reputation =
                    (self.life_domain.social_reputation + duration).clamp(-100, 100);
            }
            "illicit" => {
                self.life_domain.social_reputation =
                    (self.life_domain.social_reputation - 2 * duration).clamp(-100, 100);
            }
            _ => {}
        }

        while self.life_domain.career_xp >= 120 {
            self.life_domain.career_xp -= 120;
            self.life_domain.career_tier = self.life_domain.career_tier.saturating_add(1).min(6);
        }
        self.life_domain.last_domain_event_tick = tick;
    }

    fn arc_milestone_blocker(
        family: &str,
        milestone_index: usize,
        money: i64,
        food: i64,
        strong_relations: usize,
        sociability: i64,
        loyalty: i64,
        humor: i64,
        bravery: i64,
        curiosity: i64,
        capability_trade: i64,
        capability_physical: i64,
        capability_influence: i64,
        capability_law: i64,
        shelter_pressure: i64,
        health_pressure: i64,
        fatigue: i64,
        recreation_need: i64,
    ) -> Option<String> {
        match family {
            "livelihood" => match milestone_index {
                0 if money < 4 => Some("need_starting_coin".to_string()),
                1 if money < 10 => Some("insufficient_savings_buffer".to_string()),
                2 if money < 18 => Some("need_asset_capital".to_string()),
                3 if capability_trade < 35 && capability_physical < 45 => {
                    Some("insufficient_skill_for_advancement".to_string())
                }
                _ => None,
            },
            "social" => match milestone_index {
                0 if sociability < -20 => Some("sociability_too_low".to_string()),
                1 if strong_relations == 0 => Some("no_trusted_contact".to_string()),
                2 if strong_relations < 2 => Some("need_broader_social_base".to_string()),
                3 if loyalty < 10 => Some("low_commitment_disposition".to_string()),
                _ => None,
            },
            "household" => match milestone_index {
                0 if food <= 0 => Some("no_household_supplies".to_string()),
                1 if money < 6 => Some("insufficient_household_budget".to_string()),
                2 if shelter_pressure > 78 => Some("shelter_crisis_unresolved".to_string()),
                _ => None,
            },
            "recovery" => match milestone_index {
                0 if fatigue > 88 => Some("too_exhausted_to_stabilize".to_string()),
                1 if recreation_need > 85 => Some("overstressed".to_string()),
                2 if health_pressure > 78 => Some("health_pressure_too_high".to_string()),
                _ => None,
            },
            "leisure" => match milestone_index {
                0 if recreation_need < 20 => Some("low_recreation_pressure".to_string()),
                1 if money < 2 && humor < 10 => Some("insufficient_relaxation_options".to_string()),
                _ => None,
            },
            "civic" => match milestone_index {
                0 if capability_influence < 20 => Some("insufficient_influence".to_string()),
                1 if strong_relations == 0 => Some("no_social_backing".to_string()),
                2 if capability_law < 15 && bravery < 5 => {
                    Some("insufficient_civic_capacity".to_string())
                }
                _ => None,
            },
            _ => match milestone_index {
                0 if curiosity < -15 => Some("low_curiosity".to_string()),
                1 if money < 3 && food <= 0 => Some("no_exploration_slack".to_string()),
                _ => None,
            },
        }
    }

    fn strong_relation_count(&self, world: &PlanningWorldView, min_trust: i64) -> usize {
        let prefix = format!("social.trust:{}:", self.id);
        world
            .facts
            .iter()
            .filter(|(key, value)| {
                key.starts_with(&prefix)
                    && value
                        .parse::<i64>()
                        .map(|trust| trust >= min_trust)
                        .unwrap_or(false)
            })
            .count()
    }

    fn blocker_is_hard(blocker: &str) -> bool {
        matches!(
            blocker,
            "no_household_resources"
                | "need_starting_coin"
                | "shelter_crisis_unresolved"
                | "health_pressure_too_high"
                | "too_exhausted_to_stabilize"
                | "insufficient_skill_for_advancement"
        )
    }

    fn time_budget_bias_for_family(&self, family: &str) -> i64 {
        let bucket = match Self::canonical_family(family) {
            "livelihood" | "civic" => self.time_budget.work,
            "social" => self.time_budget.social,
            "household" => self.time_budget.household,
            "recovery" | "leisure" => self.time_budget.recovery,
            _ => self.time_budget.exploration,
        };
        (bucket - 20) / 2
    }

    fn arc_bias_for_family(&self, family: &str, determination: i64, laziness: i64) -> i64 {
        let canonical = Self::canonical_family(family);
        let Some(track) = self
            .arc_tracks
            .iter()
            .find(|track| track.family == canonical)
        else {
            return 0;
        };
        let mut bias = 0;
        match track.status {
            ArcTrackStatus::Active => {
                bias += 10;
                bias += track.progress / 20;
            }
            ArcTrackStatus::Paused => {
                bias -= 4;
            }
            ArcTrackStatus::Blocked => {
                bias -= 10;
            }
            ArcTrackStatus::Derailed => {
                bias -= 12;
            }
            ArcTrackStatus::Completed => {
                bias -= 3;
            }
        }
        if track.milestone_index + 1 >= track.milestones.len() {
            bias += 6;
        }
        bias + (determination - laziness) / 7
    }

    fn life_domain_bias_for_family(&self, family: &str) -> i64 {
        match Self::canonical_family(family) {
            "livelihood" => {
                let tier_gap = (5_i64 - i64::from(self.life_domain.career_tier)).max(0);
                let home_pressure = (60 - self.life_domain.home_quality).max(0) / 6;
                tier_gap * 2 + home_pressure
            }
            "social" => {
                if matches!(
                    self.life_domain.marital_status.as_str(),
                    "single" | "courting"
                ) {
                    8 + (40 - self.life_domain.social_reputation).max(0) / 8
                } else {
                    2
                }
            }
            "household" => {
                (70 - self.life_domain.home_quality).max(0) / 4
                    + (if self.life_domain.children_count > 0 {
                        5
                    } else {
                        0
                    })
            }
            "civic" => {
                (i64::from(self.life_domain.career_tier) * 2)
                    + self.life_domain.social_reputation.max(0) / 10
            }
            "recovery" => (self.life_domain.contract_failures as i64).min(8),
            _ => 0,
        }
    }

    fn track_family_for_plan(plan: &ActivePlan) -> String {
        plan.steps
            .first()
            .and_then(|step| step.operator_id.split(':').next())
            .unwrap_or("general")
            .to_string()
    }

    fn stage_for_plan_step(plan: &ActivePlan) -> String {
        if plan.steps.is_empty() {
            return "empty".to_string();
        }
        let next = plan.next_step_index.min(plan.steps.len().saturating_sub(1));
        format!("step_{}/{}", next + 1, plan.steps.len())
    }

    fn refresh_goal_tracks(
        &mut self,
        candidates: &[contracts::CandidatePlan],
        scores: &[contracts::PlanScore],
        tick: u64,
        inertia: i64,
    ) {
        for track in &mut self.goal_tracks {
            if matches!(track.status, GoalTrackStatus::Active)
                && tick.saturating_sub(track.last_advanced_tick) > 72
            {
                track.status = GoalTrackStatus::Paused;
                if track.last_break_reason.is_none() {
                    track.last_break_reason = Some("stale_track".to_string());
                }
            }
        }

        for candidate in candidates.iter().take(8) {
            let score = scores
                .iter()
                .find(|entry| entry.plan_id == candidate.plan_id)
                .map(|entry| entry.score)
                .unwrap_or(0);
            let family = candidate
                .steps
                .first()
                .and_then(|step| step.operator_id.split(':').next())
                .unwrap_or("general")
                .to_string();
            if let Some(track) = self
                .goal_tracks
                .iter_mut()
                .find(|track| track.plan_id == candidate.plan_id)
            {
                track.utility_hint = score;
                track.inertia_score = ((track.inertia_score + inertia) / 2).clamp(-100, 100);
                track.goal_family = family.clone();
                if matches!(
                    track.status,
                    GoalTrackStatus::Paused | GoalTrackStatus::Blocked
                ) {
                    track.status = GoalTrackStatus::Active;
                }
                continue;
            }
            self.goal_tracks.push(GoalTrack {
                track_id: format!("track:{}:{}", self.id, candidate.plan_id),
                plan_id: candidate.plan_id.clone(),
                goal_family: family,
                stage: format!("step_1/{}", candidate.steps.len().max(1)),
                status: GoalTrackStatus::Active,
                progress_ticks: 0,
                inertia_score: inertia.clamp(-100, 100),
                utility_hint: score,
                blockers: Vec::new(),
                last_advanced_tick: tick,
                last_break_reason: None,
            });
        }

        self.goal_tracks.sort_by(|a, b| {
            b.utility_hint
                .cmp(&a.utility_hint)
                .then(b.inertia_score.cmp(&a.inertia_score))
                .then(a.track_id.cmp(&b.track_id))
        });
        if self.goal_tracks.len() > 16 {
            self.goal_tracks.truncate(16);
        }
    }

    fn select_plan_id_from_portfolio(
        &self,
        default_plan_id: Option<String>,
        candidates: &[contracts::CandidatePlan],
        scores: &[contracts::PlanScore],
        determination: i64,
        laziness: i64,
        disruption: i64,
    ) -> Option<String> {
        let mut best = None::<(String, i64)>;
        for candidate in candidates {
            let base = scores
                .iter()
                .find(|entry| entry.plan_id == candidate.plan_id)
                .map(|entry| entry.score)
                .unwrap_or(0);
            let family = candidate
                .steps
                .first()
                .and_then(|step| step.operator_id.split(':').next())
                .unwrap_or("general");
            let mut score = base;
            if candidate.goal.label == self.aspiration.label {
                score += 12;
            }
            if let Some(track) = self
                .goal_tracks
                .iter()
                .find(|track| track.plan_id == candidate.plan_id)
            {
                score += track.inertia_score / 6;
                score += track.utility_hint / 8;
                if matches!(
                    track.status,
                    GoalTrackStatus::Paused
                        | GoalTrackStatus::Blocked
                        | GoalTrackStatus::Derailed
                        | GoalTrackStatus::Abandoned
                ) {
                    score -= 8;
                }
            }
            if self.recreation_need >= 60 {
                if family == "leisure"
                    || (family == "social"
                        && candidate.steps.iter().any(|s| {
                            matches!(
                                s.operator_id.as_str(),
                                "social:greet" | "social:confide" | "social:console"
                            )
                        }))
                    || candidate.steps.iter().any(|s| {
                        matches!(s.operator_id.as_str(), "health:rest" | "household:sleep")
                    })
                {
                    score += 20;
                } else {
                    score -= 10;
                }
            }
            if disruption >= 70 && matches!(family, "security" | "health") {
                score += 16;
            }
            score += self.time_budget_bias_for_family(family);
            score += self.arc_bias_for_family(family, determination, laziness);
            score += self.life_domain_bias_for_family(family);
            score += (determination - laziness) / 5;
            match &best {
                Some((_, best_score)) if score <= *best_score => {}
                _ => best = Some((candidate.plan_id.clone(), score)),
            }
        }

        best.map(|entry| entry.0).or(default_plan_id)
    }

    fn upsert_track_for_plan(&mut self, plan: &ActivePlan, score: i64, tick: u64, inertia: i64) {
        let stage = Self::stage_for_plan_step(plan);
        let family = Self::track_family_for_plan(plan);
        if let Some(track) = self
            .goal_tracks
            .iter_mut()
            .find(|track| track.plan_id == plan.plan_id)
        {
            track.status = GoalTrackStatus::Active;
            track.stage = stage;
            track.goal_family = family;
            track.utility_hint = score;
            track.inertia_score = inertia.clamp(-100, 100);
            track.last_advanced_tick = tick;
            track.last_break_reason = None;
            return;
        }
        self.goal_tracks.push(GoalTrack {
            track_id: format!("track:{}:{}", self.id, plan.plan_id),
            plan_id: plan.plan_id.clone(),
            goal_family: family,
            stage,
            status: GoalTrackStatus::Active,
            progress_ticks: 0,
            inertia_score: inertia.clamp(-100, 100),
            utility_hint: score,
            blockers: Vec::new(),
            last_advanced_tick: tick,
            last_break_reason: None,
        });
    }

    fn set_track_status(
        &mut self,
        plan_id: &str,
        status: GoalTrackStatus,
        reason: Option<String>,
        tick: u64,
    ) {
        if let Some(track) = self
            .goal_tracks
            .iter_mut()
            .find(|track| track.plan_id == plan_id)
        {
            track.status = status;
            if let Some(reason) = reason {
                track.last_break_reason = Some(reason);
            }
            track.last_advanced_tick = tick;
        }
    }

    fn set_arc_status_for_operator(
        &mut self,
        operator_id: &str,
        status: ArcTrackStatus,
        reason: Option<String>,
        tick: u64,
    ) {
        let family = operator_id.split(':').next().unwrap_or("household");
        let canonical = Self::canonical_family(family);
        if let Some(track) = self
            .arc_tracks
            .iter_mut()
            .find(|track| track.family == canonical)
        {
            track.status = status;
            if let Some(reason) = reason {
                track.last_break_reason = Some(reason);
            }
            track.last_advanced_tick = tick;
        }
    }

    fn note_track_progress(&mut self, plan_id: &str, operator_id: &str, tick: u64) {
        if let Some(track) = self
            .goal_tracks
            .iter_mut()
            .find(|track| track.plan_id == plan_id)
        {
            track.progress_ticks = track.progress_ticks.saturating_add(1);
            track.last_advanced_tick = tick;
            track.status = GoalTrackStatus::Active;
            track.stage = operator_id.to_string();
            track.blockers.clear();
        }
    }

    fn commitment_id_for(agent_id: &str, plan_id: &str) -> String {
        format!("commitment:{agent_id}:{plan_id}")
    }

    fn start_or_refresh_commitment(
        &mut self,
        plan: &ActivePlan,
        tick: u64,
        inertia_score: i64,
        progress_delta: u64,
    ) {
        let commitment_id = Self::commitment_id_for(&self.id, &plan.plan_id);
        let total_duration = plan
            .steps
            .iter()
            .map(|step| step.duration_ticks)
            .sum::<u64>();
        if let Some(existing) = self
            .commitments
            .iter_mut()
            .find(|commitment| commitment.commitment_id == commitment_id)
        {
            existing.status = "active".to_string();
            existing.progress_ticks = existing.progress_ticks.saturating_add(progress_delta);
            existing.inertia_score = inertia_score;
            existing.due_tick = tick + total_duration.max(1);
            return;
        }
        self.commitments.push(CommitmentState {
            commitment_id,
            npc_id: self.id.clone(),
            action_family: plan
                .steps
                .first()
                .and_then(|step| step.operator_id.split(':').next().map(str::to_string))
                .unwrap_or_else(|| "general".to_string()),
            started_tick: tick,
            due_tick: tick + total_duration.max(1),
            cadence_ticks: 1,
            progress_ticks: progress_delta,
            inertia_score,
            status: "active".to_string(),
        });
        if self.commitments.len() > 8 {
            self.commitments
                .sort_by(|a, b| b.started_tick.cmp(&a.started_tick));
            self.commitments.truncate(8);
        }
    }

    fn set_commitment_status(&mut self, commitment_id: &str, status: &str, tick: u64) {
        if let Some(commitment) = self
            .commitments
            .iter_mut()
            .find(|commitment| commitment.commitment_id == commitment_id)
        {
            commitment.status = status.to_string();
            if matches!(status, "completed" | "broken") {
                commitment.due_tick = tick;
            }
        }
        if let Some(plan_id) = Self::plan_id_from_commitment_id(commitment_id) {
            let (track_status, reason) = match status {
                "completed" => (GoalTrackStatus::Completed, None),
                "broken" => (
                    GoalTrackStatus::Derailed,
                    Some("commitment_broken".to_string()),
                ),
                "delayed" => (
                    GoalTrackStatus::Paused,
                    Some("commitment_delayed".to_string()),
                ),
                "abandoned" => (
                    GoalTrackStatus::Abandoned,
                    Some("commitment_abandoned".to_string()),
                ),
                "blocked" => (
                    GoalTrackStatus::Blocked,
                    Some("commitment_blocked".to_string()),
                ),
                _ => (GoalTrackStatus::Active, None),
            };
            self.set_track_status(plan_id, track_status, reason, tick);
        }
    }

    fn bump_commitment_progress(&mut self, commitment_id: &str, delta: u64, tick: u64) {
        if let Some(commitment) = self
            .commitments
            .iter_mut()
            .find(|commitment| commitment.commitment_id == commitment_id)
        {
            commitment.progress_ticks = commitment.progress_ticks.saturating_add(delta);
            commitment.status = "active".to_string();
            commitment.due_tick = commitment.due_tick.max(tick);
        }
    }

    fn queue_plan(&mut self, plan: ActivePlan) {
        if self
            .plan_backlog
            .iter()
            .any(|queued| queued.plan_id == plan.plan_id)
        {
            return;
        }
        self.upsert_track_for_plan(&plan, plan.goal.priority, plan.created_tick, 10);
        self.set_track_status(
            &plan.plan_id,
            GoalTrackStatus::Paused,
            Some("queued_for_later".to_string()),
            plan.created_tick,
        );
        self.plan_backlog.push(plan);
        self.plan_backlog.sort_by(|a, b| {
            b.goal
                .priority
                .cmp(&a.goal.priority)
                .then(a.created_tick.cmp(&b.created_tick))
        });
        if self.plan_backlog.len() > 8 {
            self.plan_backlog.truncate(8);
        }
    }

    fn pop_backlog_plan_with_policy(
        &mut self,
        determination: i64,
        laziness: i64,
        disruption: i64,
    ) -> Option<ActivePlan> {
        if self.plan_backlog.is_empty() {
            return None;
        }
        let mut best_idx = 0_usize;
        let mut best_score = i64::MIN;
        for (idx, plan) in self.plan_backlog.iter().enumerate() {
            let family = Self::track_family_for_plan(plan);
            let mut score = plan.goal.priority;
            if plan.goal.label == self.aspiration.label {
                score += 10;
            }
            if let Some(track) = self
                .goal_tracks
                .iter()
                .find(|track| track.plan_id == plan.plan_id)
            {
                score += track.utility_hint / 8;
                score += track.inertia_score / 5;
            }
            if self.recreation_need >= 55
                && matches!(family.as_str(), "leisure" | "social" | "health")
            {
                score += 16;
            }
            if self.recreation_need >= 65 && !matches!(family.as_str(), "leisure" | "social") {
                score -= 8;
            }
            if disruption >= 70 && matches!(family.as_str(), "security" | "health") {
                score += 14;
            }
            score += self.time_budget_bias_for_family(family.as_str());
            score += self.arc_bias_for_family(family.as_str(), determination, laziness);
            score += self.life_domain_bias_for_family(family.as_str());
            score += (determination - laziness) / 4;
            if score > best_score {
                best_score = score;
                best_idx = idx;
            }
        }
        Some(self.plan_backlog.remove(best_idx))
    }

    fn plan_id_from_commitment_id(commitment_id: &str) -> Option<&str> {
        let mut parts = commitment_id.splitn(3, ':');
        let kind = parts.next()?;
        if kind != "commitment" {
            return None;
        }
        let _agent = parts.next()?;
        parts.next()
    }

    fn initial_arc_tracks(id: &str, identity: &IdentityProfile) -> Vec<ArcTrack> {
        let mut families = vec!["livelihood", "household", "social", "recovery"];
        if identity.personality.curiosity >= 25 {
            families.push("exploration");
        }
        if identity.personality.ambition >= 35 {
            families.push("civic");
        }
        if identity.personality.humor >= 20 || identity.personality.impulsiveness >= 30 {
            families.push("leisure");
        }
        families
            .into_iter()
            .map(Self::canonical_family)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|family| {
                let (title, milestones) = Self::arc_blueprint_for(family);
                ArcTrack {
                    arc_id: format!("arc:{id}:{family}"),
                    title,
                    family: family.to_string(),
                    milestone_index: 0,
                    milestones,
                    progress: 0,
                    status: ArcTrackStatus::Active,
                    blockers: Vec::new(),
                    last_advanced_tick: 0,
                    last_break_reason: None,
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn from_identity(
        id: String,
        identity: IdentityProfile,
        aspiration: Aspiration,
        location_id: String,
        drives: DriveSystem,
    ) -> Self {
        let arc_tracks = Self::initial_arc_tracks(&id, &identity);
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
            plan_backlog: Vec::new(),
            commitments: Vec::new(),
            goal_tracks: Vec::new(),
            arc_tracks,
            life_domain: LifeDomainState {
                career_tier: 0,
                career_xp: 0,
                marital_status: "single".to_string(),
                spouse_npc_id: None,
                children_count: 0,
                home_quality: 35,
                property_value: 10,
                social_reputation: 0,
                contract_successes: 0,
                contract_failures: 0,
                last_domain_event_tick: 0,
            },
            fatigue: 20,
            recreation_need: 15,
            time_budget: TimeBudgetAllocation {
                work: 32,
                social: 20,
                recovery: 20,
                household: 18,
                exploration: 10,
            },
            aspiration,
            location_id,
            reactive_streak: 0,
            last_deliberate_tick: 0,
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
