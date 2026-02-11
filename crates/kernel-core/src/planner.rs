use std::collections::BTreeMap;

use contracts::{
    BoundOperator, CandidatePlan, DriveKind, DriveSystem, Goal, IdentityProfile, PlanScore,
    PlanSelection, PlanningMode,
};

use crate::operator::{OperatorCatalog, PlanningWorldView};

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub beam_width: usize,
    pub horizon: usize,
    pub idle_threshold: i64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            beam_width: 24,
            horizon: 4,
            idle_threshold: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlannerActorState<'a> {
    pub agent_id: &'a str,
    pub identity: &'a IdentityProfile,
    pub drives: &'a DriveSystem,
    pub location_id: &'a str,
}

#[derive(Debug, Clone, Default)]
pub struct GoapPlanner;

impl GoapPlanner {
    pub fn generate_candidates(
        actor: &PlannerActorState<'_>,
        world: &PlanningWorldView,
        catalog: &OperatorCatalog,
        config: &PlannerConfig,
        planning_mode: PlanningMode,
    ) -> Vec<CandidatePlan> {
        let chain_limit = match planning_mode {
            PlanningMode::Reactive => 2,
            PlanningMode::Deliberate => config.horizon.clamp(1, 6),
        };

        let goal = Self::goal_from_drives(actor.drives);
        let mut candidates = Vec::new();
        let mut single_steps = Vec::<BoundOperator>::new();

        for operator in &catalog.operators {
            if !catalog.check_preconditions(operator, world) {
                continue;
            }

            let bindings = catalog.enumerate_bindings(operator, world);
            if bindings.is_empty() {
                continue;
            }

            for binding in bindings.into_iter().take(4) {
                single_steps.push(BoundOperator {
                    operator_id: operator.operator_id.clone(),
                    parameters: binding,
                    duration_ticks: operator.duration_ticks,
                    preconditions: operator.preconditions.clone(),
                    effects: Vec::new(),
                });
            }
        }

        for (idx, step) in single_steps.iter().enumerate() {
            candidates.push(CandidatePlan {
                plan_id: format!(
                    "plan:{}:{}:{}",
                    actor.agent_id,
                    step.operator_id,
                    candidates.len() + 1
                ),
                goal: goal.clone(),
                steps: vec![step.clone()],
                planning_mode,
            });

            if candidates.len() >= config.beam_width {
                break;
            }

            if chain_limit > 1 {
                let mut chained = vec![step.clone()];
                for next in single_steps
                    .iter()
                    .skip(idx + 1)
                    .take(chain_limit.saturating_sub(1))
                {
                    chained.push(next.clone());
                }
                candidates.push(CandidatePlan {
                    plan_id: format!(
                        "plan:{}:{}:{}",
                        actor.agent_id,
                        step.operator_id,
                        candidates.len() + 1
                    ),
                    goal: goal.clone(),
                    steps: chained,
                    planning_mode,
                });
            }
        }

        candidates.truncate(config.beam_width);
        candidates
    }

    pub fn score_plan(
        candidate: &CandidatePlan,
        actor: &PlannerActorState<'_>,
        world: &PlanningWorldView,
    ) -> PlanScore {
        let drive_pressure = top_drive_pressure(actor.drives);
        let risk_penalty = candidate.steps.len() as i64 * 2
            + candidate
                .steps
                .iter()
                .filter(|step| step.operator_id.contains("illicit"))
                .count() as i64
                * 3;
        let ambition_bonus = actor.identity.personality.ambition / 10;
        let morality_guard = actor.identity.personality.morality / 10;
        let greed_bonus = actor.identity.personality.greed / 10;
        let social_bonus = social_bonus_for(candidate, actor.agent_id, world);
        let illicit_bias =
            if candidate.steps.iter().any(|step| {
                step.operator_id.contains("illicit") || step.operator_id.contains("steal")
            }) {
                greed_bonus - morality_guard
            } else {
                morality_guard / 2
            };
        let score = drive_pressure + ambition_bonus + social_bonus + illicit_bias - risk_penalty;

        let mut factors = BTreeMap::new();
        factors.insert("drive_pressure".to_string(), drive_pressure);
        factors.insert("ambition_bonus".to_string(), ambition_bonus);
        factors.insert("social_bonus".to_string(), social_bonus);
        factors.insert("illicit_bias".to_string(), illicit_bias);
        factors.insert("morality_guard".to_string(), -morality_guard);
        factors.insert("risk_penalty".to_string(), -risk_penalty);

        PlanScore {
            plan_id: candidate.plan_id.clone(),
            score,
            factors,
        }
    }

    pub fn select(
        _candidates: &[CandidatePlan],
        scores: &[PlanScore],
        idle_threshold: i64,
    ) -> PlanSelection {
        let best = scores.iter().max_by_key(|entry| entry.score);
        match best {
            Some(best_score) if best_score.score >= idle_threshold => PlanSelection {
                selected_plan_id: Some(best_score.plan_id.clone()),
                idle_reason: None,
                scores: scores.to_vec(),
            },
            _ => PlanSelection {
                selected_plan_id: None,
                idle_reason: Some("below_idle_threshold".to_string()),
                scores: scores.to_vec(),
            },
        }
    }

    fn goal_from_drives(drives: &DriveSystem) -> Goal {
        let pressures = [
            (DriveKind::Food, drives.food.current),
            (DriveKind::Shelter, drives.shelter.current),
            (DriveKind::Income, drives.income.current),
            (DriveKind::Safety, drives.safety.current),
            (DriveKind::Belonging, drives.belonging.current),
            (DriveKind::Status, drives.status.current),
            (DriveKind::Health, drives.health.current),
        ];

        let (kind, pressure) = pressures
            .into_iter()
            .max_by_key(|(_, pressure)| *pressure)
            .unwrap_or((DriveKind::Food, 0));

        Goal {
            goal_id: format!("goal:{kind:?}").to_lowercase(),
            label: format!("satisfy_{kind:?}").to_lowercase(),
            priority: pressure,
        }
    }
}

fn top_drive_pressure(drives: &DriveSystem) -> i64 {
    [
        drives.food.current,
        drives.shelter.current,
        drives.income.current,
        drives.safety.current,
        drives.belonging.current,
        drives.status.current,
        drives.health.current,
    ]
    .into_iter()
    .max()
    .unwrap_or(0)
}

fn social_bonus_for(candidate: &CandidatePlan, actor_id: &str, world: &PlanningWorldView) -> i64 {
    let Some(target_npc) = candidate
        .steps
        .first()
        .and_then(|step| step.parameters.target_npc.as_deref())
    else {
        return 0;
    };

    let key = format!("social.trust:{actor_id}:{target_npc}");
    let trust = world
        .facts
        .get(&key)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    trust / 10
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::{CapabilitySet, DriveValue, PersonalityTraits, Temperament};

    use crate::operator::OperatorCatalog;

    fn actor() -> (IdentityProfile, DriveSystem) {
        (
            IdentityProfile {
                profession: "laborer".to_string(),
                capabilities: CapabilitySet {
                    physical: 50,
                    social: 20,
                    trade: 20,
                    combat: 20,
                    literacy: 20,
                    influence: 20,
                    stealth: 30,
                    care: 20,
                    law: 10,
                },
                personality: PersonalityTraits {
                    bravery: 10,
                    morality: -20,
                    impulsiveness: 30,
                    sociability: 10,
                    ambition: 60,
                    empathy: 0,
                    patience: 0,
                    curiosity: 0,
                    jealousy: 0,
                    pride: 0,
                    vindictiveness: 0,
                    greed: 30,
                    loyalty: 0,
                    honesty: -30,
                    piety: 0,
                    vanity: 0,
                    humor: 0,
                },
                temperament: Temperament::Choleric,
                values: vec!["survival".to_string()],
                likes: vec![],
                dislikes: vec![],
            },
            DriveSystem {
                food: DriveValue {
                    current: 90,
                    decay_rate: 2,
                    urgency_threshold: 60,
                },
                shelter: DriveValue {
                    current: 30,
                    decay_rate: 1,
                    urgency_threshold: 60,
                },
                income: DriveValue {
                    current: 80,
                    decay_rate: 2,
                    urgency_threshold: 60,
                },
                safety: DriveValue {
                    current: 40,
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
                    current: 40,
                    decay_rate: 2,
                    urgency_threshold: 60,
                },
            },
        )
    }

    #[test]
    fn planner_generates_candidates_within_beam_width() {
        let catalog = OperatorCatalog::default_catalog();
        let world = PlanningWorldView {
            object_ids: vec!["obj:1".to_string()],
            location_ids: vec!["loc:1".to_string()],
            ..PlanningWorldView::default()
        };
        let (identity, drives) = actor();
        let actor = PlannerActorState {
            agent_id: "npc:1",
            identity: &identity,
            drives: &drives,
            location_id: "loc:1",
        };
        let config = PlannerConfig {
            beam_width: 5,
            ..PlannerConfig::default()
        };
        let candidates = GoapPlanner::generate_candidates(
            &actor,
            &world,
            &catalog,
            &config,
            PlanningMode::Deliberate,
        );
        assert!(!candidates.is_empty());
        assert!(candidates.len() <= 5);
    }

    #[test]
    fn idle_selected_when_scores_below_threshold() {
        let candidates = vec![CandidatePlan {
            plan_id: "p1".to_string(),
            goal: Goal {
                goal_id: "g1".to_string(),
                label: "survive".to_string(),
                priority: 10,
            },
            steps: Vec::new(),
            planning_mode: PlanningMode::Reactive,
        }];
        let scores = vec![PlanScore {
            plan_id: "p1".to_string(),
            score: 5,
            factors: BTreeMap::new(),
        }];

        let selection = GoapPlanner::select(&candidates, &scores, 10);
        assert!(selection.selected_plan_id.is_none());
    }

    #[test]
    fn social_graph_facts_change_score_for_targeted_plan() {
        let (identity, drives) = actor();
        let actor_state = PlannerActorState {
            agent_id: "npc:1",
            identity: &identity,
            drives: &drives,
            location_id: "loc:1",
        };
        let candidate = CandidatePlan {
            plan_id: "p_social".to_string(),
            goal: Goal {
                goal_id: "g".to_string(),
                label: "connect".to_string(),
                priority: 10,
            },
            steps: vec![BoundOperator {
                operator_id: "social:greet".to_string(),
                parameters: contracts::OperatorParams {
                    target_npc: Some("npc:2".to_string()),
                    ..contracts::OperatorParams::default()
                },
                duration_ticks: 1,
                preconditions: Vec::new(),
                effects: Vec::new(),
            }],
            planning_mode: PlanningMode::Deliberate,
        };
        let high_trust_world = PlanningWorldView {
            facts: BTreeMap::from([("social.trust:npc:1:npc:2".to_string(), "80".to_string())]),
            ..PlanningWorldView::default()
        };
        let low_trust_world = PlanningWorldView {
            facts: BTreeMap::from([("social.trust:npc:1:npc:2".to_string(), "-80".to_string())]),
            ..PlanningWorldView::default()
        };

        let high = GoapPlanner::score_plan(&candidate, &actor_state, &high_trust_world).score;
        let low = GoapPlanner::score_plan(&candidate, &actor_state, &low_trust_world).score;
        assert!(high > low);
    }
}
