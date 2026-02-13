use std::collections::{BTreeMap, BTreeSet};

use contracts::{
    BoundOperator, CandidatePlan, DriveKind, DriveSystem, Goal, IdentityProfile, OperatorDef,
    PlanScore, PlanSelection, PlanningMode,
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
            PlanningMode::Reactive => 1,
            PlanningMode::Deliberate => config.horizon.saturating_add(1).clamp(2, 6),
        };

        let goal = Self::goal_from_drives(actor.drives);
        let dominant_drive = goal_drive(&goal);
        let mut candidates = Vec::new();
        let mut single_steps = Vec::<(BoundOperator, i64, i64)>::new();

        for operator in &catalog.operators {
            if !catalog.check_preconditions(operator, world) {
                continue;
            }

            let bindings = catalog.enumerate_bindings(operator, world);
            if bindings.is_empty() {
                continue;
            }

            for binding in bindings.into_iter().take(4) {
                if !binding_is_feasible(operator, &binding, actor, world) {
                    continue;
                }
                let dominant_relief = operator
                    .drive_effects
                    .iter()
                    .filter(|(kind, _)| *kind == dominant_drive)
                    .map(|(_, amount)| *amount)
                    .sum::<i64>();
                single_steps.push((
                    BoundOperator {
                        operator_id: operator.operator_id.clone(),
                        parameters: binding,
                        duration_ticks: operator.duration_ticks,
                        preconditions: operator.preconditions.clone(),
                        effects: Vec::new(),
                    },
                    dominant_relief,
                    operator.risk,
                ));
            }
        }

        single_steps.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then(a.2.cmp(&b.2))
                .then(a.0.operator_id.cmp(&b.0.operator_id))
        });

        for (idx, (step, _, _)) in single_steps.iter().enumerate() {
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
                for (next, _, _) in single_steps
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
        catalog: &OperatorCatalog,
    ) -> PlanScore {
        let dominant_drive = goal_drive(&candidate.goal);
        let drive_pressure = top_drive_pressure(actor.drives);
        let goal_alignment = candidate
            .steps
            .iter()
            .filter_map(|step| catalog.operator_by_id(&step.operator_id))
            .map(|operator| drive_alignment_for_operator(operator, dominant_drive))
            .sum::<i64>();
        let capability_alignment = candidate
            .steps
            .iter()
            .filter_map(|step| catalog.operator_by_id(&step.operator_id))
            .map(|operator| capability_alignment(operator, actor.identity))
            .sum::<i64>();
        let risk_penalty = candidate
            .steps
            .iter()
            .filter_map(|step| catalog.operator_by_id(&step.operator_id))
            .map(|operator| {
                let tolerance = (actor.identity.personality.bravery
                    + actor.identity.personality.impulsiveness
                    - actor.identity.personality.morality)
                    .clamp(-100, 100);
                ((operator.risk - tolerance).abs() / 18).max(1)
            })
            .sum::<i64>();
        let ambition_bonus = actor.identity.personality.ambition / 8;
        let morality_guard = actor.identity.personality.morality / 10;
        let greed_bonus = actor.identity.personality.greed / 10;
        let social_bonus = social_bonus_for(candidate, actor.agent_id, world);
        let opportunity_bonus = opportunity_context_bonus(candidate, dominant_drive, world);
        let goal_family_adjustment =
            goal_family_adjustment(candidate, dominant_drive, actor.identity, world);
        let novelty_bonus = novelty_bonus(candidate, world);
        let repetition_penalty = repeated_action_penalty(candidate, world);
        let illicit_bias =
            if candidate.steps.iter().any(|step| {
                step.operator_id.contains("illicit") || step.operator_id.contains("steal")
            }) {
                greed_bonus - morality_guard
            } else {
                morality_guard / 2
            };
        let goal_miss_penalty = if goal_alignment <= 0 { 34 } else { 0 };
        let score = drive_pressure
            + goal_alignment
            + capability_alignment
            + ambition_bonus
            + social_bonus
            + opportunity_bonus
            + goal_family_adjustment
            + novelty_bonus
            + illicit_bias
            - risk_penalty
            - repetition_penalty
            - goal_miss_penalty;

        let mut factors = BTreeMap::new();
        factors.insert("drive_pressure".to_string(), drive_pressure);
        factors.insert("goal_alignment".to_string(), goal_alignment);
        factors.insert("capability_alignment".to_string(), capability_alignment);
        factors.insert("ambition_bonus".to_string(), ambition_bonus);
        factors.insert("social_bonus".to_string(), social_bonus);
        factors.insert("opportunity_bonus".to_string(), opportunity_bonus);
        factors.insert("goal_family_adjustment".to_string(), goal_family_adjustment);
        factors.insert("novelty_bonus".to_string(), novelty_bonus);
        factors.insert("illicit_bias".to_string(), illicit_bias);
        factors.insert("repetition_penalty".to_string(), -repetition_penalty);
        factors.insert("goal_miss_penalty".to_string(), -goal_miss_penalty);
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
        let best = scores.iter().max_by_key(|entry| {
            (
                entry.score,
                entry.factors.get("goal_alignment").copied().unwrap_or(0),
                entry.factors.get("opportunity_bonus").copied().unwrap_or(0),
                deterministic_plan_tie_break(&entry.plan_id),
            )
        });
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

fn drive_alignment_for_operator(operator: &OperatorDef, dominant_drive: DriveKind) -> i64 {
    let dominant = operator
        .drive_effects
        .iter()
        .filter(|(kind, _)| *kind == dominant_drive)
        .map(|(_, amount)| *amount * 3)
        .sum::<i64>();
    let secondary = operator
        .drive_effects
        .iter()
        .filter(|(kind, _)| *kind != dominant_drive)
        .map(|(_, amount)| *amount / 4)
        .sum::<i64>();
    dominant + secondary
}

fn capability_alignment(operator: &OperatorDef, identity: &IdentityProfile) -> i64 {
    operator
        .capability_requirements
        .iter()
        .map(|(capability, required)| {
            let available = capability_value(identity, capability);
            if available >= *required {
                3
            } else {
                -((required - available) / 10).max(1)
            }
        })
        .sum::<i64>()
}

fn capability_value(identity: &IdentityProfile, capability: &str) -> i64 {
    match capability {
        "physical" => identity.capabilities.physical,
        "social" => identity.capabilities.social,
        "trade" => identity.capabilities.trade,
        "combat" => identity.capabilities.combat,
        "literacy" => identity.capabilities.literacy,
        "influence" => identity.capabilities.influence,
        "stealth" => identity.capabilities.stealth,
        "care" => identity.capabilities.care,
        "law" => identity.capabilities.law,
        _ => 40,
    }
}

fn opportunity_context_bonus(
    candidate: &CandidatePlan,
    dominant_drive: DriveKind,
    world: &PlanningWorldView,
) -> i64 {
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
    let fatigue = world
        .facts
        .get("agent.fatigue")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let recreation_need = world
        .facts
        .get("agent.recreation_need")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let budget_work = world
        .facts
        .get("agent.time_budget.work")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20);
    let budget_social = world
        .facts
        .get("agent.time_budget.social")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20);
    let budget_recovery = world
        .facts
        .get("agent.time_budget.recovery")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20);
    let budget_household = world
        .facts
        .get("agent.time_budget.household")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20);
    let budget_exploration = world
        .facts
        .get("agent.time_budget.exploration")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20);

    let has_income_step = candidate.steps.iter().any(|step| {
        step.operator_id.contains("work")
            || step.operator_id.contains("trade")
            || step.operator_id.contains("barter")
            || step.operator_id.contains("steal")
    });
    let has_food_step = candidate
        .steps
        .iter()
        .any(|step| step.operator_id.contains("eat") || step.operator_id.contains("harvest"));
    let weather = world
        .facts
        .get("world.weather")
        .map(String::as_str)
        .unwrap_or("clear");
    let shelter_step = candidate
        .steps
        .iter()
        .any(|step| step.operator_id.contains("repair") || step.operator_id.contains("sleep"));
    let relief_step = candidate.steps.iter().any(|step| {
        step.operator_id.starts_with("leisure:")
            || matches!(
                step.operator_id.as_str(),
                "health:rest"
                    | "household:sleep"
                    | "social:greet"
                    | "social:confide"
                    | "social:console"
            )
    });
    let passive_recovery_step = candidate.steps.iter().any(|step| {
        matches!(
            step.operator_id.as_str(),
            "health:rest" | "health:seek_healer" | "household:sleep"
        )
    });
    let drain_step = candidate.steps.iter().any(|step| {
        step.operator_id.starts_with("livelihood:")
            || step.operator_id.starts_with("security:")
            || step.operator_id.starts_with("governance:")
    });

    let mut bonus = 0;
    if money <= 3 && has_income_step {
        bonus += 8;
    }
    if food <= 1 && has_food_step {
        bonus += 10;
    }
    if dominant_drive == DriveKind::Shelter && shelter_step {
        bonus += 6;
    }
    if matches!(weather, "storm" | "snow") && shelter_step {
        bonus += 5;
    }
    if recreation_need >= 55 && relief_step {
        let relief_bonus = 10 + (recreation_need - 55) / 4;
        if passive_recovery_step && dominant_drive != DriveKind::Health {
            bonus += relief_bonus / 3;
        } else {
            bonus += relief_bonus;
        }
    }
    if fatigue >= 60 && relief_step {
        let fatigue_bonus = 8 + (fatigue - 60) / 5;
        if passive_recovery_step && dominant_drive != DriveKind::Health {
            bonus += fatigue_bonus / 3;
        } else {
            bonus += fatigue_bonus;
        }
    }
    if recreation_need >= 65 && drain_step {
        bonus -= 8;
    }
    if fatigue >= 70 && drain_step {
        bonus -= 6;
    }

    let family = candidate
        .steps
        .first()
        .and_then(|step| step.operator_id.split(':').next())
        .unwrap_or("household");
    let budget_bonus = match family {
        "livelihood" | "governance" | "security" => budget_work / 5,
        "social" => budget_social / 5,
        "leisure" | "health" => budget_recovery / 5,
        "household" => budget_household / 5,
        "information" => budget_exploration / 5,
        _ => 0,
    };
    bonus += budget_bonus - 4;

    bonus
}

fn repeated_action_penalty(candidate: &CandidatePlan, world: &PlanningWorldView) -> i64 {
    let Some(last_operator) = world.facts.get("agent.last_operator") else {
        return 0;
    };
    let last_family = world.facts.get("agent.last_family").map(String::as_str);
    let family_streak = world
        .facts
        .get("agent.family_streak")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let candidate_family = first_family(candidate);
    let repeats = candidate
        .steps
        .iter()
        .filter(|step| step.operator_id == *last_operator)
        .count() as i64;
    let mut penalty = repeats * 8;
    if let Some(last_family) = last_family {
        if candidate_family == last_family {
            penalty += (family_streak * 6).clamp(0, 36);
        }
    }
    penalty
}

fn novelty_bonus(candidate: &CandidatePlan, world: &PlanningWorldView) -> i64 {
    let Some(last_family) = world.facts.get("agent.last_family") else {
        return 0;
    };
    let family_streak = world
        .facts
        .get("agent.family_streak")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let candidate_family = first_family(candidate);
    if candidate_family != last_family && family_streak >= 2 {
        return 12.min(family_streak * 4);
    }
    0
}

fn goal_family_adjustment(
    candidate: &CandidatePlan,
    dominant_drive: DriveKind,
    identity: &IdentityProfile,
    world: &PlanningWorldView,
) -> i64 {
    let family = first_family(candidate);
    let fatigue = world
        .facts
        .get("agent.fatigue")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let recreation_need = world
        .facts
        .get("agent.recreation_need")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let has_rest_step = candidate.steps.iter().any(|step| {
        matches!(
            step.operator_id.as_str(),
            "health:rest" | "health:seek_healer" | "household:sleep"
        )
    });

    let mut adjustment = match dominant_drive {
        DriveKind::Shelter => match family {
            "household" => 20,
            "livelihood" => 8,
            "illicit" => 2,
            "governance" => -16,
            "leisure" | "social" | "information" => -10,
            "security" => -4,
            "health" => -20,
            _ => 0,
        },
        DriveKind::Food => match family {
            "household" | "livelihood" | "illicit" => 12,
            "governance" | "social" => -8,
            "health" => -10,
            _ => 0,
        },
        DriveKind::Income => match family {
            "livelihood" | "illicit" => 12,
            "health" => -12,
            "household" => -4,
            _ => 0,
        },
        DriveKind::Safety => match family {
            "security" | "information" => 12,
            "health" => -4,
            _ => 0,
        },
        DriveKind::Belonging => match family {
            "social" | "leisure" => 12,
            "health" => -6,
            _ => 0,
        },
        DriveKind::Status => match family {
            "governance" | "social" | "livelihood" | "leisure" => 10,
            "health" => -8,
            _ => 0,
        },
        DriveKind::Health => match family {
            "health" | "household" | "leisure" => 14,
            _ => -4,
        },
    };

    if has_rest_step && dominant_drive != DriveKind::Health {
        if fatigue < 65 && recreation_need < 70 {
            adjustment -= 24;
        } else {
            adjustment -= 8;
        }
    }
    if matches!(dominant_drive, DriveKind::Income | DriveKind::Shelter)
        && has_rest_step
        && identity.personality.ambition > 30
    {
        adjustment -= 8;
    }
    adjustment
}

fn binding_is_feasible(
    operator: &OperatorDef,
    binding: &contracts::OperatorParams,
    actor: &PlannerActorState<'_>,
    world: &PlanningWorldView,
) -> bool {
    if let Some(target_npc) = binding.target_npc.as_deref() {
        let location_key = format!("npc.location:{target_npc}");
        if let Some(target_location) = world.facts.get(&location_key) {
            if target_location != actor.location_id {
                return false;
            }
        }
    }

    let blocked_channels = blocked_channels(world);
    if blocked_channels.is_empty() {
        return true;
    }
    let required_channels = inferred_channels_for(operator, binding);
    !required_channels
        .iter()
        .any(|channel| blocked_channels.contains(channel))
}

fn blocked_channels(world: &PlanningWorldView) -> BTreeSet<String> {
    world
        .facts
        .iter()
        .filter_map(|(key, value)| {
            if !key.starts_with("agent.channel_blocked.") || value != "true" {
                return None;
            }
            Some(key.trim_start_matches("agent.channel_blocked.").to_string())
        })
        .collect::<BTreeSet<_>>()
}

fn inferred_channels_for(
    operator: &OperatorDef,
    params: &contracts::OperatorParams,
) -> Vec<String> {
    let mut channels = Vec::<String>::new();
    push_unique_channel(&mut channels, "attention");
    match operator.family.as_str() {
        "mobility" => push_unique_channel(&mut channels, "locomotion"),
        "social" | "leisure" | "governance" => push_unique_channel(&mut channels, "speech"),
        "livelihood" | "household" | "health" | "security" | "illicit" => {
            push_unique_channel(&mut channels, "hands")
        }
        _ => {}
    }

    for param in operator
        .param_schema
        .required
        .iter()
        .chain(operator.param_schema.optional.iter())
    {
        match &param.param_type {
            contracts::ParamType::LocationId => push_unique_channel(&mut channels, "locomotion"),
            contracts::ParamType::NpcId | contracts::ParamType::InstitutionId => {
                push_unique_channel(&mut channels, "speech")
            }
            contracts::ParamType::ObjectId
            | contracts::ParamType::ResourceType
            | contracts::ParamType::Quantity => push_unique_channel(&mut channels, "hands"),
            contracts::ParamType::Method(_) | contracts::ParamType::FreeText => {}
        }
    }
    for (capability, _) in &operator.capability_requirements {
        match capability.as_str() {
            "physical" | "trade" | "combat" | "stealth" | "care" => {
                push_unique_channel(&mut channels, "hands")
            }
            "social" | "influence" | "law" => push_unique_channel(&mut channels, "speech"),
            _ => {}
        }
    }

    if params.target_location.is_some() {
        push_unique_channel(&mut channels, "locomotion");
    }
    if params.target_npc.is_some() || params.target_institution.is_some() {
        push_unique_channel(&mut channels, "speech");
    }
    if params.target_object.is_some() || params.resource_type.is_some() || params.quantity.is_some()
    {
        push_unique_channel(&mut channels, "hands");
    }
    channels
}

fn push_unique_channel(channels: &mut Vec<String>, channel: &str) {
    if !channels.iter().any(|existing| existing == channel) {
        channels.push(channel.to_string());
    }
}

fn first_family(candidate: &CandidatePlan) -> &str {
    candidate
        .steps
        .first()
        .and_then(|step| step.operator_id.split(':').next())
        .unwrap_or("general")
}

fn deterministic_plan_tie_break(plan_id: &str) -> i64 {
    let mut hash = 0_i64;
    for byte in plan_id.as_bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(i64::from(*byte));
    }
    hash
}

fn goal_drive(goal: &Goal) -> DriveKind {
    if goal.goal_id.contains("shelter") {
        DriveKind::Shelter
    } else if goal.goal_id.contains("income") {
        DriveKind::Income
    } else if goal.goal_id.contains("safety") {
        DriveKind::Safety
    } else if goal.goal_id.contains("belonging") {
        DriveKind::Belonging
    } else if goal.goal_id.contains("status") {
        DriveKind::Status
    } else if goal.goal_id.contains("health") {
        DriveKind::Health
    } else {
        DriveKind::Food
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

        let catalog = OperatorCatalog::default_catalog();
        let high =
            GoapPlanner::score_plan(&candidate, &actor_state, &high_trust_world, &catalog).score;
        let low =
            GoapPlanner::score_plan(&candidate, &actor_state, &low_trust_world, &catalog).score;
        assert!(high > low);
    }
}
