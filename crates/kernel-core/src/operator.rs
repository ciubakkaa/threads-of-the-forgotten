use std::collections::BTreeMap;

use contracts::{
    DriveKind, FactPredicate, OperatorDef, OperatorParams, ParamDef, ParamSchema, ParamType,
};

#[derive(Debug, Clone, Default)]
pub struct PlanningWorldView {
    pub facts: BTreeMap<String, String>,
    pub npc_ids: Vec<String>,
    pub location_ids: Vec<String>,
    pub object_ids: Vec<String>,
    pub institution_ids: Vec<String>,
    pub resource_types: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct OperatorCatalog {
    pub operators: Vec<OperatorDef>,
    pub by_family: BTreeMap<String, Vec<usize>>,
    pub by_id: BTreeMap<String, usize>,
}

impl OperatorCatalog {
    pub fn new(operators: Vec<OperatorDef>) -> Self {
        let mut by_family: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut by_id = BTreeMap::new();
        for (index, operator) in operators.iter().enumerate() {
            by_family
                .entry(operator.family.clone())
                .or_default()
                .push(index);
            by_id.insert(operator.operator_id.clone(), index);
        }
        Self {
            operators,
            by_family,
            by_id,
        }
    }

    pub fn default_catalog() -> Self {
        let families = [
            (
                "livelihood",
                vec!["work", "trade", "barter", "harvest", "craft"],
            ),
            ("household", vec!["eat", "sleep", "repair", "clean", "cook"]),
            (
                "social",
                vec![
                    "greet",
                    "flatter",
                    "threaten",
                    "gossip",
                    "confide",
                    "propose_alliance",
                    "betray",
                    "court",
                    "console",
                    "argue",
                    "mediate",
                    "negotiate",
                ],
            ),
            (
                "information",
                vec![
                    "observe",
                    "eavesdrop",
                    "investigate",
                    "read",
                    "teach",
                    "learn",
                ],
            ),
            (
                "security",
                vec!["patrol", "guard", "lock", "barricade", "flee", "fight"],
            ),
            (
                "health",
                vec!["rest", "treat_wound", "seek_healer", "tend_sick"],
            ),
            ("mobility", vec!["travel", "explore", "scout"]),
            (
                "illicit",
                vec!["steal", "smuggle", "bribe", "extort", "fence"],
            ),
            (
                "governance",
                vec!["petition", "vote", "decree", "enforce_law", "collect_tax"],
            ),
            (
                "leisure",
                vec![
                    "drink",
                    "gamble",
                    "perform",
                    "attend_event",
                    "pray",
                    "socialize",
                ],
            ),
        ];

        let mut operators = Vec::new();
        for (family, names) in families {
            for name in names {
                operators.push(OperatorDef {
                    operator_id: format!("{family}:{name}"),
                    family: family.to_string(),
                    display_name: name.to_string(),
                    param_schema: default_schema_for(name),
                    duration_ticks: default_duration_ticks(family, name),
                    risk: if family == "illicit" { 65 } else { 20 },
                    visibility: if family == "illicit" { 70 } else { 40 },
                    preconditions: Vec::new(),
                    effects: Vec::new(),
                    drive_effects: default_drive_effects(family, name),
                    capability_requirements: default_capability_requirements(family, name),
                });
            }
        }

        Self::new(operators)
    }

    pub fn enumerate_bindings(
        &self,
        operator: &OperatorDef,
        world: &PlanningWorldView,
    ) -> Vec<OperatorParams> {
        let mut candidates = vec![OperatorParams::default()];

        for requirement in &operator.param_schema.required {
            let mut next = Vec::new();
            let values = values_for_param(requirement, world);
            if values.is_empty() {
                return Vec::new();
            }

            for existing in &candidates {
                for value in &values {
                    let mut bound = existing.clone();
                    set_param(&mut bound, &requirement.name, value.clone());
                    next.push(bound);
                }
            }
            candidates = next;
            if candidates.len() > 32 {
                candidates.truncate(32);
            }
        }

        candidates
    }

    pub fn check_preconditions(&self, operator: &OperatorDef, world: &PlanningWorldView) -> bool {
        operator
            .preconditions
            .iter()
            .all(|predicate| predicate_holds(predicate, world))
    }

    pub fn operator_by_id(&self, operator_id: &str) -> Option<&OperatorDef> {
        self.by_id
            .get(operator_id)
            .and_then(|index| self.operators.get(*index))
    }
}

fn default_schema_for(name: &str) -> ParamSchema {
    if matches!(
        name,
        "greet"
            | "flatter"
            | "threaten"
            | "gossip"
            | "confide"
            | "propose_alliance"
            | "betray"
            | "court"
            | "console"
            | "argue"
            | "mediate"
            | "negotiate"
    ) {
        return ParamSchema {
            required: vec![ParamDef {
                name: "target_npc".to_string(),
                param_type: ParamType::NpcId,
            }],
            optional: vec![ParamDef {
                name: "context".to_string(),
                param_type: ParamType::FreeText,
            }],
        };
    }

    if matches!(
        name,
        "petition" | "vote" | "decree" | "enforce_law" | "collect_tax"
    ) {
        return ParamSchema {
            required: vec![ParamDef {
                name: "target_institution".to_string(),
                param_type: ParamType::InstitutionId,
            }],
            optional: vec![ParamDef {
                name: "target_npc".to_string(),
                param_type: ParamType::NpcId,
            }],
        };
    }

    if matches!(name, "work" | "trade" | "barter") {
        return ParamSchema {
            required: vec![ParamDef {
                name: "resource_type".to_string(),
                param_type: ParamType::ResourceType,
            }],
            optional: vec![ParamDef {
                name: "quantity".to_string(),
                param_type: ParamType::Quantity,
            }],
        };
    }

    if name == "eat" {
        return ParamSchema {
            required: Vec::new(),
            optional: Vec::new(),
        };
    }

    if name == "steal" {
        return ParamSchema {
            required: vec![
                ParamDef {
                    name: "target_object".to_string(),
                    param_type: ParamType::ObjectId,
                },
                ParamDef {
                    name: "method".to_string(),
                    param_type: ParamType::Method(vec![
                        "pickpocket".to_string(),
                        "shoplift".to_string(),
                        "burgle".to_string(),
                    ]),
                },
            ],
            optional: vec![
                ParamDef {
                    name: "target_npc".to_string(),
                    param_type: ParamType::NpcId,
                },
                ParamDef {
                    name: "target_location".to_string(),
                    param_type: ParamType::LocationId,
                },
            ],
        };
    }

    if name == "travel" {
        return ParamSchema {
            required: vec![ParamDef {
                name: "target_location".to_string(),
                param_type: ParamType::LocationId,
            }],
            optional: Vec::new(),
        };
    }

    ParamSchema {
        required: Vec::new(),
        optional: Vec::new(),
    }
}

fn default_drive_effects(family: &str, name: &str) -> Vec<(DriveKind, i64)> {
    match (family, name) {
        ("livelihood", "work") | ("livelihood", "trade") | ("livelihood", "barter") => {
            vec![(DriveKind::Income, 16)]
        }
        ("livelihood", "harvest") => vec![(DriveKind::Food, 12), (DriveKind::Income, 6)],
        ("livelihood", "craft") => vec![(DriveKind::Income, 10), (DriveKind::Status, 6)],
        ("household", "eat") | ("household", "cook") => vec![(DriveKind::Food, 20)],
        ("household", "sleep") => vec![(DriveKind::Health, 14)],
        ("household", "repair") => vec![(DriveKind::Shelter, 14)],
        ("social", "greet") | ("social", "confide") | ("social", "console") => {
            vec![(DriveKind::Belonging, 12)]
        }
        ("social", "court") | ("social", "propose_alliance") => {
            vec![(DriveKind::Belonging, 8), (DriveKind::Status, 8)]
        }
        ("social", "flatter") => vec![(DriveKind::Status, 10)],
        ("information", "investigate") | ("information", "observe") => {
            vec![(DriveKind::Safety, 8)]
        }
        ("security", "patrol") | ("security", "guard") | ("security", "lock") => {
            vec![(DriveKind::Safety, 14)]
        }
        ("health", "rest") | ("health", "treat_wound") | ("health", "seek_healer") => {
            vec![(DriveKind::Health, 16)]
        }
        ("mobility", "travel") => vec![(DriveKind::Income, 4), (DriveKind::Belonging, 4)],
        ("illicit", "steal")
        | ("illicit", "smuggle")
        | ("illicit", "extort")
        | ("illicit", "fence") => vec![(DriveKind::Income, 18), (DriveKind::Food, 8)],
        ("governance", "petition") | ("governance", "vote") => {
            vec![(DriveKind::Status, 8), (DriveKind::Safety, 4)]
        }
        ("leisure", "drink") => vec![(DriveKind::Health, 8), (DriveKind::Belonging, 6)],
        ("leisure", "gamble") => vec![(DriveKind::Status, 6), (DriveKind::Belonging, 4)],
        ("leisure", "perform") => vec![(DriveKind::Status, 8), (DriveKind::Belonging, 5)],
        ("leisure", "attend_event") => vec![(DriveKind::Belonging, 8), (DriveKind::Status, 4)],
        ("leisure", "pray") => vec![(DriveKind::Health, 6), (DriveKind::Safety, 5)],
        ("leisure", "socialize") => vec![(DriveKind::Belonging, 10), (DriveKind::Health, 5)],
        _ => Vec::new(),
    }
}

fn default_duration_ticks(family: &str, name: &str) -> u64 {
    match (family, name) {
        ("mobility", _) => 2,
        ("security", "patrol") | ("security", "guard") => 2,
        ("livelihood", "harvest") | ("livelihood", "craft") | ("livelihood", "work") => 1,
        ("household", "sleep") => 2,
        _ => 0,
    }
}

fn default_capability_requirements(family: &str, name: &str) -> Vec<(String, i64)> {
    match (family, name) {
        ("livelihood", "work") | ("livelihood", "harvest") => vec![("physical".to_string(), 35)],
        ("livelihood", "trade") | ("livelihood", "barter") => vec![("trade".to_string(), 30)],
        ("social", _) => vec![("social".to_string(), 25)],
        ("information", "read") | ("information", "teach") => vec![("literacy".to_string(), 30)],
        ("security", "fight") | ("security", "patrol") => vec![("combat".to_string(), 35)],
        ("health", "treat_wound") | ("health", "seek_healer") => vec![("care".to_string(), 30)],
        ("illicit", "steal") | ("illicit", "smuggle") => vec![("stealth".to_string(), 30)],
        ("governance", _) => vec![("law".to_string(), 20), ("influence".to_string(), 20)],
        _ => Vec::new(),
    }
}

#[derive(Debug, Clone)]
enum ParamValue {
    Text(String),
    Number(i64),
}

fn values_for_param(def: &ParamDef, world: &PlanningWorldView) -> Vec<ParamValue> {
    match &def.param_type {
        ParamType::NpcId => {
            let actor_id = world.facts.get("agent.id").cloned().unwrap_or_default();
            let actor_location = world.facts.get("agent.location").cloned();
            let tick = world
                .facts
                .get("agent.current_tick")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let mut ranked = world
                .npc_ids
                .iter()
                .map(|npc_id| {
                    let same_location = actor_location
                        .as_ref()
                        .and_then(|location| {
                            world.facts.get(&format!("npc.location:{npc_id}")).map(|other| {
                                if other == location {
                                    1_000_i64
                                } else {
                                    0_i64
                                }
                            })
                        })
                        .unwrap_or(0);
                    let trust = world
                        .facts
                        .get(&format!("social.trust:{actor_id}:{npc_id}"))
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0);
                    let jitter = (stable_param_hash(&actor_id, npc_id, tick) % 17) as i64 - 8;
                    (npc_id.clone(), same_location + trust * 2 + jitter)
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            ranked
                .into_iter()
                .take(12)
                .map(|(npc_id, _)| ParamValue::Text(npc_id))
                .collect()
        }
        ParamType::LocationId => world
            .location_ids
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
        ParamType::ObjectId => {
            let actor_id = world.facts.get("agent.id").cloned().unwrap_or_default();
            let tick = world
                .facts
                .get("agent.current_tick")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let mut ranked = world
                .object_ids
                .iter()
                .map(|object_id| {
                    let score = stable_param_hash(&actor_id, object_id, tick) as i64;
                    (object_id.clone(), score)
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            ranked
                .into_iter()
                .take(8)
                .map(|(object_id, _)| ParamValue::Text(object_id))
                .collect::<Vec<_>>()
        }
        ParamType::InstitutionId => world
            .institution_ids
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
        ParamType::ResourceType => world
            .resource_types
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
        ParamType::Quantity => vec![
            ParamValue::Number(1),
            ParamValue::Number(2),
            ParamValue::Number(3),
        ],
        ParamType::Method(methods) => methods
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
        ParamType::FreeText => vec![ParamValue::Text("default".to_string())],
    }
}

fn set_param(bound: &mut OperatorParams, name: &str, value: ParamValue) {
    match (name, value) {
        ("target_npc", ParamValue::Text(v)) => bound.target_npc = Some(v),
        ("target_location", ParamValue::Text(v)) => bound.target_location = Some(v),
        ("target_object", ParamValue::Text(v)) => bound.target_object = Some(v),
        ("target_institution", ParamValue::Text(v)) => bound.target_institution = Some(v),
        ("resource_type", ParamValue::Text(v)) => bound.resource_type = Some(v),
        ("quantity", ParamValue::Number(v)) => bound.quantity = Some(v),
        ("method", ParamValue::Text(v)) => bound.method = Some(v),
        ("context", ParamValue::Text(v)) => bound.context = Some(v),
        _ => {}
    }
}

fn stable_param_hash(actor_id: &str, npc_id: &str, tick: u64) -> u64 {
    let mut hash = tick.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in actor_id.as_bytes().iter().chain(npc_id.as_bytes()) {
        hash = hash.rotate_left(7) ^ u64::from(*byte);
        hash = hash.wrapping_mul(0x517C_C1B7_2722_0A95);
    }
    hash
}

fn predicate_holds(predicate: &FactPredicate, world: &PlanningWorldView) -> bool {
    let Some(actual) = world.facts.get(&predicate.fact_key) else {
        return false;
    };

    match predicate.operator.as_str() {
        "==" => actual == &predicate.expected,
        "!=" => actual != &predicate.expected,
        "contains" => actual.contains(&predicate.expected),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_parameter_binding_produces_concrete_instances() {
        let catalog = OperatorCatalog::default_catalog();
        let steal = catalog
            .operators
            .iter()
            .find(|op| op.display_name == "steal")
            .expect("steal exists");
        let world = PlanningWorldView {
            object_ids: vec!["item:coin_purse".to_string()],
            npc_ids: vec!["npc:a".to_string()],
            ..PlanningWorldView::default()
        };

        let bindings = catalog.enumerate_bindings(steal, &world);
        assert!(!bindings.is_empty());
        assert!(bindings
            .iter()
            .all(|binding| binding.target_object.is_some()));
        assert!(bindings.iter().all(|binding| binding.method.is_some()));
    }
}
