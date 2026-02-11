use std::collections::BTreeMap;

use contracts::{FactPredicate, OperatorDef, OperatorParams, ParamDef, ParamSchema, ParamType};

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
                    duration_ticks: if name == "travel" { 2 } else { 1 },
                    risk: if family == "illicit" { 65 } else { 20 },
                    visibility: if family == "illicit" { 70 } else { 40 },
                    preconditions: Vec::new(),
                    effects: Vec::new(),
                    drive_effects: Vec::new(),
                    capability_requirements: Vec::new(),
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
            required: vec![ParamDef {
                name: "resource_type".to_string(),
                param_type: ParamType::ResourceType,
            }],
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

#[derive(Debug, Clone)]
enum ParamValue {
    Text(String),
    Number(i64),
}

fn values_for_param(def: &ParamDef, world: &PlanningWorldView) -> Vec<ParamValue> {
    match &def.param_type {
        ParamType::NpcId => world
            .npc_ids
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
        ParamType::LocationId => world
            .location_ids
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
        ParamType::ObjectId => world
            .object_ids
            .iter()
            .take(8)
            .cloned()
            .map(ParamValue::Text)
            .collect(),
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
