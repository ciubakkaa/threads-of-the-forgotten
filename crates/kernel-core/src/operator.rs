//! Operator catalog: registry of all operator definitions with parameter binding
//! and precondition checking.
//!
//! The catalog organizes operators across 10 families (livelihood, household, social,
//! information, security, health, mobility, illicit, governance, leisure) and provides
//! methods to enumerate concrete parameter bindings from world state and validate
//! preconditions.

use std::collections::BTreeMap;

use contracts::agency::{
    DriveKind, EffectTemplate, FactPredicate, OperatorDef, OperatorParams, ParamDef, ParamSchema,
    ParamType, PredicateOp,
};

// ---------------------------------------------------------------------------
// World-query abstraction
// ---------------------------------------------------------------------------

/// Minimal view of the world state needed for operator binding and precondition
/// checking. This will be implemented by the full `WorldState` in task 17.
/// For now it provides the query surface the catalog needs.
pub struct WorldView {
    /// NPC ids present at each location.
    pub npcs_by_location: BTreeMap<String, Vec<String>>,
    /// All known location ids.
    pub location_ids: Vec<String>,
    /// Object ids present at each location.
    pub objects_by_location: BTreeMap<String, Vec<String>>,
    /// Institution ids present at each location.
    pub institutions_by_location: BTreeMap<String, Vec<String>>,
    /// Available resource types at each location.
    pub resources_by_location: BTreeMap<String, Vec<String>>,
    /// Fact values queryable by key (e.g. "food:npc_1", "money:npc_1").
    pub facts: BTreeMap<String, i64>,
}

/// Minimal agent view needed for binding — just id and location.
pub struct AgentView {
    pub agent_id: String,
    pub location_id: String,
}

// ---------------------------------------------------------------------------
// OperatorCatalog
// ---------------------------------------------------------------------------

/// Registry of all operator definitions with indexes for fast lookup.
#[derive(Debug)]
pub struct OperatorCatalog {
    operators: Vec<OperatorDef>,
    by_family: BTreeMap<String, Vec<usize>>,
    by_id: BTreeMap<String, usize>,
}

impl OperatorCatalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            by_family: BTreeMap::new(),
            by_id: BTreeMap::new(),
        }
    }

    /// Register an operator definition. Panics on duplicate operator_id.
    pub fn register(&mut self, op: OperatorDef) {
        assert!(
            !self.by_id.contains_key(&op.operator_id),
            "duplicate operator_id: {}",
            op.operator_id
        );
        let idx = self.operators.len();
        self.by_family
            .entry(op.family.clone())
            .or_default()
            .push(idx);
        self.by_id.insert(op.operator_id.clone(), idx);
        self.operators.push(op);
    }

    /// All operator definitions.
    pub fn operators(&self) -> &[OperatorDef] {
        &self.operators
    }

    /// Lookup by operator id.
    pub fn get(&self, operator_id: &str) -> Option<&OperatorDef> {
        self.by_id.get(operator_id).map(|&i| &self.operators[i])
    }

    /// All operators in a given family.
    pub fn by_family(&self, family: &str) -> Vec<&OperatorDef> {
        self.by_family
            .get(family)
            .map(|idxs| idxs.iter().map(|&i| &self.operators[i]).collect())
            .unwrap_or_default()
    }

    /// All distinct family names.
    pub fn families(&self) -> Vec<&str> {
        self.by_family.keys().map(|s| s.as_str()).collect()
    }

    /// Enumerate all valid concrete parameter bindings for an operator given
    /// the agent and world state. Each returned `OperatorParams` has all
    /// required parameters bound to existing world entities.
    pub fn enumerate_bindings(
        &self,
        operator: &OperatorDef,
        agent: &AgentView,
        world: &WorldView,
    ) -> Vec<OperatorParams> {
        // Build per-param candidate lists, then take the cartesian product.
        let required = &operator.param_schema.required;
        if required.is_empty() {
            // No required params — single binding with defaults.
            return vec![OperatorParams::default()];
        }

        let candidate_lists: Vec<Vec<(&str, ParamSetter)>> = required
            .iter()
            .map(|def| candidates_for_param(def, agent, world))
            .collect();

        // If any required param has zero candidates, no valid bindings.
        if candidate_lists.iter().any(|c| c.is_empty()) {
            return Vec::new();
        }

        // Cartesian product of all candidate lists.
        let mut results: Vec<OperatorParams> = vec![OperatorParams::default()];
        for candidates in &candidate_lists {
            let mut next = Vec::with_capacity(results.len() * candidates.len());
            for base in &results {
                for (_label, setter) in candidates {
                    let mut params = base.clone();
                    setter(&mut params);
                    next.push(params);
                }
            }
            results = next;
        }

        results
    }

    /// Check whether all of an operator's preconditions are satisfied against
    /// the current world state.
    pub fn check_preconditions(
        &self,
        operator: &OperatorDef,
        _params: &OperatorParams,
        world: &WorldView,
    ) -> bool {
        operator.preconditions.iter().all(|pred| {
            let actual = world.facts.get(&pred.fact_key).copied().unwrap_or(0);
            match pred.operator {
                PredicateOp::Eq => actual == pred.value,
                PredicateOp::Neq => actual != pred.value,
                PredicateOp::Gt => actual > pred.value,
                PredicateOp::Gte => actual >= pred.value,
                PredicateOp::Lt => actual < pred.value,
                PredicateOp::Lte => actual <= pred.value,
            }
        })
    }
}

impl Default for OperatorCatalog {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Parameter binding helpers
// ---------------------------------------------------------------------------

/// A closure that sets one field on an `OperatorParams`.
type ParamSetter = Box<dyn Fn(&mut OperatorParams)>;

/// Produce candidate (label, setter) pairs for a single parameter definition.
fn candidates_for_param(
    def: &ParamDef,
    agent: &AgentView,
    world: &WorldView,
) -> Vec<(&'static str, ParamSetter)> {
    match &def.param_type {
        ParamType::NpcId => {
            // All NPCs at the agent's location, excluding self.
            let loc = &agent.location_id;
            world
                .npcs_by_location
                .get(loc)
                .map(|npcs| {
                    npcs.iter()
                        .filter(|id| *id != &agent.agent_id)
                        .map(|id| {
                            let id = id.clone();
                            (
                                "npc" as &'static str,
                                Box::new(move |p: &mut OperatorParams| {
                                    p.target_npc = Some(id.clone());
                                }) as ParamSetter,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        ParamType::LocationId => world
            .location_ids
            .iter()
            .map(|id| {
                let id = id.clone();
                (
                    "location" as &'static str,
                    Box::new(move |p: &mut OperatorParams| {
                        p.target_location = Some(id.clone());
                    }) as ParamSetter,
                )
            })
            .collect(),
        ParamType::ObjectId => {
            let loc = &agent.location_id;
            world
                .objects_by_location
                .get(loc)
                .map(|objs| {
                    objs.iter()
                        .map(|id| {
                            let id = id.clone();
                            (
                                "object" as &'static str,
                                Box::new(move |p: &mut OperatorParams| {
                                    p.target_object = Some(id.clone());
                                }) as ParamSetter,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        ParamType::InstitutionId => {
            let loc = &agent.location_id;
            world
                .institutions_by_location
                .get(loc)
                .map(|insts| {
                    insts
                        .iter()
                        .map(|id| {
                            let id = id.clone();
                            (
                                "institution" as &'static str,
                                Box::new(move |p: &mut OperatorParams| {
                                    p.target_institution = Some(id.clone());
                                }) as ParamSetter,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        ParamType::ResourceType => {
            let loc = &agent.location_id;
            world
                .resources_by_location
                .get(loc)
                .map(|res| {
                    res.iter()
                        .map(|r| {
                            let r = r.clone();
                            (
                                "resource" as &'static str,
                                Box::new(move |p: &mut OperatorParams| {
                                    p.resource_type = Some(r.clone());
                                }) as ParamSetter,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        ParamType::Quantity => {
            // Default quantity of 1 — the planner can refine later.
            vec![(
                "quantity",
                Box::new(|p: &mut OperatorParams| {
                    p.quantity = Some(1);
                }) as ParamSetter,
            )]
        }
        ParamType::Method(methods) => methods
            .iter()
            .map(|m| {
                let m = m.clone();
                (
                    "method" as &'static str,
                    Box::new(move |p: &mut OperatorParams| {
                        p.method = Some(m.clone());
                    }) as ParamSetter,
                )
            })
            .collect(),
        ParamType::FreeText => {
            // Free text gets a placeholder — the planner fills in context.
            vec![(
                "text",
                Box::new(|p: &mut OperatorParams| {
                    p.context = Some(String::new());
                }) as ParamSetter,
            )]
        }
    }
}

// ---------------------------------------------------------------------------
// Default catalog population — all 10 families
// ---------------------------------------------------------------------------

/// Build the default operator catalog with all 10 families populated.
pub fn default_catalog() -> OperatorCatalog {
    let mut cat = OperatorCatalog::new();

    // --- Livelihood ---
    register_livelihood(&mut cat);
    // --- Household ---
    register_household(&mut cat);
    // --- Social ---
    register_social(&mut cat);
    // --- Information ---
    register_information(&mut cat);
    // --- Security ---
    register_security(&mut cat);
    // --- Health ---
    register_health(&mut cat);
    // --- Mobility ---
    register_mobility(&mut cat);
    // --- Illicit ---
    register_illicit(&mut cat);
    // --- Governance ---
    register_governance(&mut cat);
    // --- Leisure ---
    register_leisure(&mut cat);

    cat
}

// ---- helper to build an OperatorDef concisely ----

fn op(
    id: &str,
    family: &str,
    display: &str,
    required: Vec<ParamDef>,
    optional: Vec<ParamDef>,
    duration: u64,
    risk: i64,
    visibility: i64,
    preconditions: Vec<FactPredicate>,
    effects: Vec<EffectTemplate>,
    drive_effects: Vec<(DriveKind, i64)>,
    capability_requirements: Vec<(&str, i64)>,
) -> OperatorDef {
    OperatorDef {
        operator_id: id.to_string(),
        family: family.to_string(),
        display_name: display.to_string(),
        param_schema: ParamSchema { required, optional },
        duration_ticks: duration,
        risk,
        visibility,
        preconditions,
        effects,
        drive_effects,
        capability_requirements: capability_requirements
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

fn param(name: &str, pt: ParamType) -> ParamDef {
    ParamDef {
        name: name.to_string(),
        param_type: pt,
    }
}

fn pre(key: &str, operator: PredicateOp, value: i64) -> FactPredicate {
    FactPredicate {
        fact_key: key.to_string(),
        operator,
        value,
    }
}

fn eff(key: &str, expr: &str) -> EffectTemplate {
    EffectTemplate {
        fact_key: key.to_string(),
        delta_expr: expr.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Family registration functions
// ---------------------------------------------------------------------------

fn register_livelihood(cat: &mut OperatorCatalog) {
    cat.register(op(
        "work", "livelihood", "Work",
        vec![],
        vec![param("target_institution", ParamType::InstitutionId)],
        2, 5, 50, vec![], vec![eff("income", "+wage")],
        vec![(DriveKind::Income, 20), (DriveKind::Food, -5)],
        vec![("physical", 10)],
    ));
    cat.register(op(
        "trade", "livelihood", "Trade",
        vec![param("target_npc", ParamType::NpcId), param("resource_type", ParamType::ResourceType)],
        vec![param("quantity", ParamType::Quantity)],
        1, 10, 60, vec![], vec![eff("goods", "exchange")],
        vec![(DriveKind::Income, 15)],
        vec![("trade", 20)],
    ));
    cat.register(op(
        "barter", "livelihood", "Barter",
        vec![param("target_npc", ParamType::NpcId), param("resource_type", ParamType::ResourceType)],
        vec![],
        1, 5, 50, vec![], vec![eff("goods", "exchange")],
        vec![(DriveKind::Income, 10)],
        vec![("trade", 10)],
    ));
    cat.register(op(
        "harvest", "livelihood", "Harvest",
        vec![],
        vec![param("target_object", ParamType::ObjectId)],
        3, 10, 40, vec![], vec![eff("food", "+harvest_yield")],
        vec![(DriveKind::Food, 25), (DriveKind::Income, 5)],
        vec![("physical", 20)],
    ));
    cat.register(op(
        "craft", "livelihood", "Craft",
        vec![param("resource_type", ParamType::ResourceType)],
        vec![],
        2, 5, 40, vec![], vec![eff("goods", "+craft_output")],
        vec![(DriveKind::Income, 15)],
        vec![("trade", 15)],
    ));
}

fn register_household(cat: &mut OperatorCatalog) {
    cat.register(op(
        "eat", "household", "Eat",
        vec![],
        vec![],
        1, 0, 20,
        vec![pre("food_available", PredicateOp::Gt, 0)],
        vec![eff("food_available", "-1")],
        vec![(DriveKind::Food, 30)],
        vec![],
    ));
    cat.register(op(
        "sleep", "household", "Sleep",
        vec![],
        vec![],
        8, 0, 10, vec![], vec![],
        vec![(DriveKind::Health, 20), (DriveKind::Shelter, 15)],
        vec![],
    ));
    cat.register(op(
        "repair", "household", "Repair",
        vec![param("target_object", ParamType::ObjectId)],
        vec![],
        2, 5, 30, vec![], vec![eff("object_condition", "+repair")],
        vec![(DriveKind::Shelter, 10)],
        vec![("physical", 10)],
    ));
    cat.register(op(
        "clean", "household", "Clean",
        vec![],
        vec![],
        1, 0, 20, vec![], vec![],
        vec![(DriveKind::Health, 5), (DriveKind::Shelter, 5)],
        vec![],
    ));
    cat.register(op(
        "cook", "household", "Cook",
        vec![],
        vec![param("resource_type", ParamType::ResourceType)],
        1, 5, 30,
        vec![pre("food_raw", PredicateOp::Gt, 0)],
        vec![eff("food_available", "+1"), eff("food_raw", "-1")],
        vec![(DriveKind::Food, 10)],
        vec![("trade", 5)],
    ));
}

fn register_social(cat: &mut OperatorCatalog) {
    let social_ops = vec![
        ("greet", "Greet", 1, 0, 60, vec![(DriveKind::Belonging, 5)], vec![("social", 5)]),
        ("flatter", "Flatter", 1, 5, 60, vec![(DriveKind::Belonging, 10), (DriveKind::Status, 5)], vec![("social", 15)]),
        ("threaten", "Threaten", 1, 30, 70, vec![(DriveKind::Safety, -10), (DriveKind::Status, 10)], vec![("social", 10)]),
        ("gossip", "Gossip", 1, 10, 50, vec![(DriveKind::Belonging, 10)], vec![("social", 10)]),
        ("confide", "Confide", 1, 15, 30, vec![(DriveKind::Belonging, 15)], vec![("social", 15)]),
        ("propose_alliance", "Propose Alliance", 1, 20, 60, vec![(DriveKind::Safety, 10), (DriveKind::Belonging, 10)], vec![("social", 25)]),
        ("betray", "Betray", 1, 50, 40, vec![(DriveKind::Belonging, -20)], vec![("social", 10)]),
        ("court", "Court", 1, 20, 50, vec![(DriveKind::Belonging, 15), (DriveKind::Status, 5)], vec![("social", 20)]),
        ("console", "Console", 1, 5, 40, vec![(DriveKind::Belonging, 10)], vec![("social", 15), ("care", 10)]),
        ("argue", "Argue", 1, 20, 70, vec![(DriveKind::Belonging, -5), (DriveKind::Status, 5)], vec![("social", 10)]),
        ("mediate", "Mediate", 2, 15, 60, vec![(DriveKind::Belonging, 10), (DriveKind::Safety, 5)], vec![("social", 25)]),
        ("negotiate", "Negotiate", 2, 15, 60, vec![(DriveKind::Income, 10)], vec![("social", 20), ("trade", 10)]),
    ];

    for (id, display, dur, risk, vis, drives, caps) in social_ops {
        cat.register(op(
            id, "social", display,
            vec![param("target_npc", ParamType::NpcId)],
            vec![],
            dur, risk, vis, vec![], vec![],
            drives, caps,
        ));
    }
}

fn register_information(cat: &mut OperatorCatalog) {
    cat.register(op(
        "observe", "information", "Observe",
        vec![], vec![], 1, 0, 20, vec![], vec![],
        vec![], vec![],
    ));
    cat.register(op(
        "eavesdrop", "information", "Eavesdrop",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 1, 15, 20, vec![], vec![],
        vec![], vec![("stealth", 15)],
    ));
    cat.register(op(
        "investigate", "information", "Investigate",
        vec![], vec![param("target_object", ParamType::ObjectId)],
        2, 10, 40, vec![], vec![],
        vec![], vec![("literacy", 10)],
    ));
    cat.register(op(
        "read", "information", "Read",
        vec![param("target_object", ParamType::ObjectId)],
        vec![], 1, 0, 20, vec![], vec![],
        vec![], vec![("literacy", 20)],
    ));
    cat.register(op(
        "teach", "information", "Teach",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 2, 0, 50, vec![], vec![],
        vec![(DriveKind::Status, 5), (DriveKind::Belonging, 5)],
        vec![("literacy", 15)],
    ));
    cat.register(op(
        "learn", "information", "Learn",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 2, 0, 30, vec![], vec![],
        vec![(DriveKind::Status, 5)],
        vec![],
    ));
}

fn register_security(cat: &mut OperatorCatalog) {
    cat.register(op(
        "patrol", "security", "Patrol",
        vec![], vec![], 2, 15, 70, vec![], vec![],
        vec![(DriveKind::Safety, 10)],
        vec![("combat", 10)],
    ));
    cat.register(op(
        "guard", "security", "Guard",
        vec![], vec![param("target_object", ParamType::ObjectId)],
        4, 20, 70, vec![], vec![],
        vec![(DriveKind::Safety, 15)],
        vec![("combat", 15)],
    ));
    cat.register(op(
        "lock", "security", "Lock",
        vec![param("target_object", ParamType::ObjectId)],
        vec![], 1, 0, 30, vec![], vec![eff("locked", "+1")],
        vec![(DriveKind::Safety, 5)],
        vec![],
    ));
    cat.register(op(
        "barricade", "security", "Barricade",
        vec![], vec![], 2, 5, 50, vec![], vec![eff("fortified", "+1")],
        vec![(DriveKind::Safety, 15)],
        vec![("physical", 15)],
    ));
    cat.register(op(
        "flee", "security", "Flee",
        vec![param("target_location", ParamType::LocationId)],
        vec![], 1, 20, 80, vec![], vec![],
        vec![(DriveKind::Safety, 20)],
        vec![],
    ));
    cat.register(op(
        "fight", "security", "Fight",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 1, 60, 90, vec![], vec![],
        vec![(DriveKind::Safety, -10), (DriveKind::Health, -15)],
        vec![("combat", 20)],
    ));
}

fn register_health(cat: &mut OperatorCatalog) {
    cat.register(op(
        "rest", "health", "Rest",
        vec![], vec![], 4, 0, 10, vec![], vec![],
        vec![(DriveKind::Health, 15)],
        vec![],
    ));
    cat.register(op(
        "treat_wound", "health", "Treat Wound",
        vec![], vec![], 2, 5, 30, vec![], vec![],
        vec![(DriveKind::Health, 20)],
        vec![("care", 15)],
    ));
    cat.register(op(
        "seek_healer", "health", "Seek Healer",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 1, 5, 40, vec![], vec![],
        vec![(DriveKind::Health, 25)],
        vec![],
    ));
    cat.register(op(
        "tend_sick", "health", "Tend Sick",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 2, 10, 40, vec![], vec![],
        vec![(DriveKind::Belonging, 10)],
        vec![("care", 20)],
    ));
}

fn register_mobility(cat: &mut OperatorCatalog) {
    cat.register(op(
        "travel", "mobility", "Travel",
        vec![param("target_location", ParamType::LocationId)],
        vec![], 4, 15, 60, vec![], vec![],
        vec![], vec![],
    ));
    cat.register(op(
        "explore", "mobility", "Explore",
        vec![], vec![], 3, 25, 50, vec![], vec![],
        vec![], vec![],
    ));
    cat.register(op(
        "scout", "mobility", "Scout",
        vec![], vec![param("target_location", ParamType::LocationId)],
        2, 20, 40, vec![], vec![],
        vec![(DriveKind::Safety, 5)],
        vec![("stealth", 10)],
    ));
}

fn register_illicit(cat: &mut OperatorCatalog) {
    cat.register(op(
        "steal", "illicit", "Steal",
        vec![param("method", ParamType::Method(vec![
            "pickpocket".to_string(),
            "shoplift".to_string(),
            "burgle".to_string(),
        ]))],
        vec![param("target_npc", ParamType::NpcId), param("target_object", ParamType::ObjectId)],
        1, 50, 30, vec![], vec![],
        vec![(DriveKind::Income, 15)],
        vec![("stealth", 20)],
    ));
    cat.register(op(
        "smuggle", "illicit", "Smuggle",
        vec![param("resource_type", ParamType::ResourceType)],
        vec![param("target_location", ParamType::LocationId)],
        3, 40, 30, vec![], vec![],
        vec![(DriveKind::Income, 20)],
        vec![("stealth", 20), ("trade", 10)],
    ));
    cat.register(op(
        "bribe", "illicit", "Bribe",
        vec![param("target_npc", ParamType::NpcId)],
        vec![param("quantity", ParamType::Quantity)],
        1, 35, 30,
        vec![pre("money", PredicateOp::Gt, 0)],
        vec![eff("money", "-bribe_amount")],
        vec![(DriveKind::Safety, 10)],
        vec![("social", 10)],
    ));
    cat.register(op(
        "extort", "illicit", "Extort",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 1, 50, 40, vec![], vec![],
        vec![(DriveKind::Income, 20)],
        vec![("social", 15), ("combat", 10)],
    ));
    cat.register(op(
        "fence", "illicit", "Fence Goods",
        vec![param("target_npc", ParamType::NpcId)],
        vec![param("resource_type", ParamType::ResourceType)],
        1, 30, 30, vec![], vec![],
        vec![(DriveKind::Income, 15)],
        vec![("trade", 15), ("stealth", 10)],
    ));
}

fn register_governance(cat: &mut OperatorCatalog) {
    cat.register(op(
        "petition", "governance", "Petition",
        vec![param("target_institution", ParamType::InstitutionId)],
        vec![], 1, 5, 60, vec![], vec![],
        vec![(DriveKind::Safety, 5)],
        vec![("social", 10)],
    ));
    cat.register(op(
        "vote", "governance", "Vote",
        vec![param("target_institution", ParamType::InstitutionId)],
        vec![], 1, 0, 50, vec![], vec![],
        vec![(DriveKind::Status, 5)],
        vec![],
    ));
    cat.register(op(
        "decree", "governance", "Decree",
        vec![], vec![], 1, 15, 90, vec![], vec![],
        vec![(DriveKind::Status, 15)],
        vec![("influence", 30), ("law", 20)],
    ));
    cat.register(op(
        "enforce_law", "governance", "Enforce Law",
        vec![], vec![param("target_npc", ParamType::NpcId)],
        2, 25, 80, vec![], vec![],
        vec![(DriveKind::Safety, 10), (DriveKind::Status, 5)],
        vec![("law", 20), ("combat", 10)],
    ));
    cat.register(op(
        "collect_tax", "governance", "Collect Tax",
        vec![], vec![param("target_npc", ParamType::NpcId)],
        1, 10, 70, vec![], vec![],
        vec![(DriveKind::Income, 15)],
        vec![("law", 15), ("influence", 10)],
    ));
}

fn register_leisure(cat: &mut OperatorCatalog) {
    cat.register(op(
        "drink", "leisure", "Drink",
        vec![], vec![], 1, 5, 50, vec![], vec![],
        vec![(DriveKind::Belonging, 10), (DriveKind::Health, -5)],
        vec![],
    ));
    cat.register(op(
        "gamble", "leisure", "Gamble",
        vec![], vec![], 1, 20, 50, vec![], vec![],
        vec![(DriveKind::Belonging, 5)],
        vec![],
    ));
    cat.register(op(
        "perform", "leisure", "Perform",
        vec![], vec![], 2, 5, 70, vec![], vec![],
        vec![(DriveKind::Status, 10), (DriveKind::Belonging, 10)],
        vec![("social", 15)],
    ));
    cat.register(op(
        "attend_event", "leisure", "Attend Event",
        vec![], vec![], 2, 5, 60, vec![], vec![],
        vec![(DriveKind::Belonging, 10)],
        vec![],
    ));
    cat.register(op(
        "pray", "leisure", "Pray",
        vec![], vec![param("target_institution", ParamType::InstitutionId)],
        1, 0, 30, vec![], vec![],
        vec![(DriveKind::Belonging, 5), (DriveKind::Health, 5)],
        vec![],
    ));
    cat.register(op(
        "socialize", "leisure", "Socialize",
        vec![param("target_npc", ParamType::NpcId)],
        vec![], 1, 0, 50, vec![], vec![],
        vec![(DriveKind::Belonging, 15)],
        vec![("social", 5)],
    ));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_world() -> WorldView {
        let mut npcs_by_location = BTreeMap::new();
        npcs_by_location.insert(
            "town_square".to_string(),
            vec!["npc_1".to_string(), "npc_2".to_string(), "npc_3".to_string()],
        );
        let mut objects_by_location = BTreeMap::new();
        objects_by_location.insert(
            "town_square".to_string(),
            vec!["door_1".to_string(), "chest_1".to_string()],
        );
        let mut institutions_by_location = BTreeMap::new();
        institutions_by_location.insert(
            "town_square".to_string(),
            vec!["market".to_string(), "court".to_string()],
        );
        let mut resources_by_location = BTreeMap::new();
        resources_by_location.insert(
            "town_square".to_string(),
            vec!["food".to_string(), "fuel".to_string()],
        );
        let mut facts = BTreeMap::new();
        facts.insert("food_available".to_string(), 5);
        facts.insert("food_raw".to_string(), 3);
        facts.insert("money".to_string(), 10);

        WorldView {
            npcs_by_location,
            location_ids: vec!["town_square".to_string(), "forest".to_string()],
            objects_by_location,
            institutions_by_location,
            resources_by_location,
            facts,
        }
    }

    fn test_agent() -> AgentView {
        AgentView {
            agent_id: "npc_1".to_string(),
            location_id: "town_square".to_string(),
        }
    }

    #[test]
    fn default_catalog_has_all_10_families() {
        let cat = default_catalog();
        let families = cat.families();
        assert!(families.contains(&"livelihood"));
        assert!(families.contains(&"household"));
        assert!(families.contains(&"social"));
        assert!(families.contains(&"information"));
        assert!(families.contains(&"security"));
        assert!(families.contains(&"health"));
        assert!(families.contains(&"mobility"));
        assert!(families.contains(&"illicit"));
        assert!(families.contains(&"governance"));
        assert!(families.contains(&"leisure"));
        assert_eq!(families.len(), 10);
    }

    #[test]
    fn lookup_by_id() {
        let cat = default_catalog();
        let steal = cat.get("steal").expect("steal should exist");
        assert_eq!(steal.family, "illicit");
        // steal has a Method param for pickpocket/shoplift/burgle
        assert!(steal.param_schema.required.iter().any(|p| p.name == "method"));
    }

    #[test]
    fn lookup_by_family() {
        let cat = default_catalog();
        let social = cat.by_family("social");
        assert_eq!(social.len(), 12); // greet, flatter, threaten, gossip, confide, propose_alliance, betray, court, console, argue, mediate, negotiate
    }

    #[test]
    fn enumerate_bindings_no_params() {
        let cat = default_catalog();
        let observe = cat.get("observe").unwrap();
        let bindings = cat.enumerate_bindings(observe, &test_agent(), &test_world());
        assert_eq!(bindings.len(), 1); // single default binding
    }

    #[test]
    fn enumerate_bindings_npc_param() {
        let cat = default_catalog();
        let greet = cat.get("greet").unwrap();
        let bindings = cat.enumerate_bindings(greet, &test_agent(), &test_world());
        // npc_2 and npc_3 at town_square (excluding self npc_1)
        assert_eq!(bindings.len(), 2);
        let targets: Vec<_> = bindings.iter().map(|b| b.target_npc.as_ref().unwrap().as_str()).collect();
        assert!(targets.contains(&"npc_2"));
        assert!(targets.contains(&"npc_3"));
    }

    #[test]
    fn enumerate_bindings_method_param() {
        let cat = default_catalog();
        let steal = cat.get("steal").unwrap();
        let bindings = cat.enumerate_bindings(steal, &test_agent(), &test_world());
        // 3 methods: pickpocket, shoplift, burgle
        assert_eq!(bindings.len(), 3);
        let methods: Vec<_> = bindings.iter().map(|b| b.method.as_ref().unwrap().as_str()).collect();
        assert!(methods.contains(&"pickpocket"));
        assert!(methods.contains(&"shoplift"));
        assert!(methods.contains(&"burgle"));
    }

    #[test]
    fn enumerate_bindings_cartesian_product() {
        let cat = default_catalog();
        let trade = cat.get("trade").unwrap();
        let bindings = cat.enumerate_bindings(trade, &test_agent(), &test_world());
        // 2 NPCs * 2 resources = 4 bindings
        assert_eq!(bindings.len(), 4);
    }

    #[test]
    fn enumerate_bindings_empty_when_no_candidates() {
        let cat = default_catalog();
        let greet = cat.get("greet").unwrap();
        let agent = AgentView {
            agent_id: "lonely".to_string(),
            location_id: "empty_field".to_string(),
        };
        let bindings = cat.enumerate_bindings(greet, &agent, &test_world());
        assert!(bindings.is_empty());
    }

    #[test]
    fn check_preconditions_satisfied() {
        let cat = default_catalog();
        let eat = cat.get("eat").unwrap();
        let world = test_world();
        assert!(cat.check_preconditions(eat, &OperatorParams::default(), &world));
    }

    #[test]
    fn check_preconditions_unsatisfied() {
        let cat = default_catalog();
        let eat = cat.get("eat").unwrap();
        let mut world = test_world();
        world.facts.insert("food_available".to_string(), 0);
        assert!(!cat.check_preconditions(eat, &OperatorParams::default(), &world));
    }

    #[test]
    fn check_preconditions_missing_fact_defaults_to_zero() {
        let cat = default_catalog();
        let bribe = cat.get("bribe").unwrap();
        let mut world = test_world();
        world.facts.remove("money");
        // money defaults to 0, precondition is money > 0 → false
        assert!(!cat.check_preconditions(bribe, &OperatorParams::default(), &world));
    }

    #[test]
    fn all_operators_have_unique_ids() {
        let cat = default_catalog();
        let mut seen = std::collections::HashSet::new();
        for op in cat.operators() {
            assert!(seen.insert(&op.operator_id), "duplicate: {}", op.operator_id);
        }
    }

    #[test]
    fn steal_uses_general_purpose_method_param() {
        let cat = default_catalog();
        let steal = cat.get("steal").unwrap();
        let method_param = steal.param_schema.required.iter().find(|p| p.name == "method").unwrap();
        match &method_param.param_type {
            ParamType::Method(methods) => {
                assert_eq!(methods.len(), 3);
                assert!(methods.contains(&"pickpocket".to_string()));
                assert!(methods.contains(&"shoplift".to_string()));
                assert!(methods.contains(&"burgle".to_string()));
            }
            _ => panic!("steal method param should be Method type"),
        }
    }
}
