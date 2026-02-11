//! Social graph and rumor network: manages directed relationships between NPCs,
//! group dynamics, and information propagation with distortion.

use std::collections::{BTreeMap, VecDeque};

use contracts::agency::{PersonalityTraits, RelationshipEdge, RumorPayload};

// ---------------------------------------------------------------------------
// EdgeUpdate
// ---------------------------------------------------------------------------

/// Describes how to modify a relationship edge based on an action.
#[derive(Debug, Clone)]
pub struct EdgeUpdate {
    pub trust_delta: i64,
    pub reputation_delta: i64,
    pub obligation_delta: i64,
    pub grievance_delta: i64,
    pub fear_delta: i64,
    pub respect_delta: i64,
}

impl Default for EdgeUpdate {
    fn default() -> Self {
        Self {
            trust_delta: 0,
            reputation_delta: 0,
            obligation_delta: 0,
            grievance_delta: 0,
            fear_delta: 0,
            respect_delta: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// GroupState
// ---------------------------------------------------------------------------

/// Tracks a social group's formation, cohesion, and membership.
#[derive(Debug, Clone)]
pub struct GroupState {
    pub group_id: String,
    pub leader_id: String,
    pub members: Vec<String>,
    pub cohesion: i64,
    pub formed_tick: u64,
}

impl GroupState {
    /// Whether the group should dissolve (cohesion below threshold).
    pub fn should_dissolve(&self, threshold: i64) -> bool {
        self.cohesion < threshold
    }

    /// Whether the group should split (large membership with low cohesion).
    pub fn should_split(&self, cohesion_threshold: i64, min_split_size: usize) -> bool {
        self.members.len() >= min_split_size && self.cohesion < cohesion_threshold * 2
    }
}

// ---------------------------------------------------------------------------
// SocialGraph
// ---------------------------------------------------------------------------

/// Directed graph of NPC relationships with edge management and group dynamics.
///
/// Edges are keyed by `(from_npc_id, to_npc_id)` and carry trust, reputation,
/// obligation, grievance, fear, and respect values clamped to [-100, 100].
#[derive(Debug, Clone)]
pub struct SocialGraph {
    edges: BTreeMap<(String, String), RelationshipEdge>,
    groups: BTreeMap<String, GroupState>,
}

impl SocialGraph {
    pub fn new() -> Self {
        Self {
            edges: BTreeMap::new(),
            groups: BTreeMap::new(),
        }
    }

    /// Update the edge from `from` to `to` based on the action and affected NPC's personality.
    ///
    /// Personality modulates the magnitude of changes:
    /// - High empathy amplifies trust/respect changes
    /// - High vindictiveness amplifies grievance changes
    /// - High loyalty amplifies obligation changes
    pub fn update_edge(
        &mut self,
        from: &str,
        to: &str,
        update: &EdgeUpdate,
        personality: &PersonalityTraits,
    ) {
        let edge = self
            .edges
            .entry((from.to_string(), to.to_string()))
            .or_insert_with(RelationshipEdge::default);

        // Personality modulation: scale deltas by personality traits.
        // Traits are in [-100, 100]; we use (100 + trait) / 100 as a multiplier
        // so trait=0 → 1.0x, trait=100 → 2.0x, trait=-100 → 0.0x.
        let empathy_mod = (100 + personality.empathy).max(0) as f64 / 100.0;
        let vindictive_mod = (100 + personality.vindictiveness).max(0) as f64 / 100.0;
        let loyalty_mod = (100 + personality.loyalty).max(0) as f64 / 100.0;

        edge.trust = (edge.trust + scale(update.trust_delta, empathy_mod)).clamp(-100, 100);
        edge.reputation =
            (edge.reputation + scale(update.reputation_delta, empathy_mod)).clamp(-100, 100);
        edge.obligation =
            (edge.obligation + scale(update.obligation_delta, loyalty_mod)).clamp(-100, 100);
        edge.grievance =
            (edge.grievance + scale(update.grievance_delta, vindictive_mod)).clamp(-100, 100);
        edge.fear = (edge.fear + update.fear_delta).clamp(-100, 100);
        edge.respect = (edge.respect + scale(update.respect_delta, empathy_mod)).clamp(-100, 100);
    }

    /// Get the edge from `from` to `to`, if it exists.
    pub fn get_edge(&self, from: &str, to: &str) -> Option<&RelationshipEdge> {
        self.edges.get(&(from.to_string(), to.to_string()))
    }

    /// Get all neighbors of an NPC (outgoing edges).
    pub fn neighbors(&self, npc_id: &str) -> Vec<(&str, &RelationshipEdge)> {
        self.edges
            .iter()
            .filter(|((from, _), _)| from == npc_id)
            .map(|((_, to), edge)| (to.as_str(), edge))
            .collect()
    }

    /// BFS shortest path distance between two NPCs in the social graph.
    /// Returns `None` if no path exists. Treats edges as undirected for reachability.
    pub fn social_distance(&self, from: &str, to: &str) -> Option<u32> {
        if from == to {
            return Some(0);
        }

        let mut visited = BTreeMap::new();
        let mut queue = VecDeque::new();
        visited.insert(from.to_string(), 0u32);
        queue.push_back(from.to_string());

        while let Some(current) = queue.pop_front() {
            let dist = visited[&current];

            // Collect neighbors (both directions for undirected reachability).
            for ((a, b), _) in &self.edges {
                let neighbor = if a == &current {
                    b
                } else if b == &current {
                    a
                } else {
                    continue;
                };

                if neighbor == to {
                    return Some(dist + 1);
                }

                if !visited.contains_key(neighbor.as_str()) {
                    visited.insert(neighbor.clone(), dist + 1);
                    queue.push_back(neighbor.clone());
                }
            }
        }

        None
    }

    /// Add a group to the social graph.
    pub fn add_group(&mut self, group: GroupState) {
        self.groups.insert(group.group_id.clone(), group);
    }

    /// Get a group by ID.
    pub fn get_group(&self, group_id: &str) -> Option<&GroupState> {
        self.groups.get(group_id)
    }

    /// Get a mutable reference to a group.
    pub fn get_group_mut(&mut self, group_id: &str) -> Option<&mut GroupState> {
        self.groups.get_mut(group_id)
    }

    /// Remove groups whose cohesion has fallen below the threshold.
    /// Returns the IDs of dissolved groups.
    pub fn dissolve_weak_groups(&mut self, threshold: i64) -> Vec<String> {
        let to_remove: Vec<String> = self
            .groups
            .iter()
            .filter(|(_, g)| g.should_dissolve(threshold))
            .map(|(id, _)| id.clone())
            .collect();
        for id in &to_remove {
            self.groups.remove(id);
        }
        to_remove
    }

    /// Read-only access to all edges.
    pub fn edges(&self) -> &BTreeMap<(String, String), RelationshipEdge> {
        &self.edges
    }

    /// Read-only access to all groups.
    pub fn groups(&self) -> &BTreeMap<String, GroupState> {
        &self.groups
    }
}

impl Default for SocialGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Scale a delta by a floating-point modifier, rounding to nearest integer.
fn scale(delta: i64, modifier: f64) -> i64 {
    (delta as f64 * modifier).round() as i64
}

// ---------------------------------------------------------------------------
// RumorNetwork
// ---------------------------------------------------------------------------

/// A pending rumor delivery to a specific NPC.
#[derive(Debug, Clone)]
pub struct PendingRumor {
    pub target_npc_id: String,
    pub payload: RumorPayload,
    pub source_trust: i64,
}

/// Propagates rumors through the social graph based on proximity and trust.
///
/// At each hop, there is a configurable probability of distortion. Rumor
/// credibility is weighted by the source NPC's trust level on the connecting edge.
#[derive(Debug, Clone)]
pub struct RumorNetwork {
    /// Probability of distortion per hop, 0–100.
    distortion_probability: u32,
}

impl RumorNetwork {
    pub fn new(distortion_probability: u32) -> Self {
        Self {
            distortion_probability: distortion_probability.min(100),
        }
    }

    /// Propagate a rumor through the social graph from `origin_npc_id`.
    ///
    /// Returns a list of `PendingRumor` deliveries sorted by social distance
    /// (closer NPCs first), with trust-weighted credibility and per-hop distortion.
    ///
    /// `max_hops` limits how far the rumor can travel.
    /// `seed` is used for deterministic distortion decisions.
    pub fn propagate(
        &self,
        rumor: &RumorPayload,
        graph: &SocialGraph,
        max_hops: u32,
        seed: u64,
    ) -> Vec<PendingRumor> {
        let origin = &rumor.origin_npc_id;
        let mut deliveries = Vec::new();
        let mut visited = BTreeMap::new();
        let mut queue: VecDeque<(String, u32, RumorPayload)> = VecDeque::new();

        visited.insert(origin.clone(), 0u32);
        queue.push_back((origin.clone(), 0, rumor.clone()));

        while let Some((current_npc, hops, current_rumor)) = queue.pop_front() {
            if hops >= max_hops {
                continue;
            }

            // Find neighbors of current NPC (outgoing edges only for directed propagation).
            let neighbors: Vec<(String, i64)> = graph
                .edges()
                .iter()
                .filter(|((from, _), _)| from == &current_npc)
                .map(|((_, to), edge)| (to.clone(), edge.trust))
                .collect();

            for (neighbor_id, trust) in neighbors {
                if visited.contains_key(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id.clone(), hops + 1);

                // Apply per-hop distortion.
                let hop_seed = seed
                    .wrapping_mul(neighbor_id.len() as u64 + 1)
                    .wrapping_add(hops as u64 * 7919);
                let distorted = self.should_distort(hop_seed);
                let mut next_rumor = current_rumor.clone();
                next_rumor.hops = hops + 1;
                if distorted {
                    next_rumor.distorted = true;
                    // Distortion alters content while preserving topic.
                    next_rumor.content = format!("{} (distorted)", next_rumor.content);
                }

                // Trust on the edge determines credibility.
                let source_trust = trust.clamp(0, 100);

                deliveries.push(PendingRumor {
                    target_npc_id: neighbor_id.clone(),
                    payload: next_rumor.clone(),
                    source_trust,
                });

                queue.push_back((neighbor_id, hops + 1, next_rumor));
            }
        }

        // Sort by hop count (social distance) — closer NPCs first.
        deliveries.sort_by_key(|d| d.payload.hops);
        deliveries
    }

    /// Deterministic distortion check using a simple hash-based approach.
    fn should_distort(&self, seed: u64) -> bool {
        // Use seed to produce a value in [0, 100).
        let roll = (seed % 100) as u32;
        roll < self.distortion_probability
    }
}

impl Default for RumorNetwork {
    fn default() -> Self {
        Self::new(15) // 15% default distortion probability
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn neutral_personality() -> PersonalityTraits {
        PersonalityTraits {
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
        }
    }

    // -- SocialGraph tests --

    #[test]
    fn update_edge_creates_and_modifies() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 10,
            respect_delta: 5,
            ..Default::default()
        };
        graph.update_edge("a", "b", &update, &neutral_personality());
        let edge = graph.get_edge("a", "b").unwrap();
        assert_eq!(edge.trust, 10);
        assert_eq!(edge.respect, 5);
        assert_eq!(edge.grievance, 0);
    }

    #[test]
    fn update_edge_clamps_to_range() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 200,
            grievance_delta: -200,
            ..Default::default()
        };
        graph.update_edge("a", "b", &update, &neutral_personality());
        let edge = graph.get_edge("a", "b").unwrap();
        assert_eq!(edge.trust, 100);
        assert_eq!(edge.grievance, -100);
    }

    #[test]
    fn personality_modulates_edge_update() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 10,
            grievance_delta: 10,
            ..Default::default()
        };

        // High empathy amplifies trust changes.
        let empathic = PersonalityTraits {
            empathy: 100,
            ..neutral_personality()
        };
        graph.update_edge("a", "b", &update, &empathic);
        let edge = graph.get_edge("a", "b").unwrap();
        assert_eq!(edge.trust, 20); // 10 * 2.0

        // High vindictiveness amplifies grievance changes.
        let vindictive = PersonalityTraits {
            vindictiveness: 100,
            ..neutral_personality()
        };
        let mut graph2 = SocialGraph::new();
        graph2.update_edge("a", "b", &update, &vindictive);
        let edge2 = graph2.get_edge("a", "b").unwrap();
        assert_eq!(edge2.grievance, 20); // 10 * 2.0
    }

    #[test]
    fn neighbors_returns_outgoing_edges() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 5,
            ..Default::default()
        };
        graph.update_edge("a", "b", &update, &neutral_personality());
        graph.update_edge("a", "c", &update, &neutral_personality());
        graph.update_edge("b", "a", &update, &neutral_personality());

        let neighbors = graph.neighbors("a");
        assert_eq!(neighbors.len(), 2);
        let neighbor_ids: Vec<&str> = neighbors.iter().map(|(id, _)| *id).collect();
        assert!(neighbor_ids.contains(&"b"));
        assert!(neighbor_ids.contains(&"c"));
    }

    #[test]
    fn social_distance_bfs() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 5,
            ..Default::default()
        };
        // a -> b -> c
        graph.update_edge("a", "b", &update, &neutral_personality());
        graph.update_edge("b", "c", &update, &neutral_personality());

        assert_eq!(graph.social_distance("a", "a"), Some(0));
        assert_eq!(graph.social_distance("a", "b"), Some(1));
        assert_eq!(graph.social_distance("a", "c"), Some(2));
        assert_eq!(graph.social_distance("a", "z"), None);
    }

    #[test]
    fn group_dissolve_below_threshold() {
        let mut graph = SocialGraph::new();
        graph.add_group(GroupState {
            group_id: "g1".into(),
            leader_id: "a".into(),
            members: vec!["a".into(), "b".into()],
            cohesion: 5,
            formed_tick: 0,
        });
        graph.add_group(GroupState {
            group_id: "g2".into(),
            leader_id: "c".into(),
            members: vec!["c".into(), "d".into()],
            cohesion: 50,
            formed_tick: 0,
        });

        let dissolved = graph.dissolve_weak_groups(20);
        assert_eq!(dissolved, vec!["g1".to_string()]);
        assert!(graph.get_group("g1").is_none());
        assert!(graph.get_group("g2").is_some());
    }

    // -- RumorNetwork tests --

    #[test]
    fn rumor_propagates_through_graph() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 50,
            ..Default::default()
        };
        // a -> b -> c
        graph.update_edge("a", "b", &update, &neutral_personality());
        graph.update_edge("b", "c", &update, &neutral_personality());

        let network = RumorNetwork::new(0); // no distortion
        let rumor = RumorPayload {
            rumor_id: "r1".into(),
            origin_npc_id: "a".into(),
            topic: "scandal".into(),
            content: "mayor is corrupt".into(),
            hops: 0,
            distorted: false,
        };

        let deliveries = network.propagate(&rumor, &graph, 5, 42);
        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0].target_npc_id, "b");
        assert_eq!(deliveries[0].payload.hops, 1);
        assert_eq!(deliveries[1].target_npc_id, "c");
        assert_eq!(deliveries[1].payload.hops, 2);
    }

    #[test]
    fn rumor_respects_max_hops() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 50,
            ..Default::default()
        };
        graph.update_edge("a", "b", &update, &neutral_personality());
        graph.update_edge("b", "c", &update, &neutral_personality());
        graph.update_edge("c", "d", &update, &neutral_personality());

        let network = RumorNetwork::new(0);
        let rumor = RumorPayload {
            rumor_id: "r1".into(),
            origin_npc_id: "a".into(),
            topic: "news".into(),
            content: "something".into(),
            hops: 0,
            distorted: false,
        };

        let deliveries = network.propagate(&rumor, &graph, 2, 42);
        // Should reach b (hop 1) and c (hop 2), but not d (hop 3).
        assert_eq!(deliveries.len(), 2);
        assert!(deliveries.iter().all(|d| d.target_npc_id != "d"));
    }

    #[test]
    fn rumor_trust_from_edge() {
        let mut graph = SocialGraph::new();
        // a -> b with high trust, a -> c with low trust
        graph.update_edge(
            "a",
            "b",
            &EdgeUpdate { trust_delta: 80, ..Default::default() },
            &neutral_personality(),
        );
        graph.update_edge(
            "a",
            "c",
            &EdgeUpdate { trust_delta: 10, ..Default::default() },
            &neutral_personality(),
        );

        let network = RumorNetwork::new(0);
        let rumor = RumorPayload {
            rumor_id: "r1".into(),
            origin_npc_id: "a".into(),
            topic: "news".into(),
            content: "info".into(),
            hops: 0,
            distorted: false,
        };

        let deliveries = network.propagate(&rumor, &graph, 3, 42);
        let b_delivery = deliveries.iter().find(|d| d.target_npc_id == "b").unwrap();
        let c_delivery = deliveries.iter().find(|d| d.target_npc_id == "c").unwrap();
        assert!(b_delivery.source_trust > c_delivery.source_trust);
    }

    #[test]
    fn rumor_distortion_increases_with_hops() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 50,
            ..Default::default()
        };
        // Chain: a -> b -> c -> d -> e
        graph.update_edge("a", "b", &update, &neutral_personality());
        graph.update_edge("b", "c", &update, &neutral_personality());
        graph.update_edge("c", "d", &update, &neutral_personality());
        graph.update_edge("d", "e", &update, &neutral_personality());

        let network = RumorNetwork::new(100); // 100% distortion
        let rumor = RumorPayload {
            rumor_id: "r1".into(),
            origin_npc_id: "a".into(),
            topic: "news".into(),
            content: "original".into(),
            hops: 0,
            distorted: false,
        };

        let deliveries = network.propagate(&rumor, &graph, 10, 42);
        // With 100% distortion, every hop distorts.
        // Later hops should have more "(distorted)" markers.
        let last = deliveries.last().unwrap();
        let first = deliveries.first().unwrap();
        let last_distortion_count = last.payload.content.matches("(distorted)").count();
        let first_distortion_count = first.payload.content.matches("(distorted)").count();
        assert!(
            last_distortion_count >= first_distortion_count,
            "later hops should have more distortion: first={first_distortion_count}, last={last_distortion_count}"
        );
    }

    #[test]
    fn rumor_no_delivery_to_origin() {
        let mut graph = SocialGraph::new();
        let update = EdgeUpdate {
            trust_delta: 50,
            ..Default::default()
        };
        graph.update_edge("a", "b", &update, &neutral_personality());
        graph.update_edge("b", "a", &update, &neutral_personality());

        let network = RumorNetwork::new(0);
        let rumor = RumorPayload {
            rumor_id: "r1".into(),
            origin_npc_id: "a".into(),
            topic: "news".into(),
            content: "info".into(),
            hops: 0,
            distorted: false,
        };

        let deliveries = network.propagate(&rumor, &graph, 5, 42);
        assert!(deliveries.iter().all(|d| d.target_npc_id != "a"));
    }
}
