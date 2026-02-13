use std::collections::{BTreeMap, BTreeSet, VecDeque};

use contracts::{BeliefClaim, BeliefSource, RelationshipEdge, RumorPayload};

#[derive(Debug, Clone, Default)]
pub struct EdgeUpdate {
    pub trust_delta: i64,
    pub reputation_delta: i64,
    pub obligation_delta: i64,
    pub grievance_delta: i64,
    pub fear_delta: i64,
    pub respect_delta: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SocialGraph {
    pub edges: BTreeMap<(String, String), RelationshipEdge>,
}

impl SocialGraph {
    pub fn update_edge(&mut self, from: &str, to: &str, update: EdgeUpdate) {
        let key = (from.to_string(), to.to_string());
        let entry = self.edges.entry(key.clone()).or_insert(RelationshipEdge {
            source_npc_id: key.0.clone(),
            target_npc_id: key.1.clone(),
            trust: 0,
            reputation: 0,
            obligation: 0,
            grievance: 0,
            fear: 0,
            respect: 0,
        });

        entry.trust = (entry.trust + update.trust_delta).clamp(-100, 100);
        entry.reputation = (entry.reputation + update.reputation_delta).clamp(-100, 100);
        entry.obligation = (entry.obligation + update.obligation_delta).clamp(-100, 100);
        entry.grievance = (entry.grievance + update.grievance_delta).clamp(-100, 100);
        entry.fear = (entry.fear + update.fear_delta).clamp(-100, 100);
        entry.respect = (entry.respect + update.respect_delta).clamp(-100, 100);
    }

    pub fn get_edge(&self, from: &str, to: &str) -> Option<&RelationshipEdge> {
        self.edges.get(&(from.to_string(), to.to_string()))
    }

    pub fn neighbors(&self, npc_id: &str) -> Vec<(&str, &RelationshipEdge)> {
        self.edges
            .iter()
            .filter_map(|((from, to), edge)| {
                if from == npc_id {
                    Some((to.as_str(), edge))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn social_distance(&self, from: &str, to: &str) -> Option<u32> {
        if from == to {
            return Some(0);
        }

        self.bfs_distances(from).get(to).copied()
    }

    fn bfs_distances(&self, from: &str) -> BTreeMap<String, u32> {
        let mut distances = BTreeMap::<String, u32>::new();
        let mut queue = VecDeque::new();
        queue.push_back((from.to_string(), 0_u32));

        while let Some((node, depth)) = queue.pop_front() {
            if distances.contains_key(&node) {
                continue;
            }
            distances.insert(node.clone(), depth);

            for (neighbor, _) in self.neighbors(&node) {
                if !distances.contains_key(neighbor) {
                    queue.push_back((neighbor.to_string(), depth + 1));
                }
            }
        }

        distances
    }
}

#[derive(Debug, Clone)]
pub struct RumorNetwork {
    pub distortion_bps: u16,
}

impl Default for RumorNetwork {
    fn default() -> Self {
        Self {
            distortion_bps: 1_200,
        }
    }
}

impl RumorNetwork {
    pub fn with_distortion_bps(distortion_bps: u16) -> Self {
        Self {
            distortion_bps: distortion_bps.min(10_000),
        }
    }
}

impl RumorNetwork {
    const MAX_PROPAGATION_FANOUT: usize = 8;

    pub fn propagate(
        &self,
        rumor: &RumorPayload,
        source_id: &str,
        graph: &SocialGraph,
    ) -> Vec<(String, RumorPayload)> {
        let distances = graph.bfs_distances(source_id);
        let mut targets = graph
            .edges
            .keys()
            .flat_map(|(from, to)| [from.clone(), to.clone()])
            .filter(|npc_id| npc_id != source_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|target_npc| {
                let distance = distances.get(&target_npc).copied()?;
                let trust = graph
                    .get_edge(source_id, &target_npc)
                    .map(|edge| edge.trust)
                    .unwrap_or(0);
                Some((target_npc, trust, distance))
            })
            .collect::<Vec<_>>();
        targets.sort_by(|a, b| a.2.cmp(&b.2).then(b.1.cmp(&a.1)).then(a.0.cmp(&b.0)));

        let mut out = Vec::new();
        for (target_npc, trust_i64, distance) in
            targets.into_iter().take(Self::MAX_PROPAGATION_FANOUT)
        {
            let trust = trust_i64.max(0) as u32;
            let hop = rumor.hop_count + distance;
            let mut details = rumor.details.clone();
            if self.should_distort(&rumor.rumor_id, hop, trust) {
                details = format!("{} (distorted)", rumor.details);
            }
            out.push((
                target_npc,
                RumorPayload {
                    rumor_id: rumor.rumor_id.clone(),
                    source_npc_id: source_id.to_string(),
                    core_claim: rumor.core_claim.clone(),
                    details,
                    hop_count: hop,
                },
            ));
        }
        out
    }

    pub fn credibility_claim(
        &self,
        npc_id: &str,
        rumor: &RumorPayload,
        source_trust: i64,
        tick: u64,
    ) -> BeliefClaim {
        let confidence = (35 + source_trust / 2 - rumor.hop_count as i64 * 5).clamp(0, 100);
        BeliefClaim {
            claim_id: format!("belief:{}:{}", npc_id, rumor.rumor_id),
            npc_id: npc_id.to_string(),
            topic: format!("rumor:{}", rumor.rumor_id),
            content: rumor.details.clone(),
            confidence,
            source: if source_trust >= 50 {
                BeliefSource::TrustedRumor
            } else {
                BeliefSource::UntrustedRumor
            },
            last_updated_tick: tick,
            uncertain: confidence < 25,
        }
    }

    fn should_distort(&self, rumor_id: &str, hop: u32, trust: u32) -> bool {
        // Deterministic pseudo-random decision from identifiers.
        let mut acc = 0_u64;
        for byte in rumor_id.as_bytes() {
            acc = acc.wrapping_mul(131).wrapping_add(u64::from(*byte));
        }
        acc = acc.wrapping_add(u64::from(hop) * 97);
        acc = acc.wrapping_add(u64::from(100_u32.saturating_sub(trust)) * 53);
        let hop_bps = (u64::from(hop) * 700).min(8_000);
        let trust_bps = u64::from(100_u32.saturating_sub(trust)) * 50;
        let threshold_bps = (u64::from(self.distortion_bps) + hop_bps + trust_bps).min(10_000);
        (acc % 10_000) < threshold_bps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn social_edge_updates_on_action() {
        let mut graph = SocialGraph::default();
        graph.update_edge(
            "npc:a",
            "npc:b",
            EdgeUpdate {
                trust_delta: -10,
                grievance_delta: 15,
                ..EdgeUpdate::default()
            },
        );
        let edge = graph.get_edge("npc:a", "npc:b").expect("edge exists");
        assert_eq!(edge.trust, -10);
        assert_eq!(edge.grievance, 15);
    }

    #[test]
    fn rumor_distortion_tends_to_increase_with_hops() {
        let network = RumorNetwork::default();
        let rumor = RumorPayload {
            rumor_id: "r1".to_string(),
            source_npc_id: "npc:a".to_string(),
            core_claim: "market burned".to_string(),
            details: "market burned at dawn".to_string(),
            hop_count: 1,
        };
        let low = network.credibility_claim("npc:b", &rumor, 80, 1).confidence;
        let high = network
            .credibility_claim(
                "npc:c",
                &RumorPayload {
                    hop_count: 5,
                    ..rumor
                },
                80,
                1,
            )
            .confidence;
        assert!(high < low);
    }

    #[test]
    fn rumor_credibility_respects_source_trust() {
        let network = RumorNetwork::default();
        let rumor = RumorPayload {
            rumor_id: "r2".to_string(),
            source_npc_id: "npc:a".to_string(),
            core_claim: "guard bribed".to_string(),
            details: "captain took coin".to_string(),
            hop_count: 1,
        };
        let high = network.credibility_claim("npc:b", &rumor, 80, 1).confidence;
        let low = network.credibility_claim("npc:b", &rumor, 10, 1).confidence;
        assert!(high > low);
    }

    #[test]
    fn rumor_propagation_prioritizes_distance_then_trust() {
        let mut graph = SocialGraph::default();
        graph.update_edge(
            "npc:a",
            "npc:b",
            EdgeUpdate {
                trust_delta: 90,
                ..EdgeUpdate::default()
            },
        );
        graph.update_edge(
            "npc:b",
            "npc:c",
            EdgeUpdate {
                trust_delta: 80,
                ..EdgeUpdate::default()
            },
        );
        graph.update_edge(
            "npc:a",
            "npc:d",
            EdgeUpdate {
                trust_delta: 10,
                ..EdgeUpdate::default()
            },
        );

        let network = RumorNetwork::default();
        let rumor = RumorPayload {
            rumor_id: "r3".to_string(),
            source_npc_id: "npc:a".to_string(),
            core_claim: "claim".to_string(),
            details: "details".to_string(),
            hop_count: 0,
        };
        let routed = network.propagate(&rumor, "npc:a", &graph);
        assert!(!routed.is_empty());
        // "npc:b" and "npc:d" are both 1 hop from source; higher trust wins tie.
        let first = routed
            .first()
            .map(|(npc, _)| npc.as_str())
            .unwrap_or_default();
        assert_eq!(first, "npc:b");
    }
}
