use contracts::{BeliefClaim, BeliefSource, MemoryEntry, Observation, RumorPayload};

#[derive(Debug, Clone)]
pub struct MemorySystem {
    pub memories: Vec<MemoryEntry>,
    pub max_capacity: usize,
}

impl MemorySystem {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            memories: Vec::new(),
            max_capacity: max_capacity.max(1),
        }
    }

    pub fn store_perception(&mut self, perception: &Observation) {
        let entry = MemoryEntry {
            memory_id: format!("mem:{}:{}", perception.event_id, perception.tick),
            tick: perception.tick,
            topic: format!("event:{}", perception.event_type as u32),
            details: perception.details.clone(),
            confidence: 80,
            salience: perception.visibility.clamp(0, 100),
            source: BeliefSource::Witnessed,
        };
        self.memories.push(entry);
        self.enforce_capacity();
    }

    pub fn decay(&mut self, current_tick: u64) {
        for memory in &mut self.memories {
            let age = current_tick.saturating_sub(memory.tick) as i64;
            let decay = 1 + (age / 24) + ((100 - memory.salience).max(0) / 25);
            memory.confidence = (memory.confidence - decay).clamp(0, 100);
        }
        self.memories.retain(|memory| memory.confidence > 0);
    }

    fn enforce_capacity(&mut self) {
        if self.memories.len() <= self.max_capacity {
            return;
        }

        self.memories.sort_by(|a, b| {
            a.salience
                .cmp(&b.salience)
                .then(a.tick.cmp(&b.tick))
                .then(a.confidence.cmp(&b.confidence))
        });

        let over = self.memories.len() - self.max_capacity;
        self.memories.drain(0..over);
    }
}

#[derive(Debug, Clone)]
pub struct BeliefModel {
    pub claims: Vec<BeliefClaim>,
    pub uncertainty_threshold: i64,
}

impl BeliefModel {
    pub fn new(uncertainty_threshold: i64) -> Self {
        Self {
            claims: Vec::new(),
            uncertainty_threshold,
        }
    }

    pub fn update_from_perception(&mut self, perception: &Observation, npc_id: &str, trust: i64) {
        let topic = format!("event:{}", perception.event_id);
        let content = perception.details.to_string();
        self.upsert_claim(
            npc_id,
            topic,
            content,
            perception.tick,
            BeliefSource::Witnessed,
            trust,
        );
    }

    pub fn update_from_rumor(
        &mut self,
        rumor: &RumorPayload,
        npc_id: &str,
        tick: u64,
        source_trust: i64,
    ) {
        self.upsert_claim(
            npc_id,
            format!("rumor:{}", rumor.rumor_id),
            rumor.details.clone(),
            tick,
            if source_trust >= 50 {
                BeliefSource::TrustedRumor
            } else {
                BeliefSource::UntrustedRumor
            },
            source_trust,
        );
    }

    pub fn decay(&mut self, tick: u64) {
        for claim in &mut self.claims {
            let age = tick.saturating_sub(claim.last_updated_tick) as i64;
            let decay = 1 + (age / 48);
            claim.confidence = (claim.confidence - decay).clamp(0, 100);
            claim.uncertain = claim.confidence < self.uncertainty_threshold;
        }
    }

    pub fn get_belief(&self, topic: &str) -> Option<&BeliefClaim> {
        self.claims.iter().find(|claim| claim.topic == topic)
    }

    fn upsert_claim(
        &mut self,
        npc_id: &str,
        topic: String,
        content: String,
        tick: u64,
        source: BeliefSource,
        trust: i64,
    ) {
        let influence = (trust / 10).clamp(-5, 10);
        if let Some(existing) = self.claims.iter_mut().find(|claim| claim.topic == topic) {
            if existing.content == content {
                existing.confidence = (existing.confidence + 5 + influence).clamp(0, 100);
            } else {
                existing.confidence = (existing.confidence - 7 + influence).clamp(0, 100);
                if existing.confidence <= self.uncertainty_threshold {
                    existing.content = content;
                }
            }
            existing.source = source;
            existing.last_updated_tick = tick;
            existing.uncertain = existing.confidence < self.uncertainty_threshold;
            return;
        }

        let confidence = (55 + influence).clamp(1, 100);
        self.claims.push(BeliefClaim {
            claim_id: format!("belief:{}:{}", npc_id, topic),
            npc_id: npc_id.to_string(),
            topic,
            content,
            confidence,
            source,
            last_updated_tick: tick,
            uncertain: confidence < self.uncertainty_threshold,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::EventType;
    use serde_json::json;

    fn observation(tick: u64) -> Observation {
        Observation {
            event_id: format!("evt_{tick}"),
            tick,
            location_id: "loc:a".to_string(),
            event_type: EventType::ObservationLogged,
            actors: vec!["npc:1".to_string()],
            visibility: 80,
            details: json!({ "claim": "guards arrived" }),
        }
    }

    #[test]
    fn memory_is_created_on_perception() {
        let mut memory = MemorySystem::new(8);
        memory.store_perception(&observation(2));
        assert_eq!(memory.memories.len(), 1);
        assert!(memory.memories[0].confidence > 0);
    }

    #[test]
    fn memory_confidence_decays() {
        let mut memory = MemorySystem::new(8);
        memory.store_perception(&observation(1));
        let baseline = memory.memories[0].confidence;
        memory.decay(100);
        assert!(memory.memories[0].confidence < baseline);
    }

    #[test]
    fn belief_updates_from_confirming_and_contradicting_info() {
        let mut model = BeliefModel::new(25);
        let obs = observation(1);
        model.update_from_perception(&obs, "npc:1", 80);
        let start_conf = model
            .get_belief("event:evt_1")
            .expect("belief exists")
            .confidence;

        model.update_from_perception(&obs, "npc:1", 80);
        let confirm_conf = model
            .get_belief("event:evt_1")
            .expect("belief exists")
            .confidence;
        assert!(confirm_conf > start_conf);

        let mut contradicted = obs.clone();
        contradicted.details = json!({ "claim": "no guards" });
        model.update_from_perception(&contradicted, "npc:1", 20);
        let end_conf = model
            .get_belief("event:evt_1")
            .expect("belief exists")
            .confidence;
        assert!(end_conf <= confirm_conf);
    }
}
