use contracts::Observation;

use crate::memory::MemorySystem;

#[derive(Debug, Clone, Default)]
pub struct ObservationBus {
    pub pending: Vec<Observation>,
}

impl ObservationBus {
    pub fn broadcast(&mut self, observation: Observation) {
        self.pending.push(observation);
    }

    pub fn drain_for_agent(&mut self, location_id: &str) -> Vec<Observation> {
        let mut out = Vec::new();
        let mut kept = Vec::new();
        for observation in self.pending.drain(..) {
            if observation.location_id == location_id {
                out.push(observation);
            } else {
                kept.push(observation);
            }
        }
        self.pending = kept;
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct PerceptionContext {
    pub attention: i64,
    pub proximity: i64,
    pub sensory: i64,
}

#[derive(Debug, Clone, Default)]
pub struct PerceptionSystem;

impl PerceptionSystem {
    pub fn filter_observation(
        &self,
        observation: &Observation,
        context: &PerceptionContext,
    ) -> Observation {
        let mut filtered = observation.clone();
        let confidence =
            ((context.attention + context.proximity + context.sensory) / 3).clamp(0, 100);
        filtered.visibility = filtered.visibility.min(confidence);
        filtered
    }

    pub fn ingest(
        &self,
        memory: &mut MemorySystem,
        observation: &Observation,
        context: &PerceptionContext,
    ) -> Observation {
        let filtered = self.filter_observation(observation, context);
        memory.store_perception(&filtered);
        filtered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::EventType;
    use serde_json::json;

    #[test]
    fn observation_bus_delivers_by_location() {
        let mut bus = ObservationBus::default();
        bus.broadcast(Observation {
            event_id: "e1".to_string(),
            tick: 1,
            location_id: "loc:a".to_string(),
            event_type: EventType::ObservationLogged,
            actors: vec![],
            visibility: 80,
            details: json!({}),
        });

        let a = bus.drain_for_agent("loc:a");
        let b = bus.drain_for_agent("loc:b");
        assert_eq!(a.len(), 1);
        assert!(b.is_empty());
    }

    #[test]
    fn perception_filter_reduces_detail_visibility() {
        let system = PerceptionSystem;
        let observation = Observation {
            event_id: "e1".to_string(),
            tick: 1,
            location_id: "loc:a".to_string(),
            event_type: EventType::ObservationLogged,
            actors: vec![],
            visibility: 90,
            details: json!({"a": 1, "b": 2}),
        };
        let filtered = system.filter_observation(
            &observation,
            &PerceptionContext {
                attention: 20,
                proximity: 20,
                sensory: 20,
            },
        );
        assert!(filtered.visibility <= observation.visibility);
    }
}
