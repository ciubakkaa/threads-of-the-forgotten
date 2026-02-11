//! Observation bus and perception system for the NPC agency architecture.
//!
//! The `ObservationBus` distributes world events to nearby agents by location.
//! The `PerceptionSystem` filters event details based on agent capabilities,
//! integrates with `MemorySystem` to store perception records, and supports
//! route-limited perception for traveling agents.

use contracts::agency::{CapabilitySet, Observation};
use serde_json::Value;

use crate::memory::MemorySystem;

// ---------------------------------------------------------------------------
// ObservationBus
// ---------------------------------------------------------------------------

/// Distributes world events to agents by location.
///
/// Events are broadcast into a pending queue and drained per-agent based on
/// the agent's current location. Agents only receive observations matching
/// their location (Requirement 2.1, 9.1).
#[derive(Debug, Clone, Default)]
pub struct ObservationBus {
    pending: Vec<Observation>,
}

impl ObservationBus {
    /// Create a new, empty observation bus.
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// Broadcast an observation into the pending queue.
    pub fn broadcast(&mut self, observation: Observation) {
        self.pending.push(observation);
    }

    /// Drain and return all observations matching the agent's location.
    ///
    /// Observations at the given `location_id` are removed from the pending
    /// queue and returned. Observations at other locations remain pending.
    pub fn drain_for_agent(&mut self, _agent_id: &str, location_id: &str) -> Vec<Observation> {
        let mut matched = Vec::new();
        let mut remaining = Vec::new();

        for obs in self.pending.drain(..) {
            if obs.location_id == location_id {
                matched.push(obs);
            } else {
                remaining.push(obs);
            }
        }

        self.pending = remaining;
        matched
    }

    /// Number of pending observations.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Whether the bus has no pending observations.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

// ---------------------------------------------------------------------------
// PerceptionSystem
// ---------------------------------------------------------------------------

/// Minimum visibility threshold — events below this are not perceived.
const MIN_VISIBILITY: i64 = 10;

/// Filters event details based on agent capabilities and stores perception
/// records in the agent's memory system.
///
/// Perception filtering ensures that perceived details are a subset of actual
/// event details (Requirement 2.2). The system also integrates with
/// `MemorySystem` to store perception records on receipt (Requirement 2.3)
/// and supports route-limited perception for traveling agents (Requirement 7.4).
pub struct PerceptionSystem;

impl PerceptionSystem {
    /// Filter an observation based on agent capabilities, producing a
    /// perceived observation whose details are a subset of the original.
    ///
    /// The filtering considers:
    /// - Visibility vs. agent perception ability (physical + social capabilities)
    /// - Low-capability agents lose some detail fields from the event
    ///
    /// Returns `None` if the event is below the agent's perception threshold.
    pub fn filter_observation(
        observation: &Observation,
        capabilities: &CapabilitySet,
    ) -> Option<Observation> {
        // Perception ability is the average of physical and social capabilities,
        // representing general awareness.
        let perception_ability = (capabilities.physical + capabilities.social) / 2;

        // Events below minimum visibility are never perceived.
        if observation.visibility < MIN_VISIBILITY {
            return None;
        }

        // If the agent's perception ability is below the event's visibility,
        // they can't perceive it at all.
        if perception_ability < observation.visibility {
            return None;
        }

        // Build filtered details — higher capability retains more detail.
        let filtered_details = Self::filter_details(&observation.details, perception_ability);

        Some(Observation {
            event_id: observation.event_id.clone(),
            tick: observation.tick,
            location_id: observation.location_id.clone(),
            event_type: observation.event_type.clone(),
            actors: observation.actors.clone(),
            visibility: observation.visibility,
            details: filtered_details,
        })
    }

    /// Process observations for an agent: filter each one and store perceived
    /// events in the agent's memory system.
    ///
    /// Returns the filtered observations that were successfully perceived.
    pub fn perceive_and_remember(
        observations: &[Observation],
        capabilities: &CapabilitySet,
        memory: &mut MemorySystem,
    ) -> Vec<Observation> {
        let mut perceived = Vec::new();

        for obs in observations {
            if let Some(filtered) = Self::filter_observation(obs, capabilities) {
                memory.store_perception(&filtered);
                perceived.push(filtered);
            }
        }

        perceived
    }

    /// Filter observations for a traveling agent on a specific route.
    ///
    /// A traveling agent can only perceive events at the route's origin,
    /// destination, or along the route itself. Events at other locations
    /// are invisible to the traveler (Requirement 7.4).
    pub fn filter_for_traveler(
        observations: &[Observation],
        route_origin: &str,
        route_destination: &str,
        route_id: &str,
    ) -> Vec<Observation> {
        observations
            .iter()
            .filter(|obs| {
                obs.location_id == route_origin
                    || obs.location_id == route_destination
                    || obs.location_id == route_id
            })
            .cloned()
            .collect()
    }

    /// Filter detail fields based on perception ability.
    ///
    /// High-capability agents (>= 70) see all details.
    /// Medium-capability agents (>= 40) see top-level string/number fields only.
    /// Low-capability agents (< 40) see only a minimal summary.
    fn filter_details(details: &Value, perception_ability: i64) -> Value {
        if perception_ability >= 70 {
            // Full details
            details.clone()
        } else if perception_ability >= 40 {
            // Medium: keep only top-level primitive fields from objects
            match details {
                Value::Object(map) => {
                    let filtered: serde_json::Map<String, Value> = map
                        .iter()
                        .filter(|(_, v)| v.is_string() || v.is_number() || v.is_boolean())
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    Value::Object(filtered)
                }
                other => other.clone(),
            }
        } else {
            // Low: minimal — just indicate something happened
            Value::String("something happened".to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_observation(event_id: &str, location: &str, visibility: i64) -> Observation {
        Observation {
            event_id: event_id.to_string(),
            tick: 1,
            location_id: location.to_string(),
            event_type: "test_event".to_string(),
            actors: vec!["npc_1".to_string()],
            visibility,
            details: json!({"action": "steal", "target": "bread", "amount": 3}),
        }
    }

    fn default_capabilities() -> CapabilitySet {
        CapabilitySet {
            physical: 50,
            social: 50,
            trade: 30,
            combat: 20,
            literacy: 40,
            influence: 30,
            stealth: 20,
            care: 30,
            law: 10,
        }
    }

    // --- ObservationBus tests ---

    #[test]
    fn broadcast_adds_to_pending() {
        let mut bus = ObservationBus::new();
        assert!(bus.is_empty());

        bus.broadcast(make_observation("e1", "town_square", 50));
        assert_eq!(bus.pending_count(), 1);
    }

    #[test]
    fn drain_returns_matching_location_only() {
        let mut bus = ObservationBus::new();
        bus.broadcast(make_observation("e1", "town_square", 50));
        bus.broadcast(make_observation("e2", "market", 50));
        bus.broadcast(make_observation("e3", "town_square", 50));

        let drained = bus.drain_for_agent("npc_1", "town_square");
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].event_id, "e1");
        assert_eq!(drained[1].event_id, "e3");

        // market observation remains
        assert_eq!(bus.pending_count(), 1);
    }

    #[test]
    fn drain_removes_matched_observations() {
        let mut bus = ObservationBus::new();
        bus.broadcast(make_observation("e1", "town_square", 50));
        bus.broadcast(make_observation("e2", "town_square", 50));

        let drained = bus.drain_for_agent("npc_1", "town_square");
        assert_eq!(drained.len(), 2);
        assert!(bus.is_empty());
    }

    #[test]
    fn drain_no_match_returns_empty() {
        let mut bus = ObservationBus::new();
        bus.broadcast(make_observation("e1", "market", 50));

        let drained = bus.drain_for_agent("npc_1", "town_square");
        assert!(drained.is_empty());
        assert_eq!(bus.pending_count(), 1);
    }

    // --- PerceptionSystem tests ---

    #[test]
    fn filter_observation_below_min_visibility_returns_none() {
        let obs = make_observation("e1", "town_square", 5); // below MIN_VISIBILITY
        let caps = default_capabilities();
        assert!(PerceptionSystem::filter_observation(&obs, &caps).is_none());
    }

    #[test]
    fn filter_observation_above_capability_returns_none() {
        let obs = make_observation("e1", "town_square", 80); // visibility > capability avg (50)
        let caps = default_capabilities();
        assert!(PerceptionSystem::filter_observation(&obs, &caps).is_none());
    }

    #[test]
    fn filter_observation_within_capability_returns_filtered() {
        let obs = make_observation("e1", "town_square", 40);
        let caps = default_capabilities(); // avg = 50, >= 40
        let filtered = PerceptionSystem::filter_observation(&obs, &caps).unwrap();
        assert_eq!(filtered.event_id, "e1");
        assert_eq!(filtered.location_id, "town_square");
    }

    #[test]
    fn high_capability_sees_full_details() {
        let obs = make_observation("e1", "town_square", 30);
        let caps = CapabilitySet {
            physical: 80,
            social: 80,
            ..default_capabilities()
        };
        let filtered = PerceptionSystem::filter_observation(&obs, &caps).unwrap();
        // Full details preserved
        assert_eq!(filtered.details["amount"], 3);
        assert_eq!(filtered.details["target"], "bread");
    }

    #[test]
    fn medium_capability_sees_primitive_fields_only() {
        let obs = Observation {
            event_id: "e1".to_string(),
            tick: 1,
            location_id: "town_square".to_string(),
            event_type: "test".to_string(),
            actors: vec![],
            visibility: 30,
            details: json!({"action": "steal", "nested": {"deep": true}, "count": 5}),
        };
        let caps = CapabilitySet {
            physical: 50,
            social: 50,
            ..default_capabilities()
        }; // avg = 50, medium tier
        let filtered = PerceptionSystem::filter_observation(&obs, &caps).unwrap();
        // Nested object should be stripped
        assert!(filtered.details.get("nested").is_none());
        // Primitives kept
        assert_eq!(filtered.details["action"], "steal");
        assert_eq!(filtered.details["count"], 5);
    }

    #[test]
    fn low_capability_sees_minimal_details() {
        let obs = make_observation("e1", "town_square", 20);
        let caps = CapabilitySet {
            physical: 30,
            social: 30,
            ..default_capabilities()
        }; // avg = 30, low tier
        let filtered = PerceptionSystem::filter_observation(&obs, &caps).unwrap();
        assert_eq!(filtered.details, json!("something happened"));
    }

    #[test]
    fn perceive_and_remember_stores_in_memory() {
        let observations = vec![
            make_observation("e1", "town_square", 40),
            make_observation("e2", "town_square", 40),
        ];
        let caps = default_capabilities();
        let mut memory = MemorySystem::new(100);

        let perceived = PerceptionSystem::perceive_and_remember(&observations, &caps, &mut memory);
        assert_eq!(perceived.len(), 2);
        assert_eq!(memory.len(), 2);
    }

    #[test]
    fn perceive_and_remember_skips_imperceptible() {
        let observations = vec![
            make_observation("e1", "town_square", 40), // perceivable
            make_observation("e2", "town_square", 90), // too high visibility
        ];
        let caps = default_capabilities();
        let mut memory = MemorySystem::new(100);

        let perceived = PerceptionSystem::perceive_and_remember(&observations, &caps, &mut memory);
        assert_eq!(perceived.len(), 1);
        assert_eq!(memory.len(), 1);
    }

    #[test]
    fn filter_for_traveler_limits_to_route() {
        let observations = vec![
            make_observation("e1", "town_a", 50),
            make_observation("e2", "town_b", 50),
            make_observation("e3", "route_ab", 50),
            make_observation("e4", "town_c", 50), // not on route
        ];

        let filtered =
            PerceptionSystem::filter_for_traveler(&observations, "town_a", "town_b", "route_ab");
        assert_eq!(filtered.len(), 3);
        let ids: Vec<&str> = filtered.iter().map(|o| o.event_id.as_str()).collect();
        assert!(ids.contains(&"e1"));
        assert!(ids.contains(&"e2"));
        assert!(ids.contains(&"e3"));
        assert!(!ids.contains(&"e4"));
    }
}
