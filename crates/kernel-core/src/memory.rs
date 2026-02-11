//! Memory and belief systems: stores NPC observations with confidence decay,
//! and maintains belief claims that update from perception and rumor.

use contracts::agency::{
    AgencyBeliefSource, BeliefClaim, MemoryDetails, MemoryEntry, MemorySource, Observation,
    RumorPayload,
};

// ---------------------------------------------------------------------------
// MemorySystem
// ---------------------------------------------------------------------------

/// Capacity-bounded memory store with confidence decay.
///
/// Memories are created from perceptions and decay over time. When capacity
/// is exceeded, the oldest lowest-salience memories are evicted first.
#[derive(Debug, Clone)]
pub struct MemorySystem {
    memories: Vec<MemoryEntry>,
    max_capacity: usize,
    next_id: u64,
}

/// Default confidence assigned to a freshly perceived memory.
const INITIAL_CONFIDENCE: i64 = 80;
/// Default salience for a perception-based memory.
const DEFAULT_SALIENCE: i64 = 50;
/// Per-tick base confidence decay.
const BASE_DECAY_PER_TICK: i64 = 1;
/// Extra decay per 10 ticks of age (older memories decay faster).
const AGE_DECAY_FACTOR: i64 = 1;
/// Ticks per age bracket for accelerated decay.
const AGE_BRACKET_TICKS: u64 = 10;

impl MemorySystem {
    /// Create a new memory system with the given capacity.
    pub fn new(max_capacity: usize) -> Self {
        Self {
            memories: Vec::new(),
            max_capacity,
            next_id: 1,
        }
    }

    /// Store a perception as a new memory entry.
    ///
    /// Creates a memory with perceived details, timestamp, and initial confidence > 0.
    /// If capacity is exceeded after insertion, evicts the oldest lowest-salience memory.
    pub fn store_perception(&mut self, perception: &Observation) {
        let entry = MemoryEntry {
            memory_id: format!("mem_{}", self.next_id),
            tick: perception.tick,
            topic: perception.event_type.clone(),
            details: MemoryDetails {
                description: perception
                    .details
                    .as_str()
                    .unwrap_or("observed event")
                    .to_string(),
                actors: perception.actors.clone(),
                location_id: perception.location_id.clone(),
                extra: Some(perception.details.clone()),
            },
            confidence: INITIAL_CONFIDENCE,
            salience: DEFAULT_SALIENCE,
            source: MemorySource::DirectObservation,
        };
        self.next_id += 1;
        self.memories.push(entry);
        self.evict_if_over_capacity();
    }

    /// Decay confidence of all memories. Older memories decay faster.
    pub fn decay(&mut self, current_tick: u64) {
        for mem in &mut self.memories {
            let age = current_tick.saturating_sub(mem.tick);
            let age_bonus = (age / AGE_BRACKET_TICKS) as i64 * AGE_DECAY_FACTOR;
            let total_decay = BASE_DECAY_PER_TICK + age_bonus;
            mem.confidence = (mem.confidence - total_decay).max(0);
        }
    }

    /// Evict oldest, lowest-salience memories when over capacity.
    fn evict_if_over_capacity(&mut self) {
        while self.memories.len() > self.max_capacity {
            // Find the index of the memory with lowest salience, breaking ties by oldest tick.
            let evict_idx = self
                .memories
                .iter()
                .enumerate()
                .min_by_key(|(_, m)| (m.salience, m.tick))
                .map(|(i, _)| i)
                .expect("memories is non-empty");
            self.memories.remove(evict_idx);
        }
    }

    /// Read-only access to stored memories.
    pub fn memories(&self) -> &[MemoryEntry] {
        &self.memories
    }

    /// Number of stored memories.
    pub fn len(&self) -> usize {
        self.memories.len()
    }

    /// Whether the memory store is empty.
    pub fn is_empty(&self) -> bool {
        self.memories.is_empty()
    }
}


// ---------------------------------------------------------------------------
// BeliefModel
// ---------------------------------------------------------------------------

/// Default confidence threshold below which a belief is marked uncertain.
const UNCERTAINTY_THRESHOLD: i64 = 30;
/// Per-tick base belief confidence decay.
const BELIEF_DECAY_PER_TICK: i64 = 1;
/// Confidence boost when new info confirms an existing belief.
const CONFIRM_BOOST: i64 = 15;
/// Confidence penalty when new info contradicts an existing belief.
const CONTRADICT_PENALTY: i64 = 20;

/// Maintains an NPC's set of belief claims, each with confidence and source.
///
/// Beliefs are updated from direct perception or rumor, decay over time,
/// and are marked uncertain when confidence drops below a threshold.
#[derive(Debug, Clone)]
pub struct BeliefModel {
    claims: Vec<BeliefClaim>,
    next_id: u64,
    /// Confidence threshold below which beliefs are marked uncertain.
    pub uncertainty_threshold: i64,
}

impl BeliefModel {
    pub fn new() -> Self {
        Self {
            claims: Vec::new(),
            next_id: 1,
            uncertainty_threshold: UNCERTAINTY_THRESHOLD,
        }
    }

    /// Update beliefs from a direct perception.
    ///
    /// If a belief on the same topic already exists:
    /// - Confirming info (same content) increases confidence.
    /// - Contradicting info (different content) decreases existing confidence
    ///   and creates a new belief with the new info.
    ///
    /// `trust` is the observer's self-trust (typically high, e.g. 100 for direct observation).
    pub fn update_from_perception(&mut self, perception: &Observation, trust: i64) {
        let topic = &perception.event_type;
        let content = perception
            .details
            .as_str()
            .unwrap_or("observed")
            .to_string();
        let source = AgencyBeliefSource::DirectPerception {
            observer_id: perception
                .actors
                .first()
                .cloned()
                .unwrap_or_default(),
        };
        self.integrate_info(topic, &content, trust, source, perception.tick);
    }

    /// Update beliefs from a rumor, weighted by source trust.
    ///
    /// Lower source trust produces smaller confidence changes.
    pub fn update_from_rumor(&mut self, rumor: &RumorPayload, source_trust: i64) {
        let source = AgencyBeliefSource::Rumor {
            source_npc_id: rumor.origin_npc_id.clone(),
            trust: source_trust,
        };
        // Rumor tick is not carried in the payload; use 0 as fallback.
        // In practice the caller should set last_updated_tick after.
        self.integrate_info(&rumor.topic, &rumor.content, source_trust, source, 0);
    }

    /// Decay all belief confidences over time.
    pub fn decay(&mut self, _tick: u64) {
        for claim in &mut self.claims {
            claim.confidence = (claim.confidence - BELIEF_DECAY_PER_TICK).max(0);
        }
    }

    /// Get a belief by topic.
    pub fn get_belief(&self, topic: &str) -> Option<&BeliefClaim> {
        self.claims.iter().find(|c| c.topic == topic)
    }

    /// Read-only access to all claims.
    pub fn claims(&self) -> &[BeliefClaim] {
        &self.claims
    }

    /// Whether a belief is uncertain (below threshold).
    pub fn is_uncertain(&self, topic: &str) -> bool {
        self.get_belief(topic)
            .map(|c| c.confidence < self.uncertainty_threshold)
            .unwrap_or(true)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Core integration logic shared by perception and rumor updates.
    fn integrate_info(
        &mut self,
        topic: &str,
        content: &str,
        trust: i64,
        source: AgencyBeliefSource,
        tick: u64,
    ) {
        // Trust-weighted scaling: trust is expected in [0, 100].
        let trust_scale = trust.clamp(0, 100);

        if let Some(existing) = self.claims.iter_mut().find(|c| c.topic == topic) {
            if existing.content == content {
                // Confirming — boost confidence proportional to trust.
                let boost = (CONFIRM_BOOST * trust_scale) / 100;
                existing.confidence = (existing.confidence + boost.max(1)).min(100);
            } else {
                // Contradicting — reduce existing confidence proportional to trust.
                let penalty = (CONTRADICT_PENALTY * trust_scale) / 100;
                existing.confidence = (existing.confidence - penalty.max(1)).max(0);
            }
            existing.last_updated_tick = tick;
            existing.source = source;
        } else {
            // New belief — initial confidence proportional to trust.
            let initial_conf = (60 * trust_scale / 100).max(1);
            let claim = BeliefClaim {
                claim_id: format!("belief_{}", self.next_id),
                topic: topic.to_string(),
                content: content.to_string(),
                confidence: initial_conf,
                source,
                last_updated_tick: tick,
            };
            self.next_id += 1;
            self.claims.push(claim);
        }
    }
}

impl Default for BeliefModel {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_observation(tick: u64, event_type: &str, location: &str) -> Observation {
        Observation {
            event_id: format!("evt_{tick}"),
            tick,
            location_id: location.to_string(),
            event_type: event_type.to_string(),
            actors: vec!["npc_1".to_string()],
            visibility: 100,
            details: json!("something happened"),
        }
    }

    // -- MemorySystem tests --

    #[test]
    fn store_perception_creates_memory_with_positive_confidence() {
        let mut mem = MemorySystem::new(10);
        let obs = make_observation(5, "theft", "market");
        mem.store_perception(&obs);
        assert_eq!(mem.len(), 1);
        let entry = &mem.memories()[0];
        assert!(entry.confidence > 0);
        assert_eq!(entry.tick, 5);
        assert_eq!(entry.topic, "theft");
    }

    #[test]
    fn decay_reduces_confidence_older_faster() {
        let mut mem = MemorySystem::new(10);
        let old_obs = make_observation(0, "old_event", "loc");
        let new_obs = make_observation(20, "new_event", "loc");
        mem.store_perception(&old_obs);
        mem.store_perception(&new_obs);

        mem.decay(25);

        let old_conf = mem.memories().iter().find(|m| m.topic == "old_event").unwrap().confidence;
        let new_conf = mem.memories().iter().find(|m| m.topic == "new_event").unwrap().confidence;
        // Old memory should have decayed more.
        assert!(old_conf < new_conf, "old={old_conf} should be < new={new_conf}");
    }

    #[test]
    fn capacity_eviction_removes_lowest_salience() {
        let mut mem = MemorySystem::new(2);
        mem.store_perception(&make_observation(1, "a", "loc"));
        mem.store_perception(&make_observation(2, "b", "loc"));
        mem.store_perception(&make_observation(3, "c", "loc"));
        assert_eq!(mem.len(), 2);
        // The oldest lowest-salience entry should have been evicted.
        // All have same salience, so oldest (tick=1) is evicted.
        let topics: Vec<&str> = mem.memories().iter().map(|m| m.topic.as_str()).collect();
        assert!(!topics.contains(&"a"), "oldest should be evicted");
    }

    #[test]
    fn confidence_never_goes_below_zero() {
        let mut mem = MemorySystem::new(10);
        mem.store_perception(&make_observation(0, "evt", "loc"));
        // Decay many times.
        for tick in 1..200 {
            mem.decay(tick);
        }
        assert!(mem.memories()[0].confidence >= 0);
    }

    // -- BeliefModel tests --

    #[test]
    fn perception_creates_new_belief() {
        let mut bm = BeliefModel::new();
        let obs = make_observation(10, "market_open", "market");
        bm.update_from_perception(&obs, 100);
        assert_eq!(bm.claims().len(), 1);
        assert!(bm.claims()[0].confidence > 0);
    }

    #[test]
    fn confirming_info_increases_confidence() {
        let mut bm = BeliefModel::new();
        let obs = make_observation(10, "market_open", "market");
        bm.update_from_perception(&obs, 100);
        let before = bm.claims()[0].confidence;
        bm.update_from_perception(&obs, 100);
        let after = bm.claims()[0].confidence;
        assert!(after > before, "confirming should increase: {before} -> {after}");
    }

    #[test]
    fn contradicting_info_decreases_confidence() {
        let mut bm = BeliefModel::new();
        let obs1 = make_observation(10, "weather", "town");
        bm.update_from_perception(&obs1, 100);
        let before = bm.claims()[0].confidence;

        // Contradicting observation (same topic, different details).
        let mut obs2 = make_observation(11, "weather", "town");
        obs2.details = json!("different info");
        bm.update_from_perception(&obs2, 100);
        let after = bm.claims()[0].confidence;
        assert!(after < before, "contradicting should decrease: {before} -> {after}");
    }

    #[test]
    fn belief_decay_reduces_confidence() {
        let mut bm = BeliefModel::new();
        bm.update_from_perception(&make_observation(1, "fact", "loc"), 100);
        let before = bm.claims()[0].confidence;
        bm.decay(2);
        let after = bm.claims()[0].confidence;
        assert!(after < before);
    }

    #[test]
    fn low_confidence_belief_is_uncertain() {
        let mut bm = BeliefModel::new();
        bm.update_from_perception(&make_observation(1, "fact", "loc"), 100);
        assert!(!bm.is_uncertain("fact"));
        // Decay until below threshold.
        for t in 2..200 {
            bm.decay(t);
        }
        assert!(bm.is_uncertain("fact"));
    }

    #[test]
    fn rumor_update_weighted_by_trust() {
        let mut bm = BeliefModel::new();
        let rumor = RumorPayload {
            rumor_id: "r1".into(),
            origin_npc_id: "npc_2".into(),
            topic: "scandal".into(),
            content: "mayor is corrupt".into(),
            hops: 1,
            distorted: false,
        };
        bm.update_from_rumor(&rumor, 80);
        let high_trust_conf = bm.claims()[0].confidence;

        let mut bm2 = BeliefModel::new();
        bm2.update_from_rumor(&rumor, 20);
        let low_trust_conf = bm2.claims()[0].confidence;

        assert!(
            high_trust_conf > low_trust_conf,
            "higher trust should produce higher confidence: {high_trust_conf} vs {low_trust_conf}"
        );
    }
}
