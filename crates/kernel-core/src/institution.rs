//! Institution system: settlement-level entities (market, court, temple, guild)
//! with finite capacity, processing latency, quality rating, and corruption/bias drift.

use std::collections::VecDeque;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from institution operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstitutionError {
    /// The institution's queue is at capacity.
    QueueFull {
        institution_id: String,
        capacity: usize,
    },
    /// The institution does not exist.
    NotFound(String),
}

impl std::fmt::Display for InstitutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstitutionError::QueueFull { institution_id, capacity } => {
                write!(f, "institution {} queue full (capacity {})", institution_id, capacity)
            }
            InstitutionError::NotFound(id) => write!(f, "institution not found: {}", id),
        }
    }
}

// ---------------------------------------------------------------------------
// Service request and outcome types
// ---------------------------------------------------------------------------

/// A service request submitted to an institution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRequest {
    pub request_id: String,
    pub requester_id: String,
    /// Social status score of the requester (higher = more connected/powerful).
    pub requester_status: i64,
    pub submitted_tick: u64,
    /// How many ticks remain before this request is processed.
    pub remaining_latency: u64,
}

/// The outcome of a processed service request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceOutcome {
    pub request_id: String,
    pub requester_id: String,
    /// Quality of the outcome (0–100). Lower quality increases grievance.
    pub quality: i64,
    /// Whether corruption biased this outcome.
    pub corruption_biased: bool,
    pub processed_tick: u64,
}

// ---------------------------------------------------------------------------
// InstitutionState
// ---------------------------------------------------------------------------

/// A settlement-level institution with finite capacity, processing latency,
/// quality rating, and corruption/bias drift.
#[derive(Debug, Clone)]
pub struct InstitutionState {
    pub institution_id: String,
    pub settlement_id: String,
    /// Maximum number of requests that can be queued simultaneously.
    pub capacity: usize,
    /// Base number of ticks to process a single request.
    pub processing_latency: u64,
    /// Current quality rating (0–100). Degrades over time or under load.
    pub quality: i64,
    /// Current corruption level (0–100). When above `corruption_threshold`,
    /// outcomes are biased toward higher-status requesters.
    pub corruption: i64,
    /// Corruption level at which bias kicks in.
    pub corruption_threshold: i64,
    /// Pending requests being processed.
    queue: VecDeque<ServiceRequest>,
    /// Total requests processed (lifetime counter).
    pub total_processed: u64,
    /// Total requests rejected due to queue overflow.
    pub total_rejected: u64,
}

impl InstitutionState {
    /// Create a new institution.
    pub fn new(
        institution_id: impl Into<String>,
        settlement_id: impl Into<String>,
        capacity: usize,
        processing_latency: u64,
        quality: i64,
        corruption: i64,
        corruption_threshold: i64,
    ) -> Self {
        Self {
            institution_id: institution_id.into(),
            settlement_id: settlement_id.into(),
            capacity,
            processing_latency,
            quality: quality.clamp(0, 100),
            corruption: corruption.clamp(0, 100),
            corruption_threshold: corruption_threshold.clamp(0, 100),
            queue: VecDeque::new(),
            total_processed: 0,
            total_rejected: 0,
        }
    }

    /// Current queue depth.
    pub fn queue_depth(&self) -> usize {
        self.queue.len()
    }

    /// Whether the queue is at capacity.
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.capacity
    }

    /// Submit a service request. Returns an error if the queue is full.
    pub fn submit_request(&mut self, request: ServiceRequest) -> Result<(), InstitutionError> {
        if self.is_full() {
            self.total_rejected += 1;
            return Err(InstitutionError::QueueFull {
                institution_id: self.institution_id.clone(),
                capacity: self.capacity,
            });
        }
        let mut req = request;
        req.remaining_latency = self.processing_latency;
        self.queue.push_back(req);
        Ok(())
    }

    /// Advance the institution by one tick: decrement latency on queued requests
    /// and produce outcomes for any that are ready.
    pub fn tick(&mut self, current_tick: u64) -> Vec<ServiceOutcome> {
        let mut outcomes = Vec::new();
        let mut still_pending = VecDeque::new();

        while let Some(mut req) = self.queue.pop_front() {
            if req.remaining_latency <= 1 {
                // Process this request.
                let outcome = self.produce_outcome(&req, current_tick);
                outcomes.push(outcome);
                self.total_processed += 1;
            } else {
                req.remaining_latency -= 1;
                still_pending.push_back(req);
            }
        }

        self.queue = still_pending;
        outcomes
    }

    /// Produce an outcome for a completed request, applying corruption bias
    /// and quality degradation.
    fn produce_outcome(&self, request: &ServiceRequest, tick: u64) -> ServiceOutcome {
        let corruption_biased = self.corruption > self.corruption_threshold;

        // Base outcome quality is the institution's quality rating.
        // If corruption is active, higher-status requesters get better outcomes.
        let quality = if corruption_biased {
            // Status bonus: each point of status above 50 adds 1 quality point,
            // each point below 50 subtracts 1. Clamped to [0, 100].
            let status_modifier = request.requester_status - 50;
            (self.quality + status_modifier).clamp(0, 100)
        } else {
            self.quality
        };

        ServiceOutcome {
            request_id: request.request_id.clone(),
            requester_id: request.requester_id.clone(),
            quality,
            corruption_biased,
            processed_tick: tick,
        }
    }

    /// Read-only access to the pending queue.
    pub fn pending_requests(&self) -> &VecDeque<ServiceRequest> {
        &self.queue
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_institution() -> InstitutionState {
        InstitutionState::new("court", "crownvale", 3, 2, 80, 30, 50)
    }

    fn make_request(id: &str, requester: &str, status: i64, tick: u64) -> ServiceRequest {
        ServiceRequest {
            request_id: id.into(),
            requester_id: requester.into(),
            requester_status: status,
            submitted_tick: tick,
            remaining_latency: 0, // will be overwritten by submit
        }
    }

    #[test]
    fn submit_and_process_request() {
        let mut inst = test_institution();
        inst.submit_request(make_request("r1", "npc_a", 50, 1)).unwrap();
        assert_eq!(inst.queue_depth(), 1);

        // Tick 1: latency decrements from 2 to 1, not yet ready.
        let outcomes = inst.tick(2);
        assert!(outcomes.is_empty());
        assert_eq!(inst.queue_depth(), 1);

        // Tick 2: latency reaches 0, request processed.
        let outcomes = inst.tick(3);
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].request_id, "r1");
        assert_eq!(outcomes[0].quality, 80); // institution quality, no corruption bias
        assert!(!outcomes[0].corruption_biased);
        assert_eq!(inst.queue_depth(), 0);
        assert_eq!(inst.total_processed, 1);
    }

    #[test]
    fn queue_overflow_rejects() {
        let mut inst = test_institution(); // capacity 3
        inst.submit_request(make_request("r1", "a", 50, 1)).unwrap();
        inst.submit_request(make_request("r2", "b", 50, 1)).unwrap();
        inst.submit_request(make_request("r3", "c", 50, 1)).unwrap();
        assert!(inst.is_full());

        let result = inst.submit_request(make_request("r4", "d", 50, 1));
        assert!(matches!(result, Err(InstitutionError::QueueFull { .. })));
        assert_eq!(inst.total_rejected, 1);
    }

    #[test]
    fn corruption_biases_high_status() {
        // Corruption above threshold → bias active.
        let mut inst = InstitutionState::new("court", "crownvale", 5, 1, 60, 70, 50);

        // High-status requester (status 80 → modifier +30).
        inst.submit_request(make_request("r1", "noble", 80, 1)).unwrap();
        // Low-status requester (status 20 → modifier -30).
        inst.submit_request(make_request("r2", "peasant", 20, 1)).unwrap();

        let outcomes = inst.tick(2);
        assert_eq!(outcomes.len(), 2);

        let noble_outcome = outcomes.iter().find(|o| o.requester_id == "noble").unwrap();
        let peasant_outcome = outcomes.iter().find(|o| o.requester_id == "peasant").unwrap();

        assert!(noble_outcome.corruption_biased);
        assert!(peasant_outcome.corruption_biased);
        // Noble gets better quality than peasant.
        assert!(noble_outcome.quality > peasant_outcome.quality);
        // Noble: 60 + (80-50) = 90, Peasant: 60 + (20-50) = 30
        assert_eq!(noble_outcome.quality, 90);
        assert_eq!(peasant_outcome.quality, 30);
    }

    #[test]
    fn no_corruption_bias_below_threshold() {
        // Corruption below threshold → no bias.
        let mut inst = InstitutionState::new("court", "crownvale", 5, 1, 60, 40, 50);

        inst.submit_request(make_request("r1", "noble", 90, 1)).unwrap();
        inst.submit_request(make_request("r2", "peasant", 10, 1)).unwrap();

        let outcomes = inst.tick(2);
        assert_eq!(outcomes.len(), 2);

        // Both get the same quality (institution quality, no bias).
        assert_eq!(outcomes[0].quality, 60);
        assert_eq!(outcomes[1].quality, 60);
        assert!(!outcomes[0].corruption_biased);
        assert!(!outcomes[1].corruption_biased);
    }

    #[test]
    fn quality_degradation_affects_outcomes() {
        // Low quality institution produces low-quality outcomes.
        let mut inst = InstitutionState::new("court", "crownvale", 5, 1, 20, 0, 50);
        inst.submit_request(make_request("r1", "npc_a", 50, 1)).unwrap();

        let outcomes = inst.tick(2);
        assert_eq!(outcomes[0].quality, 20);
    }

    #[test]
    fn quality_clamped_to_valid_range() {
        let inst = InstitutionState::new("court", "crownvale", 5, 1, 150, -10, 50);
        assert_eq!(inst.quality, 100);
        assert_eq!(inst.corruption, 0);
    }

    #[test]
    fn multiple_ticks_process_sequentially() {
        let mut inst = InstitutionState::new("court", "crownvale", 5, 3, 70, 0, 50);
        inst.submit_request(make_request("r1", "a", 50, 1)).unwrap();
        inst.submit_request(make_request("r2", "b", 50, 2)).unwrap();

        // Tick 1: both decrement latency (3→2).
        assert!(inst.tick(2).is_empty());
        // Tick 2: both decrement (2→1).
        assert!(inst.tick(3).is_empty());
        // Tick 3: both ready.
        let outcomes = inst.tick(4);
        assert_eq!(outcomes.len(), 2);
        assert_eq!(inst.total_processed, 2);
    }
}
