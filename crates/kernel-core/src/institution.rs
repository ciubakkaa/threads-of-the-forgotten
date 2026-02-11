use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRequest {
    pub request_id: String,
    pub npc_id: String,
    pub submitted_tick: u64,
    pub status_score: i64,
    pub connected_score: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceOutcome {
    pub request_id: String,
    pub npc_id: String,
    pub accepted: bool,
    pub quality_score: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstitutionState {
    pub institution_id: String,
    pub capacity: usize,
    pub processing_latency_ticks: u64,
    pub quality_rating: i64,
    pub corruption_bias: i64,
    pub queue: VecDeque<ServiceRequest>,
}

impl InstitutionState {
    pub fn new(
        institution_id: String,
        capacity: usize,
        processing_latency_ticks: u64,
        quality_rating: i64,
        corruption_bias: i64,
    ) -> Self {
        Self {
            institution_id,
            capacity: capacity.max(1),
            processing_latency_ticks,
            quality_rating,
            corruption_bias,
            queue: VecDeque::new(),
        }
    }

    pub fn enqueue(&mut self, request: ServiceRequest) -> bool {
        if self.queue.len() >= self.capacity {
            return false;
        }
        self.queue.push_back(request);
        true
    }

    pub fn process_cycle(&mut self) -> Vec<ServiceOutcome> {
        let mut outcomes = Vec::new();
        let mut processed = 0_usize;
        while processed < self.capacity {
            let Some(request) = self.queue.pop_front() else {
                break;
            };
            processed += 1;

            let favored =
                self.corruption_bias > 60 && (request.status_score + request.connected_score) > 100;
            let quality_penalty = if favored { -5 } else { 0 };
            outcomes.push(ServiceOutcome {
                request_id: request.request_id,
                npc_id: request.npc_id,
                accepted: true,
                quality_score: (self.quality_rating + quality_penalty).clamp(0, 100),
            });
        }
        outcomes
    }

    pub fn degrade_quality(&mut self, amount: i64) {
        self.quality_rating = (self.quality_rating - amount).clamp(0, 100);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_capacity_is_enforced() {
        let mut institution = InstitutionState::new("court".to_string(), 1, 2, 70, 10);
        let first = institution.enqueue(ServiceRequest {
            request_id: "r1".to_string(),
            npc_id: "npc:a".to_string(),
            submitted_tick: 1,
            status_score: 10,
            connected_score: 10,
        });
        let second = institution.enqueue(ServiceRequest {
            request_id: "r2".to_string(),
            npc_id: "npc:b".to_string(),
            submitted_tick: 1,
            status_score: 10,
            connected_score: 10,
        });

        assert!(first);
        assert!(!second);
    }
}
