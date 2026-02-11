use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use contracts::{
    CommitResult, ConflictResult, ResourceTransfer, ScheduledEvent, WakeReason, WorldMutation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueEvent {
    wake_tick: u64,
    priority: u64,
    agent_id: String,
    reason: WakeReason,
}

impl Ord for QueueEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.wake_tick
            .cmp(&other.wake_tick)
            .then(self.priority.cmp(&other.priority))
            .then(self.agent_id.cmp(&other.agent_id))
    }
}

impl PartialOrd for QueueEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct AgentScheduler {
    event_queue: BinaryHeap<Reverse<QueueEvent>>,
    current_tick: u64,
    seed: u64,
}

impl AgentScheduler {
    pub fn new(seed: u64) -> Self {
        Self {
            event_queue: BinaryHeap::new(),
            current_tick: 0,
            seed,
        }
    }

    pub fn schedule(&mut self, event: ScheduledEvent) {
        self.event_queue.push(Reverse(QueueEvent {
            wake_tick: event.wake_tick,
            priority: event.priority,
            agent_id: event.agent_id,
            reason: event.reason,
        }));
    }

    pub fn schedule_next(&mut self, agent_id: &str, wake_tick: u64, reason: WakeReason) {
        let priority = self.priority_for(wake_tick, agent_id);
        self.schedule(ScheduledEvent {
            wake_tick,
            priority,
            agent_id: agent_id.to_string(),
            reason,
        });
    }

    pub fn pop_next(&mut self) -> Option<ScheduledEvent> {
        let Reverse(event) = self.event_queue.pop()?;
        self.current_tick = event.wake_tick;
        Some(ScheduledEvent {
            wake_tick: event.wake_tick,
            priority: event.priority,
            agent_id: event.agent_id,
            reason: event.reason,
        })
    }

    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    pub fn advance_clock(&mut self, tick: u64) {
        self.current_tick = tick;
    }

    pub fn pending_events(&self) -> usize {
        self.event_queue.len()
    }

    pub fn peek_next_tick(&self) -> Option<u64> {
        self.event_queue.peek().map(|event| event.0.wake_tick)
    }

    pub fn pending_snapshot(&self) -> Vec<ScheduledEvent> {
        self.event_queue
            .iter()
            .map(|entry| ScheduledEvent {
                wake_tick: entry.0.wake_tick,
                priority: entry.0.priority,
                agent_id: entry.0.agent_id.clone(),
                reason: entry.0.reason,
            })
            .collect()
    }

    pub fn priority_for(&self, tick: u64, agent_id: &str) -> u64 {
        deterministic_priority(self.seed, tick, agent_id)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConflictResolver {
    claimed_resources: BTreeMap<String, (u64, String)>,
}

impl ConflictResolver {
    pub fn try_commit(
        &mut self,
        mutation: &WorldMutation,
        agent_priority: u64,
    ) -> Result<CommitResult, ConflictResult> {
        let resource_id =
            first_resource_key(mutation).unwrap_or_else(|| mutation.operator_id.clone());

        match self.claimed_resources.get(&resource_id) {
            Some((winning_priority, winner)) if *winning_priority > agent_priority => {
                Err(ConflictResult {
                    conflicting_agent_id: winner.clone(),
                    conflicting_resource: resource_id,
                    reason: "lower_priority".to_string(),
                })
            }
            Some((_, _)) => {
                self.claimed_resources.insert(
                    resource_id.clone(),
                    (agent_priority, mutation.agent_id.clone()),
                );
                Ok(CommitResult {
                    committed: true,
                    event_id: None,
                })
            }
            None => {
                self.claimed_resources
                    .insert(resource_id, (agent_priority, mutation.agent_id.clone()));
                Ok(CommitResult {
                    committed: true,
                    event_id: None,
                })
            }
        }
    }

    pub fn clear_window(&mut self) {
        self.claimed_resources.clear();
    }
}

fn first_resource_key(mutation: &WorldMutation) -> Option<String> {
    if let Some(delta) = mutation.deltas.first() {
        return Some(delta.fact_key.clone());
    }
    mutation
        .resource_transfers
        .first()
        .map(resource_key_from_transfer)
}

fn resource_key_from_transfer(transfer: &ResourceTransfer) -> String {
    format!(
        "{}:{}:{}",
        transfer.from_account, transfer.to_account, transfer.amount
    )
}

fn deterministic_priority(seed: u64, tick: u64, agent_id: &str) -> u64 {
    let mut hash = seed ^ tick.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in agent_id.as_bytes() {
        hash = hash.rotate_left(5) ^ u64::from(*byte);
        hash = hash.wrapping_mul(0x517C_C1B7_2722_0A95);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduling_order_is_deterministic() {
        let mut a = AgentScheduler::new(1337);
        let mut b = AgentScheduler::new(1337);

        for scheduler in [&mut a, &mut b] {
            scheduler.schedule_next("npc:1", 5, WakeReason::Idle);
            scheduler.schedule_next("npc:2", 5, WakeReason::Idle);
            scheduler.schedule_next("npc:3", 5, WakeReason::Idle);
        }

        let order_a = vec![
            a.pop_next().expect("event").agent_id,
            a.pop_next().expect("event").agent_id,
            a.pop_next().expect("event").agent_id,
        ];
        let order_b = vec![
            b.pop_next().expect("event").agent_id,
            b.pop_next().expect("event").agent_id,
            b.pop_next().expect("event").agent_id,
        ];

        assert_eq!(order_a, order_b);
    }

    #[test]
    fn conflict_resolver_prefers_higher_priority() {
        let mut resolver = ConflictResolver::default();
        let base = WorldMutation {
            agent_id: "npc:1".to_string(),
            operator_id: "op:a".to_string(),
            deltas: Vec::new(),
            resource_transfers: vec![ResourceTransfer {
                from_account: "a".to_string(),
                to_account: "b".to_string(),
                resource_kind: contracts::ResourceKind::Money,
                amount: 1,
            }],
        };

        resolver
            .try_commit(&base, 100)
            .expect("first mutation should commit");
        let losing = WorldMutation {
            agent_id: "npc:2".to_string(),
            ..base
        };

        let rejected = resolver.try_commit(&losing, 10);
        assert!(rejected.is_err());
    }
}
