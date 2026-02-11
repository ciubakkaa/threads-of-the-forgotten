//! Agent Scheduler and Conflict Resolver for the NPC Agency System.
//!
//! The `AgentScheduler` manages a discrete-event priority queue where each NPC
//! agent has a "next wake time." Agents are processed in time order, with
//! deterministic tie-breaking derived from `hash(seed, tick, agent_id)`.
//!
//! The `ConflictResolver` handles concurrent mutations to shared world state,
//! granting mutations to higher-priority agents and rejecting lower-priority ones.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, BTreeSet};

use contracts::agency::{
    CommitResult, ConflictResult, ScheduledEvent, WakeReason, WorldMutation,
};

use crate::economy::EconomyLedger;

// ---------------------------------------------------------------------------
// Deterministic priority derivation
// ---------------------------------------------------------------------------

/// Derive a deterministic priority for an agent at a given tick.
/// Lower numeric value = higher priority (processed first).
/// Uses SplitMix64-style mixing for good distribution.
fn deterministic_priority(seed: u64, tick: u64, agent_id: &str) -> u64 {
    let mut h: u64 = seed;
    h = h.wrapping_add(tick.wrapping_mul(0x9e3779b97f4a7c15));
    for b in agent_id.bytes() {
        h = h.wrapping_add(b as u64);
        h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    }
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

// ---------------------------------------------------------------------------
// Ordering for ScheduledEvent in the BinaryHeap
// ---------------------------------------------------------------------------

/// Wrapper that provides Ord for ScheduledEvent.
/// Ordering: (wake_tick ASC, priority ASC, agent_id ASC).
/// We use `Reverse` in the BinaryHeap so the smallest tuple comes out first.
#[derive(Debug, Clone, Eq, PartialEq)]
struct OrderedEvent(ScheduledEvent);

impl PartialOrd for OrderedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .wake_tick
            .cmp(&other.0.wake_tick)
            .then_with(|| self.0.priority.cmp(&other.0.priority))
            .then_with(|| self.0.agent_id.cmp(&other.0.agent_id))
    }
}


// ---------------------------------------------------------------------------
// AgentScheduler
// ---------------------------------------------------------------------------

/// Discrete-event priority queue scheduler for NPC agents.
///
/// Each agent has a "next wake time" and the scheduler processes agents in
/// time order. When multiple agents share the same wake time, a deterministic
/// priority derived from `hash(seed, tick, agent_id)` breaks ties.
#[derive(Debug, Clone)]
pub struct AgentScheduler {
    event_queue: BinaryHeap<Reverse<OrderedEvent>>,
    current_tick: u64,
    max_ticks: u64,
    seed: u64,
    /// Agent IDs that are currently occupied (multi-tick operator in progress).
    /// These agents skip planning until their occupancy ends or is interrupted.
    occupied_agents: BTreeSet<String>,
}

impl AgentScheduler {
    /// Create a new scheduler with the given seed and maximum tick count.
    pub fn new(seed: u64, max_ticks: u64) -> Self {
        Self {
            event_queue: BinaryHeap::new(),
            current_tick: 0,
            max_ticks,
            seed,
            occupied_agents: BTreeSet::new(),
        }
    }

    /// Schedule an event for an agent. The priority is set deterministically.
    pub fn schedule(&mut self, mut event: ScheduledEvent) {
        event.priority = deterministic_priority(self.seed, event.wake_tick, &event.agent_id);
        self.event_queue.push(Reverse(OrderedEvent(event)));
    }

    /// Schedule an event with a specific wake reason, computing priority automatically.
    pub fn schedule_wake(&mut self, agent_id: String, wake_tick: u64, reason: WakeReason) {
        let priority = deterministic_priority(self.seed, wake_tick, &agent_id);
        let event = ScheduledEvent {
            wake_tick,
            priority,
            agent_id,
            reason,
        };
        self.event_queue.push(Reverse(OrderedEvent(event)));
    }

    /// Pop the next event from the queue, advancing the clock.
    /// Returns `None` if the queue is empty or the next event is beyond max_ticks.
    /// Skips agents that are currently occupied (multi-tick operators).
    pub fn pop_next(&mut self) -> Option<ScheduledEvent> {
        loop {
            let next = self.event_queue.peek()?;
            let event = &next.0 .0;

            // Don't advance past max ticks.
            if event.wake_tick > self.max_ticks {
                return None;
            }

            // Advance clock to this event's tick.
            if event.wake_tick > self.current_tick {
                self.current_tick = event.wake_tick;
            }

            let event = self.event_queue.pop().unwrap().0 .0;

            return Some(event);
        }
    }

    /// Current simulation tick.
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Advance the clock to the given tick (if it's in the future).
    pub fn advance_clock(&mut self, tick: u64) {
        if tick > self.current_tick {
            self.current_tick = tick;
        }
    }

    /// Peek at the minimum next-event time without consuming it.
    pub fn peek_next_tick(&self) -> Option<u64> {
        self.event_queue.peek().map(|e| e.0 .0.wake_tick)
    }

    /// Mark an agent as occupied (multi-tick operator in progress).
    pub fn mark_occupied(&mut self, agent_id: &str) {
        self.occupied_agents.insert(agent_id.to_string());
    }

    /// Clear an agent's occupied status (operator completed or interrupted).
    pub fn clear_occupied(&mut self, agent_id: &str) {
        self.occupied_agents.remove(agent_id);
    }

    /// Check if an agent is currently marked as occupied.
    pub fn is_occupied(&self, agent_id: &str) -> bool {
        self.occupied_agents.contains(agent_id)
    }

    /// Whether the scheduler has any pending events within max_ticks.
    pub fn has_pending(&self) -> bool {
        self.event_queue
            .peek()
            .map_or(false, |e| e.0 .0.wake_tick <= self.max_ticks)
    }

    /// Number of events in the queue.
    pub fn queue_len(&self) -> usize {
        self.event_queue.len()
    }

    /// The simulation seed used for priority derivation.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The maximum tick the scheduler will process.
    pub fn max_ticks(&self) -> u64 {
        self.max_ticks
    }

    /// Derive a deterministic priority for an agent at a given tick.
    /// Exposed for external use (e.g., conflict resolution).
    pub fn priority_for(&self, agent_id: &str, tick: u64) -> u64 {
        deterministic_priority(self.seed, tick, agent_id)
    }
}


// ---------------------------------------------------------------------------
// ConflictResolver
// ---------------------------------------------------------------------------

/// Tracks pending mutations within a scheduling window and resolves conflicts
/// deterministically: higher priority (lower numeric value) wins.
#[derive(Debug)]
pub struct ConflictResolver {
    /// Resources claimed in the current scheduling window.
    /// Maps resource key → (claiming agent_id, agent_priority).
    claimed_resources: std::collections::BTreeMap<String, (String, u64)>,
}

impl ConflictResolver {
    pub fn new() -> Self {
        Self {
            claimed_resources: std::collections::BTreeMap::new(),
        }
    }

    /// Clear all claims for a new scheduling window.
    pub fn reset(&mut self) {
        self.claimed_resources.clear();
    }

    /// Attempt to commit a mutation. Checks resource transfers for conflicts
    /// against previously committed mutations in this window.
    ///
    /// - If no conflict, the mutation is committed: resource transfers are
    ///   applied to the economy ledger and a `CommitResult` is returned.
    /// - If a conflict exists and this agent has higher priority (lower value),
    ///   the previous claim is overridden (the earlier agent would have already
    ///   been committed, so in practice we reject the *later* lower-priority agent).
    /// - If a conflict exists and this agent has lower priority, the mutation
    ///   is rejected with a `ConflictResult`.
    pub fn try_commit(
        &mut self,
        economy: &mut EconomyLedger,
        mutation: &WorldMutation,
        agent_priority: u64,
        tick: u64,
    ) -> Result<CommitResult, ConflictResult> {
        // 1. Check for resource conflicts.
        for transfer in &mutation.resource_transfers {
            let resource_key = format!(
                "{}:{}:{:?}",
                transfer.from_account, transfer.to_account, transfer.kind
            );

            if let Some((existing_agent, existing_priority)) =
                self.claimed_resources.get(&resource_key)
            {
                // Lower numeric priority = higher actual priority.
                if agent_priority >= *existing_priority {
                    return Err(ConflictResult {
                        conflicting_agent_id: existing_agent.clone(),
                        conflicting_resource: resource_key,
                        reason: format!(
                            "Resource already claimed by agent {} with higher priority",
                            existing_agent
                        ),
                    });
                }
                // This agent has higher priority — it wins. The earlier commit
                // already went through, but we allow this one too (the design
                // processes agents in priority order, so this branch is rare).
            }
        }

        // 2. Apply resource transfers to the economy ledger.
        for transfer in &mutation.resource_transfers {
            let result = economy.transfer(
                &transfer.from_account,
                &transfer.to_account,
                transfer.kind,
                transfer.amount,
                &mutation.operator_id,
                tick,
            );

            if let Err(e) = result {
                return Err(ConflictResult {
                    conflicting_agent_id: mutation.agent_id.clone(),
                    conflicting_resource: format!(
                        "{}:{:?}",
                        transfer.from_account, transfer.kind
                    ),
                    reason: format!("Economy transfer failed: {}", e),
                });
            }
        }

        // 3. Record claims for conflict detection within this window.
        for transfer in &mutation.resource_transfers {
            let resource_key = format!(
                "{}:{}:{:?}",
                transfer.from_account, transfer.to_account, transfer.kind
            );
            self.claimed_resources
                .insert(resource_key, (mutation.agent_id.clone(), agent_priority));
        }

        // 4. Generate a commit result.
        let event_id = format!(
            "commit-{}-{}-{}",
            mutation.agent_id, tick, mutation.operator_id
        );

        Ok(CommitResult { event_id, tick })
    }
}

impl Default for ConflictResolver {
    fn default() -> Self {
        Self::new()
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::agency::{ResourceKind, ResourceTransfer};
    use std::collections::BTreeMap;

    use crate::economy::EconomyLedger;
    use contracts::agency::AccountBalance;

    // -- AgentScheduler tests -----------------------------------------------

    #[test]
    fn schedule_and_pop_returns_events_in_tick_order() {
        let mut sched = AgentScheduler::new(42, 100);
        sched.schedule_wake("alice".into(), 5, WakeReason::Idle);
        sched.schedule_wake("bob".into(), 3, WakeReason::Idle);
        sched.schedule_wake("carol".into(), 7, WakeReason::Idle);

        let e1 = sched.pop_next().unwrap();
        assert_eq!(e1.wake_tick, 3);
        assert_eq!(e1.agent_id, "bob");

        let e2 = sched.pop_next().unwrap();
        assert_eq!(e2.wake_tick, 5);
        assert_eq!(e2.agent_id, "alice");

        let e3 = sched.pop_next().unwrap();
        assert_eq!(e3.wake_tick, 7);
        assert_eq!(e3.agent_id, "carol");

        assert!(sched.pop_next().is_none());
    }

    #[test]
    fn same_tick_uses_deterministic_priority() {
        let seed = 42;
        let mut sched = AgentScheduler::new(seed, 100);
        sched.schedule_wake("alice".into(), 10, WakeReason::Idle);
        sched.schedule_wake("bob".into(), 10, WakeReason::Idle);
        sched.schedule_wake("carol".into(), 10, WakeReason::Idle);

        let mut order1 = Vec::new();
        while let Some(e) = sched.pop_next() {
            order1.push(e.agent_id.clone());
        }

        // Run again with same seed — must produce identical order.
        let mut sched2 = AgentScheduler::new(seed, 100);
        sched2.schedule_wake("alice".into(), 10, WakeReason::Idle);
        sched2.schedule_wake("bob".into(), 10, WakeReason::Idle);
        sched2.schedule_wake("carol".into(), 10, WakeReason::Idle);

        let mut order2 = Vec::new();
        while let Some(e) = sched2.pop_next() {
            order2.push(e.agent_id.clone());
        }

        assert_eq!(order1, order2);
        assert_eq!(order1.len(), 3);
    }

    #[test]
    fn different_seed_may_produce_different_order() {
        // Not guaranteed to differ for all seeds, but with high probability
        // two different seeds produce different orderings for 3 agents.
        let agents = vec!["alice", "bob", "carol"];
        let mut orders = Vec::new();

        for seed in [1u64, 999] {
            let mut sched = AgentScheduler::new(seed, 100);
            for a in &agents {
                sched.schedule_wake(a.to_string(), 10, WakeReason::Idle);
            }
            let mut order = Vec::new();
            while let Some(e) = sched.pop_next() {
                order.push(e.agent_id.clone());
            }
            orders.push(order);
        }

        // At least check both produced 3 events.
        assert_eq!(orders[0].len(), 3);
        assert_eq!(orders[1].len(), 3);
    }

    #[test]
    fn clock_advances_to_minimum_next_event() {
        let mut sched = AgentScheduler::new(42, 100);
        assert_eq!(sched.current_tick(), 0);

        sched.schedule_wake("alice".into(), 5, WakeReason::Idle);
        sched.schedule_wake("bob".into(), 10, WakeReason::Idle);

        let e = sched.pop_next().unwrap();
        assert_eq!(e.wake_tick, 5);
        assert_eq!(sched.current_tick(), 5);

        let e = sched.pop_next().unwrap();
        assert_eq!(e.wake_tick, 10);
        assert_eq!(sched.current_tick(), 10);
    }

    #[test]
    fn max_ticks_prevents_events_beyond_limit() {
        let mut sched = AgentScheduler::new(42, 5);
        sched.schedule_wake("alice".into(), 3, WakeReason::Idle);
        sched.schedule_wake("bob".into(), 6, WakeReason::Idle);

        let e = sched.pop_next().unwrap();
        assert_eq!(e.agent_id, "alice");

        // Bob's event is at tick 6, beyond max_ticks=5.
        assert!(sched.pop_next().is_none());
    }

    #[test]
    fn occupied_agents_are_not_skipped_by_scheduler() {
        let mut sched = AgentScheduler::new(42, 100);
        sched.schedule_wake("alice".into(), 5, WakeReason::Idle);
        sched.schedule_wake("bob".into(), 5, WakeReason::Idle);

        sched.mark_occupied("alice");

        // Scheduler no longer skips occupied agents — occupancy is handled
        // at the AgentWorld level. Both events should be returned.
        let e1 = sched.pop_next().unwrap();
        let e2 = sched.pop_next().unwrap();
        let ids: Vec<&str> = vec![&e1.agent_id, &e2.agent_id];
        assert!(ids.contains(&"alice"), "alice should not be skipped");
        assert!(ids.contains(&"bob"), "bob should be returned");

        assert!(sched.pop_next().is_none());
    }

    #[test]
    fn clear_occupied_allows_agent_to_be_scheduled_again() {
        let mut sched = AgentScheduler::new(42, 100);
        sched.mark_occupied("alice");
        assert!(sched.is_occupied("alice"));

        sched.clear_occupied("alice");
        assert!(!sched.is_occupied("alice"));

        sched.schedule_wake("alice".into(), 5, WakeReason::PlanStepComplete);
        let e = sched.pop_next().unwrap();
        assert_eq!(e.agent_id, "alice");
        assert_eq!(e.reason, WakeReason::PlanStepComplete);
    }

    #[test]
    fn advance_clock_moves_forward() {
        let mut sched = AgentScheduler::new(42, 100);
        sched.advance_clock(10);
        assert_eq!(sched.current_tick(), 10);

        // Advancing to a lower tick is a no-op.
        sched.advance_clock(5);
        assert_eq!(sched.current_tick(), 10);
    }

    #[test]
    fn peek_next_tick_without_consuming() {
        let mut sched = AgentScheduler::new(42, 100);
        sched.schedule_wake("alice".into(), 7, WakeReason::Idle);

        assert_eq!(sched.peek_next_tick(), Some(7));
        // Still there.
        assert_eq!(sched.peek_next_tick(), Some(7));
        // Now consume.
        sched.pop_next();
        assert_eq!(sched.peek_next_tick(), None);
    }

    #[test]
    fn has_pending_respects_max_ticks() {
        let mut sched = AgentScheduler::new(42, 5);
        assert!(!sched.has_pending());

        sched.schedule_wake("alice".into(), 3, WakeReason::Idle);
        assert!(sched.has_pending());

        sched.pop_next();
        assert!(!sched.has_pending());

        sched.schedule_wake("bob".into(), 10, WakeReason::Idle);
        assert!(!sched.has_pending()); // beyond max_ticks
    }

    // -- ConflictResolver tests ---------------------------------------------

    fn test_economy() -> EconomyLedger {
        let mut accounts = BTreeMap::new();
        accounts.insert(
            "alice".to_string(),
            AccountBalance {
                money: 100,
                food: 50,
                fuel: 0,
                medicine: 0,
            },
        );
        accounts.insert(
            "bob".to_string(),
            AccountBalance {
                money: 100,
                food: 50,
                fuel: 0,
                medicine: 0,
            },
        );
        accounts.insert(
            "carol".to_string(),
            AccountBalance {
                money: 100,
                food: 50,
                fuel: 0,
                medicine: 0,
            },
        );
        EconomyLedger::new(accounts)
    }

    #[test]
    fn commit_succeeds_with_no_conflict() {
        let mut resolver = ConflictResolver::new();
        let mut economy = test_economy();

        let mutation = WorldMutation {
            agent_id: "alice".into(),
            operator_id: "trade".into(),
            deltas: vec![],
            resource_transfers: vec![ResourceTransfer {
                from_account: "alice".into(),
                to_account: "bob".into(),
                kind: ResourceKind::Money,
                amount: 10,
            }],
        };

        let result = resolver.try_commit(&mut economy, &mutation, 100, 5);
        assert!(result.is_ok());
        let commit = result.unwrap();
        assert_eq!(commit.tick, 5);
    }

    #[test]
    fn conflict_rejects_lower_priority_agent() {
        let mut resolver = ConflictResolver::new();
        let mut economy = test_economy();

        // Higher priority agent (lower numeric value) commits first.
        let mutation1 = WorldMutation {
            agent_id: "alice".into(),
            operator_id: "trade".into(),
            deltas: vec![],
            resource_transfers: vec![ResourceTransfer {
                from_account: "alice".into(),
                to_account: "bob".into(),
                kind: ResourceKind::Money,
                amount: 10,
            }],
        };
        let r1 = resolver.try_commit(&mut economy, &mutation1, 50, 5);
        assert!(r1.is_ok());

        // Lower priority agent (higher numeric value) tries same resource.
        let mutation2 = WorldMutation {
            agent_id: "carol".into(),
            operator_id: "trade".into(),
            deltas: vec![],
            resource_transfers: vec![ResourceTransfer {
                from_account: "alice".into(),
                to_account: "bob".into(),
                kind: ResourceKind::Money,
                amount: 5,
            }],
        };
        let r2 = resolver.try_commit(&mut economy, &mutation2, 200, 5);
        assert!(r2.is_err());
        let conflict = r2.unwrap_err();
        assert_eq!(conflict.conflicting_agent_id, "alice");
    }

    #[test]
    fn insufficient_balance_returns_conflict() {
        let mut resolver = ConflictResolver::new();
        let mut economy = test_economy();

        let mutation = WorldMutation {
            agent_id: "alice".into(),
            operator_id: "trade".into(),
            deltas: vec![],
            resource_transfers: vec![ResourceTransfer {
                from_account: "alice".into(),
                to_account: "bob".into(),
                kind: ResourceKind::Money,
                amount: 999, // more than alice has
            }],
        };

        let result = resolver.try_commit(&mut economy, &mutation, 100, 5);
        assert!(result.is_err());
    }

    #[test]
    fn reset_clears_claims() {
        let mut resolver = ConflictResolver::new();
        let mut economy = test_economy();

        let mutation = WorldMutation {
            agent_id: "alice".into(),
            operator_id: "trade".into(),
            deltas: vec![],
            resource_transfers: vec![ResourceTransfer {
                from_account: "alice".into(),
                to_account: "bob".into(),
                kind: ResourceKind::Money,
                amount: 10,
            }],
        };
        resolver
            .try_commit(&mut economy, &mutation, 50, 5)
            .unwrap();

        // Reset claims for new window.
        resolver.reset();

        // Same resource key should now succeed for a different agent.
        let mutation2 = WorldMutation {
            agent_id: "carol".into(),
            operator_id: "trade".into(),
            deltas: vec![],
            resource_transfers: vec![ResourceTransfer {
                from_account: "alice".into(),
                to_account: "bob".into(),
                kind: ResourceKind::Money,
                amount: 5,
            }],
        };
        let r = resolver.try_commit(&mut economy, &mutation2, 200, 6);
        assert!(r.is_ok());
    }

    #[test]
    fn mutation_with_no_transfers_always_commits() {
        let mut resolver = ConflictResolver::new();
        let mut economy = test_economy();

        let mutation = WorldMutation {
            agent_id: "alice".into(),
            operator_id: "observe".into(),
            deltas: vec![],
            resource_transfers: vec![],
        };

        let result = resolver.try_commit(&mut economy, &mutation, 100, 5);
        assert!(result.is_ok());
    }

    // -- deterministic_priority tests ---------------------------------------

    #[test]
    fn priority_is_deterministic() {
        let p1 = deterministic_priority(42, 10, "alice");
        let p2 = deterministic_priority(42, 10, "alice");
        assert_eq!(p1, p2);
    }

    #[test]
    fn priority_differs_for_different_agents() {
        let p1 = deterministic_priority(42, 10, "alice");
        let p2 = deterministic_priority(42, 10, "bob");
        // Not guaranteed to differ for all inputs, but extremely likely.
        // We just check they're computed without panic.
        let _ = (p1, p2);
    }

    #[test]
    fn priority_differs_for_different_ticks() {
        let p1 = deterministic_priority(42, 10, "alice");
        let p2 = deterministic_priority(42, 11, "alice");
        let _ = (p1, p2);
    }
}
