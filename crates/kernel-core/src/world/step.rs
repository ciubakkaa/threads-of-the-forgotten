use super::*;

impl AgentWorld {
    pub fn start(&mut self) {
        if !self.status.is_complete() {
            self.status.mode = RunMode::Running;
        }
    }

    pub fn pause(&mut self) {
        self.status.mode = RunMode::Paused;
    }

    pub fn run_id(&self) -> &str {
        &self.status.run_id
    }

    pub fn config(&self) -> &RunConfig {
        &self.config
    }

    pub fn status(&self) -> &RunStatus {
        &self.status
    }

    pub fn enqueue_command(&mut self, command: Command, effective_tick: u64) {
        self.queued_commands.push(QueuedCommand {
            effective_tick,
            insertion_sequence: self.next_command_sequence,
            command,
        });
        self.next_command_sequence = self.next_command_sequence.saturating_add(1);
        self.sync_queue_depth();
    }

    pub fn events(&self) -> &[Event] {
        &self.event_log
    }

    pub fn reason_packets(&self) -> &[ReasonPacket] {
        &self.reason_packet_log
    }

    pub fn reason_envelopes(&self) -> &[ReasonEnvelope] {
        &self.reason_envelopes
    }

    pub fn replay_hash(&self) -> u64 {
        self.replay_hash
    }

    pub fn last_step_metrics(&self) -> StepMetrics {
        self.last_step_metrics
    }

    pub fn step(&mut self) -> bool {
        let previous_tick = self.status.current_tick;
        self.last_step_metrics = StepMetrics::default();
        if self.status.is_complete() {
            self.status.mode = RunMode::Paused;
            return false;
        }
        self.status.mode = RunMode::Running;
        let tick = self.status.current_tick.saturating_add(1);
        if tick > self.status.max_ticks {
            self.status.mode = RunMode::Paused;
            return false;
        }
        self.status.current_tick = tick;
        let mut sequence_in_tick = 0_u64;

        self.process_due_commands(tick, &mut sequence_in_tick);
        self.last_processed_tick = Some(tick);
        self.action_count_this_tick.clear();
        self.prune_channel_reservations(tick);
        self.claimed_resources_this_tick.clear();
        self.process_cadence_updates(tick, &mut sequence_in_tick);
        self.process_joint_contracts(tick, &mut sequence_in_tick);
        self.process_institutions(tick, &mut sequence_in_tick);
        self.emit_pressure_event(tick, &mut sequence_in_tick);

        let mut processed_agents = 0_u64;
        for _round in 0..usize::from(self.config.max_same_tick_rounds.max(1)) {
            let ready = self.pop_ready_agents(tick);
            if ready.is_empty() {
                break;
            }
            let evaluations = self.evaluate_ready_agents(ready, tick);
            if evaluations.is_empty() {
                break;
            }
            processed_agents = processed_agents.saturating_add(evaluations.len() as u64);
            for evaluation in evaluations {
                self.commit_agent_evaluation(evaluation, tick, &mut sequence_in_tick);
            }
        }

        self.emit_new_accounting_transfer_events(tick, &mut sequence_in_tick);

        self.state_hash = mix_state_hash(self.state_hash, tick, sequence_in_tick);
        self.last_step_metrics = StepMetrics {
            advanced_ticks: self.status.current_tick.saturating_sub(previous_tick),
            processed_batch_tick: tick,
            processed_agents,
        };

        if self.status.current_tick >= self.status.max_ticks {
            self.status.mode = RunMode::Paused;
        }
        self.sync_queue_depth();

        true
    }

    pub fn step_n(&mut self, n: u64) -> u64 {
        let mut committed = 0_u64;
        for _ in 0..n {
            if !self.step() {
                break;
            }
            committed += 1;
        }
        committed
    }

    pub fn run_to_tick(&mut self, tick: u64) -> u64 {
        let mut committed = 0_u64;
        while self.status.current_tick < tick {
            if !self.step() {
                break;
            }
            committed += 1;
        }
        committed
    }

    pub fn inject_command(&mut self, command: Command) {
        let effective_tick = self.status.current_tick + 1;
        self.enqueue_command(command, effective_tick);
    }

    pub(super) fn sync_queue_depth(&mut self) {
        self.status.queue_depth = self.queued_commands.len() + self.next_wake_tick_by_agent.len();
    }

    pub(super) fn schedule_unique(
        &mut self,
        agent_id: &str,
        wake_tick: u64,
        reason: contracts::WakeReason,
    ) {
        let mut desired_wake_tick = wake_tick;
        let mut min_wake_tick = wake_tick;
        if let Some(agent) = self.agents.get(agent_id) {
            if !agent.occupancy.interruptible {
                min_wake_tick = agent.occupancy.until_tick.max(min_wake_tick);
                desired_wake_tick = desired_wake_tick.max(min_wake_tick);
            }
        }
        if let Some(existing_tick) = self.next_wake_tick_by_agent.get(agent_id).copied() {
            if existing_tick < min_wake_tick {
                self.next_wake_tick_by_agent
                    .insert(agent_id.to_string(), min_wake_tick);
                self.wake_reason_by_agent
                    .insert(agent_id.to_string(), reason);
                return;
            }
        }
        match self.next_wake_tick_by_agent.get(agent_id).copied() {
            Some(existing_tick) if existing_tick <= desired_wake_tick => {}
            _ => {
                self.next_wake_tick_by_agent
                    .insert(agent_id.to_string(), desired_wake_tick);
                self.wake_reason_by_agent
                    .insert(agent_id.to_string(), reason);
            }
        }
    }

    pub(super) fn pop_ready_agents(&mut self, tick: u64) -> Vec<String> {
        let ready = self
            .next_wake_tick_by_agent
            .iter()
            .filter_map(|(agent_id, wake_tick)| {
                if *wake_tick <= tick && self.agents.contains_key(agent_id) {
                    Some(agent_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for agent_id in &ready {
            self.next_wake_tick_by_agent.remove(agent_id);
            self.wake_reason_by_agent.remove(agent_id);
        }
        ready
    }
}
