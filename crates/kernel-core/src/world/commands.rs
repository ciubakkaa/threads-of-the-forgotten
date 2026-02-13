use super::*;

impl AgentWorld {
    pub(super) fn process_due_commands(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        self.queued_commands.sort_by(|a, b| {
            a.effective_tick
                .cmp(&b.effective_tick)
                .then(a.insertion_sequence.cmp(&b.insertion_sequence))
        });

        let mut future = Vec::new();
        let mut due = Vec::new();
        for queued in self.queued_commands.drain(..) {
            if queued.effective_tick <= tick {
                due.push(queued);
            } else {
                future.push(queued);
            }
        }
        self.queued_commands = future;
        self.sync_queue_depth();

        for queued in due {
            self.apply_command(queued.command, tick, sequence_in_tick);
        }
    }

    pub(super) fn apply_command(
        &mut self,
        command: Command,
        tick: u64,
        sequence_in_tick: &mut u64,
    ) {
        let command_ref = format!("cmd:{}", command.command_id);
        match &command.payload {
            CommandPayload::SimStart => self.start(),
            CommandPayload::SimPause => self.pause(),
            CommandPayload::SimStepTick { .. } | CommandPayload::SimRunToTick { .. } => {}
            CommandPayload::InjectRumor {
                location_id,
                rumor_text,
            } => {
                let root_event_id = self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::RumorInjected,
                    location_id.clone(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    Some(json!({ "rumor_text": rumor_text })),
                );
                let rumor = RumorPayload {
                    rumor_id: format!("rumor:{tick}:{}", self.event_log.len()),
                    source_npc_id: "system".to_string(),
                    core_claim: rumor_text.clone(),
                    details: rumor_text.clone(),
                    hop_count: 0,
                };
                self.propagate_rumor(
                    &rumor,
                    "system",
                    location_id,
                    tick,
                    sequence_in_tick,
                    &root_event_id,
                );
            }
            CommandPayload::InjectSpawnCaravan {
                origin_settlement_id,
                destination_settlement_id,
            } => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::CaravanSpawned,
                    origin_settlement_id.clone(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    Some(json!({
                        "destination_settlement_id": destination_settlement_id,
                    })),
                );
            }
            CommandPayload::InjectRemoveNpc { npc_id } => {
                if let Some(agent) = self.agents.remove(npc_id) {
                    self.observation_bus
                        .remove_agent(npc_id, &agent.location_id);
                    self.next_wake_tick_by_agent.remove(npc_id);
                    self.wake_reason_by_agent.remove(npc_id);
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::NpcRemoved,
                        "settlement:greywall".to_string(),
                        vec![ActorRef {
                            actor_id: npc_id.clone(),
                            actor_kind: "removed".to_string(),
                        }],
                        None,
                        vec![command_ref.clone()],
                        None,
                    );
                }
            }
            CommandPayload::InjectForceBadHarvest { settlement_id } => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::BadHarvestForced,
                    settlement_id.clone(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    None,
                );
            }
            CommandPayload::InjectSetWinterSeverity { severity } => {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::WinterSeveritySet,
                    "region:crownvale".to_string(),
                    Vec::new(),
                    None,
                    vec![command_ref.clone()],
                    Some(json!({ "severity": severity })),
                );
            }
        }

        self.push_event(
            tick,
            sequence_in_tick,
            EventType::CommandApplied,
            "system".to_string(),
            Vec::new(),
            None,
            vec![command_ref],
            Some(json!({ "command_type": command.command_type })),
        );
    }
}
