use super::*;

impl AgentWorld {
    pub(super) fn propagate_rumor(
        &mut self,
        rumor: &RumorPayload,
        source_id: &str,
        location_id: &str,
        tick: u64,
        sequence_in_tick: &mut u64,
        root_event_id: &str,
    ) {
        let mut delivered = Vec::new();
        let max_depth = u32::from(self.config.max_reaction_depth.max(1));
        if rumor.hop_count >= max_depth {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::ReactionDepthExceeded,
                location_id.to_string(),
                Vec::new(),
                None,
                vec![root_event_id.to_string()],
                Some(json!({ "depth": rumor.hop_count })),
            );
            return;
        }
        if source_id == "system" {
            delivered.extend(
                self.agents
                    .values()
                    .filter(|agent| agent.location_id == location_id)
                    .map(|agent| {
                        (
                            agent.id.clone(),
                            RumorPayload {
                                hop_count: 1,
                                source_npc_id: source_id.to_string(),
                                ..rumor.clone()
                            },
                        )
                    }),
            );
        } else {
            delivered.extend(
                self.rumor_network
                    .propagate(rumor, source_id, &self.social_graph),
            );
        }

        for (target_npc, payload) in delivered {
            if payload.hop_count > max_depth {
                continue;
            }
            let mut target_location = None;
            if let Some(agent) = self.agents.get_mut(&target_npc) {
                let source_trust = self
                    .social_graph
                    .get_edge(source_id, &target_npc)
                    .map(|edge| edge.trust)
                    .unwrap_or(30);
                agent
                    .beliefs
                    .update_from_rumor(&payload, &target_npc, tick, source_trust);
                target_location = Some(agent.location_id.clone());
            }
            if let Some(location) = target_location {
                let event_type = if payload.details.contains("distorted") {
                    EventType::RumorDistorted
                } else {
                    EventType::RumorPropagated
                };
                self.push_event(
                    tick,
                    sequence_in_tick,
                    event_type,
                    location,
                    vec![ActorRef {
                        actor_id: target_npc.clone(),
                        actor_kind: "receiver".to_string(),
                    }],
                    None,
                    vec![root_event_id.to_string()],
                    Some(json!({
                        "rumor_id": payload.rumor_id,
                        "source": source_id,
                        "hop_count": payload.hop_count,
                    })),
                );
                let latency = self.reaction_latency_for(true);
                self.schedule_unique(&target_npc, tick + latency, contracts::WakeReason::Reactive);
            }
        }
    }

    pub(super) fn schedule_observer_reactions(
        &mut self,
        location_id: &str,
        actor_id: &str,
        source_event_id: &str,
        directed_target: Option<&str>,
        tick: u64,
        sequence_in_tick: &mut u64,
    ) {
        let depth = self
            .reaction_depth_by_event
            .get(source_event_id)
            .copied()
            .unwrap_or(0);
        if depth >= self.config.max_reaction_depth {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::ReactionDepthExceeded,
                location_id.to_string(),
                Vec::new(),
                None,
                vec![source_event_id.to_string()],
                Some(json!({ "depth": depth })),
            );
            return;
        }

        let observation = self
            .event_index_by_id
            .get(source_event_id)
            .and_then(|idx| self.event_log.get(*idx))
            .map(|event| Observation {
                event_id: event.event_id.clone(),
                tick: event.tick,
                location_id: event.location_id.clone(),
                event_type: event.event_type,
                actors: event
                    .actors
                    .iter()
                    .map(|actor| actor.actor_id.clone())
                    .collect::<Vec<_>>(),
                visibility: 70,
                details: event.details.clone().unwrap_or_else(|| json!({})),
            });

        if let Some(target_npc) = directed_target {
            if target_npc != actor_id {
                if let Some(ref obs) = observation {
                    self.observation_bus
                        .publish_to_agent(target_npc, obs.clone());
                }
                let latency = self.reaction_latency_for(true);
                let wake_tick = tick + latency;
                self.schedule_unique(target_npc, wake_tick, contracts::WakeReason::Reactive);
            }
        }

        for observer in self.observation_bus.local_agents(location_id) {
            if observer == actor_id || directed_target == Some(observer.as_str()) {
                continue;
            }
            if let Some(ref obs) = observation {
                self.observation_bus
                    .publish_to_agent(&observer, obs.clone());
            }
            let latency = self.reaction_latency_for(false);
            let wake_tick = tick + latency;
            self.schedule_unique(&observer, wake_tick, contracts::WakeReason::Reactive);
        }
    }

    pub(super) fn reaction_latency_for(&self, directed: bool) -> u64 {
        if directed {
            0
        } else {
            1
        }
    }
}
