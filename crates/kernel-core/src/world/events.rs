use super::*;

impl AgentWorld {
    pub(super) fn emit_new_accounting_transfer_events(
        &mut self,
        tick: u64,
        sequence_in_tick: &mut u64,
    ) {
        let pending = self.economy_ledger.transfers[self.emitted_transfer_count..].to_vec();
        self.emitted_transfer_count = self.economy_ledger.transfers.len();
        for transfer in pending {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::AccountingTransferRecorded,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                transfer.cause_event_id.iter().cloned().collect::<Vec<_>>(),
                Some(json!({
                    "transfer_id": transfer.transfer_id,
                    "from": transfer.from_account,
                    "to": transfer.to_account,
                    "amount": transfer.amount,
                    "resource_kind": format!("{:?}", transfer.resource_kind).to_lowercase(),
                })),
            );
        }
    }

    pub(super) fn emit_pressure_event(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let pressure = self
            .agents
            .values()
            .map(|agent| agent.drives.drives.food.current + agent.drives.drives.income.current)
            .sum::<i64>();

        self.push_event(
            tick,
            sequence_in_tick,
            EventType::PressureEconomyUpdated,
            "region:crownvale".to_string(),
            Vec::new(),
            None,
            Vec::new(),
            Some(json!({ "aggregate_pressure": pressure })),
        );
    }

    pub(super) fn process_cadence_updates(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let agent_ids = self.agents.keys().cloned().collect::<Vec<_>>();
        if tick % self.config.rent_period_ticks.max(1) == 0 {
            for agent_id in &agent_ids {
                let tenant = format!("wallet:{agent_id}");
                let outcome = self
                    .economy_ledger
                    .process_rent(&tenant, "wallet:landlord", 2, tick);
                let event_type = if outcome.is_ok() {
                    EventType::RentPaid
                } else {
                    EventType::RentUnpaid
                };
                if let Some(agent) = self.agents.get_mut(agent_id) {
                    if outcome.is_ok() {
                        agent.life_domain.home_quality =
                            (agent.life_domain.home_quality + 1).clamp(0, 100);
                        agent.life_domain.property_value =
                            agent.life_domain.property_value.saturating_add(1);
                    } else {
                        agent.life_domain.home_quality =
                            (agent.life_domain.home_quality - 4).clamp(0, 100);
                        agent.life_domain.contract_failures =
                            agent.life_domain.contract_failures.saturating_add(1);
                    }
                    agent.life_domain.last_domain_event_tick = tick;
                }
                self.push_event(
                    tick,
                    sequence_in_tick,
                    event_type,
                    "settlement:greywall".to_string(),
                    vec![ActorRef {
                        actor_id: agent_id.to_string(),
                        actor_kind: "tenant".to_string(),
                    }],
                    None,
                    Vec::new(),
                    None,
                );
            }
        }

        if tick % self.config.wage_period_ticks_daily.max(1) == 0 {
            for agent_id in &agent_ids {
                let worker = format!("wallet:{agent_id}");
                let outcome = self
                    .economy_ledger
                    .process_wage("wallet:employer", &worker, 1, tick);
                let mut level_ups = 0_u8;
                let mut career_tier = 0_u8;
                let mut career_xp = 0_i64;
                if let Some(agent) = self.agents.get_mut(agent_id) {
                    if outcome.is_ok() {
                        agent.life_domain.career_xp = agent.life_domain.career_xp.saturating_add(5);
                        while agent.life_domain.career_xp >= 120 {
                            agent.life_domain.career_xp -= 120;
                            agent.life_domain.career_tier =
                                agent.life_domain.career_tier.saturating_add(1).min(6);
                            level_ups = level_ups.saturating_add(1);
                        }
                        career_tier = agent.life_domain.career_tier;
                        career_xp = agent.life_domain.career_xp;
                    } else {
                        agent.life_domain.social_reputation =
                            (agent.life_domain.social_reputation - 2).clamp(-100, 100);
                        agent.life_domain.contract_failures =
                            agent.life_domain.contract_failures.saturating_add(1);
                    }
                    agent.life_domain.last_domain_event_tick = tick;
                }
                for _ in 0..level_ups {
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::ApprenticeshipProgressed,
                        "settlement:greywall".to_string(),
                        vec![ActorRef {
                            actor_id: agent_id.to_string(),
                            actor_kind: "worker".to_string(),
                        }],
                        None,
                        Vec::new(),
                        Some(json!({
                            "career_tier": career_tier,
                            "career_xp": career_xp,
                        })),
                    );
                }
                self.push_event(
                    tick,
                    sequence_in_tick,
                    if outcome.is_ok() {
                        EventType::WagePaid
                    } else {
                        EventType::WageDelayed
                    },
                    "settlement:greywall".to_string(),
                    vec![ActorRef {
                        actor_id: agent_id.to_string(),
                        actor_kind: "worker".to_string(),
                    }],
                    None,
                    Vec::new(),
                    None,
                );
            }
        }

        if tick % self.config.production_rate_ticks.max(1) == 0 {
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::ProductionStarted,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "node_id": "node:mill" })),
            );
            if self.stock_fuel > 0 {
                self.stock_fuel -= 1;
                self.stock_staples += 3;
                self.production_backlog = (self.production_backlog - 2).max(0);
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::ProductionCompleted,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "staples_added": 3 })),
                );
            } else {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::StockShortage,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "resource": "fuel" })),
                );
            }
        }

        if tick % self.config.spoilage_rate_ticks.max(1) == 0 {
            let spoiled = (self.stock_staples / 20).max(1);
            self.stock_staples = (self.stock_staples - spoiled).max(0);
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::SpoilageOccurred,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "staples_spoiled": spoiled })),
            );
        }

        if tick % (self.config.market_clearing_frequency_ticks.max(1)) == 0 {
            self.market_price_index = if self.stock_staples < 60 {
                (self.market_price_index + 2).min(180)
            } else if self.stock_staples > 140 {
                (self.market_price_index - 2).max(60)
            } else {
                self.market_price_index
            };
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::MarketCleared,
                "settlement:greywall".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "price_index": self.market_price_index })),
            );
            if self.stock_staples < 40 {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::MarketFailed,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "reason": "insufficient_supply" })),
                );
            } else {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::StockRecovered,
                    "settlement:greywall".to_string(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Some(json!({ "staples": self.stock_staples })),
                );
            }
        }

        if tick % 24 == 0 {
            let weather = match (tick / 24) % 4 {
                0 => Weather::Clear,
                1 => Weather::Rain,
                2 => Weather::Storm,
                _ => Weather::Snow,
            };
            self.spatial_model.update_weather(weather);
            self.weather = format!("{weather:?}").to_lowercase();
            self.push_event(
                tick,
                sequence_in_tick,
                EventType::RouteRiskUpdated,
                "region:crownvale".to_string(),
                Vec::new(),
                None,
                Vec::new(),
                Some(json!({ "weather": self.weather })),
            );

            for edge in self.social_graph.edges.values_mut() {
                if edge.trust > 0 {
                    edge.trust = (edge.trust - self.config.trust_decay_rate.max(0)).max(0);
                } else if edge.trust < 0 {
                    edge.trust = (edge.trust + self.config.trust_decay_rate.max(0)).min(0);
                }
            }

            self.update_groups_from_social_graph(tick, sequence_in_tick);
        }
    }

    pub(super) fn update_groups_from_social_graph(
        &mut self,
        tick: u64,
        sequence_in_tick: &mut u64,
    ) {
        let cohesion = self.config.group_cohesion_threshold;
        let edge_view = self
            .social_graph
            .edges
            .iter()
            .map(|((from, to), edge)| (from.clone(), to.clone(), edge.trust))
            .collect::<Vec<_>>();
        for (from, to, trust) in edge_view {
            let group_id = format!("group:{}:{}", from, to);
            if trust >= cohesion {
                if !self.groups.contains_key(&group_id) {
                    self.groups
                        .insert(group_id.clone(), vec![from.clone(), to.clone()]);
                    self.push_event(
                        tick,
                        sequence_in_tick,
                        EventType::GroupFormed,
                        "settlement:greywall".to_string(),
                        vec![ActorRef {
                            actor_id: from.clone(),
                            actor_kind: "founder".to_string(),
                        }],
                        None,
                        Vec::new(),
                        Some(json!({ "group_id": group_id, "member": to })),
                    );
                }
            } else if trust <= cohesion / 2 && self.groups.remove(&group_id).is_some() {
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::GroupDissolved,
                    "settlement:greywall".to_string(),
                    vec![ActorRef {
                        actor_id: from.clone(),
                        actor_kind: "former_member".to_string(),
                    }],
                    None,
                    Vec::new(),
                    Some(json!({ "group_id": group_id, "member": to })),
                );
            }
        }
    }

    pub(super) fn process_institutions(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let mut pending_events = Vec::new();
        for institution in self.institutions.values_mut() {
            institution.degrade_quality(1);
            let institution_id = institution.institution_id.clone();
            let outcomes = institution.process_cycle();
            for outcome in outcomes {
                pending_events.push((
                    EventType::InstitutionCaseResolved,
                    vec![ActorRef {
                        actor_id: outcome.npc_id,
                        actor_kind: "requester".to_string(),
                    }],
                    Some(json!({
                        "institution_id": institution_id,
                        "quality_score": outcome.quality_score,
                    })),
                ));
            }
            if institution.queue.len() >= institution.capacity {
                pending_events.push((
                    EventType::InstitutionQueueFull,
                    Vec::new(),
                    Some(json!({ "institution_id": institution_id })),
                ));
            }
            if institution.corruption_bias > 60 {
                pending_events.push((
                    EventType::InstitutionCorruptionEvent,
                    Vec::new(),
                    Some(json!({ "institution_id": institution_id, "corruption": institution.corruption_bias })),
                ));
            }
        }
        for (event_type, actors, details) in pending_events {
            self.push_event(
                tick,
                sequence_in_tick,
                event_type,
                "settlement:greywall".to_string(),
                actors,
                None,
                Vec::new(),
                details,
            );
        }
    }

    pub(super) fn process_joint_contracts(&mut self, tick: u64, sequence_in_tick: &mut u64) {
        let contract_ids = self.joint_contracts.keys().cloned().collect::<Vec<_>>();
        for contract_id in contract_ids {
            let Some(contract) = self.joint_contracts.get(&contract_id).cloned() else {
                continue;
            };
            if !matches!(
                contract.status.as_str(),
                "active" | "accepted" | "executing"
            ) {
                continue;
            }
            if tick < contract.due_tick {
                continue;
            }

            let mut fulfilled = true;
            for participant in &contract.participants {
                let progress =
                    contract
                        .contributions
                        .get(participant)
                        .cloned()
                        .unwrap_or(JointContribution {
                            money_paid: 0,
                            actions_done: 0,
                        });
                if progress.money_paid < contract.required_money_each
                    || progress.actions_done < contract.required_actions_each
                {
                    fulfilled = false;
                    break;
                }
            }

            if fulfilled {
                if let Some(existing) = self.joint_contracts.get_mut(&contract_id) {
                    existing.status = "completed".to_string();
                    existing.last_updated_tick = tick;
                }
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::CommitmentCompleted,
                    "settlement:greywall".to_string(),
                    contract
                        .participants
                        .iter()
                        .map(|npc_id| ActorRef {
                            actor_id: npc_id.clone(),
                            actor_kind: "joint_participant".to_string(),
                        })
                        .collect::<Vec<_>>(),
                    None,
                    Vec::new(),
                    Some(json!({
                        "contract_id": contract.contract_id,
                        "arc_id": contract.arc_id,
                        "arc_kind": contract.arc_kind,
                        "required_money_each": contract.required_money_each,
                        "required_actions_each": contract.required_actions_each,
                        "status": "completed",
                    })),
                );
                for participant in &contract.participants {
                    if let Some(agent) = self.agents.get_mut(participant) {
                        agent.life_domain.contract_successes =
                            agent.life_domain.contract_successes.saturating_add(1);
                        agent.life_domain.home_quality =
                            (agent.life_domain.home_quality + 4).clamp(0, 100);
                        agent.life_domain.property_value =
                            agent.life_domain.property_value.saturating_add(5);
                        agent.life_domain.last_domain_event_tick = tick;
                    }
                }
            } else {
                if let Some(existing) = self.joint_contracts.get_mut(&contract_id) {
                    existing.status = "breached".to_string();
                    existing.last_updated_tick = tick;
                }
                let deficits = contract
                    .participants
                    .iter()
                    .map(|participant| {
                        let progress =
                            contract
                                .contributions
                                .get(participant)
                                .cloned()
                                .unwrap_or(JointContribution {
                                    money_paid: 0,
                                    actions_done: 0,
                                });
                        json!({
                            "participant": participant,
                            "money_paid": progress.money_paid,
                            "required_money": contract.required_money_each,
                            "actions_done": progress.actions_done,
                            "required_actions": contract.required_actions_each,
                        })
                    })
                    .collect::<Vec<_>>();
                self.push_event(
                    tick,
                    sequence_in_tick,
                    EventType::ContractBreached,
                    "settlement:greywall".to_string(),
                    contract
                        .participants
                        .iter()
                        .map(|npc_id| ActorRef {
                            actor_id: npc_id.clone(),
                            actor_kind: "joint_participant".to_string(),
                        })
                        .collect::<Vec<_>>(),
                    None,
                    Vec::new(),
                    Some(json!({
                        "contract_id": contract.contract_id,
                        "arc_id": contract.arc_id,
                        "arc_kind": contract.arc_kind,
                        "stage": "joint_execution",
                        "reason": "deadline_unmet",
                        "deficits": deficits,
                        "status": "breached",
                    })),
                );
                for participant in &contract.participants {
                    if let Some(agent) = self.agents.get_mut(participant) {
                        agent.life_domain.contract_failures =
                            agent.life_domain.contract_failures.saturating_add(1);
                        agent.life_domain.social_reputation =
                            (agent.life_domain.social_reputation - 5).clamp(-100, 100);
                        agent.life_domain.last_domain_event_tick = tick;
                    }
                }
            }
        }
    }

    pub(super) fn push_reason_packet(
        &mut self,
        tick: u64,
        actor_id: &str,
        envelope: &ReasonEnvelope,
    ) -> String {
        let id = format!("reason_{tick}_{:04}", self.reason_packet_log.len() + 1);
        self.reason_packet_log.push(ReasonPacket {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, 0),
            reason_packet_id: id.clone(),
            actor_id: actor_id.to_string(),
            chosen_action: envelope
                .operator_chain
                .first()
                .cloned()
                .unwrap_or_else(|| "idle".to_string()),
            top_intents: vec![envelope.goal.label.clone()],
            top_beliefs: Vec::new(),
            top_pressures: envelope
                .drive_pressures
                .iter()
                .map(|(kind, pressure)| format!("{kind:?}:{pressure}"))
                .collect(),
            alternatives_considered: envelope
                .rejected_alternatives
                .iter()
                .map(|rejected| rejected.plan_id.clone())
                .collect(),
            motive_families: Vec::new(),
            feasibility_checks: Vec::new(),
            chosen_verb: None,
            context_constraints: envelope.contextual_constraints.clone(),
            why_chain: vec![
                format!("goal={}", envelope.goal.label),
                format!("planning_mode={:?}", envelope.planning_mode),
            ],
            expected_consequences: Vec::new(),
            goal_id: Some(envelope.goal.goal_id.clone()),
            plan_id: Some(envelope.selected_plan.plan_id.clone()),
            operator_chain_ids: envelope.operator_chain.clone(),
            blocked_plan_ids: envelope
                .rejected_alternatives
                .iter()
                .map(|rejected| rejected.plan_id.clone())
                .collect(),
            selection_rationale: "utility_maximization".to_string(),
        });
        id
    }

    pub(super) fn push_event(
        &mut self,
        tick: u64,
        sequence_in_tick: &mut u64,
        event_type: EventType,
        location_id: String,
        actors: Vec<ActorRef>,
        reason_packet_id: Option<String>,
        caused_by: Vec<String>,
        details: Option<Value>,
    ) -> String {
        *sequence_in_tick = sequence_in_tick.saturating_add(1);
        let event_id = format!("evt_{tick:06}_{:04}", *sequence_in_tick);
        let location_key = location_id.clone();
        let depth = caused_by
            .first()
            .and_then(|parent| self.reaction_depth_by_event.get(parent))
            .copied()
            .unwrap_or(0);
        self.event_log.push(Event {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick,
            created_at: synthetic_timestamp(tick, *sequence_in_tick),
            event_id: event_id.clone(),
            sequence_in_tick: *sequence_in_tick,
            event_type,
            location_id,
            actors,
            reason_packet_id,
            caused_by,
            targets: Vec::new(),
            tags: Vec::new(),
            visibility: Some("public".to_string()),
            details,
        });
        self.event_index_by_id
            .insert(event_id.clone(), self.event_log.len().saturating_sub(1));
        let location_events = self
            .recent_events_by_location
            .entry(location_key)
            .or_default();
        location_events.push_back(event_id.clone());
        while location_events.len() > 256 {
            let _ = location_events.pop_front();
        }
        self.replay_hash = mix_replay_hash(self.replay_hash, &event_id, tick, *sequence_in_tick);
        self.reaction_depth_by_event
            .insert(event_id.clone(), depth.saturating_add(1));
        event_id
    }
}
