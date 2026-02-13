use super::*;

impl AgentWorld {
    pub fn snapshot_for_current_tick(&self) -> Snapshot {
        let recent_transfer_start = self.economy_ledger.transfers.len().saturating_sub(256);
        let recent_transfers = self.economy_ledger.transfers[recent_transfer_start..]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let drives = self
            .agents
            .values()
            .map(|agent| {
                json!({
                    "npc_id": agent.id,
                    "tick": self.status.current_tick,
                    "need_food": agent.drives.drives.food.current,
                    "need_shelter": agent.drives.drives.shelter.current,
                    "need_income": agent.drives.drives.income.current,
                    "need_safety": agent.drives.drives.safety.current,
                    "need_belonging": agent.drives.drives.belonging.current,
                    "need_status": agent.drives.drives.status.current,
                    "need_recovery": agent.drives.drives.health.current,
                    "stress": (agent.drives.drives.safety.current + agent.drives.drives.health.current) / 2,
                })
            })
            .collect::<Vec<_>>();

        let occupancy = self
            .agents
            .values()
            .map(|agent| serde_json::to_value(&agent.occupancy).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();

        let active_plans = self
            .agents
            .values()
            .filter_map(|agent| agent.active_plan.as_ref())
            .map(|plan| serde_json::to_value(plan).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();
        let commitments = self
            .agents
            .values()
            .flat_map(|agent| agent.commitments.iter())
            .take(1_024)
            .map(|commitment| serde_json::to_value(commitment).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();
        let goal_tracks = self
            .agents
            .values()
            .flat_map(|agent| agent.goal_tracks.iter())
            .take(2_048)
            .map(|track| {
                json!({
                    "npc_id": track.track_id.split(':').nth(1).unwrap_or_default(),
                    "track_id": track.track_id,
                    "plan_id": track.plan_id,
                    "goal_family": track.goal_family,
                    "stage": track.stage,
                    "status": format!("{:?}", track.status).to_lowercase(),
                    "progress_ticks": track.progress_ticks,
                    "inertia_score": track.inertia_score,
                    "utility_hint": track.utility_hint,
                    "blockers": track.blockers,
                    "last_advanced_tick": track.last_advanced_tick,
                    "last_break_reason": track.last_break_reason,
                })
            })
            .collect::<Vec<_>>();
        let arc_tracks = self
            .agents
            .values()
            .flat_map(|agent| {
                agent.arc_tracks.iter().map(move |track| {
                    json!({
                        "npc_id": agent.id,
                        "arc_id": track.arc_id,
                        "title": track.title,
                        "family": track.family,
                        "milestone_index": track.milestone_index,
                        "milestones": track.milestones,
                        "stage": track
                            .milestones
                            .get(track.milestone_index)
                            .cloned()
                            .unwrap_or_else(|| "complete".to_string()),
                        "progress": track.progress,
                        "status": format!("{:?}", track.status).to_lowercase(),
                        "blockers": track.blockers,
                        "last_advanced_tick": track.last_advanced_tick,
                        "last_break_reason": track.last_break_reason,
                    })
                })
            })
            .take(2_048)
            .collect::<Vec<_>>();
        let shared_arcs = self
            .shared_arcs
            .values()
            .take(1_024)
            .map(|arc| {
                json!({
                    "arc_id": arc.arc_id,
                    "participants": arc.participants,
                    "arc_kind": arc.arc_kind,
                    "stage_index": arc.stage_index,
                    "stage": arc.stage_label,
                    "progress": arc.progress,
                    "status": arc.status,
                    "joint_plan_status": arc.joint_plan_status,
                    "joint_plan_id": arc.joint_plan_id,
                    "joint_plan_proposer": arc.joint_plan_proposer,
                    "joint_plan_last_actor": arc.joint_plan_last_actor,
                    "joint_plan_updated_tick": arc.joint_plan_updated_tick,
                    "started_tick": arc.started_tick,
                    "last_updated_tick": arc.last_updated_tick,
                    "root_event_id": arc.root_event_id,
                })
            })
            .collect::<Vec<_>>();
        let joint_contracts = self
            .joint_contracts
            .values()
            .take(1_024)
            .map(|contract| {
                json!({
                    "contract_id": contract.contract_id,
                    "arc_id": contract.arc_id,
                    "participants": contract.participants,
                    "arc_kind": contract.arc_kind,
                    "status": contract.status,
                    "proposed_tick": contract.proposed_tick,
                    "due_tick": contract.due_tick,
                    "required_money_each": contract.required_money_each,
                    "required_actions_each": contract.required_actions_each,
                    "contributions": contract
                        .contributions
                        .iter()
                        .map(|(npc, contribution)| json!({
                            "npc_id": npc,
                            "money_paid": contribution.money_paid,
                            "actions_done": contribution.actions_done,
                        }))
                        .collect::<Vec<_>>(),
                    "last_updated_tick": contract.last_updated_tick,
                })
            })
            .collect::<Vec<_>>();

        let beliefs = self
            .agents
            .values()
            .flat_map(|agent| agent.beliefs.claims.iter())
            .take(2_048)
            .map(|belief| serde_json::to_value(belief).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>();

        let npc_ledgers = self
            .agents
            .values()
            .map(|agent| {
                json!({
                    "npc_id": agent.id,
                    "household_id": format!("household:{}", agent.id),
                    "wallet": self
                        .economy_ledger
                        .accounts
                        .get(&format!("wallet:{}", agent.id))
                        .map(|acc| acc.money)
                        .unwrap_or(0),
                    "debt_balance": 0,
                    "food_reserve_days": 2,
                    "shelter_status": "stable",
                    "dependents_count": 0,
                    "profession": agent.identity.profession,
                    "aspiration": agent.aspiration.label,
                    "career_tier": agent.life_domain.career_tier,
                    "career_xp": agent.life_domain.career_xp,
                    "marital_status": agent.life_domain.marital_status,
                    "spouse_npc_id": agent.life_domain.spouse_npc_id,
                    "children_count": agent.life_domain.children_count,
                    "home_quality": agent.life_domain.home_quality,
                    "property_value": agent.life_domain.property_value,
                    "social_reputation": agent.life_domain.social_reputation,
                    "health": 100 - agent.drives.drives.health.current,
                    "illness_ticks": 0,
                })
            })
            .collect::<Vec<_>>();

        Snapshot {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: self.status.run_id.clone(),
            tick: self.status.current_tick,
            created_at: synthetic_timestamp(self.status.current_tick, 0),
            snapshot_id: format!("snap_{:06}", self.status.current_tick),
            world_state_hash: format!("{:016x}", self.state_hash),
            region_state: json!({
                "pressure_index": self.agents.values().map(|agent| agent.drives.drives.food.current).sum::<i64>(),
                "winter_severity": 40,
                "social_cohesion": 0,
                "drives": drives,
                "occupancy": occupancy,
                "active_plans": active_plans,
                "beliefs": beliefs,
                "relationships": self
                    .social_graph
                    .edges
                    .values()
                    .map(|edge| serde_json::to_value(edge).unwrap_or_else(|_| json!({})))
                    .collect::<Vec<_>>(),
                "opportunities": [],
                "commitments": commitments,
                "goal_tracks": goal_tracks,
                "arc_tracks": arc_tracks,
                "shared_arcs": shared_arcs,
                "joint_contracts": joint_contracts,
                "time_budgets": self
                    .agents
                    .values()
                    .map(|agent| json!({
                        "npc_id": agent.id,
                        "tick": self.status.current_tick,
                        "sleep_hours": agent.time_budget.recovery / 2,
                        "work_hours": agent.time_budget.work / 4,
                        "care_hours": agent.time_budget.household / 6,
                        "travel_hours": agent.time_budget.exploration / 8,
                        "social_hours": agent.time_budget.social / 4,
                        "recovery_hours": agent.time_budget.recovery / 4,
                        "free_hours": (100 - (agent.time_budget.work + agent.time_budget.social + agent.time_budget.household)).max(0) / 5,
                    }))
                    .collect::<Vec<_>>(),
                "narrative_summaries": self
                    .event_log
                    .iter()
                    .rev()
                    .filter(|event| event.event_type == EventType::NarrativeWhySummary)
                    .take(128)
                    .map(|event| json!({
                        "summary_id": event.event_id,
                        "tick": event.tick,
                        "location_id": event.location_id,
                        "actor_id": event
                            .actors
                            .first()
                            .map(|actor| actor.actor_id.clone())
                            .unwrap_or_default(),
                        "motive_chain": event
                            .details
                            .as_ref()
                            .and_then(|details| details.get("stage"))
                            .and_then(Value::as_str)
                            .map(|stage| vec![stage.to_string()])
                            .unwrap_or_default(),
                        "failed_alternatives": [],
                        "social_consequence": event
                            .details
                            .as_ref()
                            .and_then(|details| details.get("status"))
                            .and_then(Value::as_str)
                            .unwrap_or("unknown"),
                    }))
                    .collect::<Vec<_>>(),
                "operator_effect_trace": [],
                "institutions": self
                    .institutions
                    .values()
                    .map(|institution| json!({
                        "settlement_id": "settlement:greywall",
                        "institution_id": institution.institution_id,
                        "enforcement_capacity": institution.capacity,
                        "corruption_level": institution.corruption_bias,
                        "bias_level": institution.corruption_bias / 2,
                        "response_latency_ticks": institution.processing_latency_ticks,
                    }))
                    .collect::<Vec<_>>(),
                "groups": [],
                "group_entities": self
                    .groups
                    .iter()
                    .map(|(group_id, members)| json!({
                        "group_id": group_id,
                        "settlement_id": "settlement:greywall",
                        "leader_npc_id": members.first().cloned().unwrap_or_default(),
                        "member_npc_ids": members,
                        "norm_tags": ["mutual_aid"],
                        "cohesion_score": self.config.group_cohesion_threshold,
                    }))
                    .collect::<Vec<_>>(),
                "mobility": self
                    .spatial_model
                    .routes
                    .values()
                    .map(|route| json!({
                        "route_id": route.route_id,
                        "origin_settlement_id": route.origin,
                        "destination_settlement_id": route.destination,
                        "travel_time_ticks": route.travel_time,
                        "hazard_score": route.risk_level,
                        "weather_window_open": true,
                    }))
                    .collect::<Vec<_>>(),
                "institution_queue": self
                    .institutions
                    .values()
                    .map(|institution| json!({
                        "settlement_id": "settlement:greywall",
                        "pending_cases": institution.queue.len() as i64,
                        "processed_cases": 0,
                        "dropped_cases": 0,
                        "avg_response_latency": institution.processing_latency_ticks as i64,
                    }))
                    .collect::<Vec<_>>(),
                "accounting_transfers": self
                    .economy_ledger.transfers[recent_transfer_start..]
                    .iter()
                    .map(|transfer| json!({
                        "transfer_id": transfer.transfer_id,
                        "tick": transfer.tick,
                        "settlement_id": "settlement:greywall",
                        "from_account": transfer.from_account,
                        "to_account": transfer.to_account,
                        "resource_kind": format!("{:?}", transfer.resource_kind).to_lowercase(),
                        "amount": transfer.amount,
                        "cause_event_id": transfer.cause_event_id,
                    }))
                    .collect::<Vec<_>>(),
                "market_clearing": [json!({
                    "settlement_id": "settlement:greywall",
                    "staples_price_index": self.market_price_index,
                    "fuel_price_index": self.market_price_index + (100 - self.stock_fuel).max(0) / 5,
                    "medicine_price_index": self.market_price_index + (100 - self.stock_medicine).max(0) / 4,
                    "wage_pressure": 0,
                    "shortage_score": (100 - self.stock_staples).max(0) / 2,
                    "unmet_demand": (80 - self.stock_staples).max(0),
                    "cleared_tick": self.status.current_tick,
                    "market_cleared": true,
                })],
                "labor": {"contracts": [], "markets": []},
                "production": {
                    "stocks": [json!({
                        "settlement_id": "settlement:greywall",
                        "staples": self.stock_staples,
                        "fuel": self.stock_fuel,
                        "medicine": self.stock_medicine,
                        "craft_inputs": self.production_backlog.max(0),
                        "local_price_pressure": (self.market_price_index - 100),
                        "coin_reserve": self
                            .economy_ledger
                            .accounts
                            .get("wallet:market")
                            .map(|acc| acc.money)
                            .unwrap_or(0),
                    })],
                    "nodes": [json!({
                        "node_id": "node:mill",
                        "settlement_id": "settlement:greywall",
                        "node_kind": "grain_mill",
                        "input_backlog": self.production_backlog.max(0),
                        "output_backlog": self.stock_staples.max(0),
                        "spoilage_timer": self.config.spoilage_rate_ticks,
                    })]
                }
            }),
            settlement_states: json!([]),
            npc_state_refs: json!({
                "npc_ledgers": npc_ledgers,
                "households": []
            }),
            diff_from_prev_snapshot: None,
            perf_stats: Some(json!({
                "event_count": self.event_log.len(),
                "reason_packet_count": self.reason_packet_log.len(),
                "scheduler_pending": self.next_wake_tick_by_agent.len(),
            })),
            agents: self.agent_snapshots(),
            scheduler_state: Some(SchedulerSnapshot {
                current_tick: self.status.current_tick,
                pending: self
                    .next_wake_tick_by_agent
                    .iter()
                    .map(|(agent_id, wake_tick)| ScheduledEvent {
                        wake_tick: *wake_tick,
                        priority: 0,
                        agent_id: agent_id.clone(),
                        reason: self
                            .wake_reason_by_agent
                            .get(agent_id)
                            .copied()
                            .unwrap_or(WakeReason::Idle),
                    })
                    .collect::<Vec<_>>(),
            }),
            world_state_v2: Some(WorldStateSnapshot {
                tick: self.status.current_tick,
                weather: self.weather.clone(),
                payload: json!({
                    "agent_count": self.agents.len(),
                    "reason_envelope_count": self.reason_envelopes.len(),
                }),
            }),
            economy_ledger_v2: Some(EconomyLedgerSnapshot {
                accounts: self.economy_ledger.accounts.clone(),
                transfers: recent_transfers,
            }),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_for_current_tick()
    }

    pub(super) fn agent_snapshots(&self) -> Vec<AgentSnapshot> {
        self.agents
            .values()
            .map(|agent| AgentSnapshot {
                agent_id: agent.id.clone(),
                identity: agent.identity.clone(),
                drives: agent.drives.drives.clone(),
                beliefs: agent.beliefs.claims.clone(),
                memories: agent.memory.memories.clone(),
                active_plan: agent.active_plan.clone(),
                occupancy: agent.occupancy.clone(),
                aspiration: agent.aspiration.clone(),
                location_id: agent.location_id.clone(),
                next_wake_tick: self
                    .next_wake_tick_by_agent
                    .get(&agent.id)
                    .copied()
                    .unwrap_or(self.status.current_tick),
            })
            .collect()
    }
}
