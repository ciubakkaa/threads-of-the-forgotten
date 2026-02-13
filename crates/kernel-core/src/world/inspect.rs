use super::*;

impl AgentWorld {
    pub fn inspect_npc(&self, npc_id: &str) -> Option<Value> {
        self.agents.get(npc_id).map(|agent| {
            let latest_reason = self
                .reason_envelopes
                .iter()
                .rev()
                .find(|reason| reason.agent_id == npc_id)
                .cloned();
            let relationships = self
                .social_graph
                .edges
                .values()
                .filter(|edge| edge.source_npc_id == npc_id || edge.target_npc_id == npc_id)
                .cloned()
                .collect::<Vec<_>>();
            let goal_tracks = agent
                .goal_tracks
                .iter()
                .map(|track| {
                    json!({
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
            let arc_tracks = agent
                .arc_tracks
                .iter()
                .map(|track| {
                    json!({
                        "arc_id": track.arc_id,
                        "title": track.title,
                        "family": track.family,
                        "milestone_index": track.milestone_index,
                        "milestones": track.milestones,
                        "current_stage": track
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
                .collect::<Vec<_>>();
            let shared_arcs = self
                .shared_arcs
                .values()
                .filter(|arc| {
                    arc.participants
                        .iter()
                        .any(|participant| participant == npc_id)
                })
                .map(|arc| {
                    json!({
                        "arc_id": arc.arc_id,
                        "arc_kind": arc.arc_kind,
                        "participants": arc.participants,
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
            let arc_timeline = self
                .event_log
                .iter()
                .rev()
                .filter(|event| {
                    matches!(
                        event.event_type,
                        EventType::NarrativeWhySummary
                            | EventType::RomanceAdvanced
                            | EventType::GroupMembershipChanged
                            | EventType::CommitmentBroken
                    ) && event.actors.iter().any(|actor| actor.actor_id == npc_id)
                })
                .take(20)
                .cloned()
                .collect::<Vec<_>>();
            let arc_summary =
                arc_tracks
                    .iter()
                    .fold(BTreeMap::<String, i64>::new(), |mut acc, track| {
                        let status = track
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        *acc.entry(status.to_string()).or_insert(0) += 1;
                        acc
                    });
            let joint_contracts = self
                .joint_contracts
                .values()
                .filter(|contract| {
                    contract
                        .participants
                        .iter()
                        .any(|participant| participant == npc_id)
                })
                .map(|contract| {
                    json!({
                        "contract_id": contract.contract_id,
                        "arc_id": contract.arc_id,
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
            let storyline = self.synthesize_storyline_for_npc(npc_id);

            json!({
                "npc_id": npc_id,
                "current_location": agent.location_id,
                "drives": agent.drives.drives,
                "active_plan": agent.active_plan,
                "queued_plans": agent.plan_backlog,
                "commitments": agent.commitments,
                "goal_tracks": goal_tracks,
                "arc_tracks": arc_tracks,
                "shared_arcs": shared_arcs,
                "joint_contracts": joint_contracts,
                "arc_timeline": arc_timeline,
                "arc_summary": arc_summary,
                "storyline": storyline,
                "fatigue": agent.fatigue,
                "recreation_need": agent.recreation_need,
                "time_budget": {
                    "work": agent.time_budget.work,
                    "social": agent.time_budget.social,
                    "recovery": agent.time_budget.recovery,
                    "household": agent.time_budget.household,
                    "exploration": agent.time_budget.exploration,
                },
                "life_domain": {
                    "career_tier": agent.life_domain.career_tier,
                    "career_xp": agent.life_domain.career_xp,
                    "marital_status": agent.life_domain.marital_status,
                    "spouse_npc_id": agent.life_domain.spouse_npc_id,
                    "children_count": agent.life_domain.children_count,
                    "home_quality": agent.life_domain.home_quality,
                    "property_value": agent.life_domain.property_value,
                    "social_reputation": agent.life_domain.social_reputation,
                    "contract_successes": agent.life_domain.contract_successes,
                    "contract_failures": agent.life_domain.contract_failures,
                    "last_domain_event_tick": agent.life_domain.last_domain_event_tick,
                },
                "occupancy": agent.occupancy,
                "beliefs": agent
                    .beliefs
                    .claims
                    .iter()
                    .rev()
                    .take(128)
                    .cloned()
                    .collect::<Vec<_>>(),
                "relationships": relationships,
                "last_committed_operator": self.last_operator_by_agent.get(npc_id).cloned(),
                "last_operator_family": self.last_family_by_agent.get(npc_id).cloned(),
                "operator_family_streak": self.family_streak_by_agent.get(npc_id).copied().unwrap_or(0),
                "reactive_streak": agent.reactive_streak,
                "last_plan_id": agent.active_plan.as_ref().map(|plan| plan.plan_id.clone()),
                "last_reason_envelope": latest_reason.clone(),
                "pending_observations_count": self.observation_bus.pending_count(npc_id),
                "recent_reason_envelope": latest_reason,
                "recent_actions": self
                    .event_log
                    .iter()
                    .rev()
                    .filter(|event| {
                        event.event_type == EventType::NpcActionCommitted
                            && event.actors.iter().any(|actor| actor.actor_id == npc_id)
                    })
                    .take(8)
                    .cloned()
                    .collect::<Vec<_>>(),
            })
        })
    }

    pub fn inspect_settlement(&self, settlement_id: &str) -> Option<Value> {
        if settlement_id.is_empty() {
            return None;
        }

        let recent_events = self
            .event_log
            .iter()
            .rev()
            .filter(|event| event.location_id == settlement_id)
            .take(96)
            .cloned()
            .collect::<Vec<_>>();
        let action_summary = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .fold(BTreeMap::<String, i64>::new(), |mut acc, event| {
                let key = event
                    .details
                    .as_ref()
                    .and_then(|details| details.get("operator_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                *acc.entry(key).or_insert(0) += 1;
                acc
            });
        let action_family_summary = action_summary.iter().fold(
            BTreeMap::<String, i64>::new(),
            |mut acc, (operator_id, count)| {
                let family = operator_id
                    .split(':')
                    .next()
                    .unwrap_or("unknown")
                    .to_string();
                *acc.entry(family).or_insert(0) += *count;
                acc
            },
        );
        let local_npcs = self
            .agents
            .values()
            .filter(|agent| agent.location_id == settlement_id)
            .map(|agent| agent.id.clone())
            .collect::<BTreeSet<_>>();
        let shared_arc_summary = self
            .shared_arcs
            .values()
            .filter(|arc| {
                arc.participants
                    .iter()
                    .any(|participant| local_npcs.contains(participant))
            })
            .fold(BTreeMap::<String, i64>::new(), |mut acc, arc| {
                *acc.entry(format!("{}:{}", arc.arc_kind, arc.status))
                    .or_insert(0) += 1;
                acc
            });
        let npc_arc_summary = self
            .agents
            .values()
            .filter(|agent| agent.location_id == settlement_id)
            .flat_map(|agent| agent.arc_tracks.iter())
            .fold(BTreeMap::<String, i64>::new(), |mut acc, track| {
                *acc.entry(format!(
                    "{}:{}",
                    track.family,
                    format!("{:?}", track.status).to_lowercase()
                ))
                .or_insert(0) += 1;
                acc
            });
        let joint_contract_summary = self
            .joint_contracts
            .values()
            .filter(|contract| {
                contract
                    .participants
                    .iter()
                    .any(|participant| local_npcs.contains(participant))
            })
            .fold(BTreeMap::<String, i64>::new(), |mut acc, contract| {
                *acc.entry(contract.status.clone()).or_insert(0) += 1;
                acc
            });

        Some(json!({
            "settlement_id": settlement_id,
            "current_tick": self.status.current_tick,
            "local_npc_count": self
                .agents
                .values()
                .filter(|agent| agent.location_id == settlement_id)
                .count(),
            "recent_events": recent_events,
            "institution_queues": self
                .institutions
                .values()
                .map(|institution| json!({
                    "institution_id": institution.institution_id,
                    "pending": institution.queue.len(),
                    "capacity": institution.capacity,
                    "quality_rating": institution.quality_rating,
                    "corruption_bias": institution.corruption_bias,
                }))
                .collect::<Vec<_>>(),
            "market_state": {
                "price_index": self.market_price_index,
                "stock_staples": self.stock_staples,
                "stock_fuel": self.stock_fuel,
                "stock_medicine": self.stock_medicine,
                "clearing": if self.stock_staples < 40 { "failed" } else { "stable" },
            },
            "economic_state": {
                "accounts": self.economy_ledger.accounts.len(),
                "transfers": self.economy_ledger.transfers.len(),
            },
            "local_activity_summary": action_summary,
            "local_activity_family_summary": action_family_summary,
            "local_arc_summary": npc_arc_summary,
            "shared_arc_summary": shared_arc_summary,
            "joint_contract_summary": joint_contract_summary,
            "shared_arcs": self
                .shared_arcs
                .values()
                .filter(|arc| {
                    arc.participants
                        .iter()
                        .any(|participant| local_npcs.contains(participant))
                })
                .take(64)
                .map(|arc| json!({
                    "arc_id": arc.arc_id,
                    "arc_kind": arc.arc_kind,
                    "participants": arc.participants,
                    "stage": arc.stage_label,
                    "progress": arc.progress,
                    "status": arc.status,
                    "joint_plan_status": arc.joint_plan_status,
                    "joint_plan_id": arc.joint_plan_id,
                    "joint_plan_proposer": arc.joint_plan_proposer,
                    "joint_plan_last_actor": arc.joint_plan_last_actor,
                    "joint_plan_updated_tick": arc.joint_plan_updated_tick,
                    "last_updated_tick": arc.last_updated_tick,
                }))
                .collect::<Vec<_>>(),
            "joint_contracts": self
                .joint_contracts
                .values()
                .filter(|contract| {
                    contract
                        .participants
                        .iter()
                        .any(|participant| local_npcs.contains(participant))
                })
                .take(96)
                .map(|contract| json!({
                    "contract_id": contract.contract_id,
                    "arc_id": contract.arc_id,
                    "arc_kind": contract.arc_kind,
                    "participants": contract.participants,
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
                }))
                .collect::<Vec<_>>(),
        }))
    }

    pub fn inspect_npc_arc_summary(&self, npc_id: &str) -> Option<Value> {
        let agent = self.agents.get(npc_id)?;
        let mut arcs = agent
            .arc_tracks
            .iter()
            .map(|track| {
                json!({
                    "arc_id": track.arc_id,
                    "title": track.title,
                    "family": track.family,
                    "stage": track
                        .milestones
                        .get(track.milestone_index)
                        .cloned()
                        .unwrap_or_else(|| "complete".to_string()),
                    "milestone_index": track.milestone_index,
                    "milestone_count": track.milestones.len(),
                    "progress": track.progress,
                    "status": format!("{:?}", track.status).to_lowercase(),
                    "blockers": track.blockers,
                    "last_advanced_tick": track.last_advanced_tick,
                    "last_break_reason": track.last_break_reason,
                })
            })
            .collect::<Vec<_>>();
        arcs.sort_by(|a, b| {
            let a_status = a
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let b_status = b
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            a_status.cmp(&b_status)
        });

        let shared_arcs = self
            .shared_arcs
            .values()
            .filter(|arc| {
                arc.participants
                    .iter()
                    .any(|participant| participant == npc_id)
            })
            .map(|arc| {
                json!({
                    "arc_id": arc.arc_id,
                    "arc_kind": arc.arc_kind,
                    "participants": arc.participants,
                    "stage": arc.stage_label,
                    "status": arc.status,
                    "progress": arc.progress,
                    "joint_plan_status": arc.joint_plan_status,
                    "joint_plan_id": arc.joint_plan_id,
                    "joint_plan_proposer": arc.joint_plan_proposer,
                    "joint_plan_last_actor": arc.joint_plan_last_actor,
                    "joint_plan_updated_tick": arc.joint_plan_updated_tick,
                    "last_updated_tick": arc.last_updated_tick,
                })
            })
            .collect::<Vec<_>>();

        let timeline = self
            .event_log
            .iter()
            .filter(|event| {
                event.actors.iter().any(|actor| actor.actor_id == npc_id)
                    && matches!(
                        event.event_type,
                        EventType::NarrativeWhySummary
                            | EventType::OpportunityOpened
                            | EventType::OpportunityAccepted
                            | EventType::OpportunityRejected
                            | EventType::RomanceAdvanced
                            | EventType::CommitmentStarted
                            | EventType::CommitmentCompleted
                            | EventType::CommitmentBroken
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let joint_contracts = self
            .joint_contracts
            .values()
            .filter(|contract| {
                contract
                    .participants
                    .iter()
                    .any(|participant| participant == npc_id)
            })
            .map(|contract| {
                json!({
                    "contract_id": contract.contract_id,
                    "arc_id": contract.arc_id,
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

        let summary_counts = arcs
            .iter()
            .fold(BTreeMap::<String, i64>::new(), |mut acc, arc| {
                let status = arc
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                *acc.entry(status).or_insert(0) += 1;
                acc
            });
        let open_blockers = arcs
            .iter()
            .filter_map(|arc| {
                let blockers = arc.get("blockers").and_then(Value::as_array)?;
                if blockers.is_empty() {
                    return None;
                }
                let title = arc
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let blocker_text = blockers
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",");
                Some(format!("{title}:{blocker_text}"))
            })
            .collect::<Vec<_>>();

        Some(json!({
            "npc_id": npc_id,
            "tick": self.status.current_tick,
            "life_domain": {
                "career_tier": agent.life_domain.career_tier,
                "career_xp": agent.life_domain.career_xp,
                "marital_status": agent.life_domain.marital_status,
                "spouse_npc_id": agent.life_domain.spouse_npc_id,
                "children_count": agent.life_domain.children_count,
                "home_quality": agent.life_domain.home_quality,
                "property_value": agent.life_domain.property_value,
                "social_reputation": agent.life_domain.social_reputation,
                "contract_successes": agent.life_domain.contract_successes,
                "contract_failures": agent.life_domain.contract_failures,
                "last_domain_event_tick": agent.life_domain.last_domain_event_tick,
            },
            "arc_summary": summary_counts,
            "arc_tracks": arcs,
            "shared_arcs": shared_arcs,
            "joint_contracts": joint_contracts,
            "open_blockers": open_blockers,
            "timeline": timeline,
            "storyline": self.synthesize_storyline_for_npc(npc_id),
        }))
    }

    pub fn inspect_npc_storyline(&self, npc_id: &str) -> Option<Value> {
        self.agents.get(npc_id)?;
        Some(self.synthesize_storyline_for_npc(npc_id))
    }

    fn synthesize_storyline_for_npc(&self, npc_id: &str) -> Value {
        let Some(agent) = self.agents.get(npc_id) else {
            return json!({
                "headline": "unknown_npc",
                "chapters": [],
                "summary_text": "NPC not found."
            });
        };
        let completed_arcs = agent
            .arc_tracks
            .iter()
            .filter(|track| matches!(track.status, crate::agent::ArcTrackStatus::Completed))
            .map(|track| track.title.clone())
            .collect::<Vec<_>>();
        let blocked_arcs = agent
            .arc_tracks
            .iter()
            .filter(|track| matches!(track.status, crate::agent::ArcTrackStatus::Blocked))
            .map(|track| {
                format!(
                    "{}({})",
                    track.title,
                    track
                        .blockers
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "blocked".to_string())
                )
            })
            .collect::<Vec<_>>();
        let shared_arc_snippets = self
            .shared_arcs
            .values()
            .filter(|arc| {
                arc.participants
                    .iter()
                    .any(|participant| participant == npc_id)
            })
            .take(6)
            .map(|arc| format!("{}:{}:{}", arc.arc_kind, arc.stage_label, arc.status))
            .collect::<Vec<_>>();
        let joint_contracts = self
            .joint_contracts
            .values()
            .filter(|contract| {
                contract
                    .participants
                    .iter()
                    .any(|participant| participant == npc_id)
            })
            .map(|contract| format!("{}:{}", contract.arc_kind, contract.status))
            .collect::<Vec<_>>();
        let chapter_1 = format!(
            "{} is a {} focused on {}.",
            npc_id, agent.identity.profession, agent.aspiration.label
        );
        let chapter_2 = if completed_arcs.is_empty() {
            format!(
                "Current arcs are still unfolding; active families include {}.",
                agent
                    .arc_tracks
                    .iter()
                    .map(|track| track.family.clone())
                    .take(4)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            format!("Completed arcs so far: {}.", completed_arcs.join(", "))
        };
        let chapter_3 = format!(
            "Life domain: career tier {}, marital status {}, children {}, home quality {}.",
            agent.life_domain.career_tier,
            agent.life_domain.marital_status,
            agent.life_domain.children_count,
            agent.life_domain.home_quality
        );
        let chapter_4 = if blocked_arcs.is_empty() {
            "No major blockers currently reported.".to_string()
        } else {
            format!("Open blockers: {}.", blocked_arcs.join("; "))
        };
        let chapter_5 = if shared_arc_snippets.is_empty() {
            "No shared social arcs yet.".to_string()
        } else {
            format!(
                "Shared arcs recently: {}. Contracts: {}.",
                shared_arc_snippets.join(", "),
                if joint_contracts.is_empty() {
                    "none".to_string()
                } else {
                    joint_contracts.join(", ")
                }
            )
        };

        json!({
            "headline": format!("storyline:{}:t{}", npc_id, self.status.current_tick),
            "chapters": [chapter_1, chapter_2, chapter_3, chapter_4, chapter_5],
            "summary_text": format!(
                "{} Career tier {}, reputation {}, household value {}.",
                npc_id,
                agent.life_domain.career_tier,
                agent.life_domain.social_reputation,
                agent.life_domain.property_value
            ),
        })
    }

    pub fn traverse_causal_chain(&self, event_id: &str) -> Vec<Event> {
        let mut chain = Vec::new();
        let mut cursor = self
            .event_log
            .iter()
            .find(|event| event.event_id == event_id)
            .cloned();
        let mut guard = 0_usize;

        while let Some(event) = cursor {
            if guard > self.event_log.len() + 1 {
                break;
            }
            guard += 1;
            let parent = event.caused_by.first().cloned();
            chain.push(event.clone());
            cursor = parent.and_then(|parent_id| {
                self.event_log
                    .iter()
                    .find(|candidate| candidate.event_id == parent_id)
                    .cloned()
            });
        }

        chain
    }
}
