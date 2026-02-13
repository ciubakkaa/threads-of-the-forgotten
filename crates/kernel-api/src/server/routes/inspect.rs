async fn get_npc_inspector(
    Path((run_id, npc_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

        if let Some(data) = engine.inspect_npc(&npc_id) {
            return Ok(Json(QueryResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                query_type: "npc.inspect".to_string(),
                run_id: run_id.clone(),
                generated_at_tick: engine.status().current_tick,
                data,
            }));
        }

        let snapshot = engine.snapshot_for_current_tick();
        let npc_events = engine
            .events()
            .iter()
            .filter(|event| event.actors.iter().any(|actor| actor.actor_id == npc_id))
            .cloned()
            .collect::<Vec<_>>();

        if npc_events.is_empty() {
            return Err(HttpApiError::invalid_query(
                "npc_id not found in run events",
                Some(format!("npc_id={npc_id}")),
            ));
        }

        let current_location = npc_events
            .last()
            .map(|event| event.location_id.clone())
            .unwrap_or_else(|| "unknown".to_string());

        let recent_actions = npc_events
            .iter()
            .rev()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .take(8)
            .cloned()
            .collect::<Vec<_>>();

        let reason_packets_by_id = engine
            .reason_packets()
            .iter()
            .map(|packet| (packet.reason_packet_id.as_str(), packet))
            .collect::<HashMap<_, _>>();

        let latest_reason_packet = recent_actions
            .first()
            .and_then(|event| event.reason_packet_id.as_ref())
            .and_then(|reason_id| reason_packets_by_id.get(reason_id.as_str()))
            .copied();

        let recent_belief_updates = recent_actions
            .iter()
            .filter_map(|event| event.reason_packet_id.as_ref())
            .filter_map(|reason_id| reason_packets_by_id.get(reason_id.as_str()))
            .flat_map(|packet| packet.top_beliefs.clone())
            .take(12)
            .collect::<Vec<_>>();
        let npc_ledger = snapshot
            .npc_state_refs
            .get("npc_ledgers")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("npc_id")
                        .and_then(Value::as_str)
                        .map(|value| value == npc_id)
                        .unwrap_or(false)
                })
            })
            .cloned();
        let household_id = npc_ledger
            .as_ref()
            .and_then(|entry| entry.get("household_id"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let household_status = household_id.as_ref().and_then(|current_household_id| {
            snapshot
                .npc_state_refs
                .get("households")
                .and_then(Value::as_array)
                .and_then(|entries| {
                    entries.iter().find(|entry| {
                        entry
                            .get("household_id")
                            .and_then(Value::as_str)
                            .map(|value| value == current_household_id)
                            .unwrap_or(false)
                    })
                })
                .cloned()
        });
        let contract_status = snapshot
            .region_state
            .get("labor")
            .and_then(|labor| labor.get("contracts"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("worker_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let relationship_edges = snapshot
            .region_state
            .get("relationships")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        let source_matches = entry
                            .get("source_npc_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false);
                        let target_matches = entry
                            .get("target_npc_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false);
                        source_matches || target_matches
                    })
                    .take(10)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let active_beliefs = snapshot
            .region_state
            .get("beliefs")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("npc_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false)
                    })
                    .take(10)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let opportunities = snapshot
            .region_state
            .get("opportunities")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("npc_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false)
                    })
                    .take(8)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let blocked_opportunities = opportunities
            .iter()
            .filter(|entry| {
                entry
                    .get("status")
                    .and_then(Value::as_str)
                    .map(|value| value.eq_ignore_ascii_case("blocked"))
                    .unwrap_or(false)
            })
            .take(8)
            .cloned()
            .collect::<Vec<_>>();
        let commitments = snapshot
            .region_state
            .get("commitments")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("npc_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let time_budget = snapshot
            .region_state
            .get("time_budgets")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("npc_id")
                        .and_then(Value::as_str)
                        .map(|value| value == npc_id)
                        .unwrap_or(false)
                })
            })
            .cloned();
        let occupancy_state = snapshot
            .region_state
            .get("occupancy")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("npc_id")
                        .and_then(Value::as_str)
                        .map(|value| value == npc_id)
                        .unwrap_or(false)
                })
            })
            .cloned();
        let active_plan = snapshot
            .region_state
            .get("active_plans")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("npc_id")
                        .and_then(Value::as_str)
                        .map(|value| value == npc_id)
                        .unwrap_or(false)
                })
            })
            .cloned();
        let drive_state = snapshot
            .region_state
            .get("drives")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("npc_id")
                        .and_then(Value::as_str)
                        .map(|value| value == npc_id)
                        .unwrap_or(false)
                })
            })
            .cloned();
        let why_summaries = snapshot
            .region_state
            .get("narrative_summaries")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("actor_id")
                            .and_then(Value::as_str)
                            .map(|value| value == npc_id)
                            .unwrap_or(false)
                    })
                    .take(6)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let motive_chain = latest_reason_packet
            .map(|packet| packet.why_chain.clone())
            .unwrap_or_default();
        let recent_action_event_ids = recent_actions
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<HashSet<_>>();
        let active_plan_id = active_plan
            .as_ref()
            .and_then(|entry| entry.get("plan_id"))
            .and_then(Value::as_str);
        let operator_effect_trace = snapshot
            .region_state
            .get("operator_effect_trace")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        let source_event_id = entry
                            .get("source_event_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let source_plan_id = entry
                            .get("source_plan_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        recent_action_event_ids.contains(source_event_id)
                            || active_plan_id
                                .map(|plan_id| plan_id == source_plan_id)
                                .unwrap_or(false)
                    })
                    .take(16)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        QueryResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            query_type: "npc.inspect".to_string(),
            run_id: run_id.clone(),
            generated_at_tick: engine.status().current_tick,
            data: json!({
                "npc_id": npc_id,
                "current_location": current_location,
                "top_intents": latest_reason_packet.map(|packet| packet.top_intents.clone()).unwrap_or_default(),
                "last_action": recent_actions.first(),
                "reason_packet": latest_reason_packet,
                "recent_belief_updates": recent_belief_updates,
                "recent_actions": recent_actions,
                "household_status": household_status,
                "npc_ledger": npc_ledger,
                "contract_status": contract_status,
                "relationship_edges": relationship_edges,
                "active_beliefs": active_beliefs,
                "opportunities": opportunities,
                "blocked_opportunities": blocked_opportunities,
                "commitments": commitments,
                "time_budget": time_budget,
                "occupancy_state": occupancy_state,
                "active_plan": active_plan,
                "drive_state": drive_state,
                "operator_effect_trace": operator_effect_trace,
                "motive_chain": motive_chain,
                "why_summaries": why_summaries,
                "key_relationships": relationship_edges.clone(),
            }),
        }
    };

    Ok(Json(response))
}

async fn get_npc_arc_summary(
    Path((run_id, npc_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;
        if let Some(data) = engine.inspect_npc_arc_summary(&npc_id) {
            QueryResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                query_type: "npc.arc_summary".to_string(),
                run_id: run_id.clone(),
                generated_at_tick: engine.status().current_tick,
                data,
            }
        } else {
            return Err(HttpApiError::invalid_query(
                "npc_id not found in run state",
                Some(format!("npc_id={npc_id}")),
            ));
        }
    };

    Ok(Json(response))
}

async fn get_npc_storyline(
    Path((run_id, npc_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;
        if let Some(data) = engine.inspect_npc_storyline(&npc_id) {
            QueryResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                query_type: "npc.storyline".to_string(),
                run_id: run_id.clone(),
                generated_at_tick: engine.status().current_tick,
                data,
            }
        } else {
            return Err(HttpApiError::invalid_query(
                "npc_id not found in run state",
                Some(format!("npc_id={npc_id}")),
            ));
        }
    };

    Ok(Json(response))
}

async fn get_settlement_inspector(
    Path((run_id, settlement_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

        if let Some(data) = engine.inspect_settlement(&settlement_id) {
            return Ok(Json(QueryResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                query_type: "settlement.inspect".to_string(),
                run_id: run_id.clone(),
                generated_at_tick: engine.status().current_tick,
                data,
            }));
        }

        let snapshot = engine.snapshot_for_current_tick();
        let current_tick = engine.status().current_tick;
        let recent_from_tick = current_tick.saturating_sub(48);

        let settlement_events = engine
            .events()
            .iter()
            .filter(|event| event.location_id == settlement_id)
            .cloned()
            .collect::<Vec<_>>();

        if settlement_events.is_empty() {
            return Err(HttpApiError::invalid_query(
                "settlement_id not found in run events",
                Some(format!("settlement_id={settlement_id}")),
            ));
        }

        let recent_events = settlement_events
            .iter()
            .filter(|event| event.tick >= recent_from_tick)
            .cloned()
            .collect::<Vec<_>>();

        let rumor_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::RumorInjected)
            .count();
        let bad_harvest_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::BadHarvestForced)
            .count();
        let caravan_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::CaravanSpawned)
            .count();
        let removed_npc_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::NpcRemoved)
            .count();
        let theft_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::TheftCommitted)
            .count();
        let investigation_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::InvestigationProgressed)
            .count();
        let arrest_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::ArrestMade)
            .count();
        let discovery_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::SiteDiscovered)
            .count();
        let leverage_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::LeverageGained)
            .count();
        let relationship_shift_count = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::RelationshipShifted)
            .count();
        let recent_action_events = recent_events
            .iter()
            .filter(|event| event.event_type == EventType::NpcActionCommitted)
            .collect::<Vec<_>>();
        let active_actor_count = recent_action_events
            .iter()
            .filter_map(|event| event.actors.first().map(|actor| actor.actor_id.as_str()))
            .collect::<HashSet<_>>()
            .len();
        let window_ticks = current_tick.saturating_sub(recent_from_tick).max(1) + 1;
        let actions_per_tick = recent_action_events.len() as f64 / window_ticks as f64;
        let actions_per_npc_per_day = if active_actor_count == 0 {
            0.0
        } else {
            recent_action_events.len() as f64
                / active_actor_count as f64
                / (window_ticks as f64 / 24.0)
        };
        let occupancy_mix = snapshot
            .region_state
            .get("occupancy")
            .and_then(Value::as_array)
            .map(|entries| {
                let mut mix = HashMap::<String, usize>::new();
                for entry in entries {
                    let is_local = entry
                        .get("location_id")
                        .and_then(Value::as_str)
                        .map(|value| value == settlement_id)
                        .unwrap_or(false);
                    if !is_local {
                        continue;
                    }
                    let occupancy = entry
                        .get("occupancy")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    *mix.entry(occupancy).or_insert(0) += 1;
                }
                mix
            })
            .unwrap_or_default();

        let stock_ledger = snapshot
            .region_state
            .get("production")
            .and_then(|value| value.get("stocks"))
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("settlement_id")
                        .and_then(Value::as_str)
                        .map(|value| value == settlement_id)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .unwrap_or_else(|| json!({}));
        let staples = stock_ledger
            .get("staples")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let food_status = if staples <= 6 {
            "low"
        } else if staples >= 14 {
            "surplus"
        } else {
            "stable"
        };

        let security_signal = rumor_count + removed_npc_count + theft_count;
        let security_status = if security_signal >= 4 {
            "unrest"
        } else if security_signal >= 2 {
            "tense"
        } else {
            "calm"
        };

        let institution_profile = snapshot
            .region_state
            .get("institutions")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("settlement_id")
                        .and_then(Value::as_str)
                        .map(|value| value == settlement_id)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .unwrap_or_else(|| json!({}));
        let corruption = institution_profile
            .get("corruption_level")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let capacity = institution_profile
            .get("enforcement_capacity")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let institutional_health = if corruption >= 55 {
            "corrupt"
        } else if capacity <= 35 {
            "fragile"
        } else {
            "clean"
        };

        let law_status = if investigation_count + arrest_count >= 3 {
            "active_enforcement"
        } else if theft_count > 0 {
            "fragile"
        } else if investigation_count > 0 {
            "watchful"
        } else {
            "quiet"
        };
        let labor_market = snapshot
            .region_state
            .get("labor")
            .and_then(|value| value.get("markets"))
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("settlement_id")
                        .and_then(Value::as_str)
                        .map(|value| value == settlement_id)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .unwrap_or_else(|| json!({}));
        let production_nodes = snapshot
            .region_state
            .get("production")
            .and_then(|value| value.get("nodes"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("settlement_id")
                            .and_then(Value::as_str)
                            .map(|value| value == settlement_id)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let groups = snapshot
            .region_state
            .get("groups")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("settlement_id")
                            .and_then(Value::as_str)
                            .map(|value| value == settlement_id)
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let routes = snapshot
            .region_state
            .get("mobility")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        let origin = entry
                            .get("origin_settlement_id")
                            .and_then(Value::as_str)
                            .map(|value| value == settlement_id)
                            .unwrap_or(false);
                        let destination = entry
                            .get("destination_settlement_id")
                            .and_then(Value::as_str)
                            .map(|value| value == settlement_id)
                            .unwrap_or(false);
                        origin || destination
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let market_clearing = snapshot
            .region_state
            .get("market_clearing")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("settlement_id")
                        .and_then(Value::as_str)
                        .map(|value| value == settlement_id)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .unwrap_or_else(|| json!({}));
        let institution_queue = snapshot
            .region_state
            .get("institution_queue")
            .and_then(Value::as_array)
            .and_then(|entries| {
                entries.iter().find(|entry| {
                    entry
                        .get("settlement_id")
                        .and_then(Value::as_str)
                        .map(|value| value == settlement_id)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .unwrap_or_else(|| json!({}));
        let accounting_transfers = snapshot
            .region_state
            .get("accounting_transfers")
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .get("settlement_id")
                            .and_then(Value::as_str)
                            .map(|value| value == settlement_id)
                            .unwrap_or(false)
                    })
                    .take(20)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        QueryResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            query_type: "settlement.inspect".to_string(),
            run_id: run_id.clone(),
            generated_at_tick: current_tick,
            data: json!({
                "settlement_id": settlement_id,
                "food_status": food_status,
                "security_status": security_status,
                "institutional_health": institutional_health,
                "pressure_readouts": {
                    "rumors_recent": rumor_count,
                    "bad_harvest_recent": bad_harvest_count,
                    "caravan_recent": caravan_count,
                    "npc_removed_recent": removed_npc_count,
                    "theft_recent": theft_count,
                    "investigation_recent": investigation_count,
                    "arrest_recent": arrest_count,
                    "discoveries_recent": discovery_count,
                    "leverage_gains_recent": leverage_count,
                    "relationship_shifts_recent": relationship_shift_count,
                },
                "law_status": law_status,
                "labor_market": labor_market,
                "stock_ledger": stock_ledger,
                "institution_profile": institution_profile,
                "production_nodes": production_nodes,
                "groups": groups,
                "routes": routes,
                "occupancy_mix": occupancy_mix,
                "action_cadence": {
                    "window_ticks": window_ticks,
                    "actions_per_tick": actions_per_tick,
                    "actions_per_npc_per_day": actions_per_npc_per_day,
                    "active_actor_count": active_actor_count,
                },
                "market_clearing": market_clearing,
                "institution_queue": institution_queue,
                "accounting_transfers": accounting_transfers,
                "notable_events": recent_events.into_iter().rev().take(20).collect::<Vec<_>>(),
            }),
        }
    };

    Ok(Json(response))
}
