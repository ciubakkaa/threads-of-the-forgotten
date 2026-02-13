#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SubmitCommandRequest {
    Raw(Command),
    Wrapped {
        command: Command,
        effective_tick: Option<u64>,
    },
}

impl SubmitCommandRequest {
    fn into_parts(self) -> (Command, Option<u64>) {
        match self {
            Self::Raw(command) => (command, None),
            Self::Wrapped {
                command,
                effective_tick,
            } => (command, effective_tick),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct PaginationQuery {
    cursor: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Debug, Serialize)]
struct CommandAuditPage {
    schema_version: String,
    run_id: String,
    cursor: usize,
    next_cursor: Option<usize>,
    entries: Vec<PersistedCommandEntry>,
}

async fn get_commands(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<CommandAuditPage>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;
        let entries = engine.command_log();
        let (start, end, next_cursor) = paginate(entries.len(), query.cursor, query.page_size)?;

        CommandAuditPage {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: run_id.clone(),
            cursor: start,
            next_cursor,
            entries: entries[start..end].to_vec(),
        }
    };

    Ok(Json(response))
}

#[derive(Debug, Deserialize, Default)]
struct TimelineQuery {
    from_tick: Option<u64>,
    to_tick: Option<u64>,
    #[serde(default)]
    event_types: Vec<String>,
    #[serde(rename = "event_types[]", default)]
    event_types_bracket: Vec<String>,
    actor_id: Option<String>,
    location_id: Option<String>,
    cursor: Option<usize>,
    page_size: Option<usize>,
}

async fn get_timeline(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Query(query): Query<TimelineQuery>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

        let current_tick = engine.status().current_tick;
        let from_tick = query.from_tick.unwrap_or(1);
        let to_tick = query.to_tick.unwrap_or(current_tick);

        if to_tick < from_tick {
            return Err(HttpApiError::invalid_query(
                "to_tick must be >= from_tick",
                Some(format!("from_tick={from_tick} to_tick={to_tick}")),
            ));
        }

        let mut requested_types = query.event_types;
        requested_types.extend(query.event_types_bracket);
        let event_type_filter = parse_event_type_filter(&requested_types)?;

        let mut filtered = Vec::new();
        for event in engine.events() {
            if event.tick < from_tick || event.tick > to_tick {
                continue;
            }

            if let Some(filter) = &event_type_filter {
                if !filter.contains(&event.event_type) {
                    continue;
                }
            }

            if let Some(actor_id) = &query.actor_id {
                if !event.actors.iter().any(|actor| actor.actor_id == *actor_id) {
                    continue;
                }
            }

            if let Some(location_id) = &query.location_id {
                if event.location_id != *location_id {
                    continue;
                }
            }

            filtered.push(event.clone());
        }

        let (start, end, next_cursor) = paginate(filtered.len(), query.cursor, query.page_size)?;

        QueryResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            query_type: "timeline.window".to_string(),
            run_id: run_id.clone(),
            generated_at_tick: current_tick,
            data: json!({
                "cursor": start,
                "next_cursor": next_cursor,
                "from_tick": from_tick,
                "to_tick": to_tick,
                "total": filtered.len(),
                "events": filtered[start..end].to_vec(),
            }),
        }
    };

    Ok(Json(response))
}

async fn get_event_detail(
    Path((run_id, event_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

        let Some(event) = engine
            .events()
            .iter()
            .find(|event| event.event_id == event_id)
        else {
            return Err(HttpApiError::invalid_query(
                "event_id not found",
                Some(format!("event_id={event_id}")),
            ));
        };

        let reason_packet = event.reason_packet_id.as_ref().and_then(|id| {
            engine
                .reason_packets()
                .iter()
                .find(|packet| packet.reason_packet_id == *id)
        });

        QueryResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            query_type: "event.detail".to_string(),
            run_id: run_id.clone(),
            generated_at_tick: engine.status().current_tick,
            data: json!({
                "event": event,
                "reason_packet": reason_packet,
            }),
        }
    };

    Ok(Json(response))
}

#[derive(Debug, Deserialize, Default)]
struct TraceQuery {
    depth: Option<usize>,
}

#[derive(Debug, Serialize)]
struct TraceNode {
    depth: usize,
    event: Event,
    reason_packet: Option<ReasonPacket>,
}

async fn get_trace(
    Path((run_id, event_id)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(query): Query<TraceQuery>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

        let Some(root_event) = engine
            .events()
            .iter()
            .find(|event| event.event_id == event_id)
        else {
            return Err(HttpApiError::invalid_query(
                "event_id not found",
                Some(format!("event_id={event_id}")),
            ));
        };

        let depth = query
            .depth
            .unwrap_or(DEFAULT_TRACE_DEPTH)
            .min(MAX_TRACE_DEPTH);

        let nodes = build_trace_nodes(root_event, engine.events(), engine.reason_packets(), depth);

        QueryResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            query_type: "trace.chain".to_string(),
            run_id: run_id.clone(),
            generated_at_tick: engine.status().current_tick,
            data: json!({
                "root_event_id": event_id,
                "depth": depth,
                "nodes": nodes,
            }),
        }
    };

    Ok(Json(response))
}

#[derive(Debug, Deserialize, Default)]
struct SnapshotsQuery {
    around_tick: Option<u64>,
    from_tick: Option<u64>,
    to_tick: Option<u64>,
}

async fn get_snapshots(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Query(query): Query<SnapshotsQuery>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

        let snapshots = if let Some(around_tick) = query.around_tick {
            match engine.load_latest_snapshot_at_or_before(&run_id, around_tick) {
                Ok(snapshot) => snapshot.into_iter().collect::<Vec<_>>(),
                Err(PersistenceError::NotAttached) => {
                    fallback_snapshot_window(engine, Some(around_tick), None, None)
                }
                Err(other) => return Err(HttpApiError::from_persistence(other)),
            }
        } else {
            let from_tick = query.from_tick.unwrap_or(0);
            let to_tick = query.to_tick.unwrap_or(engine.status().current_tick);

            if to_tick < from_tick {
                return Err(HttpApiError::invalid_query(
                    "to_tick must be >= from_tick",
                    Some(format!("from_tick={from_tick} to_tick={to_tick}")),
                ));
            }

            match engine.load_snapshots_range(&run_id, from_tick, to_tick) {
                Ok(snapshots) => snapshots,
                Err(PersistenceError::NotAttached) => {
                    fallback_snapshot_window(engine, None, Some(from_tick), Some(to_tick))
                }
                Err(other) => return Err(HttpApiError::from_persistence(other)),
            }
        };

        QueryResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            query_type: "snapshots.window".to_string(),
            run_id: run_id.clone(),
            generated_at_tick: engine.status().current_tick,
            data: json!({
                "count": snapshots.len(),
                "snapshots": snapshots,
            }),
        }
    };

    Ok(Json(response))
}

async fn submit_command(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<SubmitCommandRequest>,
) -> Result<Json<contracts::CommandResult>, HttpApiError> {
    let (command, effective_tick) = request.into_parts();
    if command.run_id != run_id {
        return Err(HttpApiError::invalid_command(
            "command.run_id must match path run_id",
            Some(format!(
                "path_run_id={run_id} command_run_id={}",
                command.run_id
            )),
        ));
    }

    let (result, messages) = {
        let mut inner = state.inner.lock().await;
        let (result, entry, status) = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            let result = engine.submit_command(command, effective_tick);
            let entry = engine.command_log().last().cloned();
            let status = engine.status().clone();
            (result, entry, status)
        };

        let mut messages = Vec::new();
        if let Some(entry) = entry {
            messages.push(StreamMessage::command_result(&entry, status.current_tick));
        }
        messages.extend(collect_delta_messages(&mut inner));
        messages.push(StreamMessage::run_status(&status));

        (result, messages)
    };

    broadcast_messages(&state, messages);

    Ok(Json(result))
}

