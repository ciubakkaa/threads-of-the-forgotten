use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::net::SocketAddr;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::header::{HeaderName, HeaderValue};
use axum::http::Method;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use contracts::{
    ApiError, Command, ErrorCode, Event, EventType, QueryResponse, ReasonPacket, RunConfig,
    RunStatus, Snapshot, SCHEMA_VERSION_V1,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

use crate::{EngineApi, PersistedCommandEntry, PersistenceError};

const DEFAULT_PAGE_SIZE: usize = 500;
const MAX_PAGE_SIZE: usize = 5000;
const DEFAULT_TRACE_DEPTH: usize = 12;
const MAX_TRACE_DEPTH: usize = 64;
const DEFAULT_SQLITE_PATH: &str = "threads_runs.sqlite";

#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "server io error: {err}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Clone)]
struct AppState {
    inner: std::sync::Arc<Mutex<ServerInner>>,
    stream_tx: broadcast::Sender<StreamMessage>,
}

impl AppState {
    fn new() -> Self {
        let (stream_tx, _) = broadcast::channel(4096);
        Self {
            inner: std::sync::Arc::new(Mutex::new(ServerInner::default())),
            stream_tx,
        }
    }
}

#[derive(Debug, Default)]
struct ServerInner {
    engine: Option<EngineApi>,
    emitted_event_count: usize,
    last_snapshot_tick: Option<u64>,
}

#[derive(Debug)]
struct HttpApiError {
    status: StatusCode,
    error: ApiError,
}

impl HttpApiError {
    fn run_not_found(requested_run_id: &str, active_run_id: Option<&str>) -> Self {
        let details = active_run_id
            .map(|active| format!("requested_run_id={requested_run_id} active_run_id={active}"));
        Self {
            status: StatusCode::NOT_FOUND,
            error: ApiError::new(
                ErrorCode::RunNotFound,
                "run_id does not match an active run",
                details,
            ),
        }
    }

    fn invalid_query(message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: ApiError::new(ErrorCode::InvalidQuery, message, details),
        }
    }

    fn invalid_command(message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: ApiError::new(ErrorCode::InvalidCommand, message, details),
        }
    }

    fn internal(message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: ApiError::new(ErrorCode::InternalError, message, details),
        }
    }

    fn from_persistence(err: PersistenceError) -> Self {
        match err {
            PersistenceError::NotAttached => {
                Self::invalid_query("persistence store is not attached", None)
            }
            PersistenceError::RunAlreadyExists(run_id) => Self {
                status: StatusCode::CONFLICT,
                error: ApiError::new(
                    ErrorCode::RunStateConflict,
                    "run_id already exists; pass replace_existing=true to replace",
                    Some(format!("run_id={run_id}")),
                ),
            },
            other => Self::internal("persistence operation failed", Some(other.to_string())),
        }
    }
}

impl IntoResponse for HttpApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.error)).into_response()
    }
}

pub async fn serve(addr: SocketAddr) -> Result<(), ServerError> {
    let state = AppState::new();
    let app = router(state);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/runs", post(create_run))
        .route("/api/v1/runs/{run_id}/start", post(start_run))
        .route("/api/v1/runs/{run_id}/pause", post(pause_run))
        .route("/api/v1/runs/{run_id}/step", post(step_run))
        .route("/api/v1/runs/{run_id}/run_to_tick", post(run_to_tick))
        .route("/api/v1/runs/{run_id}/status", get(get_status))
        .route(
            "/api/v1/runs/{run_id}/commands",
            post(submit_command).get(get_commands),
        )
        .route("/api/v1/runs/{run_id}/timeline", get(get_timeline))
        .route(
            "/api/v1/runs/{run_id}/events/{event_id}",
            get(get_event_detail),
        )
        .route("/api/v1/runs/{run_id}/trace/{event_id}", get(get_trace))
        .route("/api/v1/runs/{run_id}/npc/{npc_id}", get(get_npc_inspector))
        .route(
            "/api/v1/runs/{run_id}/settlement/{settlement_id}",
            get(get_settlement_inspector),
        )
        .route("/api/v1/runs/{run_id}/snapshots", get(get_snapshots))
        .route("/api/v1/runs/{run_id}/stream", get(stream_run))
        .layer(middleware::from_fn(cors_middleware))
        .with_state(state)
}

async fn cors_middleware(request: Request, next: Next) -> Response {
    if request.method() == Method::OPTIONS {
        let mut response = Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        apply_cors_headers(response.headers_mut());
        return response;
    }

    let mut response = next.run(request).await;
    apply_cors_headers(response.headers_mut());
    response
}

fn apply_cors_headers(headers: &mut axum::http::HeaderMap) {
    headers.insert(
        HeaderName::from_static("access-control-allow-origin"),
        HeaderValue::from_static("*"),
    );
    headers.insert(
        HeaderName::from_static("access-control-allow-methods"),
        HeaderValue::from_static("GET,POST,OPTIONS,PUT,PATCH,DELETE"),
    );
    headers.insert(
        HeaderName::from_static("access-control-allow-headers"),
        HeaderValue::from_static("*"),
    );
    headers.insert(
        HeaderName::from_static("access-control-max-age"),
        HeaderValue::from_static("3600"),
    );
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CreateRunRequest {
    Config(RunConfig),
    WithOptions(CreateRunOptions),
}

#[derive(Debug, Deserialize)]
struct CreateRunOptions {
    config: RunConfig,
    auto_start: Option<bool>,
    sqlite_path: Option<String>,
    replace_existing: Option<bool>,
}

#[derive(Debug, Serialize)]
struct CreateRunResponse {
    schema_version: String,
    run_id: String,
    status: RunStatus,
    replaced_existing_run: bool,
    started: bool,
}

async fn create_run(
    State(state): State<AppState>,
    Json(request): Json<CreateRunRequest>,
) -> Result<Json<CreateRunResponse>, HttpApiError> {
    let (config, auto_start, sqlite_path, replace_existing) = match request {
        CreateRunRequest::Config(config) => (config, false, Some(default_sqlite_path()), true),
        CreateRunRequest::WithOptions(options) => (
            options.config,
            options.auto_start.unwrap_or(false),
            Some(
                options
                    .sqlite_path
                    .filter(|path| !path.trim().is_empty())
                    .unwrap_or_else(default_sqlite_path),
            ),
            options.replace_existing.unwrap_or(true),
        ),
    };

    let (response, messages) = {
        let mut inner = state.inner.lock().await;
        let replaced_existing_run = inner.engine.is_some();

        let mut engine = EngineApi::from_config(config.clone());
        if let Some(path) = sqlite_path {
            engine
                .attach_sqlite_store(path)
                .map_err(HttpApiError::from_persistence)?;
            engine
                .initialize_run_storage(replace_existing)
                .map_err(HttpApiError::from_persistence)?;
        }

        if auto_start {
            engine.start();
        }

        let status = engine.status().clone();
        inner.engine = Some(engine);
        inner.emitted_event_count = 0;
        inner.last_snapshot_tick = None;

        let mut messages = Vec::new();
        if replaced_existing_run {
            messages.push(StreamMessage::warning(
                &status.run_id,
                status.current_tick,
                "existing run state was replaced by POST /runs".to_string(),
            ));
        }
        messages.push(StreamMessage::run_status(&status));

        (
            CreateRunResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                replaced_existing_run,
                started: auto_start,
            },
            messages,
        )
    };

    broadcast_messages(&state, messages);

    Ok(Json(response))
}

fn default_sqlite_path() -> String {
    std::env::var("THREADS_SQLITE_PATH")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SQLITE_PATH.to_string())
}

#[derive(Debug, Serialize)]
struct RunControlResponse {
    schema_version: String,
    run_id: String,
    status: RunStatus,
    committed: Option<u64>,
}

async fn start_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RunControlResponse>, HttpApiError> {
    let (response, messages) = {
        let mut inner = state.inner.lock().await;
        let status = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            engine.start().clone()
        };

        let mut messages = collect_delta_messages(&mut inner);
        messages.push(StreamMessage::run_status(&status));

        (
            RunControlResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                committed: None,
            },
            messages,
        )
    };

    broadcast_messages(&state, messages);

    Ok(Json(response))
}

async fn pause_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RunControlResponse>, HttpApiError> {
    let (response, messages) = {
        let mut inner = state.inner.lock().await;
        let status = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            engine.pause().clone()
        };

        let mut messages = collect_delta_messages(&mut inner);
        messages.push(StreamMessage::run_status(&status));

        (
            RunControlResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                committed: None,
            },
            messages,
        )
    };

    broadcast_messages(&state, messages);

    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct StepRequest {
    steps: Option<u64>,
}

async fn step_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<StepRequest>,
) -> Result<Json<RunControlResponse>, HttpApiError> {
    let steps = request.steps.unwrap_or(1);
    if steps == 0 {
        return Err(HttpApiError::invalid_query(
            "steps must be >= 1",
            Some("steps=0".to_string()),
        ));
    }

    let (response, messages) = {
        let mut inner = state.inner.lock().await;
        let (status, committed) = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            let (status, committed) = engine.step(steps);
            (status.clone(), committed)
        };

        let mut messages = collect_delta_messages(&mut inner);
        messages.push(StreamMessage::run_status(&status));

        (
            RunControlResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                committed: Some(committed),
            },
            messages,
        )
    };

    broadcast_messages(&state, messages);

    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
struct RunToTickRequest {
    target_tick: u64,
}

async fn run_to_tick(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<RunToTickRequest>,
) -> Result<Json<RunControlResponse>, HttpApiError> {
    let (response, messages) = {
        let mut inner = state.inner.lock().await;
        let (status, committed) = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            let (status, committed) = engine.run_to_tick(request.target_tick);
            (status.clone(), committed)
        };

        let mut messages = collect_delta_messages(&mut inner);
        messages.push(StreamMessage::run_status(&status));

        (
            RunControlResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                committed: Some(committed),
            },
            messages,
        )
    };

    broadcast_messages(&state, messages);

    Ok(Json(response))
}

async fn get_status(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RunControlResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let status = require_run(&inner, &run_id)?.status().clone();
        RunControlResponse {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            run_id: status.run_id.clone(),
            status,
            committed: None,
        }
    };

    Ok(Json(response))
}

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

async fn get_npc_inspector(
    Path((run_id, npc_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, HttpApiError> {
    let response = {
        let inner = state.inner.lock().await;
        let engine = require_run(&inner, &run_id)?;

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
                "commitments": commitments,
                "time_budget": time_budget,
                "motive_chain": motive_chain,
                "why_summaries": why_summaries,
                "key_relationships": relationship_edges.clone(),
            }),
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
                "market_clearing": market_clearing,
                "institution_queue": institution_queue,
                "accounting_transfers": accounting_transfers,
                "notable_events": recent_events.into_iter().rev().take(20).collect::<Vec<_>>(),
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

async fn stream_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, HttpApiError> {
    let initial_message = {
        let inner = state.inner.lock().await;
        let status = require_run(&inner, &run_id)?.status().clone();
        StreamMessage::run_status(&status)
    };

    Ok(ws.on_upgrade(move |socket| stream_socket(socket, state, run_id, initial_message)))
}

async fn stream_socket(
    mut socket: WebSocket,
    state: AppState,
    run_id: String,
    initial_message: StreamMessage,
) {
    if send_stream_message(&mut socket, &initial_message)
        .await
        .is_err()
    {
        return;
    }

    let mut rx = state.stream_tx.subscribe();

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        break;
                    }
                    _ => {}
                }
            }
            outgoing = rx.recv() => {
                match outgoing {
                    Ok(message) => {
                        if message.run_id != run_id {
                            continue;
                        }

                        if send_stream_message(&mut socket, &message).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let warning = StreamMessage::warning(
                            &run_id,
                            0,
                            format!("stream client lagged and skipped {skipped} message(s)"),
                        );

                        if send_stream_message(&mut socket, &warning).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }
}

async fn send_stream_message(
    socket: &mut WebSocket,
    message: &StreamMessage,
) -> Result<(), axum::Error> {
    let payload = serde_json::to_string(message).map_err(axum::Error::new)?;
    socket.send(Message::Text(payload.into())).await
}

fn require_run<'a>(inner: &'a ServerInner, run_id: &str) -> Result<&'a EngineApi, HttpApiError> {
    let Some(engine) = inner.engine.as_ref() else {
        return Err(HttpApiError::run_not_found(run_id, None));
    };

    if engine.run_id() != run_id {
        return Err(HttpApiError::run_not_found(run_id, Some(engine.run_id())));
    }

    Ok(engine)
}

fn require_run_mut<'a>(
    inner: &'a mut ServerInner,
    run_id: &str,
) -> Result<&'a mut EngineApi, HttpApiError> {
    let active_run_id = inner
        .engine
        .as_ref()
        .map(|engine| engine.run_id().to_string());
    let Some(engine) = inner.engine.as_mut() else {
        return Err(HttpApiError::run_not_found(run_id, None));
    };

    if engine.run_id() != run_id {
        return Err(HttpApiError::run_not_found(
            run_id,
            active_run_id.as_deref(),
        ));
    }

    Ok(engine)
}

fn collect_delta_messages(inner: &mut ServerInner) -> Vec<StreamMessage> {
    let mut messages = Vec::new();

    let Some(engine) = inner.engine.as_ref() else {
        return messages;
    };

    let new_events = &engine.events()[inner.emitted_event_count..];
    for event in new_events {
        messages.push(StreamMessage::event_appended(event));
    }
    inner.emitted_event_count = engine.events().len();

    let status = engine.status();
    let cadence = engine.config().snapshot_every_ticks.max(1);
    let snapshot_due = status.current_tick > 0
        && ((status.current_tick % cadence == 0) || status.is_complete())
        && inner.last_snapshot_tick != Some(status.current_tick);

    if snapshot_due {
        let snapshot = engine.snapshot_for_current_tick();
        inner.last_snapshot_tick = Some(snapshot.tick);
        messages.push(StreamMessage::snapshot_created(&snapshot));
    }

    if let Some(last_error) = engine.last_persistence_error() {
        messages.push(StreamMessage::warning(
            engine.run_id(),
            status.current_tick,
            last_error.to_string(),
        ));
    }

    messages
}

fn fallback_snapshot_window(
    engine: &EngineApi,
    around_tick: Option<u64>,
    from_tick: Option<u64>,
    to_tick: Option<u64>,
) -> Vec<Snapshot> {
    let current_tick = engine.status().current_tick;
    let snapshot = engine.snapshot_for_current_tick();
    if current_tick == 0 {
        if let Some(around) = around_tick {
            return if around >= snapshot.tick {
                vec![snapshot]
            } else {
                Vec::new()
            };
        }
        let from = from_tick.unwrap_or(0);
        let to = to_tick.unwrap_or(snapshot.tick);
        return if snapshot.tick >= from && snapshot.tick <= to {
            vec![snapshot]
        } else {
            Vec::new()
        };
    }

    if let Some(around) = around_tick {
        if around >= snapshot.tick {
            return vec![snapshot];
        }
        return Vec::new();
    }

    let from_tick = from_tick.unwrap_or(1);
    let to_tick = to_tick.unwrap_or(current_tick);

    if snapshot.tick >= from_tick && snapshot.tick <= to_tick {
        vec![snapshot]
    } else {
        Vec::new()
    }
}

fn paginate(
    total: usize,
    cursor: Option<usize>,
    page_size: Option<usize>,
) -> Result<(usize, usize, Option<usize>), HttpApiError> {
    let start = cursor.unwrap_or(0);
    if start > total {
        return Err(HttpApiError::invalid_query(
            "cursor is out of bounds",
            Some(format!("cursor={start} total={total}")),
        ));
    }

    let size = page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .max(1)
        .min(MAX_PAGE_SIZE);
    let end = start.saturating_add(size).min(total);
    let next_cursor = if end < total { Some(end) } else { None };

    Ok((start, end, next_cursor))
}

fn parse_event_type_filter(
    requested_types: &[String],
) -> Result<Option<HashSet<EventType>>, HttpApiError> {
    if requested_types.is_empty() {
        return Ok(None);
    }

    let mut filter = HashSet::new();

    for value in requested_types {
        let normalized = value.trim().to_lowercase();
        let event_type = match normalized.as_str() {
            "system_tick" | "systemtick" => EventType::SystemTick,
            "npc_action_committed" | "npcactioncommitted" => EventType::NpcActionCommitted,
            "command_applied" | "commandapplied" => EventType::CommandApplied,
            "rumor_injected" | "rumorinjected" => EventType::RumorInjected,
            "caravan_spawned" | "caravanspawned" => EventType::CaravanSpawned,
            "npc_removed" | "npcremoved" => EventType::NpcRemoved,
            "bad_harvest_forced" | "badharvestforced" => EventType::BadHarvestForced,
            "winter_severity_set" | "winterseverityset" => EventType::WinterSeveritySet,
            "theft_committed" | "theftcommitted" => EventType::TheftCommitted,
            "item_transferred" | "itemtransferred" => EventType::ItemTransferred,
            "investigation_progressed" | "investigationprogressed" => {
                EventType::InvestigationProgressed
            }
            "arrest_made" | "arrestmade" => EventType::ArrestMade,
            "site_discovered" | "sitediscovered" => EventType::SiteDiscovered,
            "leverage_gained" | "leveragegained" => EventType::LeverageGained,
            "relationship_shifted" | "relationshipshifted" => EventType::RelationshipShifted,
            "pressure_economy_updated" | "pressureeconomyupdated" => {
                EventType::PressureEconomyUpdated
            }
            "household_consumption_applied" | "householdconsumptionapplied" => {
                EventType::HouseholdConsumptionApplied
            }
            "rent_due" | "rentdue" => EventType::RentDue,
            "rent_unpaid" | "rentunpaid" => EventType::RentUnpaid,
            "eviction_risk_changed" | "evictionriskchanged" => EventType::EvictionRiskChanged,
            "household_buffer_exhausted" | "householdbufferexhausted" => {
                EventType::HouseholdBufferExhausted
            }
            "job_sought" | "jobsought" => EventType::JobSought,
            "contract_signed" | "contractsigned" => EventType::ContractSigned,
            "wage_paid" | "wagepaid" => EventType::WagePaid,
            "wage_delayed" | "wagedelayed" => EventType::WageDelayed,
            "contract_breached" | "contractbreached" => EventType::ContractBreached,
            "employment_terminated" | "employmentterminated" => EventType::EmploymentTerminated,
            "production_started" | "productionstarted" => EventType::ProductionStarted,
            "production_completed" | "productioncompleted" => EventType::ProductionCompleted,
            "spoilage_occurred" | "spoilageoccurred" => EventType::SpoilageOccurred,
            "stock_shortage" | "stockshortage" => EventType::StockShortage,
            "stock_recovered" | "stockrecovered" => EventType::StockRecovered,
            "trust_changed" | "trustchanged" => EventType::TrustChanged,
            "obligation_created" | "obligationcreated" => EventType::ObligationCreated,
            "obligation_called" | "obligationcalled" => EventType::ObligationCalled,
            "grievance_recorded" | "grievancerecorded" => EventType::GrievanceRecorded,
            "relationship_status_changed" | "relationshipstatuschanged" => {
                EventType::RelationshipStatusChanged
            }
            "belief_formed" | "beliefformed" => EventType::BeliefFormed,
            "belief_updated" | "beliefupdated" => EventType::BeliefUpdated,
            "belief_disputed" | "beliefdisputed" => EventType::BeliefDisputed,
            "rumor_mutated" | "rumormutated" => EventType::RumorMutated,
            "belief_forgotten" | "beliefforgotten" => EventType::BeliefForgotten,
            "institution_profile_updated" | "institutionprofileupdated" => {
                EventType::InstitutionProfileUpdated
            }
            "institution_case_resolved" | "institutioncaseresolved" => {
                EventType::InstitutionCaseResolved
            }
            "institutional_error_recorded" | "institutionalerrorrecorded" => {
                EventType::InstitutionalErrorRecorded
            }
            "group_formed" | "groupformed" => EventType::GroupFormed,
            "group_membership_changed" | "groupmembershipchanged" => {
                EventType::GroupMembershipChanged
            }
            "group_split" | "groupsplit" => EventType::GroupSplit,
            "group_dissolved" | "groupdissolved" => EventType::GroupDissolved,
            "conversation_held" | "conversationheld" => EventType::ConversationHeld,
            "loan_extended" | "loanextended" => EventType::LoanExtended,
            "romance_advanced" | "romanceadvanced" => EventType::RomanceAdvanced,
            "illness_contracted" | "illnesscontracted" => EventType::IllnessContracted,
            "illness_recovered" | "illnessrecovered" => EventType::IllnessRecovered,
            "observation_logged" | "observationlogged" => EventType::ObservationLogged,
            "insult_exchanged" | "insultexchanged" => EventType::InsultExchanged,
            "punch_thrown" | "punchthrown" => EventType::PunchThrown,
            "brawl_started" | "brawlstarted" => EventType::BrawlStarted,
            "brawl_stopped" | "brawlstopped" => EventType::BrawlStopped,
            "guards_dispatched" | "guardsdispatched" => EventType::GuardsDispatched,
            "apprenticeship_progressed" | "apprenticeshipprogressed" => {
                EventType::ApprenticeshipProgressed
            }
            "succession_transferred" | "successiontransferred" => EventType::SuccessionTransferred,
            "route_risk_updated" | "routeriskupdated" => EventType::RouteRiskUpdated,
            "travel_window_shifted" | "travelwindowshifted" => EventType::TravelWindowShifted,
            "narrative_why_summary" | "narrativewhysummary" => EventType::NarrativeWhySummary,
            "opportunity_opened" | "opportunityopened" => EventType::OpportunityOpened,
            "opportunity_expired" | "opportunityexpired" => EventType::OpportunityExpired,
            "opportunity_accepted" | "opportunityaccepted" => EventType::OpportunityAccepted,
            "opportunity_rejected" | "opportunityrejected" => EventType::OpportunityRejected,
            "commitment_started" | "commitmentstarted" => EventType::CommitmentStarted,
            "commitment_continued" | "commitmentcontinued" => EventType::CommitmentContinued,
            "commitment_completed" | "commitmentcompleted" => EventType::CommitmentCompleted,
            "commitment_broken" | "commitmentbroken" => EventType::CommitmentBroken,
            "market_cleared" | "marketcleared" => EventType::MarketCleared,
            "market_failed" | "marketfailed" => EventType::MarketFailed,
            "accounting_transfer_recorded" | "accountingtransferrecorded" => {
                EventType::AccountingTransferRecorded
            }
            "institution_queue_updated" | "institutionqueueupdated" => {
                EventType::InstitutionQueueUpdated
            }
            _ => {
                return Err(HttpApiError::invalid_query(
                    "invalid event type filter",
                    Some(format!("event_type={value}")),
                ))
            }
        };

        filter.insert(event_type);
    }

    Ok(Some(filter))
}

fn build_trace_nodes(
    root_event: &Event,
    events: &[Event],
    reason_packets: &[ReasonPacket],
    max_depth: usize,
) -> Vec<TraceNode> {
    let events_by_id = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();

    let reason_packets_by_id = reason_packets
        .iter()
        .map(|packet| (packet.reason_packet_id.as_str(), packet))
        .collect::<HashMap<_, _>>();

    let mut nodes = Vec::new();
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back((root_event.event_id.clone(), 0_usize));

    while let Some((event_id, depth)) = queue.pop_front() {
        if !visited.insert(event_id.clone()) {
            continue;
        }

        let Some(event) = events_by_id.get(event_id.as_str()).copied() else {
            continue;
        };

        let reason_packet = event.reason_packet_id.as_ref().and_then(|reason_id| {
            reason_packets_by_id
                .get(reason_id.as_str())
                .copied()
                .cloned()
        });

        nodes.push(TraceNode {
            depth,
            event: event.clone(),
            reason_packet,
        });

        if depth >= max_depth {
            continue;
        }

        for parent_id in &event.caused_by {
            queue.push_back((parent_id.clone(), depth + 1));
        }
    }

    nodes
}

fn broadcast_messages(state: &AppState, messages: Vec<StreamMessage>) {
    for message in messages {
        let _ = state.stream_tx.send(message);
    }
}

#[derive(Debug, Clone, Serialize)]
struct StreamMessage {
    schema_version: String,
    #[serde(rename = "type")]
    message_type: String,
    run_id: String,
    tick: u64,
    sequence_in_tick: Option<u64>,
    reconnect_token: String,
    payload: Value,
}

impl StreamMessage {
    fn run_status(status: &RunStatus) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            message_type: "run.status".to_string(),
            run_id: status.run_id.clone(),
            tick: status.current_tick,
            sequence_in_tick: None,
            reconnect_token: reconnect_token(status.current_tick, None, "status"),
            payload: json!(status),
        }
    }

    fn event_appended(event: &Event) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            message_type: "event.appended".to_string(),
            run_id: event.run_id.clone(),
            tick: event.tick,
            sequence_in_tick: Some(event.sequence_in_tick),
            reconnect_token: reconnect_token(event.tick, Some(event.sequence_in_tick), "event"),
            payload: json!(event),
        }
    }

    fn snapshot_created(snapshot: &Snapshot) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            message_type: "snapshot.created".to_string(),
            run_id: snapshot.run_id.clone(),
            tick: snapshot.tick,
            sequence_in_tick: None,
            reconnect_token: reconnect_token(snapshot.tick, None, "snapshot"),
            payload: json!(snapshot),
        }
    }

    fn command_result(entry: &PersistedCommandEntry, emitted_at_tick: u64) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            message_type: "command.result".to_string(),
            run_id: entry.command.run_id.clone(),
            tick: emitted_at_tick,
            sequence_in_tick: None,
            reconnect_token: reconnect_token(emitted_at_tick, None, "command"),
            payload: json!({
                "command": entry.command,
                "result": entry.result,
                "effective_tick": entry.effective_tick,
            }),
        }
    }

    fn warning(run_id: &str, tick: u64, warning: String) -> Self {
        Self {
            schema_version: SCHEMA_VERSION_V1.to_string(),
            message_type: "warning".to_string(),
            run_id: run_id.to_string(),
            tick,
            sequence_in_tick: None,
            reconnect_token: reconnect_token(tick, None, "warning"),
            payload: json!({ "message": warning }),
        }
    }
}

fn reconnect_token(tick: u64, sequence_in_tick: Option<u64>, label: &str) -> String {
    match sequence_in_tick {
        Some(sequence) => format!("{label}:{tick}:{sequence}"),
        None => format!("{label}:{tick}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_builder_walks_causal_chain() {
        let mut config = RunConfig::default();
        config.npc_count_min = 3;
        config.npc_count_max = 3;
        config.snapshot_every_ticks = 2;

        let mut engine = EngineApi::from_config(config.clone());
        engine.run_to_tick(3);

        let root_event = engine
            .events()
            .iter()
            .rev()
            .find(|event| event.event_type == EventType::PressureEconomyUpdated)
            .expect("expected aggregate pressure event");

        let nodes = build_trace_nodes(root_event, engine.events(), engine.reason_packets(), 5);
        assert!(!nodes.is_empty());
        assert_eq!(nodes[0].event.event_id, root_event.event_id);
    }

    #[test]
    fn pagination_enforces_max_bounds() {
        let (start, end, next_cursor) =
            paginate(100, Some(10), Some(20)).expect("page should work");
        assert_eq!(start, 10);
        assert_eq!(end, 30);
        assert_eq!(next_cursor, Some(30));

        let out_of_range = paginate(5, Some(10), Some(1));
        assert!(out_of_range.is_err());
    }
}
