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

use crate::{EngineApi, PersistedCommandEntry, PersistedRunSummary, PersistenceError};

const DEFAULT_PAGE_SIZE: usize = 500;
const MAX_PAGE_SIZE: usize = 5000;
const DEFAULT_TRACE_DEPTH: usize = 12;
const MAX_TRACE_DEPTH: usize = 64;
const DEFAULT_SQLITE_PATH: &str = "threads_runs.sqlite";

include!("error.rs");
include!("state.rs");
include!("routes/control.rs");
include!("routes/query.rs");
include!("routes/inspect.rs");
include!("routes/stream.rs");
include!("util.rs");

pub async fn serve(addr: SocketAddr) -> Result<(), ServerError> {
    let state = AppState::new();
    let app = router(state);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/runs", post(create_run).get(list_runs))
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
            "/api/v1/runs/{run_id}/npc/{npc_id}/arc_summary",
            get(get_npc_arc_summary),
        )
        .route(
            "/api/v1/runs/{run_id}/npc/{npc_id}/storyline",
            get(get_npc_storyline),
        )
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

#[cfg(test)]
mod tests;
