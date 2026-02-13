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

#[derive(Debug, Deserialize)]
struct ListRunsQuery {
    page_size: Option<usize>,
    sqlite_path: Option<String>,
}

#[derive(Debug, Serialize)]
struct ListRunsResponse {
    schema_version: String,
    active_run_id: Option<String>,
    runs: Vec<PersistedRunSummary>,
}

async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<ListRunsResponse>, HttpApiError> {
    let page_size = query.page_size.unwrap_or(200).max(1).min(MAX_PAGE_SIZE);

    let sqlite_path = query
        .sqlite_path
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(default_sqlite_path);

    let active_run_id = {
        let inner = state.inner.lock().await;
        inner
            .engine
            .as_ref()
            .map(|engine| engine.run_id().to_string())
    };

    let store = crate::persistence::SqliteRunStore::open(sqlite_path)
        .map_err(HttpApiError::from_persistence)?;
    let runs = store
        .list_runs(page_size)
        .map_err(HttpApiError::from_persistence)?;

    Ok(Json(ListRunsResponse {
        schema_version: SCHEMA_VERSION_V1.to_string(),
        active_run_id,
        runs,
    }))
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

#[derive(Debug, Serialize)]
struct RunControlResponse {
    schema_version: String,
    run_id: String,
    status: RunStatus,
    committed: Option<u64>,
    advanced_ticks: Option<u64>,
    processed_batch_tick: Option<u64>,
    processed_agents: Option<u64>,
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
                advanced_ticks: None,
                processed_batch_tick: None,
                processed_agents: None,
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
                advanced_ticks: None,
                processed_batch_tick: None,
                processed_agents: None,
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
        let (status, committed, metrics) = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            let (status, committed) = engine.step(steps);
            (status.clone(), committed, engine.last_step_metrics())
        };

        let mut messages = collect_delta_messages(&mut inner);
        messages.push(StreamMessage::run_status(&status));

        (
            RunControlResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                committed: Some(committed),
                advanced_ticks: Some(metrics.advanced_ticks),
                processed_batch_tick: Some(metrics.processed_batch_tick),
                processed_agents: Some(metrics.processed_agents),
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
        let (status, committed, metrics) = {
            let engine = require_run_mut(&mut inner, &run_id)?;
            let (status, committed) = engine.run_to_tick(request.target_tick);
            (status.clone(), committed, engine.last_step_metrics())
        };

        let mut messages = collect_delta_messages(&mut inner);
        messages.push(StreamMessage::run_status(&status));

        (
            RunControlResponse {
                schema_version: SCHEMA_VERSION_V1.to_string(),
                run_id: status.run_id.clone(),
                status,
                committed: Some(committed),
                advanced_ticks: Some(metrics.advanced_ticks),
                processed_batch_tick: Some(metrics.processed_batch_tick),
                processed_agents: Some(metrics.processed_agents),
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
            advanced_ticks: None,
            processed_batch_tick: None,
            processed_agents: None,
        }
    };

    Ok(Json(response))
}
