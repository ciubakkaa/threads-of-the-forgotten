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

fn broadcast_messages(state: &AppState, messages: Vec<StreamMessage>) {
    for message in messages {
        let _ = state.stream_tx.send(message);
    }
}

