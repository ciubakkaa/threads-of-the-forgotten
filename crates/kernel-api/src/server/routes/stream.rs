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

