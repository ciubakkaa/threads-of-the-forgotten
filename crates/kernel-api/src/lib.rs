//! In-process API facade with command validation, deterministic queueing, and SQLite persistence.

mod persistence;
mod server;

use std::path::Path;

use contracts::{
    ApiError, Command, CommandPayload, CommandResult, CommandType, ErrorCode, Event, ReasonPacket,
    RunConfig, RunStatus, Snapshot, SCHEMA_VERSION_V1,
};
use kernel_core::Kernel;
use persistence::SqliteRunStore;
pub use persistence::{PersistedCommandEntry, PersistenceError, ReplaySlice};
pub use server::{serve, ServerError};

#[derive(Debug)]
struct PersistenceState {
    store: SqliteRunStore,
    persisted_command_count: usize,
    persisted_event_count: usize,
    persisted_reason_count: usize,
    last_snapshot_tick: Option<u64>,
}

#[derive(Debug)]
pub struct EngineApi {
    kernel: Kernel,
    command_audit: Vec<CommandResult>,
    command_log: Vec<PersistedCommandEntry>,
    persistence: Option<PersistenceState>,
    last_persistence_error: Option<String>,
}

impl EngineApi {
    pub fn from_config(config: RunConfig) -> Self {
        Self {
            kernel: Kernel::new(config),
            command_audit: Vec::new(),
            command_log: Vec::new(),
            persistence: None,
            last_persistence_error: None,
        }
    }

    pub fn attach_sqlite_store(&mut self, path: impl AsRef<Path>) -> Result<(), PersistenceError> {
        let store = SqliteRunStore::open(path)?;
        self.persistence = Some(PersistenceState {
            store,
            persisted_command_count: 0,
            persisted_event_count: 0,
            persisted_reason_count: 0,
            last_snapshot_tick: None,
        });
        Ok(())
    }

    pub fn initialize_run_storage(
        &mut self,
        replace_existing_run: bool,
    ) -> Result<(), PersistenceError> {
        let Some(state) = self.persistence.as_mut() else {
            return Err(PersistenceError::NotAttached);
        };

        let run_id = self.kernel.run_id().to_string();
        if state.store.run_exists(&run_id)? {
            if replace_existing_run {
                state.store.delete_run(&run_id)?;
                state.persisted_command_count = 0;
                state.persisted_event_count = 0;
                state.persisted_reason_count = 0;
                state.last_snapshot_tick = None;
            } else {
                return Err(PersistenceError::RunAlreadyExists(run_id));
            }
        }

        let bootstrap_snapshot = self.kernel.snapshot_for_current_tick();
        state.store.persist_delta(
            self.kernel.config(),
            self.kernel.status(),
            &[],
            &[],
            &[],
            Some(&bootstrap_snapshot),
        )?;
        state.last_snapshot_tick = Some(bootstrap_snapshot.tick);
        self.last_persistence_error = None;
        Ok(())
    }

    pub fn flush_persistence_checked(&mut self) -> Result<(), PersistenceError> {
        let Some(state) = self.persistence.as_mut() else {
            return Err(PersistenceError::NotAttached);
        };

        let new_commands = &self.command_log[state.persisted_command_count..];
        let new_events = &self.kernel.events()[state.persisted_event_count..];
        let new_reason_packets = &self.kernel.reason_packets()[state.persisted_reason_count..];

        let current_tick = self.kernel.status().current_tick;
        let cadence = self.kernel.config().snapshot_every_ticks.max(1);
        let snapshot_due = ((current_tick == 0 && state.last_snapshot_tick.is_none())
            || (current_tick > 0
                && ((current_tick % cadence == 0) || self.kernel.status().is_complete())))
            && state.last_snapshot_tick != Some(current_tick);

        let snapshot = if snapshot_due {
            Some(self.kernel.snapshot_for_current_tick())
        } else {
            None
        };

        state.store.persist_delta(
            self.kernel.config(),
            self.kernel.status(),
            new_commands,
            new_events,
            new_reason_packets,
            snapshot.as_ref(),
        )?;

        state.persisted_command_count = self.command_log.len();
        state.persisted_event_count = self.kernel.events().len();
        state.persisted_reason_count = self.kernel.reason_packets().len();

        if let Some(snapshot_payload) = snapshot {
            state.last_snapshot_tick = Some(snapshot_payload.tick);
        }

        self.last_persistence_error = None;
        Ok(())
    }

    pub fn replay_at_tick(&self, run_id: &str, tick: u64) -> Result<ReplaySlice, PersistenceError> {
        let Some(state) = self.persistence.as_ref() else {
            return Err(PersistenceError::NotAttached);
        };

        state.store.load_replay_at_tick(run_id, tick)
    }

    pub fn load_latest_snapshot_at_or_before(
        &self,
        run_id: &str,
        tick: u64,
    ) -> Result<Option<Snapshot>, PersistenceError> {
        let Some(state) = self.persistence.as_ref() else {
            return Err(PersistenceError::NotAttached);
        };

        state.store.load_latest_snapshot_at_or_before(run_id, tick)
    }

    pub fn load_snapshots_range(
        &self,
        run_id: &str,
        from_tick: u64,
        to_tick: u64,
    ) -> Result<Vec<Snapshot>, PersistenceError> {
        let Some(state) = self.persistence.as_ref() else {
            return Err(PersistenceError::NotAttached);
        };

        state.store.load_snapshots_range(run_id, from_tick, to_tick)
    }

    pub fn last_persistence_error(&self) -> Option<&str> {
        self.last_persistence_error.as_deref()
    }

    pub fn run_id(&self) -> &str {
        self.kernel.run_id()
    }

    pub fn config(&self) -> &RunConfig {
        self.kernel.config()
    }

    pub fn snapshot_for_current_tick(&self) -> Snapshot {
        self.kernel.snapshot_for_current_tick()
    }

    pub fn start(&mut self) -> &RunStatus {
        self.kernel.start();
        self.flush_persistence_if_enabled();
        self.kernel.status()
    }

    pub fn pause(&mut self) -> &RunStatus {
        self.kernel.pause();
        self.flush_persistence_if_enabled();
        self.kernel.status()
    }

    pub fn step(&mut self, steps: u64) -> (&RunStatus, u64) {
        let mut committed = 0_u64;
        for _ in 0..steps.max(1) {
            if !self.kernel.step_tick() {
                break;
            }
            committed += 1;
            self.flush_persistence_if_enabled();
        }
        (self.kernel.status(), committed)
    }

    pub fn run_to_tick(&mut self, tick: u64) -> (&RunStatus, u64) {
        let mut committed = 0_u64;
        while self.kernel.status().current_tick < tick {
            if !self.kernel.step_tick() {
                break;
            }
            committed += 1;
            self.flush_persistence_if_enabled();
        }
        (self.kernel.status(), committed)
    }

    pub fn submit_command(
        &mut self,
        command: Command,
        effective_tick: Option<u64>,
    ) -> CommandResult {
        let validation_error = self.validate_command(&command, effective_tick);

        let scheduled_tick = effective_tick.unwrap_or(command.issued_at_tick);
        let result = match validation_error {
            Some(error) => CommandResult::rejected(&command, error),
            None => {
                self.kernel.enqueue_command(command.clone(), scheduled_tick);
                CommandResult::accepted(&command)
            }
        };

        self.command_audit.push(result.clone());
        self.command_log.push(PersistedCommandEntry {
            command,
            result: result.clone(),
            effective_tick: scheduled_tick,
        });
        self.flush_persistence_if_enabled();
        result
    }

    pub fn status(&self) -> &RunStatus {
        self.kernel.status()
    }

    pub fn command_audit(&self) -> &[CommandResult] {
        &self.command_audit
    }

    pub fn command_log(&self) -> &[PersistedCommandEntry] {
        &self.command_log
    }

    pub fn events(&self) -> &[Event] {
        self.kernel.events()
    }

    pub fn reason_packets(&self) -> &[ReasonPacket] {
        self.kernel.reason_packets()
    }

    fn flush_persistence_if_enabled(&mut self) {
        if self.persistence.is_none() {
            return;
        }

        if let Err(err) = self.flush_persistence_checked() {
            self.last_persistence_error = Some(err.to_string());
        }
    }

    fn validate_command(&self, command: &Command, effective_tick: Option<u64>) -> Option<ApiError> {
        if command.schema_version != SCHEMA_VERSION_V1 {
            return Some(ApiError::new(
                ErrorCode::ContractVersionUnsupported,
                "Unsupported schema_version",
                Some(format!(
                    "got={} expected={}",
                    command.schema_version, SCHEMA_VERSION_V1
                )),
            ));
        }

        if command.run_id != self.kernel.run_id() {
            return Some(ApiError::new(
                ErrorCode::RunNotFound,
                "command.run_id does not match active run",
                None,
            ));
        }

        if !command_type_matches_payload(command.command_type, &command.payload) {
            return Some(ApiError::new(
                ErrorCode::InvalidCommand,
                "command_type does not match payload variant",
                None,
            ));
        }

        match &command.payload {
            CommandPayload::SimStepTick { steps } if *steps == 0 => {
                return Some(ApiError::new(
                    ErrorCode::InvalidCommand,
                    "sim.step_tick requires steps >= 1",
                    None,
                ))
            }
            CommandPayload::InjectSetWinterSeverity { severity } if *severity > 100 => {
                return Some(ApiError::new(
                    ErrorCode::InvalidCommand,
                    "winter severity must be in [0, 100]",
                    None,
                ))
            }
            _ => {}
        }

        let scheduled_tick = effective_tick.unwrap_or(command.issued_at_tick);
        let min_tick = self.status().current_tick + 1;
        if scheduled_tick < min_tick {
            return Some(ApiError::new(
                ErrorCode::TickOutOfRange,
                "cannot schedule command in the past",
                Some(format!(
                    "scheduled_tick={} min_tick={}",
                    scheduled_tick, min_tick
                )),
            ));
        }

        None
    }
}

fn command_type_matches_payload(command_type: CommandType, payload: &CommandPayload) -> bool {
    matches!(
        (command_type, payload),
        (CommandType::SimStart, CommandPayload::SimStart)
            | (CommandType::SimPause, CommandPayload::SimPause)
            | (CommandType::SimStepTick, CommandPayload::SimStepTick { .. })
            | (
                CommandType::SimRunToTick,
                CommandPayload::SimRunToTick { .. }
            )
            | (CommandType::InjectRumor, CommandPayload::InjectRumor { .. })
            | (
                CommandType::InjectSpawnCaravan,
                CommandPayload::InjectSpawnCaravan { .. }
            )
            | (
                CommandType::InjectRemoveNpc,
                CommandPayload::InjectRemoveNpc { .. }
            )
            | (
                CommandType::InjectForceBadHarvest,
                CommandPayload::InjectForceBadHarvest { .. }
            )
            | (
                CommandType::InjectSetWinterSeverity,
                CommandPayload::InjectSetWinterSeverity { .. }
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RunConfig {
        let mut config = RunConfig::default();
        config.npc_count_min = 3;
        config.npc_count_max = 3;
        config
    }

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();

        std::env::temp_dir().join(format!("threads_kernel_{name}_{nanos}.sqlite"))
    }

    #[test]
    fn step_returns_committed_count() {
        let mut api = EngineApi::from_config(test_config());
        let (_, committed) = api.step(3);

        assert_eq!(committed, 3);
        assert_eq!(api.status().current_tick, 3);
    }

    #[test]
    fn rejects_mismatched_payload_type() {
        let api_config = test_config();
        let mut api = EngineApi::from_config(api_config.clone());

        let bad = Command::new(
            "cmd_bad",
            api_config.run_id,
            1,
            CommandType::InjectRumor,
            CommandPayload::InjectRemoveNpc {
                npc_id: "npc_01".to_string(),
            },
        );

        let result = api.submit_command(bad, None);
        assert!(!result.accepted);
        assert!(result.error.is_some());
    }

    #[test]
    fn accepts_and_applies_valid_injection_command() {
        let api_config = test_config();
        let mut api = EngineApi::from_config(api_config.clone());

        let command = Command::new(
            "cmd_rumor_1",
            api_config.run_id,
            2,
            CommandType::InjectRumor,
            CommandPayload::InjectRumor {
                location_id: "settlement:greywall".to_string(),
                rumor_text: "Something stalks the pass".to_string(),
            },
        );

        let result = api.submit_command(command, Some(2));
        assert!(result.accepted);

        api.run_to_tick(2);
        assert!(api
            .events()
            .iter()
            .any(|event| event.event_type == contracts::EventType::RumorInjected));
    }

    #[test]
    fn persists_and_replays_by_tick() {
        let mut config = test_config();
        config.snapshot_every_ticks = 4;
        let run_id = config.run_id.clone();

        let mut api = EngineApi::from_config(config);
        let db_path = temp_db_path("replay");

        api.attach_sqlite_store(&db_path)
            .expect("should attach sqlite store");

        api.run_to_tick(9);
        api.flush_persistence_checked()
            .expect("flush should succeed");

        let replay = api
            .replay_at_tick(&run_id, 9)
            .expect("replay should load at tick");

        assert!(replay.snapshot.is_some());
        assert!(!replay.events.is_empty());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(db_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(db_path.with_extension("sqlite-shm"));
    }
}
