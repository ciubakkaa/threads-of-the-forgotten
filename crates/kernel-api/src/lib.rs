//! In-process API facade with command validation, deterministic queueing, and SQLite persistence.

mod persistence;
mod server;

use std::path::Path;

use contracts::{
    ApiError, Command, CommandPayload, CommandResult, CommandType, ErrorCode, Event, ReasonPacket,
    RunConfig, RunStatus, Snapshot, SCHEMA_VERSION_V1,
};
use contracts::agency::{DriveKind, ReasonEnvelope};
use kernel_core::world::AgentWorld;
use persistence::SqliteRunStore;
pub use persistence::{PersistedCommandEntry, PersistedRunSummary, PersistenceError, ReplaySlice};
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
    engine: AgentWorld,
    command_audit: Vec<CommandResult>,
    command_log: Vec<PersistedCommandEntry>,
    persistence: Option<PersistenceState>,
    last_persistence_error: Option<String>,
    /// Cached reason packets converted from ReasonEnvelopes for backward compat.
    reason_packet_cache: Vec<ReasonPacket>,
}

impl EngineApi {
    pub fn from_config(config: RunConfig) -> Self {
        Self {
            engine: AgentWorld::new(config),
            command_audit: Vec::new(),
            command_log: Vec::new(),
            persistence: None,
            last_persistence_error: None,
            reason_packet_cache: Vec::new(),
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

        let run_id = self.engine.config.run_id.clone();
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

        let bootstrap_snapshot = self.engine.snapshot();
        state.store.persist_delta(
            &self.engine.config,
            &self.engine.status,
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
        if self.persistence.is_none() {
            return Err(PersistenceError::NotAttached);
        }

        // Sync reason packet cache before borrowing persistence state.
        self.sync_reason_packet_cache();

        let state = self.persistence.as_mut().unwrap();

        let new_commands = &self.command_log[state.persisted_command_count..];
        let new_events = &self.engine.events()[state.persisted_event_count..];
        let new_reason_packets = &self.reason_packet_cache[state.persisted_reason_count..];

        let current_tick = self.engine.status.current_tick;
        let cadence = self.engine.config.snapshot_every_ticks.max(1);
        let snapshot_due = ((current_tick == 0 && state.last_snapshot_tick.is_none())
            || (current_tick > 0
                && ((current_tick % cadence == 0) || self.engine.status.is_complete())))
            && state.last_snapshot_tick != Some(current_tick);

        let snapshot = if snapshot_due {
            Some(self.engine.snapshot())
        } else {
            None
        };

        state.store.persist_delta(
            &self.engine.config,
            &self.engine.status,
            new_commands,
            new_events,
            new_reason_packets,
            snapshot.as_ref(),
        )?;

        state.persisted_command_count = self.command_log.len();
        state.persisted_event_count = self.engine.events().len();
        state.persisted_reason_count = self.reason_packet_cache.len();

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
        &self.engine.config.run_id
    }

    pub fn config(&self) -> &RunConfig {
        &self.engine.config
    }

    pub fn snapshot_for_current_tick(&self) -> Snapshot {
        self.engine.snapshot()
    }

    pub fn start(&mut self) -> &RunStatus {
        self.engine.start();
        self.flush_persistence_if_enabled();
        &self.engine.status
    }

    pub fn pause(&mut self) -> &RunStatus {
        self.engine.pause();
        self.flush_persistence_if_enabled();
        &self.engine.status
    }

    /// Advance by the requested number of scheduling windows.
    /// Auto-starts the engine if paused so that explicit step requests always advance.
    pub fn step(&mut self, steps: u64) -> (&RunStatus, u64) {
        self.engine.start();
        let committed = self.engine.step_n(steps.max(1));
        self.sync_reason_packet_cache();
        self.flush_persistence_if_enabled();
        (&self.engine.status, committed)
    }

    /// Auto-starts the engine if paused so that explicit run-to-tick requests always advance.
    pub fn run_to_tick(&mut self, tick: u64) -> (&RunStatus, u64) {
        self.engine.start();
        let committed = self.engine.run_to_tick(tick);
        self.sync_reason_packet_cache();
        self.flush_persistence_if_enabled();
        (&self.engine.status, committed)
    }

    pub fn submit_command(
        &mut self,
        command: Command,
        effective_tick: Option<u64>,
    ) -> CommandResult {
        let validation_error = self.validate_command(&command, effective_tick);

        let result = match validation_error {
            Some(error) => CommandResult::rejected(&command, error),
            None => {
                // Inject the command into the AgentWorld engine.
                self.engine.inject_command(command.clone());
                CommandResult::accepted(&command)
            }
        };

        let scheduled_tick = effective_tick.unwrap_or(command.issued_at_tick);
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
        &self.engine.status
    }

    pub fn command_audit(&self) -> &[CommandResult] {
        &self.command_audit
    }

    pub fn command_log(&self) -> &[PersistedCommandEntry] {
        &self.command_log
    }

    pub fn events(&self) -> &[Event] {
        self.engine.events()
    }

    pub fn reason_packets(&self) -> &[ReasonPacket] {
        &self.reason_packet_cache
    }

    /// Expose the underlying AgentWorld for direct inspection.
    pub fn agent_world(&self) -> &AgentWorld {
        &self.engine
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

        if command.run_id != self.engine.config.run_id {
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

    /// Convert ReasonEnvelopes from AgentWorld into ReasonPackets for backward
    /// compatibility with the persistence layer and API consumers.
    fn sync_reason_packet_cache(&mut self) {
        let envelopes = self.engine.reason_envelopes();
        let cached = self.reason_packet_cache.len();
        if envelopes.len() <= cached {
            return;
        }
        let run_id = self.engine.config.run_id.clone();
        for envelope in &envelopes[cached..] {
            self.reason_packet_cache
                .push(reason_envelope_to_packet(envelope, &run_id));
        }
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

/// Convert a `ReasonEnvelope` (from the new AgentWorld engine) into a
/// `ReasonPacket` (the legacy format used by persistence and API consumers).
fn reason_envelope_to_packet(envelope: &ReasonEnvelope, run_id: &str) -> ReasonPacket {
    let drive_label = |kind: &DriveKind| -> String {
        match kind {
            DriveKind::Food => "food".into(),
            DriveKind::Shelter => "shelter".into(),
            DriveKind::Income => "income".into(),
            DriveKind::Safety => "safety".into(),
            DriveKind::Belonging => "belonging".into(),
            DriveKind::Status => "status".into(),
            DriveKind::Health => "health".into(),
        }
    };

    let top_pressures: Vec<String> = envelope
        .drive_pressures
        .iter()
        .map(|(kind, val)| format!("{}={}", drive_label(kind), val))
        .collect();

    let alternatives_considered: Vec<String> = envelope
        .rejected_alternatives
        .iter()
        .map(|r| format!("{} (score={}, reason={})", r.plan_id, r.score, r.rejection_reason))
        .collect();

    let operator_chain_ids = envelope.operator_chain.clone();

    let chosen_action = operator_chain_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "idle".into());

    let mode_str = match envelope.planning_mode {
        contracts::agency::PlanningMode::Reactive => "reactive",
        contracts::agency::PlanningMode::Deliberate => "deliberate",
    };

    ReasonPacket {
        schema_version: SCHEMA_VERSION_V1.to_string(),
        run_id: run_id.to_string(),
        tick: envelope.tick,
        created_at: String::new(),
        reason_packet_id: format!("rp-{}-{}", envelope.agent_id, envelope.tick),
        actor_id: envelope.agent_id.clone(),
        chosen_action,
        top_intents: vec![format!("goal: {}", envelope.goal.description)],
        top_beliefs: Vec::new(),
        top_pressures,
        alternatives_considered,
        motive_families: Vec::new(),
        feasibility_checks: Vec::new(),
        chosen_verb: None,
        context_constraints: envelope.contextual_constraints.clone(),
        why_chain: vec![format!("planning_mode={}", mode_str)],
        expected_consequences: Vec::new(),
        goal_id: Some(envelope.goal.goal_id.clone()),
        plan_id: Some(envelope.selected_plan.clone()),
        operator_chain_ids,
        blocked_plan_ids: Vec::new(),
        selection_rationale: format!("selected via {} planning", mode_str),
    }
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
        api.start();
        let (_, committed) = api.step(3);

        assert!(committed > 0, "should commit at least one scheduling window");
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
        api.start();

        // Advance a tick so the command's issued_at_tick is in the future.
        api.step(1);

        let command = Command::new(
            "cmd_rumor_1",
            api_config.run_id,
            api.status().current_tick + 1,
            CommandType::InjectRumor,
            CommandPayload::InjectRumor {
                location_id: "crownvale".to_string(),
                rumor_text: "Something stalks the pass".to_string(),
            },
        );

        let result = api.submit_command(command, Some(api.status().current_tick + 1));
        assert!(result.accepted);
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

        api.start();
        api.run_to_tick(9);
        api.flush_persistence_checked()
            .expect("flush should succeed");

        let replay = api
            .replay_at_tick(&run_id, 9)
            .expect("replay should load at tick");

        assert!(replay.snapshot.is_some());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(db_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(db_path.with_extension("sqlite-shm"));
    }
}
