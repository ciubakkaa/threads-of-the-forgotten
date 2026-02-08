use std::fmt;
use std::path::Path;

use contracts::{Command, CommandResult, Event, ReasonPacket, RunConfig, RunStatus, Snapshot};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedCommandEntry {
    pub command: Command,
    pub result: CommandResult,
    pub effective_tick: u64,
}

#[derive(Debug, Clone)]
pub struct ReplaySlice {
    pub snapshot: Option<Snapshot>,
    pub events: Vec<Event>,
}

#[derive(Debug)]
pub enum PersistenceError {
    Sqlite(rusqlite::Error),
    Serde(serde_json::Error),
    NotAttached,
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(err) => write!(f, "sqlite error: {err}"),
            Self::Serde(err) => write!(f, "serde error: {err}"),
            Self::NotAttached => write!(f, "sqlite store is not attached"),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl From<rusqlite::Error> for PersistenceError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

impl From<serde_json::Error> for PersistenceError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value)
    }
}

#[derive(Debug)]
pub struct SqliteRunStore {
    conn: Connection,
}

impl SqliteRunStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PersistenceError> {
        let conn = Connection::open(path)?;
        let mut store = Self { conn };
        store.configure()?;
        store.migrate()?;
        Ok(store)
    }

    pub fn persist_delta(
        &mut self,
        config: &RunConfig,
        status: &RunStatus,
        commands: &[PersistedCommandEntry],
        events: &[Event],
        reason_packets: &[ReasonPacket],
        snapshot: Option<&Snapshot>,
    ) -> Result<(), PersistenceError> {
        let tx = self.conn.transaction()?;

        upsert_run(&tx, config, status)?;

        for entry in commands {
            let command_json = serde_json::to_string(&entry.command)?;
            let result_json = serde_json::to_string(&entry.result)?;
            tx.execute(
                "INSERT OR IGNORE INTO commands (
                    run_id,
                    command_id,
                    issued_at_tick,
                    effective_tick,
                    accepted,
                    command_json,
                    result_json,
                    created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    entry.command.run_id.as_str(),
                    entry.command.command_id.as_str(),
                    i64::try_from(entry.command.issued_at_tick).unwrap_or(i64::MAX),
                    i64::try_from(entry.effective_tick).unwrap_or(i64::MAX),
                    if entry.result.accepted { 1_i64 } else { 0_i64 },
                    command_json,
                    result_json,
                    tick_stamp(entry.effective_tick),
                ],
            )?;
        }

        for event in events {
            let payload_json = serde_json::to_string(event)?;
            tx.execute(
                "INSERT OR IGNORE INTO events (
                    run_id,
                    event_id,
                    tick,
                    sequence_in_tick,
                    event_type,
                    payload_json,
                    created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    event.run_id.as_str(),
                    event.event_id.as_str(),
                    i64::try_from(event.tick).unwrap_or(i64::MAX),
                    i64::try_from(event.sequence_in_tick).unwrap_or(i64::MAX),
                    format!("{:?}", event.event_type),
                    payload_json,
                    event.created_at.as_str(),
                ],
            )?;
        }

        for reason_packet in reason_packets {
            let payload_json = serde_json::to_string(reason_packet)?;
            tx.execute(
                "INSERT OR IGNORE INTO reason_packets (
                    run_id,
                    reason_packet_id,
                    tick,
                    actor_id,
                    payload_json,
                    created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    reason_packet.run_id.as_str(),
                    reason_packet.reason_packet_id.as_str(),
                    i64::try_from(reason_packet.tick).unwrap_or(i64::MAX),
                    reason_packet.actor_id.as_str(),
                    payload_json,
                    reason_packet.created_at.as_str(),
                ],
            )?;
        }

        if let Some(snapshot_payload) = snapshot {
            let payload_json = serde_json::to_string(snapshot_payload)?;
            tx.execute(
                "INSERT OR IGNORE INTO snapshots (
                    run_id,
                    snapshot_id,
                    tick,
                    world_state_hash,
                    payload_json,
                    created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    snapshot_payload.run_id.as_str(),
                    snapshot_payload.snapshot_id.as_str(),
                    i64::try_from(snapshot_payload.tick).unwrap_or(i64::MAX),
                    snapshot_payload.world_state_hash.as_str(),
                    payload_json,
                    snapshot_payload.created_at.as_str(),
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    pub fn load_events_range(
        &self,
        run_id: &str,
        from_tick: u64,
        to_tick: u64,
    ) -> Result<Vec<Event>, PersistenceError> {
        let mut stmt = self.conn.prepare(
            "SELECT payload_json
             FROM events
             WHERE run_id = ?1 AND tick >= ?2 AND tick <= ?3
             ORDER BY tick ASC, sequence_in_tick ASC",
        )?;

        let rows = stmt.query_map(
            params![
                run_id,
                i64::try_from(from_tick).unwrap_or(i64::MAX),
                i64::try_from(to_tick).unwrap_or(i64::MAX)
            ],
            |row| row.get::<_, String>(0),
        )?;

        let mut events = Vec::new();
        for row in rows {
            let payload = row?;
            events.push(serde_json::from_str::<Event>(&payload)?);
        }

        Ok(events)
    }

    pub fn load_latest_snapshot_at_or_before(
        &self,
        run_id: &str,
        tick: u64,
    ) -> Result<Option<Snapshot>, PersistenceError> {
        let payload: Option<String> = self
            .conn
            .query_row(
                "SELECT payload_json
                 FROM snapshots
                 WHERE run_id = ?1 AND tick <= ?2
                 ORDER BY tick DESC
                 LIMIT 1",
                params![run_id, i64::try_from(tick).unwrap_or(i64::MAX)],
                |row| row.get(0),
            )
            .optional()?;

        match payload {
            Some(raw) => Ok(Some(serde_json::from_str::<Snapshot>(&raw)?)),
            None => Ok(None),
        }
    }

    pub fn load_snapshots_range(
        &self,
        run_id: &str,
        from_tick: u64,
        to_tick: u64,
    ) -> Result<Vec<Snapshot>, PersistenceError> {
        let mut stmt = self.conn.prepare(
            "SELECT payload_json
             FROM snapshots
             WHERE run_id = ?1 AND tick >= ?2 AND tick <= ?3
             ORDER BY tick ASC",
        )?;

        let rows = stmt.query_map(
            params![
                run_id,
                i64::try_from(from_tick).unwrap_or(i64::MAX),
                i64::try_from(to_tick).unwrap_or(i64::MAX)
            ],
            |row| row.get::<_, String>(0),
        )?;

        let mut snapshots = Vec::new();
        for row in rows {
            let payload = row?;
            snapshots.push(serde_json::from_str::<Snapshot>(&payload)?);
        }

        Ok(snapshots)
    }

    pub fn load_replay_at_tick(
        &self,
        run_id: &str,
        tick: u64,
    ) -> Result<ReplaySlice, PersistenceError> {
        let snapshot = self.load_latest_snapshot_at_or_before(run_id, tick)?;
        let from_tick = snapshot.as_ref().map(|snap| snap.tick + 1).unwrap_or(1);
        let events = self.load_events_range(run_id, from_tick, tick)?;

        Ok(ReplaySlice { snapshot, events })
    }

    fn configure(&mut self) -> Result<(), PersistenceError> {
        self.conn.pragma_update(None, "journal_mode", "WAL")?;
        self.conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(())
    }

    fn migrate(&mut self) -> Result<(), PersistenceError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL,
                config_json TEXT NOT NULL,
                status_json TEXT NOT NULL,
                seed TEXT NOT NULL,
                duration_days INTEGER NOT NULL,
                region_id TEXT NOT NULL,
                snapshot_every_ticks INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS commands (
                run_id TEXT NOT NULL,
                command_id TEXT NOT NULL,
                issued_at_tick INTEGER NOT NULL,
                effective_tick INTEGER NOT NULL,
                accepted INTEGER NOT NULL,
                command_json TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, command_id)
            );

            CREATE TABLE IF NOT EXISTS events (
                run_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                tick INTEGER NOT NULL,
                sequence_in_tick INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, event_id),
                UNIQUE (run_id, tick, sequence_in_tick)
            );

            CREATE TABLE IF NOT EXISTS reason_packets (
                run_id TEXT NOT NULL,
                reason_packet_id TEXT NOT NULL,
                tick INTEGER NOT NULL,
                actor_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, reason_packet_id)
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                run_id TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                tick INTEGER NOT NULL,
                world_state_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, tick)
            );

            CREATE INDEX IF NOT EXISTS idx_events_run_tick ON events(run_id, tick);
            CREATE INDEX IF NOT EXISTS idx_events_run_type_tick ON events(run_id, event_type, tick);
            CREATE INDEX IF NOT EXISTS idx_reason_packets_run_tick_actor ON reason_packets(run_id, tick, actor_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_run_tick ON snapshots(run_id, tick);
            CREATE INDEX IF NOT EXISTS idx_commands_run_tick ON commands(run_id, issued_at_tick);
            ",
        )?;

        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at)
             VALUES(1, 'initial_v1', 'tick-000000')",
            [],
        )?;

        Ok(())
    }
}

fn upsert_run(
    tx: &rusqlite::Transaction<'_>,
    config: &RunConfig,
    status: &RunStatus,
) -> Result<(), PersistenceError> {
    let config_json = serde_json::to_string(config)?;
    let status_json = serde_json::to_string(status)?;
    let region_id = serde_json::to_string(&config.region_id)?
        .trim_matches('\"')
        .to_string();

    tx.execute(
        "INSERT INTO runs (
            run_id,
            schema_version,
            config_json,
            status_json,
            seed,
            duration_days,
            region_id,
            snapshot_every_ticks,
            created_at,
            updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(run_id) DO UPDATE SET
            schema_version = excluded.schema_version,
            config_json = excluded.config_json,
            status_json = excluded.status_json,
            seed = excluded.seed,
            duration_days = excluded.duration_days,
            region_id = excluded.region_id,
            snapshot_every_ticks = excluded.snapshot_every_ticks,
            updated_at = excluded.updated_at",
        params![
            config.run_id.as_str(),
            config.schema_version.as_str(),
            config_json,
            status_json,
            config.seed.to_string(),
            i64::from(config.duration_days),
            region_id,
            i64::try_from(config.snapshot_every_ticks).unwrap_or(i64::MAX),
            "tick-000000",
            tick_stamp(status.current_tick),
        ],
    )?;

    Ok(())
}

fn tick_stamp(tick: u64) -> String {
    format!("tick-{tick:06}")
}
