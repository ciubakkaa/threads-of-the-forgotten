use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use contracts::RunConfig;
use kernel_api::{serve, EngineApi};

fn print_usage() {
    println!("kernel-cli <command>");
    println!("commands:");
    println!("  status");
    println!("  start");
    println!("  pause");
    println!("  step [n]");
    println!("  run-to <tick>");
    println!("  serve [addr]");
    println!("    default addr: 127.0.0.1:8080");
    println!(
        "  simulate <run_id> <seed> [ticks] [sqlite_path] [--ticks N] [--db PATH] [--npc-count N|--npc-min N --npc-max N]"
    );
    println!("    runs deterministic simulation to target tick and persists to sqlite");
}

fn parse_u64(value: Option<&String>, label: &str) -> Result<u64, String> {
    let raw = value.ok_or_else(|| format!("missing {}", label))?;
    raw.parse::<u64>()
        .map_err(|_| format!("invalid {}: {}", label, raw))
}

fn parse_socket_addr(value: Option<&String>) -> Result<SocketAddr, String> {
    let raw = value.map(String::as_str).unwrap_or("127.0.0.1:8080");
    raw.parse::<SocketAddr>()
        .map_err(|_| format!("invalid addr: {raw}"))
}

fn parse_seed(value: Option<&String>) -> Result<u64, String> {
    let raw = value.ok_or_else(|| "missing seed".to_string())?;
    raw.parse::<u64>()
        .map_err(|_| format!("invalid seed: {raw}"))
}

fn parse_u16(raw: &str, label: &str) -> Result<u16, String> {
    raw.parse::<u16>()
        .map_err(|_| format!("invalid {}: {}", label, raw))
}

fn default_sqlite_path() -> String {
    std::env::var("THREADS_SQLITE_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "threads_runs.sqlite".to_string())
}

fn parse_sqlite_path(value: Option<&String>) -> String {
    value
        .map(String::to_string)
        .filter(|path| !path.trim().is_empty())
        .unwrap_or_else(default_sqlite_path)
}

fn run_simulation(args: &[String]) -> Result<(), String> {
    let run_id = args
        .get(2)
        .cloned()
        .ok_or_else(|| "missing run_id".to_string())?;
    let seed = parse_seed(args.get(3))?;
    let mut target_tick = 720_u64;
    let mut sqlite_path = default_sqlite_path();
    let mut npc_count_min = None::<u16>;
    let mut npc_count_max = None::<u16>;

    let mut rest = args
        .get(4..)
        .map(|slice| slice.to_vec())
        .unwrap_or_default();
    if let Some(value) = rest.first() {
        if !value.starts_with("--") {
            target_tick = value
                .parse::<u64>()
                .map_err(|_| format!("invalid ticks: {value}"))?;
            rest.remove(0);
        }
    }
    if let Some(value) = rest.first() {
        if !value.starts_with("--") {
            sqlite_path = parse_sqlite_path(Some(value));
            rest.remove(0);
        }
    }

    let mut idx = 0usize;
    while idx < rest.len() {
        match rest[idx].as_str() {
            "--ticks" => {
                let value = rest
                    .get(idx + 1)
                    .ok_or_else(|| "missing value for --ticks".to_string())?;
                target_tick = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid ticks: {value}"))?;
                idx += 2;
            }
            "--sqlite-path" | "--db" => {
                let value = rest
                    .get(idx + 1)
                    .ok_or_else(|| "missing value for --sqlite-path".to_string())?;
                sqlite_path = parse_sqlite_path(Some(value));
                idx += 2;
            }
            "--npc-count" => {
                let value = rest
                    .get(idx + 1)
                    .ok_or_else(|| "missing value for --npc-count".to_string())?;
                let parsed = parse_u16(value, "npc-count")?;
                npc_count_min = Some(parsed);
                npc_count_max = Some(parsed);
                idx += 2;
            }
            "--npc-min" => {
                let value = rest
                    .get(idx + 1)
                    .ok_or_else(|| "missing value for --npc-min".to_string())?;
                npc_count_min = Some(parse_u16(value, "npc-min")?);
                idx += 2;
            }
            "--npc-max" => {
                let value = rest
                    .get(idx + 1)
                    .ok_or_else(|| "missing value for --npc-max".to_string())?;
                npc_count_max = Some(parse_u16(value, "npc-max")?);
                idx += 2;
            }
            other => return Err(format!("unknown simulate argument: {}", other)),
        }
    }

    let mut config = RunConfig::default();
    config.run_id = run_id.clone();
    config.seed = seed;
    config.duration_days = ((target_tick + 23) / 24).max(1) as u32;
    config.snapshot_every_ticks = 24;
    if let Some(min) = npc_count_min {
        config.npc_count_min = min;
    }
    if let Some(max) = npc_count_max {
        config.npc_count_max = max;
    }
    if config.npc_count_min > config.npc_count_max {
        return Err(format!(
            "invalid npc bounds: min {} > max {}",
            config.npc_count_min, config.npc_count_max
        ));
    }

    let mut api = EngineApi::from_config(config);
    api.attach_sqlite_store(PathBuf::from(&sqlite_path))
        .map_err(|err| format!("failed to attach sqlite store: {err}"))?;
    api.initialize_run_storage(true)
        .map_err(|err| format!("failed to initialize run storage: {err}"))?;
    let _ = api.start();
    let (status, committed) = api.run_to_tick(target_tick);
    let current_tick = status.current_tick;
    let max_ticks = status.max_ticks;
    let _ = api.pause();

    if let Some(error) = api.last_persistence_error() {
        return Err(format!("persistence error after simulation: {error}"));
    }

    println!(
        "simulated run_id={} seed={} committed={} tick={}/{} sqlite={} npc_count_min={} npc_count_max={}",
        run_id,
        seed,
        committed,
        current_tick,
        max_ticks,
        sqlite_path,
        api.config().npc_count_min,
        api.config().npc_count_max
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(String::as_str);

    let mut api = EngineApi::from_config(RunConfig::default());

    match command {
        Some("status") => {
            println!("{}", api.status());
        }
        Some("start") => {
            let status = api.start();
            println!("started: {}", status);
        }
        Some("pause") => {
            let status = api.pause();
            println!("paused: {}", status);
        }
        Some("step") => {
            let steps = args.get(2).and_then(|v| v.parse::<u64>().ok()).unwrap_or(1);
            let (status, committed) = api.step(steps);
            println!("stepped={} {}", committed, status);
        }
        Some("run-to") => match parse_u64(args.get(2), "tick") {
            Ok(target_tick) => {
                let (status, committed) = api.run_to_tick(target_tick);
                println!("committed={} {}", committed, status);
            }
            Err(err) => {
                eprintln!("error: {}", err);
                print_usage();
                std::process::exit(2);
            }
        },
        Some("serve") => match parse_socket_addr(args.get(2)) {
            Ok(addr) => {
                println!("serving api on http://{addr}");
                if let Err(err) = serve(addr).await {
                    eprintln!("server error: {err}");
                    std::process::exit(1);
                }
            }
            Err(err) => {
                eprintln!("error: {}", err);
                print_usage();
                std::process::exit(2);
            }
        },
        Some("simulate") => {
            if let Err(err) = run_simulation(&args) {
                eprintln!("error: {err}");
                print_usage();
                std::process::exit(2);
            }
        }
        _ => {
            print_usage();
        }
    }
}
