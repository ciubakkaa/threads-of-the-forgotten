use std::env;
use std::net::SocketAddr;

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
        _ => {
            print_usage();
        }
    }
}
