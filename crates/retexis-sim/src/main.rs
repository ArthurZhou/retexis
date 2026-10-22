//! `retexis-sim` — start the simulator and its web UI.
//!
//! ```text
//! retexis-sim                          # line x4 on http://127.0.0.1:8770
//! retexis-sim --preset grid --nodes 9
//! retexis-sim --port 9000 --open
//! ```

use retexis_sim::sim::Sim;
use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "\
retexis-sim — interactive mesh simulator (real routers, virtual switch, web UI)

USAGE:
    retexis-sim [OPTIONS]

OPTIONS:
    --host <ADDR>       bind address              [default: 127.0.0.1]
    --port <PORT>       bind port                 [default: 8770]
    --preset <KIND>     line | ring | grid | star [default: line]
    --nodes <N>         node count, 2..=16        [default: 4]
    --seed <N>          loss RNG seed             [default: 1234]
    --web <DIR>         directory holding index.html  [default: crates/retexis-sim/web]
    --open              open a browser window
    --no-open           do not open a browser (default when stdout is not a TTY)
    -h, --help          print this help
";

fn main() -> ExitCode {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8770;
    let mut preset = "line".to_string();
    let mut count = 4usize;
    let mut seed: u64 = 1234;
    let mut web: Option<PathBuf> = None;
    let mut open = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| -> Option<String> {
            match args.next() {
                Some(v) => Some(v),
                None => {
                    eprintln!("{name} needs a value\n\n{USAGE}");
                    None
                }
            }
        };
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            "--host" => match value("--host") {
                Some(v) => host = v,
                None => return ExitCode::FAILURE,
            },
            "--port" => match value("--port").and_then(|v| v.parse().ok()) {
                Some(v) => port = v,
                None => {
                    eprintln!("--port needs a number");
                    return ExitCode::FAILURE;
                }
            },
            "--preset" => match value("--preset") {
                Some(v) => preset = v,
                None => return ExitCode::FAILURE,
            },
            "--nodes" => match value("--nodes").and_then(|v| v.parse().ok()) {
                Some(v) => count = v,
                None => {
                    eprintln!("--nodes needs a number");
                    return ExitCode::FAILURE;
                }
            },
            "--seed" => match value("--seed").and_then(|v| v.parse().ok()) {
                Some(v) => seed = v,
                None => {
                    eprintln!("--seed needs a number");
                    return ExitCode::FAILURE;
                }
            },
            "--web" => match value("--web") {
                Some(v) => web = Some(PathBuf::from(v)),
                None => return ExitCode::FAILURE,
            },
            "--open" => open = true,
            "--no-open" => open = false,
            other => {
                eprintln!("unknown option '{other}'\n\n{USAGE}");
                return ExitCode::FAILURE;
            }
        }
    }

    if !matches!(preset.as_str(), "line" | "ring" | "grid" | "star") {
        eprintln!("unknown preset '{preset}' (line, ring, grid, star)");
        return ExitCode::FAILURE;
    }

    let web_dir = match web.or_else(find_web_dir) {
        Some(dir) => dir,
        None => {
            eprintln!("cannot find index.html — pass --web <dir>");
            return ExitCode::FAILURE;
        }
    };

    let sim = Sim::new(seed);
    sim.reset(&preset, count);

    let addr = format!("{host}:{port}");
    let url = format!("http://{addr}/");
    println!("retexis-sim  →  {url}");
    println!(
        "  {preset} x{}   ui: {}   real routers, virtual switch",
        count.min(16),
        web_dir.join("index.html").display()
    );
    println!("  Ctrl-C to stop");

    if open {
        open_browser(&url);
    }

    match retexis_sim::http::serve(&addr, sim, web_dir) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("cannot serve on {addr}: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Looks for the UI next to the binary, then upwards from the cwd.
///
/// The HTML lives at `crates/retexis-sim/web/index.html`, i.e. inside this
/// crate, so a packaged build can ship the directory next to the executable
/// and `cargo run` can find it in the source tree. Searching relative to the
/// executable first means either layout works from any working directory.
fn find_web_dir() -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        let mut dir = exe.parent().map(|p| p.to_path_buf());
        for _ in 0..5 {
            if let Some(d) = &dir {
                candidates.push(d.join("web"));
                candidates.push(d.join("crates/retexis-sim/web"));
                dir = d.parent().map(|p| p.to_path_buf());
            }
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let mut dir = Some(cwd);
        for _ in 0..4 {
            if let Some(d) = &dir {
                candidates.push(d.join("web"));
                candidates.push(d.join("crates/retexis-sim/web"));
                dir = d.parent().map(|p| p.to_path_buf());
            }
        }
    }
    // Last resort for `cargo run` from an unrelated directory: the crate
    // directory is known at compile time.
    candidates.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web"));
    candidates
        .into_iter()
        .find(|p| p.join("index.html").is_file())
}

fn open_browser(url: &str) {
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("cmd")
        .args(["/C", "start", "", url])
        .spawn();
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(url).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let _ = std::process::Command::new("xdg-open").arg(url).spawn();
}
