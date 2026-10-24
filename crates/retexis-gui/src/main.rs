//! `retexis-gui` — boot a node, then open a window.
//!
//! Everything about *being a node* lives in [`boot`]. This file is the desktop
//! application part: parse arguments, print a banner, hand the node to Slint.

use retexis_gui::app::Options;
use retexis_gui::{boot, bridge};
use std::process::ExitCode;

const USAGE: &str = "\
retexis-gui — a chat window for a retexis mesh

USAGE:
    retexis-gui [OPTIONS]

OPTIONS:
    --name <NAME>            display name            [default: <user>-<short id>]
    --id <HEX>               fixed 32-hex node id    [default: random]
    --psk <KEY>              shared key (HMAC auth)  [default: plaintext]
    --listen <URL>           listen, repeatable      [default: tcp://0.0.0.0:7100]
    --connect <URL>          dial at startup, repeatable
    --discover               scan at startup and dial everyone found
    --say <TEXT>             broadcast TEXT once at startup
    --no-announce            do not answer discovery queries
    --discovery-port <PORT>  UDP port for discovery  [default: 47700]
    --ttl <N>                hop budget              [default: 8]
    -h, --help               print this help

URLS:
    tcp://host:port          stream, length-prefixed
    udp://host:port          datagrams, one envelope each

EXAMPLES:
    retexis-gui                                  # 7100, announces itself
    retexis-gui --name alice --psk \"our secret\"
    retexis-gui --connect tcp://10.0.0.7:7100    # join an existing mesh

Closing the window stops the node. The banner in the terminal says which
address it listens on and which font the window borrowed for CJK.
";

fn main() -> ExitCode {
    let opts = match parse_args(std::env::args().skip(1)) {
        Ok(Some(opts)) => opts,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("! {e}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    let boot = match boot::boot(&opts) {
        Ok(boot) => boot,
        Err(e) => {
            eprintln!("! {e}");
            return ExitCode::FAILURE;
        }
    };
    for warning in &boot.warnings {
        eprintln!("! {warning}");
    }

    // The banner matters: the window does not show its own listen address, and
    // "which port am I on" is the first thing a second instance needs.
    println!(
        "retexis-gui  {}/{}   {}",
        boot.app.name(),
        boot.app.id().short(),
        if boot.listening.is_empty() {
            "no listener".to_string()
        } else {
            boot.listening.join(" ")
        }
    );
    if let Some(url) = boot.dial_url() {
        println!("  advertising {url}");
    }
    if let Some(r) = &boot.responder {
        println!("  answering discovery on udp://{}", r.addr());
    }
    println!("  close the window to stop");

    match bridge::run(boot) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("! cannot open a window: {e}");
            ExitCode::FAILURE
        }
    }
}

/// `Ok(None)` means `--help`.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut opts = Options::default();
    let mut args = args.peekable();
    let mut named = false;

    while let Some(arg) = args.next() {
        let mut value = |flag: &str| -> Result<String, String> {
            args.next().ok_or_else(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--name" => {
                opts.name = value("--name")?;
                named = true;
            }
            "--id" => {
                let raw = value("--id")?;
                opts.id = Some(
                    retexis_core::NodeId::from_hex(&raw)
                        .ok_or_else(|| format!("--id: '{raw}' is not hex"))?,
                );
            }
            "--psk" => opts.psk = Some(value("--psk")?),
            "--listen" => opts.listen.push(value("--listen")?),
            "--connect" => opts.connect.push(value("--connect")?),
            "--discover" => opts.discover = true,
            "--say" => opts.say = Some(value("--say")?),
            "--no-announce" => opts.announce = false,
            "--discovery-port" => {
                opts.discovery_port = value("--discovery-port")?
                    .parse()
                    .map_err(|_| "--discovery-port needs a number".to_string())?;
            }
            "--ttl" => {
                opts.ttl = value("--ttl")?
                    .parse()
                    .map_err(|_| "--ttl needs a number".to_string())?;
            }
            other => return Err(format!("unknown option '{other}'")),
        }
    }

    // No name given: derive one that is stable for this node and
    // distinguishable from another instance on the same machine.
    if !named && opts.name.is_empty() {
        let user = std::env::var("USERNAME")
            .or_else(|_| std::env::var("USER"))
            .unwrap_or_else(|_| "guest".into());
        let user: String = user.chars().take(16).collect();
        let id = opts.id.unwrap_or_else(retexis_core::NodeId::random);
        opts.id = Some(id);
        opts.name = format!("{user}-{}", id.short());
    }

    Ok(Some(opts))
}
