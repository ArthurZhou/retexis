//! Argument parsing. Hand rolled: a handful of flags do not justify a
//! dependency.

use retexis_core::NodeId;
use retexis_crypto::Sealer;

#[derive(Debug, Clone)]
pub struct Options {
    pub name: String,
    pub id: Option<NodeId>,
    pub listen: Vec<String>,
    pub connect: Vec<String>,
    pub announce_ms: u64,
    pub route_timeout_ms: u64,
    pub ttl: u8,
    /// Pre-shared key for mesh membership, if any.
    pub psk: Option<String>,
    pub retry_max: u8,
    pub retry_ms: u64,
    pub retry_max_ms: u64,
    pub flood_rate: f64,
    pub flood_burst: f64,
    pub flood_limit: bool,
    pub reconnect: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            name: String::new(),
            id: None,
            listen: Vec::new(),
            connect: Vec::new(),
            announce_ms: 1000,
            route_timeout_ms: 6000,
            ttl: 8,
            psk: None,
            retry_max: 4,
            retry_ms: 300,
            retry_max_ms: 5000,
            flood_rate: 200.0,
            flood_burst: 100.0,
            flood_limit: true,
            reconnect: true,
        }
    }
}

pub const USAGE: &str = "\
retexis — a mesh transport node

USAGE:
    retexis [OPTIONS]

IDENTITY & SECURITY:
    --name <name>            Node name shown to peers (default: node-<id>)
    --id <hex>               Fixed 32-char node id (default: random)
    --psk <secret>           Pre-shared mesh key. A passphrase, or hex:<64 hex>.
                             Every packet gets a truncated HMAC-SHA256 tag.
                             Without it the mesh is plaintext and open.

LINKS:
    --listen <url>           Listen for inbound links, repeatable
    --connect <url>          Dial a peer, repeatable. Supervised with
                             exponential backoff unless --no-reconnect.
    --no-reconnect           Dial once and give up if the link dies

ROUTING:
    --ttl <n>                Hop budget for outgoing messages (default 8)
    --announce-ms <n>        Route advertisement interval (default 1000)
    --route-timeout-ms <n>   Route expiry (default 6000)

RELIABILITY:
    --retry-max <n>          Transmissions before a reliable unicast fails (4)
    --retry-ms <n>           First retry delay, doubles from there (300)
    --retry-max-ms <n>       Retry delay ceiling (5000)

FLOOD CONTROL:
    --flood-rate <n>         Relayed broadcasts per second per node (200)
    --flood-burst <n>        Allowed burst above that rate (100)
    --no-flood-limit         Disable suppression (shows the storm)

    -h, --help               Show this help

URLS:
    tcp://host:port          plain TCP (the only medium compiled in today)

EXAMPLES:
    retexis --name alice --listen tcp://0.0.0.0:7100 --psk \"our secret\"
    retexis --name bob   --connect tcp://127.0.0.1:7100 --psk \"our secret\"

REPL:
    <text>                   broadcast to the mesh on the chat channel
    /msg <peer|id> <text>    reliable unicast (acked, retransmitted)
    /peers /routes /status /id /kick /help /quit
";

impl Options {
    /// Parses arguments, printing usage and exiting on `--help` or bad input.
    pub fn parse(args: impl Iterator<Item = String>) -> Options {
        let mut opts = Options::default();
        let mut args = args.peekable();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                "--name" => opts.name = value(&mut args, "--name"),
                "--id" => {
                    let raw = value(&mut args, "--id");
                    match NodeId::from_hex(&raw) {
                        Some(id) => opts.id = Some(id),
                        None => die(&format!("--id must be 32 hex characters, got '{raw}'")),
                    }
                }
                "--psk" => {
                    let raw = value(&mut args, "--psk");
                    // Validate now rather than failing on the first packet.
                    if raw.starts_with("hex:") && Sealer::from_hex(&raw).is_none() {
                        die("--psk hex: form needs exactly 64 hex characters");
                    }
                    opts.psk = Some(raw);
                }
                "--listen" => opts.listen.push(value(&mut args, "--listen")),
                "--connect" => opts.connect.push(value(&mut args, "--connect")),
                "--no-reconnect" => opts.reconnect = false,
                "--no-flood-limit" => opts.flood_limit = false,
                "--ttl" => opts.ttl = parse_num(&value(&mut args, "--ttl"), "--ttl", 1, 255) as u8,
                "--announce-ms" => {
                    opts.announce_ms = parse_num(
                        &value(&mut args, "--announce-ms"),
                        "--announce-ms",
                        50,
                        3_600_000,
                    )
                }
                "--route-timeout-ms" => {
                    opts.route_timeout_ms = parse_num(
                        &value(&mut args, "--route-timeout-ms"),
                        "--route-timeout-ms",
                        100,
                        86_400_000,
                    )
                }
                "--retry-max" => {
                    opts.retry_max =
                        parse_num(&value(&mut args, "--retry-max"), "--retry-max", 1, 32) as u8
                }
                "--retry-ms" => {
                    opts.retry_ms =
                        parse_num(&value(&mut args, "--retry-ms"), "--retry-ms", 10, 60_000)
                }
                "--retry-max-ms" => {
                    opts.retry_max_ms = parse_num(
                        &value(&mut args, "--retry-max-ms"),
                        "--retry-max-ms",
                        10,
                        600_000,
                    )
                }
                "--flood-rate" => {
                    opts.flood_rate =
                        parse_float(&value(&mut args, "--flood-rate"), "--flood-rate", 0.0, 1e9)
                }
                "--flood-burst" => {
                    opts.flood_burst = parse_float(
                        &value(&mut args, "--flood-burst"),
                        "--flood-burst",
                        0.0,
                        1e9,
                    )
                }
                other => die(&format!("unknown argument '{other}' (try --help)")),
            }
        }

        if opts.route_timeout_ms <= opts.announce_ms * 2 {
            eprintln!(
                "! warning: --route-timeout-ms should be at least twice --announce-ms, otherwise routes flap"
            );
        }
        if opts.retry_max_ms < opts.retry_ms {
            die("--retry-max-ms must be >= --retry-ms");
        }
        opts
    }

    /// Builds the sealer from `--psk`, if one was given.
    pub fn sealer(&self) -> Option<Sealer> {
        self.psk.as_ref().map(|raw| match Sealer::from_hex(raw) {
            Some(s) => s,
            None => Sealer::from_passphrase(raw),
        })
    }
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    match args.next() {
        Some(v) if !v.starts_with("--") => v,
        _ => die(&format!("{flag} needs a value")),
    }
}

fn parse_num(raw: &str, flag: &str, min: u64, max: u64) -> u64 {
    match raw.parse::<u64>() {
        Ok(n) if n >= min && n <= max => n,
        _ => die(&format!(
            "{flag} must be a number in {min}..={max}, got '{raw}'"
        )),
    }
}

fn parse_float(raw: &str, flag: &str, min: f64, max: f64) -> f64 {
    match raw.parse::<f64>() {
        Ok(n) if n >= min && n <= max => n,
        _ => die(&format!(
            "{flag} must be a number in {min}..={max}, got '{raw}'"
        )),
    }
}

fn die(msg: &str) -> ! {
    eprintln!("! {msg}");
    eprintln!("\n{USAGE}");
    std::process::exit(2);
}
