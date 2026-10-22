//! `retexis` — the binary.
//!
//! Ten lines, on purpose. Everything the command does lives in
//! `retexis_app::cli`, so this crate is only a name and an entry point: the
//! protocol is in `retexis-data` and `retexis-routing`, the application is in
//! `retexis-app`, and nothing needs to be recompiled to change either.

fn main() {
    retexis_app::cli::run();
}
