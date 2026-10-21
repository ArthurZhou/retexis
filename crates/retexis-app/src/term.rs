//! Line-oriented stdout with a global lock.
//!
//! Deliveries arrive on a background thread while the REPL prints on the main
//! thread; without a lock, lines interleave mid-word.

use std::fmt;
use std::sync::Mutex;

static OUT: Mutex<()> = Mutex::new(());

pub fn line(msg: impl fmt::Display) {
    let _guard = OUT.lock().unwrap_or_else(|e| e.into_inner());
    println!("{msg}");
}

pub fn lines(msg: &str) {
    let _guard = OUT.lock().unwrap_or_else(|e| e.into_inner());
    println!("{msg}");
}
