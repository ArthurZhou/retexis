//! Compiles `ui/chat.slint` into Rust.
//!
//! The generated module is included from `lib.rs`; nothing else in the crate
//! knows that a compiler ran.

fn main() {
    slint_build::compile("ui/chat.slint").expect("ui/chat.slint does not compile");
}
