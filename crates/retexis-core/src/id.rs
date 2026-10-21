//! Node / message identifiers.
//!
//! Identifiers are 128-bit opaque values rendered as lowercase hex.
//! [`NodeId::short`] gives the 8-character form used in the UI.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_decode_16(s: &str) -> Option<[u8; 16]> {
    let b = s.as_bytes();
    if b.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = (hex_val(b[i * 2])? << 4) | hex_val(b[i * 2 + 1])?;
    }
    Some(out)
}

/// Cheap xorshift generator, seeded from wall clock + pid + a counter.
///
/// This is *not* cryptographically secure and is only used to mint
/// collision-resistant identifiers for a demo-grade node. A production
/// deployment should derive `NodeId` from a keypair (e.g. BLAKE3 of an
/// Ed25519 public key) so identity is verifiable.
fn random_bytes() -> [u8; 16] {
    static COUNTER: AtomicU64 = AtomicU64::new(0x243f_6a88_85a3_08d3);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e37_79b9);
    let mut s = nanos
        ^ COUNTER.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
        ^ ((std::process::id() as u64) << 40)
        | 1;
    let mut out = [0u8; 16];
    for chunk in out.chunks_mut(8) {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        chunk.copy_from_slice(&s.to_le_bytes());
    }
    out
}

macro_rules! id_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
        pub struct $name([u8; 16]);

        impl $name {
            pub const ZERO: $name = $name([0u8; 16]);

            pub fn random() -> Self {
                $name(random_bytes())
            }

            pub fn from_bytes(b: [u8; 16]) -> Self {
                $name(b)
            }

            pub fn from_slice(b: &[u8]) -> Option<Self> {
                if b.len() == 16 {
                    let mut out = [0u8; 16];
                    out.copy_from_slice(b);
                    Some($name(out))
                } else {
                    None
                }
            }

            pub fn from_hex(s: &str) -> Option<Self> {
                hex_decode_16(s.trim()).map($name)
            }

            pub fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            pub fn is_zero(&self) -> bool {
                self.0 == [0u8; 16]
            }

            /// Full 32-character hex form.
            pub fn hex(&self) -> String {
                hex_encode(&self.0)
            }

            /// 8-character form used in logs and the chat UI.
            pub fn short(&self) -> String {
                hex_encode(&self.0[..4])
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.short())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.short())
            }
        }
    };
}

id_type!(NodeId, "Stable identity of a mesh node.");
id_type!(
    MessageId,
    "Unique identity of one envelope, used for loop suppression."
);

impl NodeId {
    /// Parses either the short (8 char) or full (32 char) hex form, or an
    /// exact case-insensitive match against a supplied name.
    pub fn parse_loose(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some(id) = Self::from_hex(s) {
            return Some(id);
        }
        if s.len() == 8 {
            let mut padded = String::with_capacity(32);
            padded.push_str(s);
            padded.push_str("000000000000000000000000");
            return Self::from_hex(&padded);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let id = NodeId::random();
        assert_eq!(NodeId::from_hex(&id.hex()), Some(id));
        assert_eq!(id.short().len(), 8);
    }

    #[test]
    fn ids_are_not_identical() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_ne!(a, b);
    }

    #[test]
    fn loose_parse_short_form() {
        let id = NodeId::random();
        let parsed = NodeId::parse_loose(&id.short()).unwrap();
        assert_eq!(parsed.short(), id.short());
    }
}
