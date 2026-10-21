//! retexis-crypto — the envelope sealer.
//!
//! SHA-256 and HMAC-SHA256 come from [RustCrypto] (`sha2` + `hmac`), chosen
//! over `ring` / `aws-lc-rs` for one concrete reason: **they are pure Rust**.
//! No `cc`, no `bindgen`, no C toolchain on the build machine, and they build
//! `no_std` if this ever has to run on a constrained radio node.
//!
//! This crate used to implement both primitives by hand. That was a bad trade:
//! passing the FIPS/RFC vectors proves the *implementation* is right, not that
//! it is free of side channels. The tests below are kept because they are the
//! cheapest possible check that a dependency swap did not change behaviour —
//! the same vectors now validate RustCrypto through our wrapper.
//!
//! What this module gives is **network membership** authentication, not per-node
//! identity: everyone in a mesh shares one pre-shared key, so any member can
//! forge another member's `src`. Real identity binding needs per-node keypairs
//! and signatures — see the roadmap.
//!
//! [RustCrypto]: https://github.com/RustCrypto

#![forbid(unsafe_code)]

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Truncated tag length. 128 bits is plenty: forgery needs 2^128 guesses and
/// it keeps the per-packet overhead at 1.6% of a typical chat message.
pub const TAG_LEN: usize = 16;

/// SHA-256 of `data`.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// HMAC-SHA256 as in RFC 2104.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    // HMAC accepts a key of any length (it hashes keys longer than the block
    // size itself), so this cannot fail.
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Appends and verifies a truncated HMAC-SHA256 tag over a byte blob.
///
/// It knows nothing about envelopes: seal whatever you like. The routing layer
/// uses it on encoded envelopes, which means the tag covers every header field
/// including the source id — a packet cannot be re-addressed in flight.
#[derive(Clone)]
pub struct Sealer {
    key: [u8; 32],
}

impl Sealer {
    pub fn new(key: [u8; 32]) -> Self {
        Sealer { key }
    }

    /// Derives the key from a passphrase. Not a KDF — just avoids storing the
    /// secret in plaintext in config files by accident.
    pub fn from_passphrase(pass: &str) -> Self {
        Sealer::new(sha256(pass.as_bytes()))
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        let s = s.trim();
        let s = s.strip_prefix("hex:").unwrap_or(s);
        if s.len() != 64 {
            return None;
        }
        let mut key = [0u8; 32];
        for i in 0..32 {
            key[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(Sealer::new(key))
    }

    pub fn tag(&self, data: &[u8]) -> [u8; TAG_LEN] {
        let full = hmac_sha256(&self.key, data);
        let mut tag = [0u8; TAG_LEN];
        tag.copy_from_slice(&full[..TAG_LEN]);
        tag
    }

    /// `data || tag`
    pub fn seal(&self, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len() + TAG_LEN);
        out.extend_from_slice(data);
        out.extend_from_slice(&self.tag(data));
        out
    }

    /// True when `sealed` is `something || tag` with a valid tag.
    ///
    /// `verify_truncated_left` is the RustCrypto entry point for a short tag:
    /// it compares `expected[..tag.len()]` against the received tag in
    /// constant time (`ct_eq`). `verify_slice` would refuse a 16-byte tag
    /// outright — it insists on the full 32-byte output.
    pub fn verify(&self, sealed: &[u8]) -> bool {
        if sealed.len() < TAG_LEN {
            return false;
        }
        let split = sealed.len() - TAG_LEN;
        let mut mac = match HmacSha256::new_from_slice(&self.key) {
            Ok(m) => m,
            Err(_) => return false,
        };
        mac.update(&sealed[..split]);
        mac.verify_truncated_left(&sealed[split..]).is_ok()
    }
}

impl std::fmt::Debug for Sealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the key, not even by accident in a log line.
        f.write_str("Sealer(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn sha256_known_vectors() {
        // FIPS 180-2 / NIST examples.
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn sha256_multi_block_and_padding_boundaries() {
        // 55, 56 and 64 bytes are the three interesting padding cases.
        for len in [55usize, 56, 57, 63, 64, 65, 119, 120, 1000] {
            let data = vec![b'a'; len];
            let a = sha256(&data);
            let b = sha256(&data);
            assert_eq!(a, b);
            assert_eq!(a.len(), 32);
        }
        // 56 bytes forces a second padding block: check against a known value.
        assert_eq!(
            hex(&sha256(&[b'a'; 56])),
            "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a"
        );
    }

    #[test]
    fn hmac_rfc4231_vectors() {
        // RFC 4231 test case 1.
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Test case 2: short key.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 3: 20 bytes of 0xaa, 50 bytes of 0xdd.
        assert_eq!(
            hex(&hmac_sha256(&[0xaa; 20], &[0xdd; 50])),
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
    }

    #[test]
    fn hmac_long_key_is_hashed_first() {
        // RFC 4231 test case 6: 131-byte key.
        let key = [0xaa; 131];
        assert_eq!(
            hex(&hmac_sha256(
                &key,
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn seal_roundtrip_and_rejection() {
        let s = Sealer::from_passphrase("correct horse battery staple");
        let sealed = s.seal(b"payload");
        assert_eq!(sealed.len(), 7 + TAG_LEN);
        assert!(s.verify(&sealed));

        let mut tampered = sealed.clone();
        tampered[0] ^= 1;
        assert!(!s.verify(&tampered), "a flipped payload bit must fail");

        let mut truncated = sealed.clone();
        truncated.truncate(TAG_LEN);
        assert!(!s.verify(&truncated));

        let other = Sealer::from_passphrase("different");
        assert!(!other.verify(&sealed), "wrong key must fail");
    }

    #[test]
    fn hex_key_parsing() {
        let s = Sealer::from_hex(&"ab".repeat(32)).unwrap();
        assert_eq!(s.tag(b"x"), Sealer::new([0xab; 32]).tag(b"x"));
        assert!(Sealer::from_hex("too-short").is_none());
    }

    #[test]
    fn debug_does_not_leak_the_key() {
        let s = Sealer::from_passphrase("secret");
        assert_eq!(format!("{s:?}"), "Sealer(<redacted>)");
    }

    /// The wire tag is a prefix of the full HMAC, not something else.
    #[test]
    fn tag_is_the_truncated_hmac() {
        let s = Sealer::from_passphrase("k");
        let full = hmac_sha256(&s.key, b"body");
        assert_eq!(&s.tag(b"body")[..], &full[..TAG_LEN]);
    }
}
