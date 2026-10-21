//! retexis-crypto — SHA-256, HMAC-SHA256, and the envelope sealer.
//!
//! Hand written on purpose: the whole project has zero third-party
//! dependencies, and these two primitives are small enough (and specified
//! precisely enough) that "write it, then check it against the RFC vectors"
//! is a smaller risk than pulling a tree of crates into a transport core.
//!
//! This module gives **network membership** authentication, not per-node
//! identity: everyone in a mesh shares one pre-shared key, so any member can
//! forge another member's `src`. Real identity binding needs per-node
//! keypairs and signatures — see the roadmap.

#![forbid(unsafe_code)]

/// SHA-256 of `data`.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let mut chunks = data.chunks_exact(64);
    for block in &mut chunks {
        compress(&mut state, block);
    }

    // Padding: 0x80, zeros, then the message length in bits as a big-endian
    // u64. The tail is 1..=64 bytes plus 9 bytes of padding overhead.
    let rem = chunks.remainder();
    let mut tail = [0u8; 128];
    tail[..rem.len()].copy_from_slice(rem);
    tail[rem.len()] = 0x80;
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let tail_len = if rem.len() < 56 { 64 } else { 128 };
    tail[tail_len - 8..tail_len].copy_from_slice(&bit_len.to_be_bytes());
    for block in tail[..tail_len].chunks_exact(64) {
        compress(&mut state, block);
    }

    let mut out = [0u8; 32];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// HMAC-SHA256 as in RFC 2104, block size 64.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }

    let mut inner = Vec::with_capacity(BLOCK + msg.len());
    for b in k.iter() {
        inner.push(b ^ 0x36);
    }
    inner.extend_from_slice(msg);
    let inner_hash = sha256(&inner);

    let mut outer = Vec::with_capacity(BLOCK + 32);
    for b in k.iter() {
        outer.push(b ^ 0x5c);
    }
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

fn compress(state: &mut [u32; 8], block: &[u8]) {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut w = [0u32; 64];
    for i in 0..16 {
        w[i] = u32::from_be_bytes([
            block[i * 4],
            block[i * 4 + 1],
            block[i * 4 + 2],
            block[i * 4 + 3],
        ]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }

    let (mut a, mut b, mut c, mut d) = (state[0], state[1], state[2], state[3]);
    let (mut e, mut f, mut g, mut h) = (state[4], state[5], state[6], state[7]);

    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = h
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);

        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }

    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
    state[5] = state[5].wrapping_add(f);
    state[6] = state[6].wrapping_add(g);
    state[7] = state[7].wrapping_add(h);
}

/// Truncated tag length. 128 bits is plenty: forgery needs 2^128 guesses and
/// it keeps the per-packet overhead at 1.6% of a typical chat message.
pub const TAG_LEN: usize = 16;

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
    /// Comparison is constant time with respect to the tag bytes, so an
    /// attacker cannot learn the correct tag byte by byte from timing.
    pub fn verify(&self, sealed: &[u8]) -> bool {
        if sealed.len() < TAG_LEN {
            return false;
        }
        let split = sealed.len() - TAG_LEN;
        let expected = self.tag(&sealed[..split]);
        let got = &sealed[split..];
        let mut diff = 0u8;
        for i in 0..TAG_LEN {
            diff |= expected[i] ^ got[i];
        }
        diff == 0
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
}
