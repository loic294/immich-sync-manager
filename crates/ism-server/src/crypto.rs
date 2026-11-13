//! Secrets at rest, password hashing, token hashing and request signing.
//!
//! - [`SecretBox`]: XChaCha20-Poly1305 with a random 32-byte key file (mode 0600) in the data
//!   directory. Stored form `v1.<base64url(nonce || ciphertext)>`. The associated data is a
//!   purpose string, so a value encrypted for one purpose cannot be replayed as another.
//! - Admin passwords: PBKDF2-HMAC-SHA256, 210 000 iterations, random salt.
//! - Client tokens and session ids: only their SHA-256 is stored.
//! - Peer requests: HMAC-SHA256 over method, path, timestamp and body hash.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use thiserror::Error;

const KEY_FILE: &str = "secret.key";
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
const VERSION_PREFIX: &str = "v1.";
const PBKDF2_ITERATIONS: u32 = 210_000;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("key file io: {0}")]
    Io(#[from] io::Error),
    #[error("the key file is not a valid key")]
    BadKey,
    #[error("stored secret is malformed")]
    Malformed,
    #[error("stored secret cannot be decrypted with this key")]
    Decrypt,
}

pub struct SecretBox {
    cipher: XChaCha20Poly1305,
}

impl SecretBox {
    /// Opens the key of `data_dir`, creating it on first start.
    pub fn open_or_create(data_dir: &Path) -> Result<Self, CryptoError> {
        let path = data_dir.join(KEY_FILE);
        match fs::File::open(&path) {
            Ok(mut file) => {
                let mut key = Vec::new();
                file.read_to_end(&mut key)?;
                let key: [u8; KEY_LEN] = key.try_into().map_err(|_| CryptoError::BadKey)?;
                Ok(Self::new(key))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut key = [0u8; KEY_LEN];
                rand::fill(&mut key);
                let mut options = OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(&path)?;
                file.write_all(&key)?;
                file.sync_all()?;
                Ok(Self::new(key))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn new(key: [u8; KEY_LEN]) -> Self {
        Self {
            cipher: XChaCha20Poly1305::new(&Key::from(key)),
        }
    }

    pub fn encrypt(&self, purpose: &str, plaintext: &str) -> Result<String, CryptoError> {
        let mut nonce = [0u8; NONCE_LEN];
        rand::fill(&mut nonce);
        let ciphertext = self
            .cipher
            .encrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: plaintext.as_bytes(),
                    aad: purpose.as_bytes(),
                },
            )
            .map_err(|_| CryptoError::Decrypt)?;
        let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ciphertext);
        Ok(format!("{VERSION_PREFIX}{}", URL_SAFE_NO_PAD.encode(blob)))
    }

    pub fn decrypt(&self, purpose: &str, stored: &str) -> Result<String, CryptoError> {
        let encoded = stored
            .strip_prefix(VERSION_PREFIX)
            .ok_or(CryptoError::Malformed)?;
        let blob = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| CryptoError::Malformed)?;
        if blob.len() < NONCE_LEN + TAG_LEN {
            return Err(CryptoError::Malformed);
        }
        let (nonce, ciphertext) = blob.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| CryptoError::Malformed)?;
        let plaintext = self
            .cipher
            .decrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: ciphertext,
                    aad: purpose.as_bytes(),
                },
            )
            .map_err(|_| CryptoError::Decrypt)?;
        String::from_utf8(plaintext).map_err(|_| CryptoError::Malformed)
    }
}

type HmacSha256 = Hmac<Sha256>;

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    #[allow(clippy::expect_used)]
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    // PBKDF2 with one output block (dkLen = hLen = 32).
    let mut u = hmac(password, &[salt, &1u32.to_be_bytes()]);
    let mut out = u;
    for _ in 1..iterations {
        u = hmac(password, &[&u]);
        for (o, x) in out.iter_mut().zip(u.iter()) {
            *o ^= x;
        }
    }
    out
}

pub fn hash_password(password: &str) -> String {
    let mut salt = [0u8; 16];
    rand::fill(&mut salt);
    let hash = pbkdf2(password.as_bytes(), &salt, PBKDF2_ITERATIONS);
    format!(
        "pbkdf2-sha256${PBKDF2_ITERATIONS}${}${}",
        URL_SAFE_NO_PAD.encode(salt),
        URL_SAFE_NO_PAD.encode(hash)
    )
}

pub fn verify_password(password: &str, stored: &str) -> bool {
    let mut parts = stored.split('$');
    let (Some("pbkdf2-sha256"), Some(iter), Some(salt), Some(hash), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return false;
    };
    let (Ok(iter), Ok(salt), Ok(hash)) = (
        iter.parse::<u32>(),
        URL_SAFE_NO_PAD.decode(salt),
        URL_SAFE_NO_PAD.decode(hash),
    ) else {
        return false;
    };
    constant_time_eq(&pbkdf2(password.as_bytes(), &salt, iter), &hash)
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// A random URL-safe secret with `bytes` bytes of entropy.
pub fn random_secret(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::fill(buf.as_mut_slice());
    URL_SAFE_NO_PAD.encode(buf)
}

/// A random lowercase hex id, `prefix_` + 16 hex characters.
pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{:016x}", rand::random::<u64>())
}

/// The signature of one peer request.
pub fn sign_request(
    secret: &str,
    method: &str,
    path_and_query: &str,
    timestamp: i64,
    body: &[u8],
) -> String {
    let body_hash = sha256_hex(body);
    hex::encode(hmac(
        secret.as_bytes(),
        &[
            method.as_bytes(),
            b"\n",
            path_and_query.as_bytes(),
            b"\n",
            timestamp.to_string().as_bytes(),
            b"\n",
            body_hash.as_bytes(),
        ],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_box_round_trips_and_binds_purpose() {
        let dir = tempfile::tempdir().unwrap();
        let sb = SecretBox::open_or_create(dir.path()).unwrap();
        let stored = sb.encrypt("api-key", "hello").unwrap();
        assert_eq!(sb.decrypt("api-key", &stored).unwrap(), "hello");
        assert!(sb.decrypt("other", &stored).is_err());
        let again = SecretBox::open_or_create(dir.path()).unwrap();
        assert_eq!(again.decrypt("api-key", &stored).unwrap(), "hello");
    }

    #[test]
    fn pbkdf2_matches_rfc_7914_vector() {
        // RFC 7914 section 11: PBKDF2-HMAC-SHA256("passwd", "salt", 1, 64), first 32 bytes.
        assert_eq!(
            hex::encode(pbkdf2(b"passwd", b"salt", 1)),
            "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc"
        );
    }

    #[test]
    fn passwords_verify() {
        let stored = hash_password("correct horse");
        assert!(verify_password("correct horse", &stored));
        assert!(!verify_password("wrong", &stored));
        assert!(!verify_password("x", "garbage"));
    }

    #[test]
    fn signatures_depend_on_every_part() {
        let a = sign_request("s", "GET", "/a", 1, b"");
        assert_eq!(a, sign_request("s", "GET", "/a", 1, b""));
        assert_ne!(a, sign_request("s", "GET", "/a", 2, b""));
        assert_ne!(a, sign_request("s", "POST", "/a", 1, b""));
        assert_ne!(a, sign_request("t", "GET", "/a", 1, b""));
    }
}
