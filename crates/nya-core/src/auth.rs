use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn create_proof(psk: &[u8], exporter: &[u8], nonce: &[u8], user_id: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(psk).expect("hmac");
    mac.update(b"nya-create-v1");
    mac.update(exporter);
    mac.update(nonce);
    mac.update(user_id);
    mac.finalize().into_bytes().into()
}

pub fn session_key(psk: &[u8], session_id: &[u8]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(session_id), psk);
    let mut out = [0u8; 32];
    hk.expand(b"nya-session-v1", &mut out).expect("hkdf");
    out
}

pub fn join_proof(session_key: &[u8], exporter: &[u8], path_name: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(session_key).expect("hmac");
    mac.update(b"nya-join-v1");
    mac.update(exporter);
    mac.update(path_name);
    mac.finalize().into_bytes().into()
}

pub fn proofs_equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut d = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        d |= x ^ y;
    }
    d == 0
}
