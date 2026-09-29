//! Bloom filter: a bit array that answers "definitely absent" or "maybe
//! present". Each table carries one, so a lookup skips every table that
//! cannot hold the key without reading a single data block.
//!
//! Encoding: `[k: u8][bits...]`. Probes use double hashing of one 64-bit hash.

/// FNV-1a, then the SplitMix64 finaliser to spread the bits.
pub fn hash(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

pub struct Bloom {
    k: u32,
    bits: Vec<u8>,
}

impl Bloom {
    /// A filter for these key hashes at roughly `bits_per_key` bits each.
    /// Ten bits per key gives about a 1% false-positive rate.
    pub fn build(hashes: &[u64], bits_per_key: usize) -> Self {
        let nbits = (hashes.len() * bits_per_key).max(64);
        let k = ((bits_per_key as f64 * 0.69).round() as u32).clamp(1, 30);
        let mut bits = vec![0u8; nbits.div_ceil(8)];
        let nbits = (bits.len() * 8) as u64;
        for &h in hashes {
            for i in probes(h, k, nbits) {
                bits[(i / 8) as usize] |= 1 << (i % 8);
            }
        }
        Bloom { k, bits }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = (self.bits.len() * 8) as u64;
        probes(hash(key), self.k, nbits).all(|i| self.bits[(i / 8) as usize] & (1 << (i % 8)) != 0)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bits.len() + 1);
        out.push(self.k as u8);
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        let (&k, bits) = data.split_first()?;
        (k >= 1 && !bits.is_empty()).then(|| Bloom {
            k: k as u32,
            bits: bits.to_vec(),
        })
    }
}

fn probes(h: u64, k: u32, nbits: u64) -> impl Iterator<Item = u64> {
    let delta = (h >> 33) | 1;
    (0..k as u64).map(move |i| h.wrapping_add(i.wrapping_mul(delta)) % nbits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_about_one_percent_false_positives() {
        let keys: Vec<Vec<u8>> = (0..10_000u32)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        let hashes: Vec<u64> = keys.iter().map(|k| hash(k)).collect();
        let bloom = Bloom::decode(&Bloom::build(&hashes, 10).encode()).unwrap();
        assert!(
            keys.iter().all(|k| bloom.may_contain(k)),
            "a false negative"
        );
        let false_positives = (0..10_000u32)
            .filter(|i| bloom.may_contain(format!("absent-{i}").as_bytes()))
            .count();
        assert!(
            false_positives < 200,
            "{false_positives} false positives in 10,000"
        );
    }
}
