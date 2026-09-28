//! CRC-32 (IEEE 802.3), table-driven, eight bytes at a time ("slicing by
//! 8"). Every record and block that reaches disk carries one, so a torn or
//! corrupted write is detected instead of being trusted.

const fn make_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    // t[k][i] is the CRC of byte i followed by k zero bytes.
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    t
}

static T: [[u32; 256]; 8] = make_tables();

pub fn crc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let lo = c ^ u32::from_le_bytes(chunk[..4].try_into().expect("4 bytes"));
        let hi = u32::from_le_bytes(chunk[4..].try_into().expect("4 bytes"));
        c = T[7][(lo & 0xFF) as usize]
            ^ T[6][((lo >> 8) & 0xFF) as usize]
            ^ T[5][((lo >> 16) & 0xFF) as usize]
            ^ T[4][(lo >> 24) as usize]
            ^ T[3][(hi & 0xFF) as usize]
            ^ T[2][((hi >> 8) & 0xFF) as usize]
            ^ T[1][((hi >> 16) & 0xFF) as usize]
            ^ T[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        c = T[0][((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytewise(data: &[u8]) -> u32 {
        let mut c = 0xFFFF_FFFFu32;
        for &b in data {
            c = T[0][((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
        }
        c ^ 0xFFFF_FFFF
    }

    #[test]
    fn known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn slicing_matches_the_bytewise_definition() {
        let mut rng = crate::rng::Rng::new(3);
        for len in 0..300 {
            let data = rng.bytes(len);
            assert_eq!(crc32(&data), bytewise(&data), "length {len}");
        }
    }
}
