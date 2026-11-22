use crate::input::{mix, shard};
use std::hash::{Hash, Hasher};

const PREFIX: u8 = b'u';

// A raw owner spelling is inline only when every byte fits this exact format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InlineOwner([u64; 2]);

impl Hash for InlineOwner {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        hasher.write_u64(self.fingerprint());
    }
}

impl InlineOwner {
    #[inline]
    pub(crate) fn encode(name: &[u8]) -> Option<Self> {
        let (head, tail) = name.split_first()?;
        if *head != PREFIX { return None; }
        let tail: &[u8; 32] = tail.try_into().ok()?;
        let bytes = encode_tail(tail)?;
        Some(Self([u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                   u64::from_le_bytes(bytes[8..].try_into().unwrap())]))
    }

    #[inline]
    pub(crate) fn raw_shard(self) -> usize { shard(self.fingerprint()) }

    #[inline]
    fn fingerprint(self) -> u64 {
        // Fold both halves of independent wide products before selecting
        // shard and map bits. Adjacent hex suffixes can vary near bit 63.
        let lo = u128::from(self.0[0]) * 0x9e37_79b1_85eb_ca87;
        let hi = u128::from(self.0[1]) * 0xc2b2_ae3d_27d4_eb4f;
        let product = lo.wrapping_add(hi);
        let folded = product as u64 ^ (product >> 64) as u64;
        mix(folded, folded >> 32)
    }

    pub(crate) fn spelling(self) -> [u8; 33] {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut result = [0; 33];
        result[0] = PREFIX;
        for (pair, packed) in result[1..].chunks_exact_mut(2).zip(
            self.0.into_iter().flat_map(u64::to_le_bytes),
        ) {
            pair[0] = HEX[(packed >> 4) as usize];
            pair[1] = HEX[(packed & 15) as usize];
        }
        result
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn encode_tail(tail: &[u8; 32]) -> Option<[u8; 16]> {
    use std::arch::x86_64::{
        _mm256_add_epi8, _mm256_blendv_epi8, _mm256_castsi256_si128, _mm256_cmpeq_epi8,
        _mm256_extracti128_si256, _mm256_loadu_si256, _mm256_maddubs_epi16, _mm256_min_epu8,
        _mm256_movemask_epi8, _mm256_or_si256, _mm256_set1_epi8, _mm256_set1_epi16,
        _mm256_sub_epi8, _mm_packus_epi16, _mm_storeu_si128,
    };
    // The fixed array covers the input load, the output array covers the store,
    // and this function is selected only in a build with AVX2 enabled.
    unsafe {
        let ascii = _mm256_loadu_si256(tail.as_ptr().cast());
        let digit = _mm256_sub_epi8(ascii, _mm256_set1_epi8(b'0' as i8));
        let letter = _mm256_sub_epi8(ascii, _mm256_set1_epi8(b'a' as i8));
        let digit_mask = _mm256_cmpeq_epi8(digit, _mm256_min_epu8(digit, _mm256_set1_epi8(9)));
        let letter_mask = _mm256_cmpeq_epi8(letter, _mm256_min_epu8(letter, _mm256_set1_epi8(5)));
        if _mm256_movemask_epi8(_mm256_or_si256(digit_mask, letter_mask)) != -1 { return None; }
        let nibbles = _mm256_blendv_epi8(_mm256_add_epi8(letter, _mm256_set1_epi8(10)), digit, digit_mask);
        let pairs = _mm256_maddubs_epi16(nibbles, _mm256_set1_epi16(0x0110));
        let bytes = _mm_packus_epi16(_mm256_castsi256_si128(pairs), _mm256_extracti128_si256::<1>(pairs));
        let mut result = [0u8; 16];
        _mm_storeu_si128(result.as_mut_ptr().cast(), bytes);
        Some(result)
    }
}

#[cfg(any(test, not(all(target_arch = "x86_64", target_feature = "avx2"))))]
fn encode_scalar(tail: &[u8; 32]) -> Option<[u8; 16]> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    let mut result = [0; 16];
    for (byte, pair) in result.iter_mut().zip(tail.chunks_exact(2)) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(result)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline]
fn encode_tail(tail: &[u8; 32]) -> Option<[u8; 16]> { encode_scalar(tail) }

#[cfg(test)]
#[test]
fn codec_round_trips_all_nibbles_and_rejects_every_other_byte() {
    let original = *b"u0123456789abcdeffedcba9876543210";
    let other_prefix = *b"U0123456789abcdeffedcba9876543210";
    let zero = *b"u00000000000000000000000000000000";
    assert_eq!(InlineOwner::encode(&zero).unwrap().spelling(), zero);
    assert_eq!(std::mem::size_of::<InlineOwner>(), 16);
    assert_eq!(InlineOwner::encode(&original).unwrap().spelling(), original);
    assert!(InlineOwner::encode(&other_prefix).is_none());
    assert!(InlineOwner::encode(&original[..32]).is_none());
    for pos in 0..32 {
        for byte in 0..=255u8 {
            let mut name = original;
            name[pos + 1] = byte;
            let suffix = (&name[1..]).try_into().unwrap();
            assert_eq!(encode_tail(suffix), encode_scalar(suffix));
            if let Some(encoded) = InlineOwner::encode(&name) {
                assert_eq!(encoded.spelling(), name);
            }
        }
    }
}

#[cfg(test)]
#[test]
fn sparse_hex_variation_spreads_raw_shards_and_map_bits() {
    use crate::input::{FastHasher, SHARDS};

    fn assert_spread(names: impl IntoIterator<Item = String>) {
        let mut shards = [0; SHARDS];
        let mut homes = [0; SHARDS];
        let mut tags = [0; SHARDS];
        for name in names {
            let owner = InlineOwner::encode(name.as_bytes()).unwrap();
            let mut hasher = FastHasher::default();
            owner.hash(&mut hasher);
            let hash = hasher.finish();
            shards[owner.raw_shard()] += 1;
            homes[(hash as usize) & (SHARDS - 1)] += 1;
            tags[(hash >> 57) as usize] += 1;
        }
        for counts in [shards, homes, tags] {
            assert_eq!(counts.iter().sum::<usize>(), 4096);
            assert!(counts.into_iter().all(|count| count > 0 && count <= 80));
        }
    }

    assert_spread((0..4096).map(|n| format!("uabcdef{n:026x}")));
    for position in [0, 1, 7, 14, 15, 16, 23, 29] {
        assert_spread((0..4096u128).map(|n| format!("u{:032x}", n << (position * 4))));
    }
}

