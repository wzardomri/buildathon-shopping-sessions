use crate::{
    INDEX_LANES, PackedCandidates, WinnerRows, fold_hash, group, index_masks,
    mix,
};

type Losers = Vec<(u32, u32)>;
const PRIMARY_LANES: usize = 4;
const LARGE_SLOTS: usize = 1 << 19;
const HARD_TAPE_CAP: usize = 1 << 16;

#[derive(Default)]
pub(super) struct Tape {
    key: Vec<u64>,
    reference: Vec<u32>,
}

impl Tape {
    fn reset(&mut self, limit: usize) {
        self.clear();
        self.key.reserve_exact(limit);
        self.reference.reserve_exact(limit);
    }

    fn len(&self) -> usize { self.key.len() }

    fn push(&mut self, key: u64, reference: u32) {
        self.key.push(key);
        self.reference.push(reference);
    }

    fn clear(&mut self) {
        self.key.clear();
        self.reference.clear();
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn primary_masks(entries: &[u32; PRIMARY_LANES], mask: u32, tag: u32) -> (u32, u32) {
    use std::arch::x86_64::{
        _mm_and_si128, _mm_castsi128_ps, _mm_cmpeq_epi32, _mm_loadu_si128, _mm_movemask_ps,
        _mm_set1_epi32, _mm_setzero_si128,
    };
    // The complete four-lane group exists even at the end of the primary index.
    unsafe {
        let values = _mm_loadu_si128(entries.as_ptr().cast());
        let free = _mm_cmpeq_epi32(values, _mm_setzero_si128());
        let tags = _mm_and_si128(values, _mm_set1_epi32(mask as i32));
        let matches = _mm_cmpeq_epi32(tags, _mm_set1_epi32(tag as i32));
        let free = _mm_movemask_ps(_mm_castsi128_ps(free)) as u32;
        let matches = _mm_movemask_ps(_mm_castsi128_ps(matches)) as u32;
        (matches & !free, free)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline]
fn primary_masks(entries: &[u32; PRIMARY_LANES], mask: u32, tag: u32) -> (u32, u32) {
    let mut matches = 0;
    let mut free = 0;
    for (lane, &entry) in entries.iter().enumerate() {
        if entry == 0 { free |= 1 << lane; }
        else if entry & mask == tag { matches |= 1 << lane; }
    }
    (matches, free)
}

struct Input<'a> {
    parts: &'a [(u32, PackedCandidates)],
    wide: &'a [Vec<i64>],
    row_bits: u32,
    row_mask: usize,
    reference_mask: u32,
}

impl Input<'_> {
    #[inline]
    fn row(&self, reference: u32) -> (u32, &PackedCandidates, usize) {
        let previous = (reference & self.reference_mask) as usize - 1;
        let (batch, rows) = &self.parts[previous >> self.row_bits];
        (*batch, rows, previous & self.row_mask)
    }

    #[inline]
    fn confirm(&self, table: &mut [u32], group: usize, mut matches: u32, key: u64,
        reference: u32, tag: u32, losers: &mut Losers,
    ) -> bool {
        while matches != 0 {
            let slot = group + matches.trailing_zeros() as usize;
            matches &= matches - 1;
            let (previous_batch, previous_rows, previous_index) = self.row(table[slot]);
            if previous_rows.key_at(previous_index) != key { continue; }
            let previous = previous_rows.candidate_at(previous_index);
            let (batch, rows, index) = self.row(reference);
            let current = rows.candidate_at(index);
            let (loser_batch, loser) = if current.ingest(&self.wide[batch as usize])
                < previous.ingest(&self.wide[previous_batch as usize]) {
                table[slot] = tag | reference;
                (previous_batch, previous)
            } else {
                (batch, current)
            };
            if loser.location != group::NO_LOCATION { losers.push((loser_batch, loser.location)); }
            return true;
        }
        false
    }
}

#[inline]
fn init(table: &mut Vec<u32>, len: usize) {
    table.clear();
    table.resize(len, 0);
}

fn primary(input: &Input<'_>, table: &mut [u32], tape: &mut Tape, limit: usize, losers: &mut Losers) -> bool {
    let home_mask = table.len() * 2 - INDEX_LANES;
    let tag_mask = !input.reference_mask;
    for (part, (_, rows)) in input.parts.iter().enumerate() {
        let reference_base = (part as u32) << input.row_bits;
        for (index, key) in rows.keys().enumerate() {
            let fingerprint = fold_hash(mix(0, key));
            let tag = (fingerprint >> 32) as u32 & tag_mask;
            let reference = reference_base + index as u32 + 1;
            // Same home number as scored, with four slots instead of eight.
            let home = (fingerprint as usize & home_mask) >> 1;
            let entries = table[home..home + PRIMARY_LANES].try_into().unwrap();
            let (matches, free) = primary_masks(entries, tag_mask, tag);
            if matches != 0 && input.confirm(table, home, matches, key, reference, tag, losers) { continue; }
            if free != 0 {
                table[home + free.trailing_zeros() as usize] = tag | reference;
            } else {
                if tape.len() == limit { return false; }
                tape.push(key, reference);
            }
        }
    }
    true
}

fn secondary(input: &Input<'_>, table: &mut Vec<u32>, tape: &Tape, losers: &mut Losers) -> bool {
    if tape.len() < 2 { return true; }
    let count = tape.len();
    init(table, (count + count / 2).next_power_of_two().max(INDEX_LANES));
    let slot_mask = table.len() - 1;
    let tag_mask = !input.reference_mask;
    'candidate: for (&key, &reference) in tape.key.iter().zip(&tape.reference) {
        let fingerprint = fold_hash(mix(0, key));
        let tag = (fingerprint >> 32) as u32 & tag_mask;
        // Original-home saturation selects tape rows by low fingerprint bits.
        // Rotate the full permutation before a smaller table uses those bits.
        let mut home = fingerprint.rotate_right(23) as usize & slot_mask & !(INDEX_LANES - 1);
        for _ in 0..32 {
            let entries = table[home..home + INDEX_LANES].try_into().unwrap();
            let (matches, free) = index_masks(entries, tag_mask, tag);
            if matches != 0 && input.confirm(table, home, matches, key, reference, tag, losers) {
                continue 'candidate;
            }
            if free != 0 {
                table[home + free.trailing_zeros() as usize] = tag | reference;
                continue 'candidate;
            }
            home = (home + INDEX_LANES) & slot_mask;
        }
        return false;
    }
    true
}

/// Resolve large, packed-ID jobs while leaving the scored resolver available
/// for ordinary capacities and any job exceeding the checked scratch bounds.
#[inline(never)]
pub(super) fn try_large(
    parts: &[(u32, PackedCandidates)], wide: &[Vec<i64>], table: &mut Vec<u32>,
    tape: &mut Tape, losers: &mut Losers,
) -> bool {
    if !cfg!(all(target_arch = "x86_64", target_feature = "avx2")) { return false; }
    let count = parts.iter().map(|(_, rows)| rows.len()).sum::<usize>();
    let capacity = (count + count / 2).next_power_of_two().max(INDEX_LANES);
    if capacity != LARGE_SLOTS { return false; }
    let width = parts.iter().map(|(_, rows)| rows.len()).max().unwrap().next_power_of_two();
    let Some(span) = parts.len().checked_mul(width).filter(|&span| span < 1 << 24) else { return false; };
    let input = Input { parts, wide, row_bits: width.trailing_zeros(), row_mask: width - 1,
        reference_mask: (1u32 << (usize::BITS - span.leading_zeros())) - 1 };
    let limit = (count / 4).min(HARD_TAPE_CAP);
    let checkpoint = losers.len();
    tape.reset(limit);
    init(table, capacity / 2);
    let resolved = primary(&input, table, tape, limit, losers) && secondary(&input, table, tape, losers);
    tape.clear();
    if !resolved {
        // Restore the worker's output prefix before the original resolver retries
        // the unchanged candidate parts. No locator has been applied yet.
        losers.truncate(checkpoint);
    }
    resolved
}
