use crate::identities::Name;
use crate::inline_owner::InlineOwner;
use crate::input::{CHUNK_BYTES, Map, SHARDS, transpose};
use std::hash::Hash;

const FULL_LOCATION: u32 = 1 << 31;
const LOCATION_BITS: u32 = 31 - SHARDS.trailing_zeros();
const LOCATION_MASK: u32 = (1 << LOCATION_BITS) - 1;
const INLINE_LOCATION: u32 = 1 << (LOCATION_BITS - 1);
const FULL_SLOT_MASK: u32 = INLINE_LOCATION - 1;
pub const MAX_LOCATED_ROWS: usize = LOCATION_MASK as usize;
const MAX_FULL_LOCATED_ROWS: usize = FULL_SLOT_MASK as usize;
const _: () = assert!(CHUNK_BYTES + 1 <= MAX_FULL_LOCATED_ROWS);
pub const NO_LOCATION: u32 = u32::MAX;

pub struct Rows<'a, T> {
    packed: Vec<Vec<(u64, T)>>,
    full: Vec<Vec<(&'a [u8], T)>>,
    inline: Vec<Vec<(InlineOwner, T)>>,
    // The combined count bounds the ordinal for either full lane before
    // winner removal. A batch never appends after applying its loser list.
    located_full_rows: usize,
}

impl<'a, T> Rows<'a, T> {
    pub fn new() -> Self {
        Self {
            packed: (0..SHARDS).map(|_| Vec::new()).collect(),
            full: (0..SHARDS).map(|_| Vec::new()).collect(),
            inline: (0..SHARDS).map(|_| Vec::new()).collect(),
            located_full_rows: 0,
        }
    }

    pub fn push(&mut self, name: Name<'a>, row: T) {
        match name {
            Name::Packed(key) => self.packed[name.shard()].push((key, row)),
            Name::Full(key) => {
                if let Some(inline) = InlineOwner::encode(key) {
                    self.inline[inline.raw_shard()].push((inline, row));
                } else {
                    self.full[name.shard()].push((key, row));
                }
            }
        }
    }

    pub fn accepts_full_chunk(&self) -> bool {
        self.located_full_rows <= MAX_FULL_LOCATED_ROWS - CHUNK_BYTES - 1
    }

    pub fn push_located(&mut self, name: Name<'a>, row: T) -> u32 {
        // The scanner reserves room for one complete chunk in both locator
        // widths. Full locations reserve the all-one sentinel in each lane.
        match name {
            Name::Packed(key) => {
                let shard = name.shard();
                let rows = &mut self.packed[shard];
                let slot = rows.len();
                debug_assert!(slot < MAX_LOCATED_ROWS);
                rows.push((key, row));
                ((shard as u32) << LOCATION_BITS) | slot as u32
            }
            Name::Full(key) => {
                debug_assert!(self.located_full_rows < MAX_FULL_LOCATED_ROWS);
                self.located_full_rows += 1;
                if let Some(inline) = InlineOwner::encode(key) {
                    let shard = inline.raw_shard();
                    let rows = &mut self.inline[shard];
                    let slot = rows.len();
                    rows.push((inline, row));
                    FULL_LOCATION | INLINE_LOCATION | ((shard as u32) << LOCATION_BITS) | slot as u32
                } else {
                    let shard = name.shard();
                    let rows = &mut self.full[shard];
                    let slot = rows.len();
                    rows.push((key, row));
                    FULL_LOCATION | ((shard as u32) << LOCATION_BITS) | slot as u32
                }
            }
        }
    }

    pub fn remove_located(&mut self, mut locations: Vec<u32>) {
        // Sorting descending puts each shard's tail losers first. Moving a
        // survivor into a removed slot leaves the remaining indices intact.
        locations.sort_unstable_by(|left, right| right.cmp(left));
        for location in locations {
            let shard = ((location & !FULL_LOCATION) >> LOCATION_BITS) as usize;
            if location & FULL_LOCATION == 0 {
                let slot = (location & LOCATION_MASK) as usize;
                self.packed[shard].swap_remove(slot);
            } else if location & INLINE_LOCATION != 0 {
                let slot = (location & FULL_SLOT_MASK) as usize;
                self.inline[shard].swap_remove(slot);
            } else {
                let slot = (location & FULL_SLOT_MASK) as usize;
                self.full[shard].swap_remove(slot);
            }
        }
    }
}

pub struct Batch<'a, T> {
    pub packed: Vec<Vec<(u64, T)>>,
    pub full: Vec<Vec<(&'a [u8], T)>>,
    pub(crate) inline: Vec<Vec<(InlineOwner, T)>>,
}

impl<T> Default for Batch<'_, T> {
    fn default() -> Self {
        Self { packed: vec![], full: vec![], inline: vec![] }
    }
}

impl<T> Batch<'_, T> {
    pub fn len(&self) -> usize {
        self.packed.iter().map(Vec::len).sum::<usize>()
            + self.full.iter().map(Vec::len).sum::<usize>()
            + self.inline.iter().map(Vec::len).sum::<usize>()
    }
}

pub fn batch<T>(workers: Vec<Rows<'_, T>>) -> Vec<Batch<'_, T>> {
    let mut packed = Vec::with_capacity(workers.len());
    let mut full = Vec::with_capacity(workers.len());
    let mut inline = Vec::with_capacity(workers.len());
    for worker in workers {
        packed.push(worker.packed);
        full.push(worker.full);
        inline.push(worker.inline);
    }
    transpose(packed).into_iter().zip(transpose(full)).zip(transpose(inline))
        .map(|((packed, full), inline)| Batch { packed, full, inline }).collect()
}

pub struct RawPeople<K, E, C> {
    owners: Vec<K>,
    event_counts: Vec<usize>,
    click_counts: Vec<usize>,
    events: Vec<E>,
    clicks: Vec<C>,
}

impl<K: Copy + Eq + Hash, E: Copy + Default, C: Copy + Default> RawPeople<K, E, C> {
    pub fn gather(events: Vec<Vec<(K, E)>>, clicks: Vec<Vec<(K, C)>>) -> Self {
        let mut index = Map::default();
        let mut owners = vec![];
        let mut event_counts = vec![];
        let mut membership = Vec::with_capacity(events.iter().map(Vec::len).sum());
        for &(name, _) in events.iter().flatten() {
            let person = *index.entry(name).or_insert_with(|| {
                let id = owners.len() as u32;
                owners.push(name);
                event_counts.push(0);
                id
            });
            membership.push(person);
            event_counts[person as usize] += 1;
        }
        let events = scatter(events.into_iter().flatten().zip(membership.iter().copied())
            .map(|((_, event), owner)| (owner, event)), &event_counts);

        let mut click_counts = vec![0; owners.len()];
        membership.clear();
        membership.reserve(clicks.iter().map(Vec::len).sum());
        for &(name, _) in clicks.iter().flatten() {
            let person = *index.entry(name).or_insert_with(|| {
                let id = owners.len() as u32;
                owners.push(name);
                event_counts.push(0);
                click_counts.push(0);
                id
            });
            membership.push(person);
            click_counts[person as usize] += 1;
        }
        drop(index);
        let clicks = scatter(clicks.into_iter().flatten().zip(membership)
            .map(|((_, click), owner)| (owner, click)), &click_counts);
        Self { owners, event_counts, click_counts, events, clicks }
    }

    pub fn slabs(&mut self) -> impl Iterator<Item = (K, &mut [E], &mut [C])> {
        self.borrowed_slabs().map(|(key, events, clicks)| (*key, events, clicks))
    }

    pub fn borrowed_slabs(&mut self) -> impl Iterator<Item = (&K, &mut [E], &mut [C])> {
        let mut events = self.events.as_mut_slice();
        let mut clicks = self.clicks.as_mut_slice();
        self.owners.iter().zip(&self.event_counts).zip(&self.click_counts)
            .map(move |((name, &event_count), &click_count)| {
                let (person_events, rest) = std::mem::take(&mut events).split_at_mut(event_count);
                events = rest;
                let (person_clicks, rest) = std::mem::take(&mut clicks).split_at_mut(click_count);
                clicks = rest;
                (name, person_events, person_clicks)
            })
    }
}

impl<E, C> RawPeople<InlineOwner, E, C> {
    pub fn decode_owners(self) -> RawPeople<[u8; 33], E, C> {
        let RawPeople { owners, event_counts, click_counts, events, clicks } = self;
        let owners = owners.into_iter().map(InlineOwner::spelling).collect();
        RawPeople { owners, event_counts, click_counts, events, clicks }
    }
}

#[cfg(test)]
#[test]
fn full_locator_lanes_remove_their_own_descending_slots() {
    let inline = Name::from_bytes(b"u0123456789abcdeffedcba9876543210");
    let full = Name::from_bytes(b"u0123456789abcdeffedcba987654321F");
    let packed = Name::from_bytes(b"u1");
    let mut rows = Rows::new();
    let first = rows.push_located(inline, 11);
    let fallback = rows.push_located(full, 20);
    let middle = rows.push_located(inline, 12);
    let kept = rows.push_located(inline, 13);
    let packed_removed = rows.push_located(packed, 30);
    let packed_kept = rows.push_located(packed, 31);
    assert_eq!(first & (FULL_LOCATION | INLINE_LOCATION), FULL_LOCATION | INLINE_LOCATION);
    assert_eq!(fallback & (FULL_LOCATION | INLINE_LOCATION), FULL_LOCATION);
    assert_eq!(first & FULL_SLOT_MASK, 0);
    assert_eq!(middle & FULL_SLOT_MASK, 1);
    assert_eq!(kept & FULL_SLOT_MASK, 2);
    assert_eq!(packed_kept & FULL_LOCATION, 0);
    assert_ne!(kept, NO_LOCATION);
    rows.remove_located(vec![first, packed_removed, fallback, middle]);
    assert_eq!(rows.inline.iter().flatten().map(|row| row.1).collect::<Vec<_>>(), [13]);
    assert!(rows.full.iter().all(Vec::is_empty));
    assert_eq!(rows.packed.iter().flatten().map(|row| row.1).collect::<Vec<_>>(), [31]);

    rows.located_full_rows = MAX_FULL_LOCATED_ROWS - CHUNK_BYTES - 1;
    assert!(rows.accepts_full_chunk());
    rows.located_full_rows += 1;
    assert!(!rows.accepts_full_chunk());
}

pub struct Slab<'data, 'buffer, E, C> {
    pub owner: Name<'data>,
    pub events: &'buffer mut [E],
    pub clicks: &'buffer mut [C],
}

pub fn canonical_people<'data, E: Copy, C: Copy>(
    parts: Vec<Vec<Slab<'data, '_, E, C>>>,
    mut run: impl FnMut(Name<'data>, &mut [E], &mut [C]),
) {
    const END: usize = usize::MAX;
    let count = parts.iter().map(Vec::len).sum();
    let mut heads = Map::with_capacity_and_hasher(count, Default::default());
    let mut links = Vec::with_capacity(count);
    let mut slabs = Vec::with_capacity(count);
    for slab in parts.into_iter().flatten() {
        links.push(heads.insert(slab.owner, slabs.len()).unwrap_or(END));
        slabs.push(slab);
    }

    let mut events = Vec::new();
    let mut clicks = Vec::new();
    for (owner, head) in heads {
        if links[head] == END {
            let slab = &mut slabs[head];
            if !slab.events.is_empty() {
                run(owner, slab.events, slab.clicks);
            }
        } else {
            events.clear();
            clicks.clear();
            let mut index = head;
            while index != END {
                events.extend_from_slice(slabs[index].events);
                clicks.extend_from_slice(slabs[index].clicks);
                index = links[index];
            }
            if !events.is_empty() {
                run(owner, &mut events, &mut clicks);
            }
        }
    }
}

fn scatter<T: Copy + Default>(
    rows: impl IntoIterator<Item = (u32, T)>, counts: &[usize],
) -> Vec<T> {
    let mut starts = Vec::with_capacity(counts.len() + 1);
    let mut total = 0;
    for &count in counts {
        starts.push(total);
        total += count;
    }
    starts.push(total);
    let mut cursors = starts[..counts.len()].to_vec();
    let mut result = vec![T::default(); total];
    for (person, row) in rows {
        let cursor = &mut cursors[person as usize];
        result[*cursor] = row;
        *cursor += 1;
    }
    result
}
