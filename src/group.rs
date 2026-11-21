use crate::identities::Name;
use crate::input::{Map, SHARDS, hash_bytes, mix, shard, transpose};
use std::hash::Hash;

pub struct Rows<'a, T> {
    packed: Vec<Vec<(u64, T)>>,
    full: Vec<Vec<(&'a [u8], T)>>,
}

impl<'a, T> Rows<'a, T> {
    pub fn new() -> Self {
        Self {
            packed: (0..SHARDS).map(|_| Vec::new()).collect(),
            full: (0..SHARDS).map(|_| Vec::new()).collect(),
        }
    }

    pub fn push(&mut self, name: Name<'a>, row: T) {
        match name {
            Name::Packed(key) => self.packed[shard(mix(0, key))].push((key, row)),
            Name::Full(key) => self.full[shard(hash_bytes(key))].push((key, row)),
        }
    }
}

pub struct Batch<'a, T> {
    pub packed: Vec<Vec<(u64, T)>>,
    pub full: Vec<Vec<(&'a [u8], T)>>,
}

impl<T> Default for Batch<'_, T> {
    fn default() -> Self {
        Self { packed: vec![], full: vec![] }
    }
}

impl<T> Batch<'_, T> {
    pub fn len(&self) -> usize {
        self.packed.iter().map(Vec::len).sum::<usize>() + self.full.iter().map(Vec::len).sum::<usize>()
    }
}

pub fn batch<T>(workers: Vec<Rows<'_, T>>) -> Vec<Batch<'_, T>> {
    let (packed, full): (Vec<_>, Vec<_>) = workers.into_iter().map(|worker| (worker.packed, worker.full)).unzip();
    transpose(packed).into_iter().zip(transpose(full)).map(|(packed, full)| Batch { packed, full }).collect()
}

pub fn people<K: Copy + Eq + Hash, E: Copy + Default, C: Copy + Default>(
    events: Vec<Vec<(K, E)>>,
    clicks: Vec<Vec<(K, C)>>,
    mut run: impl FnMut(K, &mut [E], &mut [C]),
) {
    if events.is_empty() {
        return;
    }
    let mut index = Map::default();
    let mut owners = vec![];
    let mut counts = vec![];
    let mut membership = Vec::with_capacity(events.iter().map(Vec::len).sum());
    for &(name, _) in events.iter().flatten() {
        let person = *index.entry(name).or_insert_with(|| {
            let id = owners.len() as u32;
            owners.push(name);
            counts.push(0);
            id
        });
        membership.push(person);
        counts[person as usize] += 1;
    }
    let (mut events, event_starts) = scatter(events.into_iter().flatten().zip(membership)
        .map(|((_, event), owner)| (owner, event)), &counts);

    counts.fill(0);
    let click_membership = clicks.into_iter().flatten().filter_map(|(name, record)| {
        let &person = index.get(&name)?;
        counts[person as usize] += 1;
        Some((person, record))
    }).collect::<Vec<_>>();
    let (mut clicks, click_starts) = scatter(click_membership, &counts);
    drop(index);

    for (id, name) in owners.into_iter().enumerate() {
        run(name, &mut events[event_starts[id]..event_starts[id + 1]], &mut clicks[click_starts[id]..click_starts[id + 1]]);
    }
}

fn scatter<T: Copy + Default>(
    rows: impl IntoIterator<Item = (u32, T)>, counts: &[usize],
) -> (Vec<T>, Vec<usize>) {
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
    (result, starts)
}
