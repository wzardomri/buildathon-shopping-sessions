use crate::identities::Name;
use crate::input::{Map, SHARDS, transpose};
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
        let shard = name.shard();
        match name {
            Name::Packed(key) => self.packed[shard].push((key, row)),
            Name::Full(key) => self.full[shard].push((key, row)),
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
        let mut events = self.events.as_mut_slice();
        let mut clicks = self.clicks.as_mut_slice();
        self.owners.iter().copied().zip(&self.event_counts).zip(&self.click_counts)
            .map(move |((name, &event_count), &click_count)| {
                let (person_events, rest) = std::mem::take(&mut events).split_at_mut(event_count);
                events = rest;
                let (person_clicks, rest) = std::mem::take(&mut clicks).split_at_mut(click_count);
                clicks = rest;
                (name, person_events, person_clicks)
            })
    }
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
