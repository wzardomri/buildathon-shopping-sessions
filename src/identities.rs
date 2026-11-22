use crate::input::{Map, SHARDS, Table, fields, hash_bytes, mix, shard, worker_map};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[inline]
fn spread(key: u64) -> u64 {
    // A reversible fold spreads high bytes into the hash table's bucket bits.
    key ^ (key >> 33)
}

/// Pack a common short ID into one word without changing its exact spelling.
/// The top bit holds the prefix case. A nonzero final tail byte makes zero
/// padding unambiguous. Any byte string outside this representation takes the
/// ordinary byte-slice path, including arbitrary prefixes and trailing NULs.
#[inline]
fn packed(id: &[u8]) -> Option<u64> {
    let (&prefix, tail) = id.split_first()?;
    let case = match prefix {
        b'u' => 0,
        b'U' => 1 << 63,
        _ => return None,
    };
    if tail.len() == 8 {
        let key = u64::from_le_bytes(tail.try_into().unwrap());
        return (tail[7].wrapping_sub(1) < 127).then_some(spread(key | case));
    }
    if tail.len() > 8 || tail.last() == Some(&0) {
        return None;
    }
    let mut bytes = [0; 8];
    bytes[..tail.len()].copy_from_slice(tail);
    Some(spread(u64::from_le_bytes(bytes) | case))
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Name<'a> {
    Packed(u64),
    Full(&'a [u8]),
}

impl<'a> Name<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        packed(bytes).map_or(Self::Full(bytes), Self::Packed)
    }

    #[inline]
    pub fn shard(self) -> usize {
        match self {
            Self::Packed(key) => packed_shard(key),
            Self::Full(key) => shard(hash_bytes(key)),
        }
    }

    #[inline]
    fn precedes(self, other: Self) -> bool {
        match (self, other) {
            (Self::Packed(left), Self::Packed(right)) => packed_order(left) < packed_order(right),
            (Self::Full(left), Self::Full(right)) => left < right,
            (Self::Packed(left), Self::Full(right)) => unpack(left, &mut [0; 9]) < right,
            (Self::Full(left), Self::Packed(right)) => left < unpack(right, &mut [0; 9]),
        }
    }
}

fn packed_order(key: u64) -> (u8, u64) {
    let raw = spread(key);
    let prefix = if raw >> 63 == 0 { b'u' } else { b'U' };
    // The tail is zero-padded, and a packed tail cannot end in a zero byte.
    // Its big-endian order therefore gives the same order as a byte slice.
    (prefix, (raw & (u64::MAX >> 1)).swap_bytes())
}

pub fn unpack(key: u64, out: &mut [u8; 9]) -> &[u8] {
    let raw = spread(key); // The same fold reverses the encoding.
    out[0] = if raw >> 63 == 0 { b'u' } else { b'U' };
    let tail = raw & (u64::MAX >> 1);
    out[1..].copy_from_slice(&tail.to_le_bytes());
    let length = (64 - tail.leading_zeros()).div_ceil(8) as usize;
    &out[..1 + length]
}

#[derive(Default)]
struct Index<'a> {
    short: Map<u64, u32>,
    other: Map<&'a [u8], u32>,
}

#[derive(Default)]
struct Endpoints<'a> {
    packed: Vec<(u64, usize)>,
    full: Vec<(&'a [u8], usize)>,
}

impl Endpoints<'_> {
    fn is_empty(&self) -> bool { self.packed.is_empty() && self.full.is_empty() }
}

struct ParsedLinks<'a> {
    shards: Vec<Endpoints<'a>>,
    next: usize,
}

impl<'a> ParsedLinks<'a> {
    fn new() -> Self {
        Self { shards: (0..SHARDS).map(|_| Endpoints::default()).collect(), next: 0 }
    }

    fn add(&mut self, name: &'a [u8]) {
        if let Some(key) = packed(name) {
            self.shards[packed_shard(key)].packed.push((key, self.next));
        } else {
            self.shards[shard(hash_bytes(name))].full.push((name, self.next));
        }
        self.next += 1;
    }
}

#[inline]
fn packed_shard(key: u64) -> usize { shard(mix(0, key)) }

struct BuiltShard<'a> {
    bucket: usize,
    index: Index<'a>,
    names: Vec<Name<'a>>,
}

fn index_shard<'a>(
    bucket: usize,
    endpoints: &[(usize, Endpoints<'a>)],
    assigned: &[AtomicU64],
) -> BuiltShard<'a> {
    let packed_count = endpoints.iter().map(|(_, rows)| rows.packed.len()).sum();
    let full_count = endpoints.iter().map(|(_, rows)| rows.full.len()).sum();
    let mut index = Index {
        short: Map::with_capacity_and_hasher(packed_count, Default::default()),
        other: Map::with_capacity_and_hasher(full_count, Default::default()),
    };
    let mut names = Vec::with_capacity(packed_count + full_count);
    let shard_code = (bucket as u64) << 32;
    for (base, rows) in endpoints {
        for &(key, ordinal) in &rows.packed {
            let node = *index.short.entry(key).or_insert_with(|| {
                let node = names.len() as u32;
                names.push(Name::Packed(key));
                node
            });
            assigned[base + ordinal].store(shard_code | u64::from(node), Ordering::Relaxed);
        }
        for &(key, ordinal) in &rows.full {
            let node = *index.other.entry(key).or_insert_with(|| {
                let node = names.len() as u32;
                names.push(Name::Full(key));
                node
            });
            assigned[base + ordinal].store(shard_code | u64::from(node), Ordering::Relaxed);
        }
    }
    BuiltShard { bucket, index, names }
}

struct Parents {
    slots: Vec<AtomicU32>,
}

impl Parents {
    fn new(count: usize) -> Self {
        let count = u32::try_from(count).expect("too many distinct link endpoints");
        Self { slots: (0..count).map(AtomicU32::new).collect() }
    }

    #[inline]
    fn parent(&self, vertex: u32) -> u32 {
        self.slots[vertex as usize].load(Ordering::Relaxed)
    }

    #[inline]
    fn root(&self, mut vertex: u32) -> u32 {
        loop {
            let parent = self.parent(vertex);
            if parent == vertex {
                return vertex;
            }
            let grandparent = self.parent(parent);
            if grandparent == parent {
                return parent;
            }
            // A concurrent link or shortening may have updated the slot.
            // Either path still leads to the same connected component.
            let _ = self.slots[vertex as usize].compare_exchange_weak(
                parent, grandparent, Ordering::Relaxed, Ordering::Relaxed,
            );
            vertex = grandparent;
        }
    }

    #[inline]
    fn join(&self, mut left: u32, mut right: u32) {
        loop {
            left = self.root(left);
            right = self.root(right);
            if left == right {
                return;
            }
            let high = left.max(right);
            let low = left.min(right);
            // Parent links always point toward lower indices. Attaching a
            // still-current high root to low cannot introduce a cycle, even
            // if another edge has already attached low to a lower root.
            if self.slots[high as usize].compare_exchange(
                high, low, Ordering::Relaxed, Ordering::Relaxed,
            ).is_ok() {
                return;
            }
        }
    }

    fn name_roots(&self, names: &[Name<'_>], threads: usize) -> Vec<AtomicU32> {
        const BLOCK: usize = 4096;
        let minima: Vec<_> = (0..self.slots.len() as u32).map(AtomicU32::new).collect();
        // All unions have finished before this pass. Shortening parents to
        // their final root preserves connectivity and leaves root slots fixed.
        worker_map(threads, names.len().div_ceil(BLOCK), || (), |_, job| {
            let first = job * BLOCK;
            for vertex in first..(first + BLOCK).min(names.len()) {
                let root = self.root(vertex as u32) as usize;
                if root == vertex {
                    continue;
                }
                self.slots[vertex].store(root as u32, Ordering::Relaxed);
                let best = &minima[root];
                let mut incumbent = best.load(Ordering::Relaxed);
                while names[vertex].precedes(names[incumbent as usize]) {
                    match best.compare_exchange_weak(
                        incumbent, vertex as u32, Ordering::Relaxed, Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(updated) => incumbent = updated,
                    }
                }
            }
        });
        minima
    }
}

struct DirectShard<'a> {
    packed: Map<u64, u64>,
    packed_full: Map<u64, &'a [u8]>,
    full: Map<&'a [u8], Name<'a>>,
}

impl<'a> DirectShard<'a> {
    fn from_index(base: u32, index: Index<'a>, owner: impl Fn(u32) -> Name<'a>) -> Self {
        // Canonical spellings and singletons need no entry. Start with a
        // smaller allocation and let unusual link shapes grow it as needed.
        let mut packed = Map::with_capacity_and_hasher(index.short.len() / 2, Default::default());
        let mut packed_full = Map::default();
        let mut full = Map::default();
        for (key, node) in index.short {
            match owner(base + node) {
                Name::Packed(owner) if owner == key => {}
                Name::Packed(owner) => { packed.insert(key, owner); }
                Name::Full(owner) => { packed_full.insert(key, owner); }
            }
        }
        for (key, node) in index.other {
            match owner(base + node) {
                Name::Full(owner) if owner == key => {}
                owner => { full.insert(key, owner); }
            }
        }
        if packed.capacity() > packed.len().saturating_mul(2) {
            packed.shrink_to_fit();
        }
        Self { packed, packed_full, full }
    }
}

pub struct Identities<'a> {
    packed: Vec<Map<u64, u64>>,
    packed_full: Option<Vec<Map<u64, &'a [u8]>>>,
    full: Vec<Map<&'a [u8], Name<'a>>>,
}

impl<'a> Identities<'a> {
    pub fn read(table: &'a Table, threads: usize) -> Self {
        let workers = table.scan(threads, ParsedLinks::new, |worker, row| {
            if let Some([a, b]) = fields(row) {
                if !a.is_empty() && !b.is_empty() {
                    worker.add(a);
                    worker.add(b);
                }
            }
        });

        let mut endpoint_count = 0;
        let mut buckets: Vec<Vec<_>> = (0..SHARDS).map(|_| Vec::new()).collect();
        for worker in workers {
            for (bucket, endpoints) in buckets.iter_mut().zip(worker.shards) {
                if !endpoints.is_empty() {
                    bucket.push((endpoint_count, endpoints));
                }
            }
            endpoint_count += worker.next;
        }

        // Every endpoint ordinal occurs in exactly one bucket. Scoped workers
        // finish before numeric union reads these relaxed stores.
        let assigned: Vec<_> = (0..endpoint_count).map(|_| AtomicU64::new(0)).collect();
        let mut indexed: Vec<_> = worker_map(threads, SHARDS, Vec::new, |worker, bucket| {
            worker.push(index_shard(bucket, &buckets[bucket], &assigned));
        }).into_iter().flatten().collect();
        indexed.sort_unstable_by_key(|part| part.bucket);
        drop(buckets);

        let mut indices = Vec::with_capacity(SHARDS);
        let mut names = Vec::with_capacity(indexed.iter().map(|part| part.names.len()).sum());
        for part in indexed {
            indices.push((names.len() as u32, part.index));
            names.extend(part.names);
        }
        let parents = Parents::new(names.len());
        let global_id = |code: u64| indices[(code >> 32) as usize].0 + code as u32;
        const ENDPOINT_BLOCK: usize = 8192;
        worker_map(threads, assigned.len().div_ceil(ENDPOINT_BLOCK), || (), |_, job| {
            let first = job * ENDPOINT_BLOCK;
            let end = (first + ENDPOINT_BLOCK).min(assigned.len());
            for edge in assigned[first..end].chunks_exact(2) {
                parents.join(
                    global_id(edge[0].load(Ordering::Relaxed)),
                    global_id(edge[1].load(Ordering::Relaxed)),
                );
            }
        });
        drop(assigned);
        let minima = parents.name_roots(&names, threads);
        let canonical = |vertex| {
            let root = parents.parent(vertex);
            names[minima[root as usize].load(Ordering::Relaxed) as usize]
        };

        let sources: Vec<_> = indices.into_iter().map(|item| Mutex::new(Some(item))).collect();
        let mut shards: Vec<_> = worker_map(threads, sources.len(), Vec::new, |worker, bucket| {
            let (base, index) = sources[bucket].lock().unwrap().take().unwrap();
            worker.push((bucket, DirectShard::from_index(base, index, canonical)));
        }).into_iter().flatten().collect();
        shards.sort_unstable_by_key(|(bucket, _)| *bucket);

        let mut packed = Vec::with_capacity(shards.len());
        let mut packed_full = Vec::with_capacity(shards.len());
        let mut full = Vec::with_capacity(shards.len());
        for (_, shard) in shards {
            packed.push(shard.packed);
            packed_full.push(shard.packed_full);
            full.push(shard.full);
        }
        let packed_full = packed_full.iter().any(|map| !map.is_empty()).then_some(packed_full);
        Self { packed, packed_full, full }
    }

    #[inline]
    pub fn canonical<'view>(&self, raw: Name<'view>) -> Name<'view>
    where 'a: 'view {
        let bucket = raw.shard();
        match raw {
            Name::Packed(bits) => {
                if let Some(&owner) = self.packed[bucket].get(&bits) {
                    return Name::Packed(owner);
                }
                if let Some(&owner) = self.packed_full.as_ref().and_then(|maps| maps[bucket].get(&bits)) {
                    return Name::Full(owner);
                }
                raw
            }
            Name::Full(key) => match self.full[bucket].get(key) {
                Some(&owner) => owner,
                None => raw,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DirectShard, Index, Name, Ordering, Parents, packed, worker_map};

    #[test]
    fn canonical_name_order_matches_bytes() {
        let names: &[&[u8]] = &[
            b"A\tB",
            b"U",
            b"U\0b",
            b"U12345678",
            b"U123456789",
            b"u",
            b"u\0",
            b"u\0\0b",
            b"uA",
            b"uA\0\0b",
            b"uAB",
            b"uabcdefgh",
            b"uabcdefghi",
            "uabcdefé".as_bytes(),
            "🌲".as_bytes(),
        ];
        for &left in names {
            for &right in names {
                assert_eq!(Name::from_bytes(left).precedes(Name::from_bytes(right)), left < right);
            }
        }
    }

    #[test]
    fn parallel_components_agree_with_graph_traversal() {
        const COUNT: usize = 8192;
        let bytes: Vec<_> = (0..COUNT).map(|vertex| match vertex % 5 {
            0 => format!("u{vertex:08x}"),
            1 => format!("U{vertex:08x}"),
            2 => format!(" alias {vertex:08x}"),
            3 => format!("u{vertex:06x}\0"),
            _ => format!("é {vertex:08x}"),
        }).collect();
        let names: Vec<_> = bytes.iter().map(|name| Name::from_bytes(name.as_bytes())).collect();
        let connected = COUNT - 16;
        let mut edges: Vec<_> = (31..connected).rev().map(|vertex| (vertex, vertex - 31)).collect();
        let mut seed = 91u32;
        for _ in 0..28_000 {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let vertex = seed as usize % connected;
            edges.push((vertex % 31, vertex));
        }
        edges.extend([(COUNT - 1, COUNT - 1), (10, 10), (2, 7), (7, 2)]);
        let mut neighbors = vec![Vec::new(); COUNT];
        for &(left, right) in &edges {
            neighbors[left].push(right);
            neighbors[right].push(left);
        }

        let mut visited = vec![false; COUNT];
        let mut expected = vec![0; COUNT];
        for first in 0..COUNT {
            if visited[first] {
                continue;
            }
            let mut members = vec![first];
            visited[first] = true;
            let mut cursor = 0;
            while let Some(&vertex) = members.get(cursor) {
                for &neighbor in &neighbors[vertex] {
                    if !visited[neighbor] {
                        visited[neighbor] = true;
                        members.push(neighbor);
                    }
                }
                cursor += 1;
            }
            let owner = *members.iter().min_by_key(|&&vertex| &bytes[vertex]).unwrap();
            for vertex in members {
                expected[vertex] = owner;
            }
        }

        for threads in [1, 2, 8] {
            let parents = Parents::new(COUNT);
            worker_map(threads, edges.len().div_ceil(1024), || (), |_, block| {
                let start = block * 1024;
                for &(left, right) in &edges[start..(start + 1024).min(edges.len())] {
                    parents.join(left as u32, right as u32);
                }
            });
            let minima = parents.name_roots(&names, threads);
            for (vertex, &owner) in expected.iter().enumerate() {
                let root = parents.parent(vertex as u32);
                assert_eq!(parents.parent(root), root);
                assert_eq!(minima[root as usize].load(Ordering::Relaxed) as usize, owner);
            }
        }
    }

    #[test]
    fn only_changed_aliases_are_stored() {
        let unchanged = packed(b"ua").unwrap();
        let renamed = packed(b"ub").unwrap();
        let renamed_to_full = packed(b"uc").unwrap();
        let mut index = Index::default();
        index.short.insert(unchanged, 0);
        index.short.insert(renamed, 1);
        index.short.insert(renamed_to_full, 2);
        index.other.insert(b"same", 3);
        index.other.insert(b"renamed-long", 4);
        index.other.insert(b"alias", 5);
        let names = [
            Name::Full(b"slot from earlier shard"),
            Name::Packed(unchanged),
            Name::Packed(unchanged),
            Name::Full(b"A long canonical spelling"),
            Name::Full(b"same"),
            Name::Packed(unchanged),
            Name::Full(b"A long canonical spelling"),
        ];
        let aliases = DirectShard::from_index(1, index, |vertex| names[vertex as usize]);
        assert_eq!(aliases.packed.len(), 1);
        assert_eq!(aliases.packed.get(&renamed), Some(&unchanged));
        assert_eq!(aliases.packed_full.len(), 1);
        assert_eq!(aliases.packed_full.get(&renamed_to_full).copied(), Some(b"A long canonical spelling".as_slice()));
        assert_eq!(aliases.full.len(), 2);
        assert!(matches!(aliases.full.get(b"renamed-long".as_slice()), Some(Name::Packed(owner)) if *owner == unchanged));
        assert!(matches!(aliases.full.get(b"alias".as_slice()), Some(Name::Full(b"A long canonical spelling"))));
    }
}
