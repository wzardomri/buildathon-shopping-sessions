use crate::input::{Map, SHARDS, Table, fields, hash_bytes, mix, shard, worker_map};
use std::sync::atomic::{AtomicU64, Ordering};

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

#[derive(Clone, Copy)]
pub enum Name<'a> {
    Packed(u64),
    Full(&'a [u8]),
}

impl<'a> Name<'a> {
    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        packed(bytes).map_or(Self::Full(bytes), Self::Packed)
    }

    pub fn is_empty(self) -> bool {
        matches!(self, Self::Full(b""))
    }

    fn lexical_min(self, other: Self) -> Self {
        let left_is_first = match (self, other) {
            (Self::Packed(left), Self::Packed(right)) => packed_order(left) <= packed_order(right),
            (Self::Full(left), Self::Full(right)) => left <= right,
            (Self::Packed(left), Self::Full(right)) => unpack(left, &mut [0; 9]) <= right,
            (Self::Full(left), Self::Packed(right)) => left <= unpack(right, &mut [0; 9]),
        };
        if left_is_first { self } else { other }
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

struct Components<'a> {
    parents: Vec<u32>,
    sizes: Vec<u32>,
    names: Vec<Name<'a>>,
}

impl Components<'_> {
    #[inline]
    fn root(&mut self, mut id: u32) -> u32 {
        while self.parents[id as usize] != id {
            let parent = self.parents[id as usize];
            let grandparent = self.parents[parent as usize];
            self.parents[id as usize] = grandparent;
            id = grandparent;
        }
        id
    }

    #[inline]
    fn join(&mut self, left: u32, right: u32) {
        let (mut left, mut right) = (self.root(left) as usize, self.root(right) as usize);
        if left == right {
            return;
        }
        if self.sizes[left] < self.sizes[right] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parents[right] = left as u32;
        self.sizes[left] += self.sizes[right];
        self.names[left] = self.names[left].lexical_min(self.names[right]);
    }
}

pub struct Identities<'a> {
    indices: Vec<(u32, Index<'a>)>,
    names: Vec<Name<'a>>,
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
        let mut groups = Components {
            parents: (0..names.len() as u32).collect(),
            sizes: vec![1; names.len()],
            names,
        };
        let global_id = |code: u64| indices[(code >> 32) as usize].0 + code as u32;
        for edge in assigned.chunks_exact(2) {
            groups.join(
                global_id(edge[0].load(Ordering::Relaxed)),
                global_id(edge[1].load(Ordering::Relaxed)),
            );
        }
        let names = (0..groups.parents.len()).map(|id| {
            let root = groups.root(id as u32) as usize;
            groups.names[root]
        }).collect();
        Self { indices, names }
    }

    #[inline]
    pub fn name(&self, key: &'a [u8]) -> Name<'a> {
        let original = Name::from_bytes(key);
        let (base, local_id) = match original {
            Name::Packed(key) => {
                let (base, index) = &self.indices[packed_shard(key)];
                (*base, index.short.get(&key))
            }
            Name::Full(key) => {
                let (base, index) = &self.indices[shard(hash_bytes(key))];
                (*base, index.other.get(key))
            }
        };
        local_id.map_or(original, |&id| self.names[(base + id) as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::{Name, unpack};

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
                let mut buffer = [0; 9];
                let actual = match Name::from_bytes(left).lexical_min(Name::from_bytes(right)) {
                    Name::Packed(key) => unpack(key, &mut buffer),
                    Name::Full(bytes) => bytes,
                };
                assert_eq!(actual, left.min(right));
            }
        }
    }
}
