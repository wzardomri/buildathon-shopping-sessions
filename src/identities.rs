use crate::input::{Map, Table, fields};

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

#[derive(Default)]
struct Index<'a> {
    short: Map<u64, u32>,
    other: Map<&'a [u8], u32>,
}

#[derive(Clone, Copy)]
struct Node<'a> {
    parent: u32,
    size: u32,
    name: &'a [u8],
}

#[derive(Default)]
struct Components<'a> {
    index: Index<'a>,
    nodes: Vec<Node<'a>>,
}

impl<'a> Components<'a> {
    #[inline]
    fn intern(&mut self, name: &'a [u8]) -> u32 {
        let nodes = &mut self.nodes;
        let mut insert = || {
            let id = nodes.len() as u32;
            nodes.push(Node { parent: id, size: 1, name });
            id
        };
        match packed(name) {
            Some(key) => *self.index.short.entry(key).or_insert_with(&mut insert),
            None => *self.index.other.entry(name).or_insert_with(insert),
        }
    }

    #[inline]
    fn root(&mut self, mut id: u32) -> u32 {
        while self.nodes[id as usize].parent != id {
            let parent = self.nodes[id as usize].parent;
            let grandparent = self.nodes[parent as usize].parent;
            self.nodes[id as usize].parent = grandparent;
            id = grandparent;
        }
        id
    }

    #[inline]
    fn join(&mut self, a: &'a [u8], b: &'a [u8]) {
        let a = self.intern(a);
        let b = self.intern(b);
        let (mut left, mut right) = (self.root(a) as usize, self.root(b) as usize);
        if left == right {
            return;
        }
        if self.nodes[left].size < self.nodes[right].size {
            std::mem::swap(&mut left, &mut right);
        }
        let right_node = self.nodes[right];
        self.nodes[right].parent = left as u32;
        let left_node = &mut self.nodes[left];
        left_node.size += right_node.size;
        left_node.name = left_node.name.min(right_node.name);
    }
}

pub struct Identities<'a> {
    index: Index<'a>,
    names: Vec<&'a [u8]>,
}

impl<'a> Identities<'a> {
    pub fn read(table: &'a Table) -> Self {
        let mut groups = Components::default();
        for row in table.rows() {
            if let Some([a, b]) = fields(row) {
                if !a.is_empty() && !b.is_empty() {
                    groups.join(a, b);
                }
            }
        }
        let names = (0..groups.nodes.len()).map(|id| {
            let root = groups.root(id as u32);
            groups.nodes[root as usize].name
        }).collect();
        Self { index: groups.index, names }
    }

    #[inline]
    pub fn name(&self, key: &'a [u8]) -> &'a [u8] {
        let id = match packed(key) {
            Some(key) => self.index.short.get(&key),
            None => self.index.other.get(key),
        };
        id.map_or(key, |&id| self.names[id as usize])
    }
}
