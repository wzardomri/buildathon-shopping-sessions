mod click;
mod direct_event;
mod group;
mod identities;
mod input;
mod output;
#[cfg(test)]
mod reducer_tests;

use click::{Click, ClickRecord, PackedClick};
use identities::{Identities, Name, unpack};
use input::{CHUNK_BYTES, DAY, IngestCalendar, Map, SHARDS, Table, WEEK, event_row_fields, fields, hash_bytes, mix, timestamp, transpose, unsigned, worker_map};
use std::collections::hash_map::Entry;
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

const SESSION_GAP: i64 = 30 * 60_000;
const MAX_PRICE: i64 = 10_000_000;

#[derive(Clone, Copy)]
struct Window { start: i64, end: i64 }

impl Window {
    fn read(data: &Path) -> io::Result<Self> {
        let bytes = fs::read(data.join("manifest.json"))?;
        let text = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
        let parse = |key: &str| -> io::Result<i64> {
            let value = text.split_once(key)
                .and_then(|(_, rest)| rest.split_once(':'))
                .and_then(|(_, rest)| rest.split_once('"'))
                .and_then(|(_, rest)| rest.split_once('"'))
                .and_then(|(value, _)| timestamp(value.as_bytes()))
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("invalid manifest key {key}")))?;
            Ok(value)
        };
        let start = parse("\"window_start\"")?;
        let end = parse("\"window_end\"")?;
        if start >= end {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty manifest window"));
        }
        Ok(Self { start, end })
    }
}

struct Catalog<'a> {
    products: Map<&'a [u8], u32>,
    categories: Vec<&'a [u8]>,
    unknown: u32,
    campaigns: Map<&'a [u8], u32>,
    campaign_rows: Vec<(&'a [u8], &'a [u8])>,
    organic: u32,
}

impl<'a> Catalog<'a> {
    fn read(products: &'a Table, campaigns: &'a Table) -> Self {
        let products: Vec<[&[u8]; 2]> = products.rows().filter_map(fields).collect();
        let mut categories = products.iter().map(|row| row[1]).collect::<Vec<_>>();
        categories.push(b"unknown");
        categories.sort_unstable();
        categories.dedup();
        let category_id = |label| categories.binary_search(&label).unwrap() as u32;
        let unknown = category_id(b"unknown");
        let products = products.into_iter().map(|[key, label]| (key, category_id(label))).collect();
        let mut campaign_rows = campaigns.rows().filter_map(fields).map(|[id, channel]| (id, channel))
            .collect::<Vec<_>>();
        let campaigns = campaign_rows.iter().enumerate().map(|(index, &(key, _))| (key, index as u32)).collect();
        let organic = campaign_rows.len() as u32;
        campaign_rows.push((b"organic", b"organic"));
        Self { products, categories, unknown, campaigns, campaign_rows, organic }
    }
}

#[derive(Clone, Copy, Default)]
struct Event {
    at: i64,
    price: u32,
    category: u32,
}

trait EventRecord: Copy + Default + Send {
    fn encode(event: Event, start: i64) -> Self;
    fn sort_key(self) -> i64;
    fn timestamp(self, start: i64) -> i64;
    fn purchase(self) -> (u32, u32);
}

impl EventRecord for Event {
    fn encode(event: Event, _: i64) -> Self { event }
    fn sort_key(self) -> i64 { self.at }
    fn timestamp(self, _: i64) -> i64 { self.at }
    fn purchase(self) -> (u32, u32) { (self.price, self.category) }
}

#[derive(Clone, Copy, Default)]
struct PackedEvent(u64);

impl PackedEvent {
    const PRICE_BITS: u32 = 24;
    const CATEGORY_BITS: u32 = 9;
    const TIME_SHIFT: u32 = Self::PRICE_BITS + Self::CATEGORY_BITS;
    const PRICE_MASK: u64 = (1 << Self::PRICE_BITS) - 1;
    const CATEGORY_MASK: u64 = (1 << Self::CATEGORY_BITS) - 1;
    const MAX_DURATION: i64 = 1 << (64 - Self::TIME_SHIFT);

    fn fits(window: Window, categories: usize) -> bool {
        // Events at the end of the window are excluded. A duration of 2^31
        // therefore has a maximum event offset of 2^31 - 1.
        window.end.checked_sub(window.start).is_some_and(|duration| duration <= Self::MAX_DURATION)
            && MAX_PRICE as u64 <= Self::PRICE_MASK
            && categories <= 1 << Self::CATEGORY_BITS
    }
}

impl EventRecord for PackedEvent {
    fn encode(event: Event, start: i64) -> Self {
        Self(((event.at - start) as u64) << Self::TIME_SHIFT
            | u64::from(event.category) << Self::PRICE_BITS
            | u64::from(event.price))
    }

    fn sort_key(self) -> i64 { (self.0 >> Self::TIME_SHIFT) as i64 }
    fn timestamp(self, start: i64) -> i64 { start + self.sort_key() }

    fn purchase(self) -> (u32, u32) {
        ((self.0 & Self::PRICE_MASK) as u32, ((self.0 >> Self::PRICE_BITS) & Self::CATEGORY_MASK) as u32)
    }
}

#[derive(Clone, Copy)]
struct EventRow<'a, E> { user: Name<'a>, event: E }

#[derive(Clone, Copy, PartialEq, Eq)]
struct Key<'a> { hash: u64, id: &'a [u8] }

impl<'a> Key<'a> {
    fn new(id: &'a [u8]) -> Self { Self { hash: fold_hash(hash_bytes(id)), id } }
}

impl Hash for Key<'_> {
    fn hash<H: Hasher>(&self, hasher: &mut H) { hasher.write_u64(self.hash); }
}

// A table-specific prefix followed by at most eight bytes fits losslessly into
// u64. Trailing NUL values use the fallback since zero is the padding byte.
#[inline]
fn packed_key(id: &[u8], prefix: u8) -> Option<u64> {
    if !(2..=9).contains(&id.len()) || id[0] != prefix || id[id.len() - 1] == 0 {
        return None;
    }
    let tail = &id[1..];
    if let Ok(bytes) = tail.try_into() {
        return Some(u64::from_le_bytes(bytes));
    }
    let mut bytes = [0; 8];
    bytes[..tail.len()].copy_from_slice(tail);
    Some(u64::from_le_bytes(bytes))
}

#[inline]
fn fold_hash(hash: u64) -> u64 { hash ^ (hash >> 33) }

fn buckets<T>() -> Vec<Vec<T>> { (0..SHARDS).map(|_| Vec::new()).collect() }

const WIDE_INGEST: u32 = 1 << 31;

#[inline]
fn pack_ingest(ingest: i64, wide: &mut Vec<i64>) -> u32 {
    let seconds = ingest / 1000;
    if (0..i64::from(WIDE_INGEST)).contains(&seconds) && ingest == seconds * 1000 {
        seconds as u32
    } else {
        // A batch fits in the smaller packed output locator namespace, so
        // even a batch containing only wide ingests cannot exhaust this one.
        let index = wide.len() as u32;
        wide.push(ingest);
        WIDE_INGEST | index
    }
}

#[derive(Clone, Copy)]
struct Candidate {
    location: u32,
    ingest_code: u32,
}

impl Candidate {
    fn ingest(self, wide: &[i64]) -> i64 {
        if self.ingest_code & WIDE_INGEST == 0 {
            i64::from(self.ingest_code) * 1000
        } else {
            wide[(self.ingest_code & !WIDE_INGEST) as usize]
        }
    }
}

const WINNER_BLOCK_ROWS: usize = 8;
const WINNER_SEGMENT_ROWS: usize = 1024;
const WINNER_SEGMENT_BLOCKS: usize = WINNER_SEGMENT_ROWS / WINNER_BLOCK_ROWS;

#[repr(C, align(64))]
struct WinnerBlock {
    keys: [MaybeUninit<u64>; WINNER_BLOCK_ROWS],
    candidates: [MaybeUninit<Candidate>; WINNER_BLOCK_ROWS],
}

#[derive(Default)]
struct PackedCandidates {
    segments: Vec<Box<[WinnerBlock; WINNER_SEGMENT_BLOCKS]>>,
    len: usize,
}

impl PackedCandidates {
    #[cold]
    #[inline(never)]
    fn add_segment(&mut self) {
        // Each block consists only of MaybeUninit lanes. The array can be
        // allocated without writing any lane and is dropped without reading
        // any lane. Later, len limits reads to the initialized prefix.
        let segment = unsafe { Box::<[WinnerBlock; WINNER_SEGMENT_BLOCKS]>::new_uninit().assume_init() };
        self.segments.push(segment);
    }

    #[inline]
    fn push(&mut self, key: u64, candidate: Candidate) {
        let row = self.len % WINNER_SEGMENT_ROWS;
        if row == 0 {
            self.add_segment();
        }
        let block = &mut self.segments.last_mut().unwrap()[row / WINNER_BLOCK_ROWS];
        block.keys[row % WINNER_BLOCK_ROWS].write(key);
        block.candidates[row % WINNER_BLOCK_ROWS].write(candidate);
        self.len += 1;
    }

    #[inline]
    fn block(&self, index: usize) -> &WinnerBlock {
        assert!(index < self.len);
        let row = index % WINNER_SEGMENT_ROWS;
        &self.segments[index / WINNER_SEGMENT_ROWS][row / WINNER_BLOCK_ROWS]
    }

    #[inline]
    fn key_at(&self, index: usize) -> u64 {
        // The block accessor checks that this lane was initialized by push.
        unsafe { *self.block(index).keys[index % WINNER_BLOCK_ROWS].assume_init_ref() }
    }
}

trait WinnerRows<K> {
    fn len(&self) -> usize;
    fn keys(&self) -> impl Iterator<Item = K>;
    fn candidate_at(&self, index: usize) -> Candidate;
}

impl WinnerRows<u64> for PackedCandidates {
    fn len(&self) -> usize { self.len }

    fn keys(&self) -> impl Iterator<Item = u64> {
        self.segments.iter().flat_map(|segment| segment.iter())
            .flat_map(|block| block.keys.iter()).take(self.len)
            // The take is before the read, excluding every unwritten lane.
            .map(|key| unsafe { *key.assume_init_ref() })
    }

    #[inline]
    fn candidate_at(&self, index: usize) -> Candidate {
        // The block accessor checks that this lane was initialized by push.
        unsafe { *self.block(index).candidates[index % WINNER_BLOCK_ROWS].assume_init_ref() }
    }
}

impl<K: Copy> WinnerRows<K> for Vec<(K, Candidate)> {
    fn len(&self) -> usize { Vec::len(self) }
    fn keys(&self) -> impl Iterator<Item = K> { self.iter().map(|&(key, _)| key) }
    fn candidate_at(&self, index: usize) -> Candidate { self[index].1 }
}

struct FusedEventBatch<'a, E> {
    packed: Vec<PackedCandidates>,
    full: Vec<Vec<(Key<'a>, Candidate)>>,
    wide_ingests: Vec<i64>,
    output: group::Rows<'a, E>,
    candidates: usize,
}

// Keep each winner job's working set smaller at full scale without
// changing the partitioning used for raw or linked user IDs.
const WINNER_SHARDS: usize = 256;

#[inline]
fn winner_shard(hash: u64) -> usize { ((hash >> 32) as usize) & (WINNER_SHARDS - 1) }

impl<'a, E: EventRecord> FusedEventBatch<'a, E> {
    fn new() -> Self {
        Self {
            packed: (0..WINNER_SHARDS).map(|_| PackedCandidates::default()).collect(),
            full: (0..WINNER_SHARDS).map(|_| Vec::new()).collect(),
            wide_ingests: Vec::new(),
            output: group::Rows::new(),
            candidates: 0,
        }
    }

    fn accepts_chunk(&self) -> bool {
        // The 4 MiB ordinary and 16 MiB cold plans both have at most
        // CHUNK_BYTES admitted candidates plus one row spanning the end.
        self.candidates <= group::MAX_LOCATED_ROWS - CHUNK_BYTES - 1
    }

    fn scan(
        &mut self, bytes: &'a [u8], catalog: &Catalog<'a>, window: Window, calendar: &IngestCalendar,
    ) {
        self.scan_projected(bytes, catalog, window, calendar, |field| field);
    }

    fn scan_projected<'input>(
        &mut self, mut bytes: &'input [u8], catalog: &Catalog<'a>, window: Window, calendar: &IngestCalendar,
        retain: impl Fn(&'input [u8]) -> &'a [u8],
    ) {
        while !bytes.is_empty() {
            let (consumed, fields) = event_row_fields(bytes);
            bytes = &bytes[consumed..];
            let Some([id, user, at, ingest, kind, product, price]) = fields else { continue };
            if id.is_empty() { continue; }
            let Some((ingest, direct_code)) = calendar.parse(ingest) else { continue };
            let location = match prepare_event_row::<E>(user, at, ingest, kind, product, price, catalog, window, calendar) {
                Some(row) => {
                    let user = match row.user {
                        Name::Packed(key) => Name::Packed(key),
                        Name::Full(bytes) => Name::Full(retain(bytes)),
                    };
                    self.output.push_located(user, row.event)
                }
                None => group::NO_LOCATION,
            };
            // Rows without valid event payloads still participate in dedup;
            // the earliest copy can suppress valid rows with the same ID.
            let ingest_code = if direct_code == IngestCalendar::DEFER_CODE {
                pack_ingest(ingest, &mut self.wide_ingests)
            } else {
                direct_code
            };
            let candidate = Candidate { location, ingest_code };
            if let Some(key) = packed_key(id, b'e') {
                // Odd multiplication followed by xor-shift is reversible, so
                // the full hash identifies a packed spelling without collision.
                let hash = fold_hash(mix(0, key));
                self.packed[winner_shard(hash)].push(hash, candidate);
            } else {
                let key = Key { hash: Key::new(id).hash, id: retain(id) };
                self.full[winner_shard(key.hash)].push((key, candidate));
            }
            self.candidates += 1;
        }
    }
}

#[derive(Clone, Copy)]
struct PendingEvent {
    batch: u32,
    location: u32,
    ingest: i64,
}

impl PendingEvent {
    fn reject(self, losers: &mut Vec<(u32, u32)>) {
        if self.location != group::NO_LOCATION {
            losers.push((self.batch, self.location));
        }
    }
}

#[inline]
fn repeat_slot(hash: u64, mask: usize) -> (usize, u64) {
    let slot = hash as usize & mask;
    (slot >> 5, 1 << ((slot & 31) * 2))
}

fn mark_fused_losers<K: Eq + Hash, Rows: WinnerRows<K>>(
    parts: Vec<(u32, Rows)>, wide_ingests: &[Vec<i64>],
    fingerprint: impl Fn(&K) -> u64, losers: &mut Vec<(u32, u32)>,
) {
    let count = parts.iter().map(|(_, candidates)| candidates.len()).sum::<usize>();
    if count < 2 { return; }
    let slots = count.saturating_mul(12).next_power_of_two().max(32);
    let mut counts = vec![0u64; slots / 32];
    for key in parts.iter().flat_map(|(_, candidates)| candidates.keys()) {
        let (word, seen) = repeat_slot(fingerprint(&key), slots - 1);
        let bits = &mut counts[word];
        *bits |= seen | ((*bits & seen) << 1);
    }
    let mut winners = Map::<K, PendingEvent>::with_capacity_and_hasher(count / 6, Default::default());
    for (batch, candidates) in parts {
        let wide = &wide_ingests[batch as usize];
        for (index, key) in candidates.keys().enumerate() {
            let (word, seen) = repeat_slot(fingerprint(&key), slots - 1);
            if counts[word] & (seen << 1) == 0 { continue; }
            let candidate = candidates.candidate_at(index);
            let pending = PendingEvent { batch, location: candidate.location, ingest: candidate.ingest(wide) };
            match winners.entry(key) {
                Entry::Vacant(entry) => { entry.insert(pending); }
                Entry::Occupied(mut entry) if pending.ingest < entry.get().ingest => {
                    entry.insert(pending).reject(losers);
                }
                Entry::Occupied(_) => pending.reject(losers),
            }
        }
    }
}

const INDEX_LANES: usize = 8;

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn index_masks(entries: &[u32; INDEX_LANES], mask: u32, tag: u32) -> (u32, u32) {
    use std::arch::x86_64::{
        _mm256_and_si256, _mm256_castsi256_ps, _mm256_cmpeq_epi32,
        _mm256_loadu_si256, _mm256_movemask_ps, _mm256_set1_epi32, _mm256_setzero_si256,
    };
    // The array contains eight u32 entries and therefore covers the whole
    // unaligned AVX2 load, including at the final group of the index.
    unsafe {
        let values = _mm256_loadu_si256(entries.as_ptr().cast());
        let free = _mm256_cmpeq_epi32(values, _mm256_setzero_si256());
        let tags = _mm256_and_si256(values, _mm256_set1_epi32(mask as i32));
        let matches = _mm256_cmpeq_epi32(tags, _mm256_set1_epi32(tag as i32));
        let free = _mm256_movemask_ps(_mm256_castsi256_ps(free)) as u32;
        let matches = _mm256_movemask_ps(_mm256_castsi256_ps(matches)) as u32;
        (matches & !free, free)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline]
fn index_masks(entries: &[u32; INDEX_LANES], mask: u32, tag: u32) -> (u32, u32) {
    let mut matches = 0;
    let mut free = 0;
    for (lane, &entry) in entries.iter().enumerate() {
        if entry == 0 { free |= 1 << lane; }
        else if entry & mask == tag { matches |= 1 << lane; }
    }
    (matches, free)
}

fn mark_packed_losers(
    parts: Vec<(u32, PackedCandidates)>, wide_ingests: &[Vec<i64>],
    table: &mut Vec<u32>, losers: &mut Vec<(u32, u32)>,
) {
    let count = parts.iter().map(|(_, rows)| rows.len()).sum::<usize>();
    if count < 2 { return; }
    let longest = parts.iter().map(|(_, rows)| rows.len()).max().unwrap();
    let part_width = longest.next_power_of_two();
    let Some(reference_span) = parts.len().checked_mul(part_width).filter(|&span| span < 1 << 24) else {
        // Keep at least eight tag bits. The exact general implementation
        // also handles unusually large or uneven packed partitions.
        mark_fused_losers(parts, wide_ingests, |hash| *hash, losers);
        return;
    };
    let row_bits = part_width.trailing_zeros();
    let row_mask = (part_width - 1) as u32;
    let reference_bits = usize::BITS - reference_span.leading_zeros();
    let reference_mask = (1u32 << reference_bits) - 1;
    let capacity = (count + count / 2).next_power_of_two().max(INDEX_LANES);
    let slot_mask = capacity - 1;
    table.clear();
    table.resize(capacity, 0);

    for (part, (batch, rows)) in parts.iter().enumerate() {
        let reference_base = (part as u32) << row_bits;
        let wide = &wide_ingests[*batch as usize];
        'candidate: for (index, key) in rows.keys().enumerate() {
            let fingerprint = fold_hash(mix(0, key));
            let tag = (fingerprint >> 32) as u32 & !reference_mask;
            let reference = reference_base + index as u32 + 1; // Zero marks an empty slot.
            let mut group = fingerprint as usize & slot_mask & !(INDEX_LANES - 1);
            loop {
                let entries = table[group..group + INDEX_LANES].try_into().unwrap();
                let (mut matches, free) = index_masks(entries, !reference_mask, tag);
                while matches != 0 {
                    let slot = group + matches.trailing_zeros() as usize;
                    matches &= matches - 1;
                    let stored = table[slot];
                    let previous_ref = (stored & reference_mask) - 1;
                    let (previous_batch, previous_rows) = &parts[(previous_ref >> row_bits) as usize];
                    let previous_index = (previous_ref & row_mask) as usize;
                    if previous_rows.key_at(previous_index) == key {
                        // The complete packed key is injective. A tag match
                        // alone can only justify reading that key.
                        let candidate = rows.candidate_at(index);
                        let previous = previous_rows.candidate_at(previous_index);
                        let previous_wide = &wide_ingests[*previous_batch as usize];
                        let (loser_batch, loser) = if candidate.ingest(wide) < previous.ingest(previous_wide) {
                            table[slot] = tag | reference;
                            (*previous_batch, previous)
                        } else {
                            (*batch, candidate)
                        };
                        if loser.location != group::NO_LOCATION {
                            losers.push((loser_batch, loser.location));
                        }
                        continue 'candidate;
                    }
                }
                // Since slots are never deleted, an available slot means
                // this key could not have been inserted in a later group.
                if free != 0 {
                    table[group + free.trailing_zeros() as usize] = tag | reference;
                    break;
                }
                group = (group + INDEX_LANES) & slot_mask;
            }
        }
    }
}

fn prepare_event_row<'a, E: EventRecord>(
    user: &'a [u8], at: &[u8], ingest: i64, kind: &[u8], product: &[u8], price: &[u8],
    catalog: &Catalog<'_>, window: Window, calendar: &IngestCalendar,
) -> Option<EventRow<'a, E>> {
    if user.is_empty() || product.is_empty() || !matches!(kind, b"purchase" | b"cart" | b"view") {
        return None;
    }
    let at = calendar.parse(at).map(|(at, _)| at)
        .filter(|&at| at >= window.start && at < window.end && ingest - at <= DAY)?;
    let (price, category) = if kind == b"purchase" {
        let price = unsigned(price).filter(|&value| (1..=MAX_PRICE).contains(&value))?;
        let category = catalog.products.get(product).copied().unwrap_or(catalog.unknown);
        (price as u32, category)
    } else {
        (0, 0)
    };
    Some(EventRow { user: Name::from_bytes(user), event: E::encode(Event { at, price, category }, window.start) })
}

fn load_events<'a, E: EventRecord>(
    table: &'a Table, catalog: &Catalog<'a>, window: Window, threads: usize,
) -> Vec<group::Batch<'a, E>> {
    let stage = Instant::now();
    let calendar = IngestCalendar::new(window.start, window.end);
    let workers = match table.event_chunks(threads) {
        input::EventPlan::Mapped(chunks) => {
            worker_map(threads, chunks.len(), || vec![FusedEventBatch::<E>::new()], |batches, index| {
                if !batches.last().unwrap().accepts_chunk() {
                    batches.push(FusedEventBatch::new());
                }
                batches.last_mut().unwrap().scan(chunks[index], catalog, window, &calendar);
            })
        }
        input::EventPlan::Direct(plan) => scan_direct_event_ranges::<E>(plan, catalog, window, &calendar, threads),
    };
    eprintln!("fused event scan       {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let mut wide = Vec::new();
    let mut output = Vec::new();
    let mut jobs: Vec<_> = (0..WINNER_SHARDS).map(|_| Mutex::new((Vec::new(), Vec::new()))).collect();
    for FusedEventBatch { packed, full, wide_ingests, output: rows, .. } in workers.into_iter().flatten() {
        let batch = u32::try_from(wide.len()).expect("too many event batches");
        wide.push(wide_ingests);
        output.push((rows, Vec::new()));
        for ((job, packed), full) in jobs.iter_mut().zip(packed).zip(full) {
            let (packed_parts, full_parts) = job.get_mut().unwrap();
            if packed.len() != 0 { packed_parts.push((batch, packed)); }
            if !full.is_empty() { full_parts.push((batch, full)); }
        }
    }
    let losers = worker_map(threads, jobs.len(), || (Vec::new(), Vec::new()), |(losers, table), index| {
        let (packed, full) = std::mem::take(&mut *jobs[index].lock().unwrap());
        mark_packed_losers(packed, &wide, table, losers);
        mark_fused_losers(full, &wide, |key| key.hash, losers);
    });
    for (batch, location) in losers.into_iter().flat_map(|(losers, _)| losers) {
        output[batch as usize].1.push(location);
    }
    let output: Vec<_> = output.into_iter().map(|batch| Mutex::new(Some(batch))).collect();
    let workers = worker_map(threads, output.len(), Vec::new, |worker, index| {
        let (mut rows, locations) = output[index].lock().unwrap().take().unwrap();
        rows.remove_located(locations);
        worker.push(rows);
    });
    eprintln!("event winner removal   {:.3}s", stage.elapsed().as_secs_f64());
    group::batch(workers.into_iter().flatten().collect())
}

#[derive(Default)]
struct DirectCoverage {
    used: usize,
    fallback: usize,
    bytes: usize,
}

fn scan_direct_event_ranges<'a, E: EventRecord>(
    plan: direct_event::Plan<'a>, catalog: &Catalog<'a>, window: Window, calendar: &IngestCalendar,
    threads: usize,
) -> Vec<Vec<FusedEventBatch<'a, E>>> {
    let workers = worker_map(threads, plan.ranges.len(),
        || (vec![FusedEventBatch::<E>::new()], direct_event::AlignedBuffer::default(), DirectCoverage::default()),
        |(batches, buffer, coverage), index| {
            let chunk = &plan.ranges[index];
            if !batches.last().unwrap().accepts_chunk() {
                batches.push(FusedEventBatch::new());
            }
            let batch = batches.last_mut().unwrap();
            let source = &plan.sources[chunk.file];
            if let Some(bytes) = buffer.read(chunk, source) {
                batch.scan_projected(bytes, catalog, window, calendar, |field| chunk.retain(bytes, field));
                coverage.used += 1;
                coverage.bytes += bytes.len();
            } else {
                batch.scan(chunk.mapped, catalog, window, calendar);
                coverage.fallback += usize::from(source.eligible);
            }
        },
    );
    drop(plan.sources);
    let mut coverage = DirectCoverage::default();
    let batches = workers.into_iter().map(|(batches, _, worker)| {
        coverage.used += worker.used;
        coverage.fallback += worker.fallback;
        coverage.bytes += worker.bytes;
        batches
    }).collect();
    eprintln!("cold direct events eligible={} opened={} ranges={} used={} fallback={} bytes={}",
        plan.eligible, plan.opened, plan.ranges.len(), coverage.used, coverage.fallback, coverage.bytes);
    batches
}

fn prepare_click<'a, C: ClickRecord<'a>>(
    line: &'a [u8], catalog: &Catalog<'a>, origin: i64, end: i64,
) -> Option<(Name<'a>, C)> {
    let [id, user, campaign, at] = fields(line)?;
    if id.is_empty() || user.is_empty() { return None; }
    let campaign = *catalog.campaigns.get(campaign)?;
    let at = timestamp(at).filter(|&at| at >= origin && at < end)?;
    let user = Name::from_bytes(user);
    Some((user, C::encode(Click { at, id, campaign }, origin)))
}

fn load_clicks<'a, C: ClickRecord<'a>>(
    table: &'a Table, catalog: &Catalog<'a>,
    origin: i64, end: i64, threads: usize,
) -> Vec<group::Batch<'a, C>> {
    // The rules guarantee identical values for each repeated click ID. Giving
    // its copies to the same reducer preserves the winning timestamp and ID
    // and leaves the union of campaign assist intervals unchanged.
    let workers = table.scan(threads, group::Rows::new, |rows, line| {
        if let Some((person, click)) = prepare_click::<C>(line, catalog, origin, end) {
            rows.push(person, click);
        }
    });
    group::batch(workers)
}

#[derive(Clone, Copy, Default)]
struct CampaignDay { sessions: u64, purchasing: u64, revenue: u64, assisted: u64 }

impl CampaignDay {
    fn add(&mut self, value: Self) {
        self.sessions += value.sessions;
        self.purchasing += value.purchasing;
        self.revenue += value.revenue;
        self.assisted += value.assisted;
    }

    fn has_sessions(self) -> bool {
        self.sessions != 0 || self.assisted != 0
    }
}

enum CampaignTotals {
    Dense { first_day: i32, days: usize, rows: Vec<CampaignDay> },
    Sparse(Map<(u32, i32), CampaignDay>),
}

impl CampaignTotals {
    fn new(window: Window, campaigns: usize) -> Self {
        let first_day = day_of(window.start);
        let last_day = day_of(window.end - 1);
        let days = (last_day - first_day + 1) as usize;
        match campaigns.checked_mul(days).filter(|&size| size <= 512 * 1024) {
            Some(size) => Self::Dense { first_day, days, rows: vec![CampaignDay::default(); size] },
            None => Self::Sparse(Map::default()),
        }
    }

    fn for_day(&mut self, campaign: u32, day: i32) -> &mut CampaignDay {
        match self {
            Self::Dense { first_day, days, rows } => {
                &mut rows[campaign as usize * *days + (day - *first_day) as usize]
            }
            Self::Sparse(rows) => rows.entry((campaign, day)).or_default(),
        }
    }

    fn into_rows(self, mut consume: impl FnMut((u32, i32), CampaignDay)) {
        match self {
            Self::Dense { first_day, days, rows } => {
                for (index, row) in rows.into_iter().enumerate().filter(|(_, row)| row.has_sessions()) {
                    let campaign = (index / days) as u32;
                    let day = first_day + (index % days) as i32;
                    consume((campaign, day), row);
                }
            }
            Self::Sparse(rows) => {
                for (key, row) in rows {
                    consume(key, row);
                }
            }
        }
    }
}

#[derive(Default)]
struct UserDay {
    sessions: u64,
    events: u64,
    revenue: u64,
    categories: Vec<(u32, u64)>,
}

impl UserDay {
    fn purchase(&mut self, category: u32, revenue: u64) {
        if let Some((_, total)) = self.categories.iter_mut().find(|(id, _)| *id == category) {
            *total += revenue;
        } else {
            self.categories.push((category, revenue));
        }
        self.revenue += revenue;
    }
}

struct Work {
    users: output::Users,
    totals: CampaignTotals,
    anchors: Vec<Anchor>,
    assist_ends: Vec<usize>,
    seen: Vec<usize>,
    categories: Vec<(u32, u64)>,
}

struct Anchor {
    at: i64,
    revenue: u64,
}

impl Work {
    fn new(window: Window, campaigns: usize) -> Self {
        Self {
            users: output::Users::new(),
            totals: CampaignTotals::new(window, campaigns),
            anchors: vec![],
            assist_ends: vec![0; campaigns],
            seen: vec![],
            categories: vec![],
        }
    }

    fn process<'a, E: EventRecord, C: ClickRecord<'a>>(
        &mut self,
        name: &[u8],
        events: &mut [E],
        clicks: &mut [C],
        catalog: &Catalog<'_>,
        start_ms: i64,
        click_origin: i64,
    ) {
        events.sort_unstable_by_key(|event| event.sort_key());
        self.anchors.clear();
        let has_clicks = !clicks.is_empty();
        let mut day_totals = UserDay {
            categories: std::mem::take(&mut self.categories),
            ..UserDay::default()
        };
        let mut at = events[0].timestamp(start_ms);
        let mut day = day_of(at);
        let mut previous = events[0].sort_key();
        let mut revenue = 0;
        for &event in events.iter() {
            let key = event.sort_key();
            if key - previous > SESSION_GAP {
                day_totals.sessions += 1;
                self.finish_anchor(at, day, revenue, has_clicks, catalog.organic);
                at = event.timestamp(start_ms);
                let next_day = day_of(at);
                if day != next_day {
                    self.write_user_day(name, day, &day_totals);
                    day_totals.sessions = 0;
                    day_totals.events = 0;
                    day_totals.revenue = 0;
                    day_totals.categories.clear();
                    day = next_day;
                }
                revenue = 0;
            }
            day_totals.events += 1;
            let (price, category) = event.purchase();
            if price != 0 {
                let price = u64::from(price);
                revenue += price;
                day_totals.purchase(category, price);
            }
            previous = key;
        }
        day_totals.sessions += 1;
        self.finish_anchor(at, day, revenue, has_clicks, catalog.organic);
        self.write_user_day(name, day, &day_totals);
        self.categories = day_totals.categories;
        self.categories.clear();
        if has_clicks {
            clicks.sort_unstable_by(|a, b| a.sort_key().cmp(&b.sort_key()).then_with(|| a.id().cmp(b.id())));
            self.attribute(clicks, click_origin, catalog.organic);
        }
    }

    fn finish_anchor(&mut self, at: i64, day: i32, revenue: u64, has_clicks: bool, organic: u32) {
        if has_clicks {
            self.anchors.push(Anchor { at, revenue });
        } else {
            credit(&mut self.totals, organic, day, revenue);
        }
    }

    fn write_user_day(&mut self, name: &[u8], day: i32, totals: &UserDay) {
        let top = totals.categories.iter().min_by_key(|&&(category, cents)| (std::cmp::Reverse(cents), category))
            .map(|&(id, _)| id);
        self.users.push(name, day, totals.sessions, totals.events, totals.revenue, top);
    }

    fn attribute<'a, C: ClickRecord<'a>>(&mut self, clicks: &[C], origin: i64, organic: u32) {
        let anchors = &self.anchors;
        let mut before = 0;
        let mut through = 0;
        let mut best: Option<(i64, i64, u32)> = None; // click time, inclusive expiry, campaign
        for &click in clicks {
            let at = click.timestamp(origin);
            // Only finish anchors strictly before this click. Every click at an
            // anchor's exact timestamp participates in both credit and assists.
            while let Some(anchor) = anchors.get(before).filter(|anchor| anchor.at < at) {
                credit_anchor(&mut self.totals, anchor, best, organic);
                before += 1;
            }
            if before == anchors.len() {
                break;
            }
            through = through.max(before);
            let expiry = at.saturating_add(WEEK);
            while anchors.get(through).is_some_and(|anchor| anchor.at <= expiry) {
                through += 1;
            }
            if before == through {
                continue;
            }
            let campaign = click.campaign();
            // Timestamp ties arrive in ascending full click-ID order.
            if best.is_none_or(|(earlier, _, _)| earlier < at) {
                best = Some((at, expiry, campaign));
            }
            // Eligibility's lower bound never moves backwards. Any already
            // covered positions at or after `before` lie in the most recent
            // interval for this campaign, including under disjoint intervals.
            let end = &mut self.assist_ends[campaign as usize];
            let mut first = before.max(*end);
            if first < through {
                if *end == 0 {
                    self.seen.push(campaign as usize);
                }
                *end = through;
                while first < through {
                    let day = day_of(anchors[first].at);
                    let day_end = (i64::from(day) + 1) * DAY;
                    let mut next = first + 1;
                    while next < through && anchors[next].at < day_end {
                        next += 1;
                    }
                    self.totals.for_day(campaign, day).assisted += (next - first) as u64;
                    first = next;
                }
            }
        }
        for anchor in &anchors[before..] {
            credit_anchor(&mut self.totals, anchor, best, organic);
        }
        for campaign in self.seen.drain(..) {
            self.assist_ends[campaign] = 0;
        }
    }
}

fn day_of(at: i64) -> i32 { at.div_euclid(DAY) as i32 }

fn credit_anchor(totals: &mut CampaignTotals, anchor: &Anchor, best: Option<(i64, i64, u32)>, organic: u32) {
    let campaign = best.filter(|&(_, expiry, _)| anchor.at <= expiry)
        .map_or(organic, |(_, _, campaign)| campaign);
    credit(totals, campaign, day_of(anchor.at), anchor.revenue);
}

fn credit(totals: &mut CampaignTotals, campaign: u32, day: i32, revenue: u64) {
    let row = totals.for_day(campaign, day);
    row.sessions += 1;
    row.purchasing += u64::from(revenue != 0);
    row.revenue += revenue;
}

fn solve(data: &Path, scratch: &Path, threads: usize) -> io::Result<()> {
    let stage = Instant::now();
    let window = Window::read(data)?;
    let click_origin = window.start.checked_sub(WEEK)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "click window start overflows"))?;
    let links = Table::open(&data.join("links"))?;
    let products = Table::open(&data.join("products.csv"))?;
    let campaigns = Table::open(&data.join("campaigns.csv"))?;
    let events = Table::open_events(&data.join("events"), threads)?;
    let clicks = Table::open(&data.join("clicks"))?;
    let identities = Identities::read(&links, threads);
    let catalog = Catalog::read(&products, &campaigns);
    eprintln!("load linked identities {:.3}s", stage.elapsed().as_secs_f64());
    let compact_clicks = PackedClick::fits(click_origin, window.end);
    if compact_clicks && PackedEvent::fits(window, catalog.categories.len()) {
        solve_tables::<PackedEvent, PackedClick<'_>>(
            &events, &clicks, &identities, &catalog, window, click_origin, scratch, threads,
        )
    } else if compact_clicks {
        solve_tables::<Event, PackedClick<'_>>(
            &events, &clicks, &identities, &catalog, window, click_origin, scratch, threads,
        )
    } else {
        solve_tables::<Event, Click<'_>>(
            &events, &clicks, &identities, &catalog, window, click_origin, scratch, threads,
        )
    }
}

fn solve_tables<'a, E: EventRecord, C: ClickRecord<'a>>(
    events: &'a Table,
    clicks: &'a Table,
    identities: &Identities<'a>,
    catalog: &Catalog<'a>,
    window: Window,
    click_origin: i64,
    scratch: &Path,
    threads: usize,
) -> io::Result<()> {
    let events = load_events::<E>(events, catalog, window, threads);
    let stage = Instant::now();
    let clicks = load_clicks::<C>(clicks, catalog, click_origin, window.end, threads);
    eprintln!("partition clicks       {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let mut jobs: Vec<_> = events.into_iter().zip(clicks).map(Mutex::new).collect();
    jobs.sort_unstable_by_key(|job| {
        let (events, clicks) = &*job.lock().unwrap();
        std::cmp::Reverse(events.len() + clicks.len())
    });
    let mut arenas: Vec<_> = worker_map(threads, jobs.len(), Vec::new, |arenas, job| {
        let (events, clicks) = std::mem::take(&mut *jobs[job].lock().unwrap());
        arenas.push((
            group::RawPeople::gather(events.packed, clicks.packed),
            group::RawPeople::gather(events.full, clicks.full),
        ));
    }).into_iter().flatten().collect();
    eprintln!("group raw names        {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();

    // Each raw owner has one disjoint event and click slice. Keep click-only
    // names because another spelling of the same person may have the events.
    let sources: Vec<_> = arenas.iter_mut().map(|arena| Mutex::new(Some(arena))).collect();
    let routes = worker_map(threads, sources.len(), buckets, |routes, source| {
        let (packed, full) = sources[source].lock().unwrap().take().unwrap();
        let packed = packed.slabs().map(|(key, events, clicks)| (Name::Packed(key), events, clicks));
        let full = full.slabs().map(|(key, events, clicks)| (Name::Full(key), events, clicks));
        for (raw, events, clicks) in packed.chain(full) {
            let owner = identities.canonical(raw);
            routes[owner.shard()].push(group::Slab { owner, events, clicks });
        }
    });
    drop(sources);
    let mut jobs: Vec<_> = transpose(routes).into_iter().map(|parts| {
        let rows: usize = parts.iter().flatten().map(|slab| slab.events.len() + slab.clicks.len()).sum();
        (rows, Mutex::new(parts))
    }).collect();
    jobs.sort_unstable_by_key(|(rows, _)| std::cmp::Reverse(*rows));
    eprintln!("route canonical owners {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let workers = worker_map(threads, jobs.len(), || Work::new(window, catalog.campaign_rows.len()), |work, job| {
        let parts = std::mem::take(&mut *jobs[job].1.lock().unwrap());
        let mut name = [0; 9];
        group::canonical_people(parts, |owner, events, clicks| {
            let name = match owner {
                Name::Packed(key) => unpack(key, &mut name),
                Name::Full(key) => key,
            };
            work.process(name, events, clicks, catalog, window.start, click_origin);
        });
    });
    drop(jobs);
    drop(arenas);
    eprintln!("sessions/attribution   {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    output::write(scratch, workers, catalog)?;
    eprintln!("write native Parquet   {:.3}s", stage.elapsed().as_secs_f64());
    Ok(())
}

fn main() -> io::Result<()> {
    let args = env::args_os().collect::<Vec<_>>();
    if args.len() != 3 { return Err(io::Error::other("usage: shopping-sessions DATA_DIR SCRATCH_OUTPUT")); }
    let threads = env::var("NCPU").ok().and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count != 0).unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    solve(Path::new(&args[1]), Path::new(&args[2]), threads)
}
