mod click;
mod group;
mod identities;
mod input;
mod output;

use click::{Click, ClickRecord, PackedClick};
use identities::{Identities, Name, unpack};
use input::{CHUNK_BYTES, DAY, IngestCalendar, Map, SHARDS, Table, WEEK, fields, find, first_field, hash_bytes, mix, shard, timestamp, transpose, unsigned, worker_map};
use std::collections::hash_map::Entry;
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
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
    fn new(id: &'a [u8]) -> Self { Self { hash: hash_bytes(id), id } }
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

enum ExactKey<'a> { Packed(u64), Full(Key<'a>) }

impl<'a> ExactKey<'a> {
    fn new(id: &'a [u8], prefix: u8) -> Self {
        packed_key(id, prefix).map_or_else(|| Self::Full(Key::new(id)), Self::Packed)
    }

    fn fingerprint(&self) -> u64 {
        let hash = match self {
            Self::Packed(key) => mix(0, *key),
            Self::Full(key) => key.hash,
        };
        hash ^ (hash >> 33)
    }
}

fn buckets<T>() -> Vec<Vec<T>> { (0..SHARDS).map(|_| Vec::new()).collect() }

#[derive(Clone, Copy)]
struct Position {
    chunk: u32,
    offset: u32,
}

// Table chunks can extend through one final row, whose starting offset can
// be exactly CHUNK_BYTES. The remaining bits address a worker-local chunk.
const OFFSET_BITS: u32 = usize::BITS - CHUNK_BYTES.leading_zeros();
const OFFSET_MASK: u32 = (1 << OFFSET_BITS) - 1;
const CHUNKS_PER_BATCH: usize = 1 << (u32::BITS - OFFSET_BITS);

#[derive(Clone, Copy)]
struct EventCandidate {
    fingerprint: u32,
    cursor: u32,
}

impl EventCandidate {
    fn position(self, chunk_ids: &[u32]) -> Position {
        Position { chunk: chunk_ids[(self.cursor >> OFFSET_BITS) as usize], offset: self.cursor & OFFSET_MASK }
    }
}

struct EventBatch {
    chunk_ids: Vec<u32>,
    shards: Vec<Vec<EventCandidate>>,
}

impl EventBatch {
    fn new() -> Self {
        Self { chunk_ids: Vec::new(), shards: buckets() }
    }

    fn scan(&mut self, index: usize, bytes: &[u8]) {
        let chunk_code = (self.chunk_ids.len() as u32) << OFFSET_BITS;
        self.chunk_ids.push(u32::try_from(index).expect("too many event file segments"));
        for (offset, line) in physical_rows(bytes) {
            if let Some(id) = first_field(line) {
                let hash = ExactKey::new(id, b'e').fingerprint();
                self.shards[shard(hash)].push(EventCandidate { fingerprint: hash as u32, cursor: chunk_code | offset });
            }
        }
    }
}

struct EventPlan<'a> {
    chunks: Vec<&'a [u8]>,
    skipped: Vec<Vec<u32>>,
}

fn physical_rows(bytes: &[u8]) -> impl Iterator<Item = (u32, &[u8])> {
    let mut offset = 0;
    std::iter::from_fn(move || {
        if offset >= bytes.len() {
            return None;
        }
        let start = offset;
        let remainder = &bytes[start..];
        let len = find(remainder, b'\n').unwrap_or(remainder.len());
        offset = start + len + 1;
        // Chunking extends the 4 MiB target only through the final row.
        // Even a long final row therefore starts within the compact range.
        Some((u32::try_from(start).expect("invalid row offset"), &remainder[..len]))
    })
}

impl<'a> EventPlan<'a> {
    fn build(table: &'a Table, calendar: &IngestCalendar, threads: usize) -> Self {
        let chunks = table.data_chunks();
        let workers = worker_map(threads, chunks.len(), || vec![EventBatch::new()], |batches, index| {
            if batches.last().unwrap().chunk_ids.len() == CHUNKS_PER_BATCH {
                batches.push(EventBatch::new());
            }
            batches.last_mut().unwrap().scan(index, chunks[index]);
        });
        let mut chunk_ids = Vec::new();
        let mut parts: Vec<_> = (0..SHARDS).map(|_| Mutex::new(Vec::new())).collect();
        for EventBatch { chunk_ids: ids, shards } in workers.into_iter().flatten() {
            let batch = chunk_ids.len();
            chunk_ids.push(ids);
            for (part, candidates) in parts.iter_mut().zip(shards) {
                if !candidates.is_empty() {
                    part.get_mut().unwrap().push((batch, candidates));
                }
            }
        }
        let skipped = worker_map(threads, parts.len(), Vec::new, |worker, index| {
            let candidates = std::mem::take(&mut *parts[index].lock().unwrap());
            mark_losers(candidates, &chunk_ids, &chunks, calendar, worker);
        });
        let mut by_chunk: Vec<Vec<_>> = (0..chunks.len()).map(|_| Vec::new()).collect();
        for position in skipped.into_iter().flatten() {
            by_chunk[position.chunk as usize].push(position.offset);
        }
        for offsets in &mut by_chunk {
            offsets.sort_unstable();
        }
        Self { chunks, skipped: by_chunk }
    }
}

#[derive(Clone, Copy)]
struct PendingEvent {
    position: Position,
    // A singleton or a hash collision does not need a structure/ingest parse.
    ingest: Option<i64>,
}

fn event_id_at<'a>(position: Position, chunks: &[&'a [u8]]) -> &'a [u8] {
    // Every candidate was screened with first_field on its own physical line.
    // Therefore the first comma in this tail belongs to that line.
    first_field(&chunks[position.chunk as usize][position.offset as usize..]).unwrap()
}

fn ingest_at(position: Position, chunks: &[&[u8]], calendar: &IngestCalendar) -> Option<i64> {
    let line = &chunks[position.chunk as usize][position.offset as usize..];
    let end = find(line, b'\n').unwrap_or(line.len());
    let [_, _, _, ingest, _, _, _] = fields(&line[..end])?;
    calendar.parse(ingest)
}

fn mark_losers(
    parts: Vec<(usize, Vec<EventCandidate>)>,
    batch_chunks: &[Vec<u32>],
    chunks: &[&[u8]],
    calendar: &IngestCalendar,
    skipped: &mut Vec<Position>,
) {
    let count = parts.iter().map(|(_, rows)| rows.len()).sum::<usize>();
    if count < 2 {
        return;
    }
    let size = (count * 12).next_power_of_two().max(64);
    let mut seen = vec![0u64; size / 64];
    let mut repeated = vec![0u64; size / 64];
    let slot = |hash: u32| hash as usize & (size - 1);
    for row in parts.iter().flat_map(|(_, rows)| rows) {
        let bit = slot(row.fingerprint);
        let word = bit >> 6;
        let mask = 1u64 << (bit & 63);
        repeated[word] |= seen[word] & mask;
        seen[word] |= mask;
    }
    let mut winners = Map::<u64, PendingEvent>::with_capacity_and_hasher(count / 6, Default::default());
    let mut collisions = Map::<&[u8], PendingEvent>::default();
    for (batch, rows) in parts {
        let chunk_ids = &batch_chunks[batch];
        for row in rows {
            let bit = slot(row.fingerprint);
            if repeated[bit >> 6] & (1 << (bit & 63)) == 0 {
                continue;
            }
            let position = row.position(chunk_ids);
            let pending = PendingEvent { position, ingest: None };
            match winners.entry(u64::from(row.fingerprint)) {
                Entry::Vacant(entry) => {
                    entry.insert(pending);
                }
                Entry::Occupied(mut entry) => {
                    let id = event_id_at(position, chunks);
                    let previous = entry.get_mut();
                    if id == event_id_at(previous.position, chunks) {
                        keep_earliest(previous, position, chunks, calendar, skipped);
                    } else {
                        // Equal fingerprints can represent distinct IDs. The first
                        // ID stays in the fingerprint map and the others use exact
                        // spellings, so they each select their own winner.
                        match collisions.entry(id) {
                            Entry::Vacant(entry) => { entry.insert(pending); }
                            Entry::Occupied(mut entry) => {
                                keep_earliest(entry.get_mut(), position, chunks, calendar, skipped);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn keep_earliest(
    previous: &mut PendingEvent, position: Position, chunks: &[&[u8]],
    calendar: &IngestCalendar, skipped: &mut Vec<Position>,
) {
    // Only confirmed duplicate IDs need a second look at the CSV and ingest.
    // Leave time, type and purchase validation for the chosen winner.
    let Some(ingest) = ingest_at(position, chunks, calendar) else {
        skipped.push(position);
        return;
    };
    let old_ingest = previous.ingest.or_else(|| ingest_at(previous.position, chunks, calendar));
    if old_ingest.is_none_or(|old| ingest < old) {
        skipped.push(previous.position);
        *previous = PendingEvent { position, ingest: Some(ingest) };
    } else {
        skipped.push(position);
        previous.ingest = old_ingest;
    }
}

fn prepare_event<'a, E: EventRecord>(
    line: &'a [u8], catalog: &Catalog<'a>, window: Window, calendar: &IngestCalendar,
) -> Option<EventRow<'a, E>> {
    let [id, user, at, ingest, kind, product, price] = fields(line)?;
    if id.is_empty() { return None; }
    let ingest = calendar.parse(ingest)?;
    prepare_event_row(user, at, ingest, kind, product, price, catalog, window)
}

fn prepare_event_row<'a, E: EventRecord>(
    user: &'a [u8], at: &[u8], ingest: i64, kind: &[u8], product: &[u8], price: &[u8],
    catalog: &Catalog<'a>, window: Window,
) -> Option<EventRow<'a, E>> {
    if user.is_empty() || product.is_empty() || !matches!(kind, b"purchase" | b"cart" | b"view") {
        return None;
    }
    let at = timestamp(at).filter(|&at| at >= window.start && at < window.end && ingest - at <= DAY)?;
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
    table: &'a Table, catalog: &Catalog<'a>,
    window: Window, threads: usize,
) -> Vec<group::Batch<'a, E>> {
    let stage = Instant::now();
    let calendar = IngestCalendar::new(window.start, window.end);
    let plan = EventPlan::build(table, &calendar, threads);
    eprintln!("event winner plan      {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let workers = worker_map(threads, plan.chunks.len(), group::Rows::new, |worker, index| {
        let mut skipped = plan.skipped[index].iter().copied().peekable();
        for (offset, line) in physical_rows(plan.chunks[index]) {
            if skipped.peek() == Some(&offset) {
                skipped.next();
            } else if let Some(row) = prepare_event(line, catalog, window, &calendar) {
                worker.push(row.user, row.event);
            }
        }
    });
    eprintln!("parse/partition events {:.3}s", stage.elapsed().as_secs_f64());
    group::batch(workers)
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
    anchors: Vec<i64>,
    windows: Vec<Option<(i64, i64)>>,
    seen: Vec<usize>,
    categories: Vec<(u32, u64)>,
}

impl Work {
    fn new(window: Window, campaigns: usize) -> Self {
        Self {
            users: output::Users::new(),
            totals: CampaignTotals::new(window, campaigns),
            anchors: vec![],
            windows: vec![None; campaigns],
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
        clicks.sort_unstable_by(|a, b| a.sort_key().cmp(&b.sort_key()).then_with(|| a.id().cmp(b.id())));
        self.anchors.clear();
        let mut day_totals = UserDay {
            categories: std::mem::take(&mut self.categories),
            ..UserDay::default()
        };
        let mut cursor = 0;
        let mut best: Option<C> = None;
        let mut start = 0;
        let mut day = day_of(events[0].timestamp(start_ms));
        while start < events.len() {
            let at = events[start].timestamp(start_ms);
            let session_day = day_of(at);
            if day != session_day {
                self.write_user_day(name, day, &day_totals);
                day_totals.sessions = 0;
                day_totals.events = 0;
                day_totals.revenue = 0;
                day_totals.categories.clear();
                day = session_day;
            }
            let mut end = start + 1;
            while end < events.len() && events[end].sort_key() - events[end - 1].sort_key() <= SESSION_GAP {
                end += 1;
            }
            let mut revenue = 0;
            for event in &events[start..end] {
                let (price, category) = event.purchase();
                if price != 0 {
                    let price = u64::from(price);
                    revenue += price;
                    day_totals.purchase(category, price);
                }
            }
            day_totals.sessions += 1;
            day_totals.events += (end - start) as u64;
            self.anchors.push(at);
            let cutoff = C::time_key(at, click_origin);
            while let Some(&click) = clicks.get(cursor).filter(|click| click.sort_key() <= cutoff) {
                if best.is_none_or(|before| click.sort_key() > before.sort_key()) {
                    best = Some(click);
                }
                cursor += 1;
            }
            let campaign = best.filter(|click| click.sort_key() >= cutoff - WEEK)
                .map_or(catalog.organic, C::campaign);
            let total = self.totals.for_day(campaign, day);
            total.sessions += 1;
            total.purchasing += u64::from(revenue != 0);
            total.revenue += revenue;
            start = end;
        }
        self.write_user_day(name, day, &day_totals);
        self.categories = day_totals.categories;
        self.categories.clear();
        self.assists(&clicks[..cursor], click_origin);
    }

    fn write_user_day(&mut self, name: &[u8], day: i32, totals: &UserDay) {
        let top = totals.categories.iter().min_by_key(|&&(category, cents)| (std::cmp::Reverse(cents), category))
            .map(|&(id, _)| id);
        self.users.push(name, day, totals.sessions, totals.events, totals.revenue, top);
    }

    fn assists<'a, C: ClickRecord<'a>>(&mut self, clicks: &[C], origin: i64) {
        for &click in clicks {
            let campaign = click.campaign();
            let index = campaign as usize;
            let at = click.timestamp(origin);
            let window = (at, at.saturating_add(WEEK));
            match self.windows[index] {
                Some((first, last)) if at <= last => self.windows[index] = Some((first, window.1)),
                pending => {
                    if let Some((first, last)) = pending {
                        count_assists(&self.anchors, &mut self.totals, campaign, first, last);
                    } else {
                        self.seen.push(index);
                    }
                    self.windows[index] = Some(window);
                }
            }
        }
        for campaign in self.seen.drain(..) {
            let (first, last) = self.windows[campaign].take().unwrap();
            count_assists(&self.anchors, &mut self.totals, campaign as u32, first, last);
        }
    }
}

fn day_of(at: i64) -> i32 { at.div_euclid(DAY) as i32 }

fn count_assists(
    anchors: &[i64], totals: &mut CampaignTotals, campaign: u32, first: i64, last: i64,
) {
    let mut start = anchors.partition_point(|&at| at < first);
    let end = start + anchors[start..].partition_point(|&at| at <= last);
    while start < end {
        let day = day_of(anchors[start]);
        let day_end = (i64::from(day) + 1) * DAY;
        let matching = anchors[start..end].partition_point(|&at| at < day_end);
        totals.for_day(campaign, day).assisted += matching as u64;
        start += matching;
    }
}

fn solve(data: &Path, scratch: &Path, threads: usize) -> io::Result<()> {
    let stage = Instant::now();
    let window = Window::read(data)?;
    let click_origin = window.start.checked_sub(WEEK)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "click window start overflows"))?;
    let links = Table::open(&data.join("links"))?;
    let products = Table::open(&data.join("products.csv"))?;
    let campaigns = Table::open(&data.join("campaigns.csv"))?;
    let events = Table::open(&data.join("events"))?;
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
