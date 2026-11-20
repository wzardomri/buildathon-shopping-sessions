mod identities;
mod input;
mod output;

use identities::Identities;
use input::{DAY, Map, SHARDS, Table, WEEK, fields, first_field, hash_bytes, mix, shard, timestamp, transpose, unsigned, worker_map};
use std::collections::hash_map::Entry;
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

const SESSION_GAP: i64 = 30 * 60_000;

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

#[derive(Clone, Copy)]
struct Event {
    at: i64,
    price: u32,
    category: u32,
}

#[derive(Clone, Copy)]
struct EventRow<'a> { user: &'a [u8], event: Event }

#[derive(Clone, Copy)]
struct Click<'a> {
    at: i64,
    id: &'a [u8],
    campaign: u32,
}

#[derive(Clone, Copy)]
struct ClickRow<'a> { user: &'a [u8], click: Click<'a> }

#[derive(Clone, Copy, PartialEq, Eq)]
struct Key<'a> { hash: u64, id: &'a [u8] }

impl<'a> Key<'a> {
    fn new(id: &'a [u8]) -> Self { Self { hash: hash_bytes(id), id } }
}

impl Hash for Key<'_> {
    fn hash<H: Hasher>(&self, hasher: &mut H) { hasher.write_u64(self.hash); }
}

struct EarlyCopy<'a> { ingest: i64, row: EventRow<'a> }

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

struct Partitions<K, V> {
    packed: Vec<Vec<(u64, V)>>,
    fallback: Vec<Vec<(K, V)>>,
}

impl<K, V> Partitions<K, V> {
    fn new() -> Self {
        Self {
            packed: (0..SHARDS).map(|_| Vec::new()).collect(),
            fallback: (0..SHARDS).map(|_| Vec::new()).collect(),
        }
    }
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

impl<'a, V> Partitions<Key<'a>, V> {
    fn push(&mut self, key: ExactKey<'a>, bucket: usize, value: V) {
        match key {
            ExactKey::Packed(key) => self.packed[bucket].push((key, value)),
            ExactKey::Full(key) => self.fallback[bucket].push((key, value)),
        }
    }
}

fn dedup_jobs<K, V>(workers: Vec<Partitions<K, V>>) -> Vec<Mutex<(Vec<Vec<(u64, V)>>, Vec<Vec<(K, V)>>)>> {
    let (packed, fallback): (Vec<_>, Vec<_>) = workers.into_iter().map(|worker| (worker.packed, worker.fallback)).unzip();
    transpose(packed).into_iter().zip(transpose(fallback)).map(Mutex::new).collect()
}

struct Candidates<'a, V, R> {
    duplicates: Partitions<Key<'a>, V>,
    output: Vec<Vec<R>>,
}

impl<'a, V, R> Candidates<'a, V, R> {
    fn new() -> Self { Self { duplicates: Partitions::new(), output: buckets() } }
}

fn buckets<T>() -> Vec<Vec<T>> { (0..SHARDS).map(|_| Vec::new()).collect() }

struct DuplicateFilter { collisions: Vec<Vec<u64>> }

impl DuplicateFilter {
    fn build(table: &Table, prefix: u8, threads: usize) -> Self {
        let hashes = transpose(table.scan(threads, buckets, |worker, line| {
            if let Some(id) = first_field(line) {
                let hash = ExactKey::new(id, prefix).fingerprint();
                worker[shard(hash)].push(hash as u32);
            }
        }));
        let results = worker_map(threads, hashes.len(), Vec::new, |worker, index| {
            let hashes = &hashes[index];
            let count = hashes.iter().map(Vec::len).sum::<usize>();
            let mut repeated = vec![];
            if count > 1 {
                // The first pass can produce false positives from hash collisions or
                // malformed rows. All repeated IDs necessarily share a bit, so a
                // clear collision bit proves that the ID has at most one candidate.
                let bits = (count * 12).next_power_of_two().max(64);
                repeated = vec![0; bits / 64];
                let mut seen = vec![0u64; bits / 64];
                for &hash in hashes.iter().flatten() {
                    let slot = (hash as usize) & (bits - 1);
                    let word = slot >> 6;
                    let mask = 1u64 << (slot & 63);
                    repeated[word] |= seen[word] & mask;
                    seen[word] |= mask;
                }
            }
            worker.push((index, repeated));
        });
        let mut collisions = buckets();
        for (index, repeated) in results.into_iter().flatten() {
            collisions[index] = repeated;
        }
        Self { collisions }
    }

    fn maybe_repeated(&self, bucket: usize, fingerprint: u64) -> bool {
        let bits = &self.collisions[bucket];
        if bits.is_empty() { return false; }
        let slot = (fingerprint as u32 as usize) & (bits.len() * 64 - 1);
        bits[slot >> 6] & (1 << (slot & 63)) != 0
    }
}

fn prepare_event<'a>(
    line: &'a [u8], identities: &Identities<'a>, catalog: &Catalog<'a>, window: Window,
) -> Option<(&'a [u8], EarlyCopy<'a>)> {
    let [id, user, at, ingest, kind, product, price] = fields(line)?;
    if id.is_empty() { return None; }
    let ingest = timestamp(ingest)?;
    // Invalid copies remain in the dedup stage as tombstones. In particular,
    // a valid later copy must never rescue an invalid earliest copy.
    let row = prepare_event_row(user, at, ingest, kind, product, price, identities, catalog, window)
        .unwrap_or(EventRow { user: b"", event: Event { at: 0, price: 0, category: 0 } });
    Some((id, EarlyCopy { ingest, row }))
}

fn prepare_event_row<'a>(
    user: &'a [u8], at: &[u8], ingest: i64, kind: &[u8], product: &[u8], price: &[u8],
    identities: &Identities<'a>, catalog: &Catalog<'a>, window: Window,
) -> Option<EventRow<'a>> {
    if user.is_empty() || product.is_empty() || !matches!(kind, b"purchase" | b"cart" | b"view") {
        return None;
    }
    let at = timestamp(at).filter(|&at| at >= window.start && at < window.end && ingest - at <= DAY)?;
    let (price, category) = if kind == b"purchase" {
        let price = unsigned(price).filter(|&value| (1..=10_000_000).contains(&value))?;
        let category = catalog.products.get(product).copied().unwrap_or(catalog.unknown);
        (price as u32, category)
    } else {
        (0, 0)
    };
    Some(EventRow { user: identities.name(user), event: Event { at, price, category } })
}

fn load_events<'a>(
    table: &'a Table, identities: &Identities<'a>, catalog: &Catalog<'a>,
    window: Window, threads: usize,
) -> Vec<Vec<Vec<EventRow<'a>>>> {
    let stage = Instant::now();
    let filter = DuplicateFilter::build(table, b'e', threads);
    eprintln!("event duplicate filter {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let workers = table.scan(threads, Candidates::new, |worker, line| {
        if let Some((id, candidate)) = prepare_event(line, identities, catalog, window) {
            let key = ExactKey::new(id, b'e');
            let hash = key.fingerprint();
            let bucket = shard(hash);
            if filter.maybe_repeated(bucket, hash) {
                worker.duplicates.push(key, bucket, candidate);
            } else if !candidate.row.user.is_empty() {
                let row = candidate.row;
                worker.output[shard(hash_bytes(row.user))].push(row);
            }
        }
    });
    let (parts, mut output): (Vec<_>, Vec<_>) = workers.into_iter().map(|worker| (worker.duplicates, worker.output)).unzip();
    let parts = dedup_jobs(parts);
    eprintln!("parse/partition events {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let workers = worker_map(threads, parts.len(), || (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(), |loaded, index| {
        let (packed, fallback) = std::mem::take(&mut *parts[index].lock().unwrap());
        select_events(packed, loaded);
        select_events(fallback, loaded);
    });
    eprintln!("dedup/reroute events   {:.3}s", stage.elapsed().as_secs_f64());
    output.extend(workers);
    transpose(output)
}

fn select_events<'a, K: Hash + Eq + Copy>(parts: Vec<Vec<(K, EarlyCopy<'a>)>>, loaded: &mut [Vec<EventRow<'a>>]) {
    let count = parts.iter().map(Vec::len).sum::<usize>();
    let mut winners = Map::with_capacity_and_hasher(count, Default::default());
    for (key, candidate) in parts.iter().flatten() {
        match winners.entry(*key) {
            Entry::Vacant(entry) => { entry.insert(candidate); }
            Entry::Occupied(mut entry) if candidate.ingest < entry.get().ingest => { entry.insert(candidate); }
            Entry::Occupied(_) => {}
        }
    }
    for winner in winners.into_values() {
        let row = winner.row;
        if !row.user.is_empty() {
            loaded[shard(hash_bytes(row.user))].push(row);
        }
    }
}

fn prepare_click<'a>(
    line: &'a [u8], identities: &Identities<'a>, catalog: &Catalog<'a>, window: Window,
) -> Option<(&'a [u8], ClickRow<'a>)> {
    let [id, user, campaign, at] = fields(line)?;
    if id.is_empty() || user.is_empty() { return None; }
    let campaign = *catalog.campaigns.get(campaign)?;
    let at = timestamp(at).filter(|&at| at >= window.start - WEEK && at < window.end)?;
    let user = identities.name(user);
    Some((id, ClickRow { user, click: Click { at, id, campaign } }))
}

fn load_clicks<'a>(
    table: &'a Table, identities: &Identities<'a>, catalog: &Catalog<'a>,
    window: Window, threads: usize,
) -> Vec<Vec<Vec<ClickRow<'a>>>> {
    let filter = DuplicateFilter::build(table, b'k', threads);
    let workers = table.scan(threads, Candidates::new, |worker, line| {
        if let Some((id, row)) = prepare_click(line, identities, catalog, window) {
            let key = ExactKey::new(id, b'k');
            let hash = key.fingerprint();
            let bucket = shard(hash);
            if filter.maybe_repeated(bucket, hash) {
                worker.duplicates.push(key, bucket, row);
            } else {
                worker.output[shard(hash_bytes(row.user))].push(row);
            }
        }
    });
    let (parts, mut output): (Vec<_>, Vec<_>) = workers.into_iter().map(|worker| (worker.duplicates, worker.output)).unzip();
    let parts = dedup_jobs(parts);
    let workers = worker_map(threads, parts.len(), || (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(), |loaded, index| {
        let (packed, fallback) = std::mem::take(&mut *parts[index].lock().unwrap());
        select_clicks(packed, loaded);
        select_clicks(fallback, loaded);
    });
    output.extend(workers);
    transpose(output)
}

fn select_clicks<'a, K: Hash + Eq>(parts: Vec<Vec<(K, ClickRow<'a>)>>, loaded: &mut [Vec<ClickRow<'a>>]) {
    let count = parts.iter().map(Vec::len).sum::<usize>();
    let mut seen = Map::with_capacity_and_hasher(count, Default::default());
    // Duplicate click copies are identical. Filtering before this set is
    // equivalent to filtering the surviving copy.
    for (key, row) in parts.into_iter().flatten() {
        if seen.insert(key, ()).is_none() {
            loaded[shard(hash_bytes(row.user))].push(row);
        }
    }
}

#[derive(Default)]
struct Person<'a> { events: Vec<Event>, clicks: Vec<Click<'a>> }

#[derive(Clone, Copy, Default)]
struct CampaignDay { sessions: u64, purchasing: u64, revenue: u64, assisted: u64 }

impl CampaignDay {
    fn add(&mut self, value: Self) {
        self.sessions += value.sessions;
        self.purchasing += value.purchasing;
        self.revenue += value.revenue;
        self.assisted += value.assisted;
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
    users: Vec<u8>,
    totals: Map<(u32, i32), CampaignDay>,
    anchors: Vec<i64>,
    windows: Vec<Option<(i64, i64)>>,
    seen: Vec<usize>,
    days: Map<i32, String>,
}

impl Work {
    fn new(campaigns: usize) -> Self {
        Self { users: vec![], totals: Map::default(), anchors: vec![], windows: vec![None; campaigns], seen: vec![],
            days: Map::default() }
    }

    fn process(&mut self, name: &[u8], mut person: Person<'_>, catalog: &Catalog<'_>) {
        person.events.sort_unstable_by_key(|event| event.at);
        person.clicks.sort_unstable_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(b.id)));
        self.anchors.clear();
        let mut day_totals = UserDay::default();
        let mut cursor = 0;
        let mut best: Option<Click<'_>> = None;
        let mut start = 0;
        let mut day = day_of(person.events[0].at);
        while start < person.events.len() {
            let at = person.events[start].at;
            let session_day = day_of(at);
            if day != session_day {
                self.write_user_day(name, day, &day_totals, catalog);
                day_totals.sessions = 0;
                day_totals.events = 0;
                day_totals.revenue = 0;
                day_totals.categories.clear();
                day = session_day;
            }
            let mut end = start + 1;
            while end < person.events.len() && person.events[end].at - person.events[end - 1].at <= SESSION_GAP {
                end += 1;
            }
            let mut revenue = 0;
            for event in &person.events[start..end] {
                if event.price != 0 {
                    let price = u64::from(event.price);
                    revenue += price;
                    day_totals.purchase(event.category, price);
                }
            }
            day_totals.sessions += 1;
            day_totals.events += (end - start) as u64;
            self.anchors.push(at);
            while let Some(&click) = person.clicks.get(cursor).filter(|click| click.at <= at) {
                if best.is_none_or(|before| click.at > before.at) {
                    best = Some(click);
                }
                cursor += 1;
            }
            let campaign = best.filter(|click| click.at >= at - WEEK).map_or(catalog.organic, |click| click.campaign);
            let total = self.totals.entry((campaign, day)).or_default();
            total.sessions += 1;
            total.purchasing += u64::from(revenue != 0);
            total.revenue += revenue;
            start = end;
        }
        self.write_user_day(name, day, &day_totals, catalog);
        self.assists(&person.clicks[..cursor]);
    }

    fn write_user_day(&mut self, name: &[u8], day: i32, totals: &UserDay, catalog: &Catalog<'_>) {
        use std::io::Write;
        let top = totals.categories.iter().min_by_key(|&&(category, cents)| (std::cmp::Reverse(cents), category))
            .map(|&(id, _)| catalog.categories[id as usize]);
        let day = self.days.entry(day).or_insert_with(|| input::display_day(day));
        output::quote(&mut self.users, name).unwrap();
        write!(self.users, ",{day},{},{},{},", totals.sessions, totals.events, totals.revenue).unwrap();
        if let Some(top) = top { output::quote(&mut self.users, top).unwrap(); }
        self.users.push(b'\n');
    }

    fn assists(&mut self, clicks: &[Click<'_>]) {
        for click in clicks {
            let campaign = click.campaign as usize;
            let window = (click.at, click.at + WEEK);
            match self.windows[campaign] {
                Some((first, last)) if click.at <= last => self.windows[campaign] = Some((first, window.1)),
                pending => {
                    if let Some((first, last)) = pending {
                        count_assists(&self.anchors, &mut self.totals, click.campaign, first, last);
                    } else {
                        self.seen.push(campaign);
                    }
                    self.windows[campaign] = Some(window);
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
    anchors: &[i64], totals: &mut Map<(u32, i32), CampaignDay>, campaign: u32, first: i64, last: i64,
) {
    let mut start = anchors.partition_point(|&at| at < first);
    let end = start + anchors[start..].partition_point(|&at| at <= last);
    while start < end {
        let day = day_of(anchors[start]);
        let day_end = (i64::from(day) + 1) * DAY;
        let matching = anchors[start..end].partition_point(|&at| at < day_end);
        totals.entry((campaign, day)).or_default().assisted += matching as u64;
        start += matching;
    }
}

fn solve(data: &Path, scratch: &Path, threads: usize) -> io::Result<()> {
    let stage = Instant::now();
    let window = Window::read(data)?;
    let links = Table::open(&data.join("links"))?;
    let products = Table::open(&data.join("products.csv"))?;
    let campaigns = Table::open(&data.join("campaigns.csv"))?;
    let events = Table::open(&data.join("events"))?;
    let clicks = Table::open(&data.join("clicks"))?;
    let identities = Identities::read(&links);
    let catalog = Catalog::read(&products, &campaigns);
    eprintln!("load linked identities {:.3}s", stage.elapsed().as_secs_f64());
    let events = load_events(&events, &identities, &catalog, window, threads);
    let stage = Instant::now();
    let clicks = load_clicks(&clicks, &identities, &catalog, window, threads);
    eprintln!("partition clicks       {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let counts: Vec<_> = events.iter().map(|parts| parts.iter().map(Vec::len).sum::<usize>()).collect();
    let mut jobs: Vec<_> = (0..SHARDS).collect();
    jobs.sort_unstable_by_key(|&index| std::cmp::Reverse(counts[index]));
    let workers = worker_map(threads, jobs.len(), || Work::new(catalog.campaign_rows.len()), |work, job| {
        let bucket = jobs[job];
        let mut people = Map::<&[u8], Person<'_>>::default();
        for row in events[bucket].iter().flatten() {
            people.entry(row.user).or_default().events.push(row.event);
        }
        for row in clicks[bucket].iter().flatten() {
            if let Some(person) = people.get_mut(row.user) {
                person.clicks.push(row.click);
            }
        }
        for (name, person) in people {
            work.process(name, person, &catalog);
        }
    });
    eprintln!("sessions/attribution   {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    output::write(scratch, workers, &catalog)?;
    eprintln!("write intermediate     {:.3}s", stage.elapsed().as_secs_f64());
    Ok(())
}

fn main() -> io::Result<()> {
    let args = env::args_os().collect::<Vec<_>>();
    if args.len() != 3 { return Err(io::Error::other("usage: shopping-sessions DATA_DIR SCRATCH_OUTPUT")); }
    let threads = env::var("NCPU").ok().and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count != 0).unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    solve(Path::new(&args[1]), Path::new(&args[2]), threads)
}
