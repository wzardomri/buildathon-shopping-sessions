mod input;
mod output;

use input::{DAY, Map, Row, SHARDS, Table, WEEK, fields, hash_bytes, shard, timestamp, transpose, unsigned, worker_map};
use std::collections::hash_map::Entry;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
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

struct Identities<'a> {
    index: Map<&'a [u8], u32>,
    names: Vec<&'a [u8]>,
}

struct Components<'a> {
    index: Map<&'a [u8], u32>,
    parent: Vec<u32>,
    size: Vec<u32>,
    names: Vec<&'a [u8]>,
}

impl<'a> Components<'a> {
    fn intern(&mut self, name: &'a [u8]) -> u32 {
        *self.index.entry(name).or_insert_with(|| {
            let id = self.parent.len() as u32;
            self.parent.push(id);
            self.size.push(1);
            self.names.push(name);
            id
        })
    }

    fn root(&mut self, mut id: u32) -> u32 {
        while self.parent[id as usize] != id {
            let parent = self.parent[id as usize] as usize;
            self.parent[id as usize] = self.parent[parent];
            id = self.parent[id as usize];
        }
        id
    }

    fn link(&mut self, a: &'a [u8], b: &'a [u8]) {
        let a = self.intern(a);
        let b = self.intern(b);
        let (mut a, mut b) = (self.root(a) as usize, self.root(b) as usize);
        if a == b { return; }
        if self.size[a] < self.size[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parent[b] = a as u32;
        self.size[a] += self.size[b];
        self.names[a] = self.names[a].min(self.names[b]);
    }
}

impl<'a> Identities<'a> {
    fn read(table: &'a Table) -> Self {
        let mut groups = Components {
            index: Map::default(), parent: vec![], size: vec![], names: vec![],
        };
        for row in table.rows() {
            if let Some([a, b]) = fields(row) {
                if !a.is_empty() && !b.is_empty() {
                    groups.link(a, b);
                }
            }
        }
        let names = (0..groups.parent.len()).map(|id| {
            let root = groups.root(id as u32);
            groups.names[root as usize]
        }).collect();
        Self { index: groups.index, names }
    }

    fn name(&self, key: &'a [u8]) -> &'a [u8] {
        self.index.get(key).map_or(key, |&id| self.names[id as usize])
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

struct EventRow<'a> { user: &'a [u8], event: Event }

#[derive(Clone, Copy)]
struct Click<'a> {
    at: i64,
    id: &'a [u8],
    campaign: u32,
}

struct ClickRow<'a> { user: &'a [u8], click: Click<'a> }

// For F1 (fixed-width UTC seconds), byte order is chronological order.
struct Winner<'a> { row: Row, ingest: &'a [u8] }

fn load_events<'a>(
    table: &'a Table, identities: &Identities<'a>, catalog: &Catalog<'a>,
    window: Window, threads: usize,
) -> Vec<Vec<Vec<EventRow<'a>>>> {
    let stage = Instant::now();
    let parts = table.partition(threads);
    eprintln!("partition event IDs    {:.3}s", stage.elapsed().as_secs_f64());
    let stage = Instant::now();
    let workers = worker_map(threads, parts.len(), || (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(), |loaded, index| {
        let parts = &parts[index];
        let count = parts.iter().map(Vec::len).sum::<usize>();
        let mut winners = Map::with_capacity_and_hasher(count, Default::default());
        for &row in parts.iter().flatten() {
            if let Some([id, _, _, ingest, _, _, _]) = fields(table.line(row)) {
                match winners.entry(id) {
                    Entry::Vacant(entry) => { entry.insert(Winner { row, ingest }); }
                    Entry::Occupied(mut entry) if ingest < entry.get().ingest => {
                        entry.insert(Winner { row, ingest });
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        // Visit winners in file order for reuse of the input pages. On generated data
        // IDs are random, so iteration through the hash table would jump across files.
        let mut winners: Vec<_> = winners.into_values().map(|winner| winner.row).collect();
        winners.sort_unstable_by_key(|row| row.location);
        for row in winners {
            let Some([_, user, at, ingest, kind, product, price]) = fields(table.line(row)) else { continue };
            if user.is_empty() || product.is_empty() || !matches!(kind, b"purchase" | b"cart" | b"view") {
                continue;
            }
            let Some(at) = timestamp(at).filter(|&at| at >= window.start && at < window.end) else { continue };
            let Some(ingest) = timestamp(ingest) else { continue };
            if ingest - at > DAY { continue; }
            let (price, category) = if kind == b"purchase" {
                let Some(price) = unsigned(price).filter(|&value| (1..=10_000_000).contains(&value)) else { continue };
                let category = catalog.products.get(product).copied().unwrap_or(catalog.unknown);
                (price as u32, category)
            } else {
                (0, 0)
            };
            let user = identities.name(user);
            loaded[shard(hash_bytes(user))].push(EventRow { user, event: Event { at, price, category } });
        }
    });
    eprintln!("dedup and validate     {:.3}s", stage.elapsed().as_secs_f64());
    transpose(workers)
}

fn load_clicks<'a>(
    table: &'a Table, identities: &Identities<'a>, catalog: &Catalog<'a>,
    window: Window, threads: usize,
) -> Vec<Vec<Vec<ClickRow<'a>>>> {
    let parts = table.partition(threads);
    let workers = worker_map(threads, parts.len(), || (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(), |loaded, index| {
        let parts = &parts[index];
        let count = parts.iter().map(Vec::len).sum::<usize>();
        let mut seen = Map::with_capacity_and_hasher(count, Default::default());
        for &row in parts.iter().flatten() {
            let Some([id, user, campaign, at]) = fields(table.line(row)) else { continue };
            if seen.insert(id, ()).is_some() || user.is_empty() { continue; }
            let Some(&campaign) = catalog.campaigns.get(campaign) else { continue };
            let Some(at) = timestamp(at).filter(|&at| at >= window.start - WEEK && at < window.end) else { continue };
            let user = identities.name(user);
            loaded[shard(hash_bytes(user))].push(ClickRow { user, click: Click { at, id, campaign } });
        }
    });
    transpose(workers)
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
