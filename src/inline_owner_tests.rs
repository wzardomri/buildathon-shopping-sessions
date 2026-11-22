use super::{Catalog, Event, FusedEventBatch, IngestCalendar, Table, Window, resolve_event_batches, timestamp, worker_map};
use crate::group::{RawPeople, Slab, canonical_people};
use crate::identities::{Identities, Name};
use crate::inline_owner::InlineOwner;
use std::fs;
use std::path::PathBuf;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};

const INLINE: &[u8; 33] = b"u0123456789abcdeffedcba9876543210";
const FALLBACK: &[u8; 33] = b"u0123456789abcdeffedcba987654321F";
const SECOND_INLINE: &[u8; 33] = b"u123456789abcdef10fedcba987654321";

struct Case(PathBuf);

impl Case {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!("shopping-inline-owner-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn write(&self, path: &str, data: impl AsRef<[u8]>) {
        fs::write(self.0.join(path), data).unwrap();
    }
}

impl Drop for Case {
    fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
}

#[test]
fn earliest_invalid_and_cross_worker_survivors_keep_exact_raw_locations() {
    let case = Case::new();
    case.write("products.csv", "product_id,category\np,tools\n");
    case.write("campaigns.csv", "campaign_id,channel\nc_1,email\n");
    fs::create_dir(case.0.join("events")).unwrap();
    let inline = std::str::from_utf8(INLINE).unwrap();
    let fallback = std::str::from_utf8(FALLBACK).unwrap();
    let header = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n";
    case.write("events/a.csv", format!(
        "{header}\
         e1,{inline},bad,2026-01-01T00:00:01Z,view,p,0\n\
         opaque event 2,{fallback},2026-01-01T00:00:01Z,2026-01-01T00:00:02Z,purchase,p,+1\n\
         e3,{inline},2026-01-01T00:00:01Z,2026-01-01T00:00:09Z,purchase,p,30\n\
         opaque event 4,{fallback},2026-01-01T00:00:01Z,2026-01-01T00:00:09Z,purchase,p,40\n\
         e5,{inline},2026-01-01T00:00:01Z,2026-01-01T00:00:02Z,purchase,p,50\n"
    ));
    case.write("events/b.csv", format!(
        "{header}\
         e1,{fallback},2026-01-01T00:00:01Z,2026-01-01T00:00:06Z,purchase,p,10\n\
         opaque event 2,{inline},2026-01-01T00:00:01Z,2026-01-01T00:00:06Z,purchase,p,20\n\
         e3,{fallback},2026-01-01T00:00:01Z,2026-01-01T00:00:03Z,purchase,p,31\n\
         opaque event 4,{inline},2026-01-01T00:00:01Z,2026-01-01T00:00:03Z,purchase,p,41"
    ));
    let products = Table::open(&case.0.join("products.csv")).unwrap();
    let campaigns = Table::open(&case.0.join("campaigns.csv")).unwrap();
    let events = Table::open(&case.0.join("events")).unwrap();
    let catalog = Catalog::read(&products, &campaigns);
    let window = Window { start: timestamp(b"2026-01-01T00:00:00Z").unwrap(),
                          end: timestamp(b"2026-01-03T00:00:00Z").unwrap() };
    let calendar = IngestCalendar::new(window.start, window.end);
    let chunks = events.data_chunks();
    assert_eq!(chunks.len(), 2);
    // Each worker holds a different physical file and storage batch, even
    // on a system that could otherwise finish both tiny jobs in one worker.
    let ready = Barrier::new(2);
    let input = worker_map(2, chunks.len(), FusedEventBatch::<Event>::new, |batch, index| {
        ready.wait();
        batch.scan(chunks[index], &catalog, window, &calendar);
    });
    let batches = resolve_event_batches(input, 2);
    let mut survivors = Vec::new();
    for batch in batches {
        assert!(batch.packed.is_empty());
        for (name, row) in batch.inline.into_iter().flatten() { survivors.push((name.spelling(), row.price)); }
        for (name, row) in batch.full.into_iter().flatten() { survivors.push((name.try_into().unwrap(), row.price)); }
    }
    survivors.sort_unstable();
    assert_eq!(survivors, [(*INLINE, 41), (*INLINE, 50), (*FALLBACK, 31)]);
}

#[test]
fn arena_spelling_rejoins_both_alias_directions_and_click_only_inline_owners() {
    let case = Case::new();
    let inline = std::str::from_utf8(INLINE).unwrap();
    let second = std::str::from_utf8(SECOND_INLINE).unwrap();
    let fallback = std::str::from_utf8(FALLBACK).unwrap();
    case.write("links.csv", format!("user_id_a,user_id_b\nuZ,{fallback}\n{fallback},{inline}\nu0,{second}\n"));
    let table = Table::open(&case.0.join("links.csv")).unwrap();
    let identities = Identities::read(&table, 2);
    let inline_key = InlineOwner::encode(INLINE).unwrap();
    let click_only = InlineOwner::encode(SECOND_INLINE).unwrap();
    let mut decoded = RawPeople::gather(vec![vec![(inline_key, 41u32)]],
        vec![vec![(inline_key, 51u32), (click_only, 71)]]).decode_owners();
    let mut inline_aliased_event = [19u32];
    let mut packed_event = [99u32];
    let mut inline_aliased_clicks = [];
    let mut packed_clicks = [];
    let mut slabs = vec![
        Slab { owner: identities.canonical(Name::from_bytes(b"uZ")),
               events: &mut inline_aliased_event, clicks: &mut inline_aliased_clicks },
        Slab { owner: identities.canonical(Name::from_bytes(b"u0")),
               events: &mut packed_event, clicks: &mut packed_clicks },
    ];
    for (name, events, clicks) in decoded.borrowed_slabs() {
        let owner = identities.canonical(Name::Full(&name[..]));
        slabs.push(Slab { owner, events, clicks });
    }
    let mut visited = 0;
    canonical_people(vec![slabs], |owner, events, clicks| {
        visited += 1;
        if owner == Name::Full(INLINE) {
            events.sort_unstable();
            assert_eq!(&*events, &[19, 41]);
            assert_eq!(&*clicks, &[51]);
        } else {
            assert!(owner == Name::from_bytes(b"u0"));
            assert_eq!(&*events, &[99]);
            assert_eq!(&*clicks, &[71]);
        }
    });
    assert_eq!(visited, 2);
}
