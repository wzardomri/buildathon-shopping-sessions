use super::*;
use parquet::basic::{Compression, ConvertedType, Type};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use std::collections::{BTreeMap, BTreeSet};

fn catalog() -> Catalog<'static> {
    Catalog {
        products: Map::default(),
        categories: vec![b"books", b"garden", b"unknown"],
        unknown: 2,
        campaigns: Map::default(),
        campaign_rows: vec![(b"c_alpha", b"email"), (b"c_beta", b"ads"), (b"organic", b"organic")],
        organic: 2,
    }
}

fn rows(path: &Path) -> Vec<Vec<Field>> {
    let reader = SerializedFileReader::new(fs::File::open(path).unwrap()).unwrap();
    reader.get_row_iter(None).unwrap().map(|row| {
        row.unwrap().into_columns().into_iter().map(|(_, field)| field).collect()
    }).collect()
}

fn scratch(label: &str) -> std::path::PathBuf {
    env::temp_dir().join(format!("shopping-sweep-{label}-{}", std::process::id()))
}

type ExpectedCampaigns = BTreeMap<(u32, i32), [u64; 4]>;

fn oracle(
    name: &str, events: &[Event], clicks: &[Click<'_>], campaigns: &mut ExpectedCampaigns,
) -> Vec<Vec<Field>> {
    let mut user_days = BTreeMap::<i32, ([u64; 3], [u64; 3])>::new();
    let mut tail = events;
    while let Some(first) = tail.first() {
        let size = tail.windows(2).position(|pair| pair[1].at - pair[0].at > SESSION_GAP)
            .map_or(tail.len(), |i| i + 1);
        let day = first.at.div_euclid(86_400_000) as i32;
        let (totals, categories) = user_days.entry(day).or_default();
        let revenue: u64 = tail[..size].iter().map(|event| u64::from(event.price)).sum();
        totals[0] += 1;
        totals[1] += size as u64;
        totals[2] += revenue;
        for event in &tail[..size] {
            categories[event.category as usize] += u64::from(event.price);
        }
        let matching = clicks.iter().filter(|click| (0..=WEEK).contains(&(first.at - click.at)));
        let mut present = BTreeSet::new();
        let mut best: Option<&Click<'_>> = None;
        for click in matching {
            present.insert(click.campaign);
            if best.is_none_or(|b| (std::cmp::Reverse(click.at), click.id) < (std::cmp::Reverse(b.at), b.id)) {
                best = Some(click);
            }
        }
        let credited = campaigns.entry((best.map_or(2, |click| click.campaign), day)).or_default();
        credited[0] += 1;
        credited[1] += u64::from(revenue != 0);
        credited[2] += revenue;
        for campaign in present {
            campaigns.entry((campaign, day)).or_default()[3] += 1;
        }
        tail = &tail[size..];
    }
    user_days.into_iter().map(|(day, ([sessions, events, revenue], categories))| {
        let top = categories.into_iter().enumerate().filter(|&(_, amount)| amount != 0)
            .min_by_key(|&(category, amount)| (std::cmp::Reverse(amount), category))
            .map_or(Field::Null, |(category, _)| Field::Str(["books", "garden", "unknown"][category].into()));
        vec![Field::Str(name.into()), Field::Date(day), Field::Long(sessions as i64), Field::Long(events as i64),
             Field::Long(revenue as i64), top]
    }).collect()
}

fn sweep<E: EventRecord, C: ClickRecord<'static>>(label: &str, window: Window) {
    let catalog = catalog();
    let origin = window.start - WEEK;
    let mut work = Work::new(window, catalog.campaign_rows.len());
    let mut expected_users = vec![];
    let mut expected_campaigns = BTreeMap::new();
    let mut random = 1234567_u64;
    let mut next = || {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        usize::from((random >> 48) as u16)
    };
    let ids: [&'static [u8]; 8] = [b"id9", b"id10", b"same-prefix0", b"same-prefix1", b"z",
        "é".as_bytes(), b"a\0id", b"zz long id with punctuation!"];
    for user in 0..250 {
        let name = format!("user-{user:04}");
        let start = window.start + (next() % 3) as i64 * DAY;
        let mut events = Vec::new();
        let mut at = start;
        for _ in 0..1 + next() % 42 {
            if at >= window.end { break; }
            events.push(Event { at, price: (next() % 5) as u32, category: (next() % 3) as u32 });
            at += [0, 1, SESSION_GAP, SESSION_GAP + 1, DAY - 1, DAY + 1, WEEK + 1][next() % 7];
        }
        let mut clicks = Vec::new();
        if user % 5 != 0 {
            for _ in 0..next() % 35 {
                let anchor = events[next() % events.len()].at;
                let at = anchor + [-WEEK - 1, -WEEK, -WEEK + 1, -1, 0, 0, 1][next() % 7];
                if origin <= at && at < window.end {
                    clicks.push(Click { at, id: ids[next() % ids.len()], campaign: (next() % 2) as u32 });
                }
            }
            // Equal time and ID must resolve to equal campaigns in the oracle.
            for click in &mut clicks {
                click.campaign = u32::from(click.id[0] % 2);
            }
            if let Some(click) = clicks.first().copied() { clicks.push(click); }
        }
        expected_users.extend(oracle(&name, &events, &clicks, &mut expected_campaigns));
        let mut packed_events = events.into_iter().rev().map(|event| E::encode(event, window.start)).collect::<Vec<_>>();
        let mut packed_clicks = clicks.into_iter().map(|click| C::encode(click, origin)).collect::<Vec<_>>();
        work.process(name.as_bytes(), &mut packed_events, &mut packed_clicks, &catalog, window.start, origin);
        assert!(work.seen.is_empty());
        assert!(work.assist_ends.iter().all(|&end| end == 0));
    }
    let dir = scratch(label);
    output::write(&dir, vec![work], &catalog).unwrap();
    assert_eq!(rows(&dir.join("user_daily/part-0.parquet")), expected_users);
    let mut actual = rows(&dir.join("campaign_daily/part-0.parquet"));
    let key = |row: &Vec<Field>| {
        let (Field::Str(id), Field::Date(day)) = (&row[0], &row[2]) else { panic!("wrong campaign type") };
        (id.clone(), *day)
    };
    actual.sort_by_key(key);
    let expected = expected_campaigns.into_iter().map(|((campaign, day), [sessions, purchasing, revenue, assists])| {
        let (id, channel) = catalog.campaign_rows[campaign as usize];
        let string = |bytes: &[u8]| Field::Str(String::from_utf8(bytes.to_vec()).unwrap());
        vec![string(id), string(channel), Field::Date(day), Field::Long(sessions as i64),
             Field::Long(purchasing as i64), Field::Long(revenue as i64), Field::Long(assists as i64)]
    }).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn compact_sweep_matches_per_anchor_oracle() {
    sweep::<PackedEvent, PackedClick<'static>>("compact", Window { start: -2 * DAY, end: 12 * DAY });
}

#[test]
fn wide_sweep_matches_per_anchor_oracle() {
    sweep::<Event, Click<'static>>("wide", Window { start: -30 * DAY, end: 55 * DAY });
}

#[test]
fn native_parquet_round_trips_required_plain_binary_across_pages() {
    let dir = scratch("pages");
    let catalog = catalog();
    let mut work = Work::new(Window { start: -DAY, end: DAY }, catalog.campaign_rows.len());
    let names = [String::new(), "plain".into(), "é~\0".repeat(45_000), "user".repeat(300_000), "last".into()];
    for (i, name) in names.iter().enumerate() {
        work.users.push(name.as_bytes(), -1, (i + 1) as u64, 3, 29, (i % 2 == 0).then_some(1));
    }
    output::write(&dir, vec![work], &catalog).unwrap();
    let file = dir.join("user_daily/part-0.parquet");
    let reader = SerializedFileReader::new(fs::File::open(&file).unwrap()).unwrap();
    let row_group = reader.metadata().row_group(0);
    let column = row_group.column(0);
    assert_eq!(column.compression(), Compression::SNAPPY);
    assert_eq!(column.column_descr().physical_type(), Type::BYTE_ARRAY);
    assert_eq!(column.column_descr().converted_type(), ConvertedType::UTF8);
    assert_eq!(column.column_descr().max_def_level(), 0);
    assert_eq!(column.column_descr().max_rep_level(), 0);
    assert_eq!(row_group.column(1).column_descr().converted_type(), ConvertedType::DATE);
    let actual = rows(&file);
    let expected = names.into_iter().enumerate().map(|(i, name)| {
        vec![Field::Str(name), Field::Date(-1), Field::Long(i as i64 + 1), Field::Long(3), Field::Long(29),
             if i % 2 == 0 { Field::Str("garden".into()) } else { Field::Null }]
    }).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(rows(&dir.join("campaign_daily/part-0.parquet")).is_empty());
    fs::remove_dir_all(dir).unwrap();
}
