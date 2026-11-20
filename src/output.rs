use crate::input::{Map, display_day, find};
use crate::{CampaignDay, Catalog, Work};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

pub fn quote(writer: &mut impl Write, value: &[u8]) -> io::Result<()> {
    writer.write_all(b"\"")?;
    // Input values cannot contain quotes. Check before using the common fast path
    // so this writer can also be used unchanged by broader differential fixtures.
    let mut remaining = value;
    while let Some(at) = find(remaining, b'"') {
        writer.write_all(&remaining[..at])?;
        writer.write_all(b"\"\"")?;
        remaining = &remaining[at + 1..];
    }
    writer.write_all(remaining)?;
    writer.write_all(b"\"")
}

pub fn write(scratch: &Path, workers: Vec<Work>, catalog: &Catalog<'_>) -> io::Result<()> {
    fs::create_dir_all(scratch)?;
    let mut users = BufWriter::new(File::create(scratch.join("user_daily.csv"))?);
    users.write_all(b"user_id,day,sessions,events,revenue_cents,top_category\n")?;
    let mut totals = Map::<(u32, i32), CampaignDay>::default();
    for worker in workers {
        users.write_all(&worker.users)?;
        for (key, value) in worker.totals {
            totals.entry(key).or_default().add(value);
        }
    }
    users.flush()?;
    let mut campaigns = BufWriter::new(File::create(scratch.join("campaign_daily.csv"))?);
    campaigns.write_all(b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")?;
    let mut days = Map::<i32, String>::default();
    for ((campaign, day), total) in totals {
        let (campaign, channel) = catalog.campaign_rows[campaign as usize];
        let day = days.entry(day).or_insert_with(|| display_day(day));
        quote(&mut campaigns, campaign)?;
        campaigns.write_all(b",")?;
        quote(&mut campaigns, channel)?;
        writeln!(campaigns, ",{day},{},{},{},{}", total.sessions, total.purchasing, total.revenue, total.assisted)?;
    }
    campaigns.flush()
}
