use bytes::Bytes;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;
use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Arc;

use crate::input::Map;
use crate::{CampaignDay, Catalog, Work};

const USERS: &str = "message user_daily {
    REQUIRED BINARY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BINARY top_category (UTF8);
}";
const CAMPAIGNS: &str = "message campaign_daily {
    REQUIRED BINARY campaign_id (UTF8);
    REQUIRED BINARY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

pub struct Users {
    name_offsets: Vec<usize>,
    name_values: Vec<u8>,
    dates: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    categories: Vec<u32>,
}

impl Users {
    pub fn new() -> Self {
        Self {
            name_offsets: vec![0], name_values: vec![], dates: vec![], sessions: vec![],
            events: vec![], revenue: vec![], categories: vec![],
        }
    }

    pub fn push(&mut self, name: &[u8], day: i32, sessions: u64, events: u64,
                revenue: u64, category: Option<u32>) {
        self.name_values.extend_from_slice(name);
        self.name_offsets.push(self.name_values.len());
        self.dates.push(day);
        self.sessions.push(sessions as i64);
        self.events.push(events as i64);
        self.revenue.push(revenue as i64);
        self.categories.push(category.unwrap_or(u32::MAX));
    }

    fn write(self, path: &Path, categories: &[ByteArray], props: Arc<WriterProperties>) -> io::Result<()> {
        let schema = Arc::new(parse_message_type(USERS).map_err(io::Error::other)?);
        let file = BufWriter::new(File::create(path)?);
        let mut file = SerializedFileWriter::new(file, schema, props).map_err(io::Error::other)?;
        if self.dates.is_empty() {
            file.close().map_err(io::Error::other)?;
            return Ok(());
        }
        let bytes = Bytes::from(self.name_values);
        let names = self.name_offsets.windows(2).map(|o| ByteArray::from(bytes.slice(o[0]..o[1])))
            .collect::<Vec<_>>();
        let values = self.categories.iter().filter_map(|&code| categories.get(code as usize).cloned())
            .collect::<Vec<_>>();
        let definition_levels = self.categories.iter().map(|&code| i16::from(code != u32::MAX))
            .collect::<Vec<_>>();
        let mut group = file.next_row_group().map_err(io::Error::other)?;
        column::<ByteArrayType>(&mut group, &names, None)?;
        column::<Int32Type>(&mut group, &self.dates, None)?;
        column::<Int64Type>(&mut group, &self.sessions, None)?;
        column::<Int64Type>(&mut group, &self.events, None)?;
        column::<Int64Type>(&mut group, &self.revenue, None)?;
        column::<ByteArrayType>(&mut group, &values, Some(&definition_levels))?;
        group.close().map_err(io::Error::other)?;
        file.close().map_err(io::Error::other)?;
        Ok(())
    }
}

fn column<T: DataType>(group: &mut SerializedRowGroupWriter<'_, BufWriter<File>>,
                       values: &[T::T], definition_levels: Option<&[i16]>) -> io::Result<()> {
    let mut column = group.next_column().map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Parquet column missing"))?;
    column.typed::<T>().write_batch(values, definition_levels, None).map_err(io::Error::other)?;
    column.close().map_err(io::Error::other)?;
    Ok(())
}

fn campaigns(path: &Path, totals: Map<(u32, i32), CampaignDay>, catalog: &Catalog<'_>,
             props: Arc<WriterProperties>) -> io::Result<()> {
    let schema = Arc::new(parse_message_type(CAMPAIGNS).map_err(io::Error::other)?);
    let file = BufWriter::new(File::create(path)?);
    let mut file = SerializedFileWriter::new(file, schema, props).map_err(io::Error::other)?;
    if totals.is_empty() {
        file.close().map_err(io::Error::other)?;
        return Ok(());
    }
    let mut ids = Vec::with_capacity(totals.len());
    let mut channels = Vec::with_capacity(totals.len());
    let mut dates = Vec::with_capacity(totals.len());
    let mut sessions = Vec::with_capacity(totals.len());
    let mut purchases = Vec::with_capacity(totals.len());
    let mut revenues = Vec::with_capacity(totals.len());
    let mut assists = Vec::with_capacity(totals.len());
    for ((campaign, day), total) in totals {
        let (id, channel) = catalog.campaign_rows[campaign as usize];
        ids.push(ByteArray::from(Bytes::copy_from_slice(id)));
        channels.push(ByteArray::from(Bytes::copy_from_slice(channel)));
        dates.push(day);
        sessions.push(total.sessions as i64);
        purchases.push(total.purchasing as i64);
        revenues.push(total.revenue as i64);
        assists.push(total.assisted as i64);
    }
    let mut group = file.next_row_group().map_err(io::Error::other)?;
    column::<ByteArrayType>(&mut group, &ids, None)?;
    column::<ByteArrayType>(&mut group, &channels, None)?;
    column::<Int32Type>(&mut group, &dates, None)?;
    column::<Int64Type>(&mut group, &sessions, None)?;
    column::<Int64Type>(&mut group, &purchases, None)?;
    column::<Int64Type>(&mut group, &revenues, None)?;
    column::<Int64Type>(&mut group, &assists, None)?;
    group.close().map_err(io::Error::other)?;
    file.close().map_err(io::Error::other)?;
    Ok(())
}

pub fn write(output: &Path, workers: Vec<Work>, catalog: &Catalog<'_>) -> io::Result<()> {
    let users = output.join("user_daily");
    let campaign_path = output.join("campaign_daily");
    fs::create_dir_all(&users)?;
    fs::create_dir_all(&campaign_path)?;
    let props = Arc::new(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(false)
        .build());
    let categories = catalog.categories.iter().map(|name| ByteArray::from(Bytes::copy_from_slice(name)))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let mut writers = Vec::new();
        let mut totals = Map::<(u32, i32), CampaignDay>::default();
        for (index, worker) in workers.into_iter().enumerate() {
            if index == 0 || !worker.users.dates.is_empty() {
                let path = users.join(format!("part-{index}.parquet"));
                let props = props.clone();
                let categories = &categories;
                writers.push(scope.spawn(move || worker.users.write(&path, categories, props)));
            }
            worker.totals.into_rows(|key, value| {
                totals.entry(key).or_default().add(value);
            });
        }
        campaigns(&campaign_path.join("part-0.parquet"), totals, catalog, props)?;
        for writer in writers {
            writer.join().map_err(|_| io::Error::other("Parquet writer failed"))??;
        }
        Ok(())
    })
}
