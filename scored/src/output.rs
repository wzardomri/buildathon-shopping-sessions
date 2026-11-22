use bytes::Bytes;
use parquet::basic::{Compression, Encoding, Type};
use parquet::column::page::{CompressedPage, Page, PageWriter};
use parquet::column::writer::ColumnCloseResult;
use parquet::data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type, SliceAsBytes};
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter, SerializedRowGroupWriter, TrackedWrite};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnDescPtr;
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
    names: NamePages,
    dates: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    categories: Vec<u32>,
}

impl Users {
    pub fn new() -> Self {
        Self {
            names: NamePages::default(), dates: vec![], sessions: vec![],
            events: vec![], revenue: vec![], categories: vec![],
        }
    }

    pub fn push(&mut self, name: &[u8], day: i32, sessions: u64, events: u64,
                revenue: u64, category: Option<u32>) {
        self.names.push(name);
        self.dates.push(day);
        self.sessions.push(sessions as i64);
        self.events.push(events as i64);
        self.revenue.push(revenue as i64);
        self.categories.push(category.unwrap_or(u32::MAX));
    }

    fn write(self, path: &Path, categories: &[ByteArray], props: Arc<WriterProperties>) -> io::Result<()> {
        let schema = Arc::new(parse_message_type(USERS).map_err(io::Error::other)?);
        let file = BufWriter::new(File::create(path)?);
        let mut file = SerializedFileWriter::new(file, schema, props.clone()).map_err(io::Error::other)?;
        if self.dates.is_empty() {
            file.close().map_err(io::Error::other)?;
            return Ok(());
        }
        let [name_column, date_column, session_column, event_column, revenue_column] =
            std::array::from_fn(|index| file.schema_descr().column(index));
        let mut group = file.next_row_group().map_err(io::Error::other)?;
        {
            let (names, metadata) = self.names.encode(name_column, self.dates.len())?;
            group.append_column(&names, metadata).map_err(io::Error::other)?;
        }
        required_numeric::<Int32Type>(&mut group, date_column, &props, self.dates)?;
        required_numeric::<Int64Type>(&mut group, session_column, &props, self.sessions)?;
        required_numeric::<Int64Type>(&mut group, event_column, &props, self.events)?;
        required_numeric::<Int64Type>(&mut group, revenue_column, &props, self.revenue)?;
        let values = self.categories.iter().filter_map(|&code| categories.get(code as usize).cloned())
            .collect::<Vec<_>>();
        let definition_levels = self.categories.iter().map(|&code| i16::from(code != u32::MAX))
            .collect::<Vec<_>>();
        drop(self.categories);
        column::<ByteArrayType>(&mut group, &values, Some(&definition_levels))?;
        group.close().map_err(io::Error::other)?;
        file.close().map_err(io::Error::other)?;
        Ok(())
    }
}

const NAME_PAGE_BYTES: usize = 1 << 20;

// user_id is a required binary column with no levels, dictionary, or
// statistics. Assemble its PLAIN values directly at reduction, without
// materializing a reference-counted ByteArray for every user-day row.
#[derive(Default)]
struct NamePages {
    values: Vec<u8>,
    sealed: Vec<(usize, u32)>,
    start: usize,
    count: u32,
}

impl NamePages {
    fn push(&mut self, name: &[u8]) {
        let remaining = NAME_PAGE_BYTES.saturating_sub(self.values.len() - self.start);
        if self.count != 0 && name.len() + 4 > remaining {
            self.sealed.push((self.values.len(), self.count));
            self.start = self.values.len();
            self.count = 0;
        }
        self.values.extend_from_slice(&(name.len() as u32).to_le_bytes());
        self.values.extend_from_slice(name);
        self.count += 1;
    }

    fn encode(self, descriptor: ColumnDescPtr, rows: usize) -> io::Result<(Bytes, ColumnCloseResult)> {
        debug_assert_ne!(self.count, 0);
        let values = &self.values;
        let mut start = 0;
        let end = (values.len(), self.count);
        let pages = self.sealed.into_iter().chain([end]).map(move |(end, count)| {
            let source = &values[start..end];
            start = end;
            (source, count)
        });
        encode_plain(descriptor, rows, pages, Some((values.len() - 4 * rows) as i64))
    }
}

fn encode_plain<'a>(
    descriptor: ColumnDescPtr,
    rows: usize,
    source_pages: impl IntoIterator<Item = (&'a [u8], u32)>,
    binary_bytes: Option<i64>,
) -> io::Result<(Bytes, ColumnCloseResult)> {
    debug_assert_eq!(descriptor.max_def_level(), 0);
    debug_assert_eq!(descriptor.max_rep_level(), 0);
    let mut sink = TrackedWrite::new(Vec::new());
    let mut pages = SerializedPageWriter::new(&mut sink);
    let mut compressor = snap::raw::Encoder::new();
    let mut total_uncompressed = 0;
    for (source, count) in source_pages {
        // Parquet stores individual page sizes in signed 32-bit fields.
        if source.len() > i32::MAX as usize {
            return Err(io::Error::other("Parquet data page is too large"));
        }
        let buf = compressor.compress_vec(source).map_err(io::Error::other)?;
        if buf.len() > i32::MAX as usize {
            return Err(io::Error::other("Parquet data page is too large"));
        }
        let page = Page::DataPage {
            buf: Bytes::from(buf),
            num_values: count,
            encoding: Encoding::PLAIN,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        };
        let written = pages.write_page(CompressedPage::new(page, source.len()))
            .map_err(io::Error::other)?;
        total_uncompressed += written.uncompressed_size as i64;
    }
    pages.close().map_err(io::Error::other)?;
    let bytes = Bytes::from(sink.into_inner().map_err(io::Error::other)?);
    let metadata = ColumnChunkMetaData::builder(descriptor)
        .set_compression(Compression::SNAPPY)
        .set_num_values(rows as i64)
        .set_encodings(vec![Encoding::PLAIN, Encoding::RLE])
        .set_total_compressed_size(bytes.len() as i64)
        .set_total_uncompressed_size(total_uncompressed)
        .set_data_page_offset(0)
        .set_unencoded_byte_array_data_bytes(binary_bytes)
        .build().map_err(io::Error::other)?;
    let close = ColumnCloseResult {
        bytes_written: bytes.len() as u64,
        rows_written: rows as u64,
        metadata,
        bloom_filter: None,
        column_index: None,
        offset_index: None,
    };
    Ok((bytes, close))
}

fn required_numeric<T: DataType>(
    group: &mut SerializedRowGroupWriter<'_, BufWriter<File>>,
    descriptor: ColumnDescPtr,
    props: &WriterProperties,
    values: Vec<T::T>,
) -> io::Result<()>
where T::T: SliceAsBytes {
    let row_limit = props.data_page_row_count_limit().min(i32::MAX as usize);
    let page_rows = row_limit.min(props.data_page_size_limit() / std::mem::size_of::<T::T>());
    let physical = T::get_physical_type();
    let path = descriptor.path();
    if cfg!(target_endian = "big") || page_rows == 0
        || !matches!(physical, Type::INT32 | Type::INT64) || physical != descriptor.physical_type()
        || descriptor.max_def_level() != 0 || descriptor.max_rep_level() != 0
        || props.compression(path) != Compression::SNAPPY || props.dictionary_enabled(path)
        || props.statistics_enabled(path) != EnabledStatistics::None
        || props.writer_version() != WriterVersion::PARQUET_1_0
        || !matches!(props.encoding(path), None | Some(Encoding::PLAIN))
    {
        return column::<T>(group, &values, None);
    }
    // PLAIN INT32 and INT64 use the native signed bits in little-endian order.
    // This slice borrows initialized primitive storage through Parquet's safe API.
    let pages = values.chunks(page_rows).map(|page| (T::T::slice_as_bytes(page), page.len() as u32));
    let (bytes, metadata) = encode_plain(descriptor, values.len(), pages, None)?;
    drop(values);
    group.append_column(&bytes, metadata).map_err(io::Error::other)
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

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::ConvertedType;
    use parquet::file::properties::DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::Field;
    use parquet::schema::types::ColumnPath;

    #[test]
    fn required_numeric_pages_match_typed_fallback() {
        let dir = std::env::temp_dir().join(format!("shopping-numeric-pages-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let row_count = DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT + 1;
        let categories = [ByteArray::from("books")];
        let mut direct = Users::new();
        let mut native = Users::new();
        let mut expected = Vec::with_capacity(row_count);
        for index in 0..row_count {
            let name = ["rémi", "", "owner\0full", "a,b"][index % 4];
            let day = 19_500 - 2 * index as i32;
            let sessions = index as u64 * 31 + 1;
            let events = index as u64 * 67 + 2;
            let revenue = if index % 3 == 0 { i64::MAX as u64 - index as u64 } else { index as u64 };
            let category = (index % 2 == 0).then_some(0);
            for users in [&mut direct, &mut native] {
                users.push(name.as_bytes(), day, sessions, events, revenue, category);
            }
            expected.push(vec![
                Field::Str(name.into()),
                Field::Date(day),
                Field::Long(sessions as i64),
                Field::Long(events as i64),
                Field::Long(revenue as i64),
                category.map_or(Field::Null, |_| Field::Str("books".into())),
            ]);
        }
        let props = || WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_dictionary_enabled(false);
        let direct_path = dir.join("direct.parquet");
        direct.write(&direct_path, &categories, Arc::new(props().build())).unwrap();
        let native_path = dir.join("native.parquet");
        let mut native_props = props();
        for key in ["day", "sessions", "events", "revenue_cents"] {
            native_props = native_props.set_column_compression(ColumnPath::from(key), Compression::UNCOMPRESSED);
        }
        native.write(&native_path, &categories, Arc::new(native_props.build())).unwrap();

        for (path, compression) in [
            (direct_path, Compression::SNAPPY),
            (native_path, Compression::UNCOMPRESSED),
        ] {
            let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
            let group = reader.get_row_group(0).unwrap();
            assert_eq!(group.metadata().column(1).column_descr().converted_type(), ConvertedType::DATE);
            for index in 1..5 {
                let metadata = group.metadata().column(index);
                assert_eq!(metadata.compression(), compression);
                let physical = if index == 1 { Type::INT32 } else { Type::INT64 };
                assert_eq!(metadata.column_descr().physical_type(), physical);
                if compression == Compression::SNAPPY && cfg!(target_endian = "little") {
                    let mut pages = group.get_column_page_reader(index).unwrap();
                    for count in [row_count - 1, 1] {
                        let page = pages.get_next_page().unwrap().unwrap();
                        assert_eq!(page.encoding(), Encoding::PLAIN);
                        assert_eq!(page.num_values() as usize, count);
                    }
                    assert!(pages.get_next_page().unwrap().is_none());
                }
            }
            let actual = reader.get_row_iter(None).unwrap().map(|row| {
                row.unwrap().into_columns().into_iter().map(|(_, value)| value).collect::<Vec<_>>()
            }).collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        let empty = dir.join("empty.parquet");
        Users::new().write(&empty, &categories, Arc::new(props().build())).unwrap();
        let reader = SerializedFileReader::new(File::open(empty).unwrap()).unwrap();
        assert_eq!(reader.num_row_groups(), 0);
        assert!(reader.get_row_iter(None).unwrap().next().is_none());
        fs::remove_dir_all(dir).unwrap();
    }
}
