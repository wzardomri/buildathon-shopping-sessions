use std::ffi::c_void;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const SHARDS: usize = 128;
const FILE_SHIFT: u32 = 48;

unsafe extern "C" {
    fn mmap(address: *mut c_void, len: usize, protection: i32, flags: i32, fd: i32, offset: i64) -> *mut c_void;
    fn munmap(address: *mut c_void, len: usize) -> i32;
    fn memchr(address: *const c_void, target: i32, length: usize) -> *const c_void;
}

struct Mapping {
    address: *mut c_void,
    len: usize,
}
// The mapping remains read-only for its whole lifetime.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len().try_into().map_err(io::Error::other)?;
        if len == 0 {
            return Ok(Self { address: ptr::null_mut(), len });
        }
        // PROT_READ and MAP_PRIVATE have the same values on Linux and macOS.
        let address = unsafe { mmap(ptr::null_mut(), len, 1, 2, file.as_raw_fd(), 0) };
        if address as usize == usize::MAX {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { address, len })
    }

    fn bytes(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.address.cast(), self.len) }
        }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        if self.len != 0 {
            unsafe { munmap(self.address, self.len) };
        }
    }
}

#[inline]
pub fn find(bytes: &[u8], target: u8) -> Option<usize> {
    let found = unsafe { memchr(bytes.as_ptr().cast(), target.into(), bytes.len()) };
    if found.is_null() {
        None
    } else {
        Some(found as usize - bytes.as_ptr() as usize)
    }
}

#[inline]
fn unquote(cell: &[u8]) -> Option<&[u8]> {
    if cell.first() == Some(&b'"') {
        if cell.len() < 2 || cell.last() != Some(&b'"') {
            return None;
        }
        let value = &cell[1..cell.len() - 1];
        return find(value, b'"').is_none().then_some(value);
    }
    find(cell, b'"').is_none().then_some(cell)
}

#[inline]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut values = [b"".as_slice(); N];
    let mut rest = line;
    for value in &mut values[..N - 1] {
        let comma = find(rest, b',')?;
        *value = unquote(&rest[..comma])?;
        rest = &rest[comma + 1..];
    }
    if find(rest, b',').is_some() {
        return None;
    }
    values[N - 1] = unquote(rest)?;
    Some(values)
}

#[derive(Clone, Copy, Debug)]
pub struct Row {
    pub hash: u64,
    pub location: u64,
}

#[derive(Clone, Copy)]
struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

pub struct Table {
    files: Vec<Mapping>,
}

impl Table {
    pub fn open(path: &Path) -> io::Result<Self> {
        let paths = if path.is_dir() {
            let mut paths = fs::read_dir(path)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<io::Result<Vec<_>>>()?;
            paths.retain(|name| name.extension().is_some_and(|ext| ext == "csv"));
            paths.sort_unstable();
            paths
        } else {
            vec![path.to_owned()]
        };
        let files = paths.iter().map(|name| Mapping::open(name)).collect::<io::Result<Vec<_>>>()?;
        Ok(Self { files })
    }

    fn chunks(&self) -> Vec<Chunk> {
        const CHUNK_BYTES: usize = 4 << 20;
        let mut chunks = vec![];
        assert!(self.files.len() < 1 << (64 - FILE_SHIFT));
        for (file, mapping) in self.files.iter().enumerate() {
            let bytes = mapping.bytes();
            assert!((bytes.len() as u64) < 1 << FILE_SHIFT);
            let Some(header_end) = find(bytes, b'\n') else { continue };
            let mut start = header_end + 1;
            while start < bytes.len() {
                let limit = (start + CHUNK_BYTES).min(bytes.len());
                let end = limit + find(&bytes[limit..], b'\n').map_or(bytes.len() - limit, |i| i + 1);
                chunks.push(Chunk { file, start, end });
                start = end;
            }
        }
        chunks
    }

    pub fn rows(&self) -> impl Iterator<Item = &[u8]> {
        self.files.iter().flat_map(|mapping| mapping.bytes().split(|&byte| byte == b'\n').skip(1))
    }

    pub fn line(&self, row: Row) -> &[u8] {
        let file = (row.location >> FILE_SHIFT) as usize;
        let offset = (row.location & ((1 << FILE_SHIFT) - 1)) as usize;
        let bytes = &self.files[file].bytes()[offset..];
        &bytes[..find(bytes, b'\n').unwrap_or(bytes.len())]
    }

    pub fn partition(&self, threads: usize) -> Vec<Vec<Vec<Row>>> {
        let chunks = self.chunks();
        let workers = worker_map(threads, chunks.len(), || {
            (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>()
        }, |buckets, index| {
            let Chunk { file, start, end } = chunks[index];
            let bytes = self.files[file].bytes();
            let mut offset = start;
            while offset < end {
                let remainder = &bytes[offset..end];
                let line_len = find(remainder, b'\n').unwrap_or(remainder.len());
                let line = &remainder[..line_len];
                if let Some(comma) = find(line, b',') {
                    if let Some(id) = unquote(&line[..comma]).filter(|id| !id.is_empty()) {
                        let hash = hash_bytes(id);
                        buckets[shard(hash)].push(Row {
                            hash,
                            location: ((file as u64) << FILE_SHIFT) | offset as u64,
                        });
                    }
                }
                offset += line_len + 1;
            }
        });
        transpose(workers)
    }
}

pub fn transpose<T>(workers: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut shards = (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>();
    for buckets in workers {
        for (shard, bucket) in shards.iter_mut().zip(buckets) {
            if !bucket.is_empty() {
                shard.push(bucket);
            }
        }
    }
    shards
}

pub fn worker_map<T: Send>(
    threads: usize,
    jobs: usize,
    create: impl Fn() -> T + Sync,
    work: impl Fn(&mut T, usize) + Sync,
) -> Vec<T> {
    let next = AtomicUsize::new(0);
    thread::scope(|scope| {
        let threads: Vec<_> = (0..threads.min(jobs.max(1))).map(|_| {
            scope.spawn(|| {
                let mut worker = create();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= jobs {
                        break;
                    }
                    work(&mut worker, index);
                }
                worker
            })
        }).collect();
        threads.into_iter().map(|thread| thread.join().unwrap()).collect()
    })
}

#[inline]
fn mix(state: u64, value: u64) -> u64 {
    (state.rotate_left(5) ^ value).wrapping_mul(0x517c_c1b7_2722_0a95)
}

#[inline]
pub fn hash_bytes(mut bytes: &[u8]) -> u64 {
    let mut result = bytes.len() as u64;
    while bytes.len() >= 8 {
        result = mix(result, u64::from_le_bytes(bytes[..8].try_into().unwrap()));
        bytes = &bytes[8..];
    }
    if bytes.len() >= 4 {
        result = mix(result, u32::from_le_bytes(bytes[..4].try_into().unwrap()).into());
        bytes = &bytes[4..];
    }
    if bytes.len() >= 2 {
        result = mix(result, u16::from_le_bytes(bytes[..2].try_into().unwrap()).into());
        bytes = &bytes[2..];
    }
    if let Some(&byte) = bytes.first() {
        result = mix(result, byte.into());
    }
    result
}

#[inline]
pub fn shard(hash: u64) -> usize {
    ((hash >> 32) as usize) & (SHARDS - 1)
}

#[derive(Default)]
pub struct FastHasher(u64);

impl Hasher for FastHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = mix(self.0, hash_bytes(bytes));
    }

    fn write_u64(&mut self, value: u64) { self.0 = mix(self.0, value); }
    fn write_usize(&mut self, value: usize) { self.write_u64(value as u64); }
    fn finish(&self) -> u64 { self.0 }
}

pub type Map<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<FastHasher>>;

fn pair(bytes: &[u8]) -> Option<i64> {
    let a = bytes[0].wrapping_sub(b'0');
    let b = bytes[1].wrapping_sub(b'0');
    (a <= 9 && b <= 9).then_some(i64::from(a) * 10 + i64::from(b))
}

pub fn unsigned(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0i64;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(digit.into())?;
    }
    Some(value)
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if !matches!(bytes.len(), 20 | 25) || bytes.get(4) != Some(&b'-') {
        return unsigned(bytes);
    }
    if bytes[7] != b'-' || bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = pair(bytes)? * 100 + pair(&bytes[2..])?;
    let month = pair(&bytes[5..])?;
    let day = pair(&bytes[8..])?;
    let hour = pair(&bytes[11..])?;
    let minute = pair(&bytes[14..])?;
    let second = pair(&bytes[17..])?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_len = match month {
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if year == 0 || day == 0 || day > month_len || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset = match (bytes.len(), bytes[19]) {
        (20, b'Z') => 0,
        (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
            let hours = pair(&bytes[20..])?;
            let minutes = pair(&bytes[23..])?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            (hours * 60 + minutes) * if sign == b'+' { 1 } else { -1 }
        }
        _ => return None,
    };
    Some(days_from_civil(year, month, day) * DAY + ((hour * 60 + minute - offset) * 60 + second) * 1000)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_in_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_in_year = (153 * shifted_month + 2) / 5 + day - 1;
    era * 146_097 + year_in_era * 365 + year_in_era / 4 - year_in_era / 100 + day_in_year - 719_468
}

pub fn display_day(day: i32) -> String {
    let adjusted = i64::from(day) + 719_468;
    let era = adjusted.div_euclid(146_097);
    let day_in_era = adjusted - era * 146_097;
    let year_in_era = (day_in_era - day_in_era / 1460 + day_in_era / 36_524 - day_in_era / 146_096) / 365;
    let day_in_year = day_in_era - (365 * year_in_era + year_in_era / 4 - year_in_era / 100);
    let shifted_month = (5 * day_in_year + 2) / 153;
    let month = shifted_month + if shifted_month < 10 { 3 } else { -9 };
    let day_of_month = day_in_year - (153 * shifted_month + 2) / 5 + 1;
    let year = year_in_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day_of_month:02}")
}
