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
pub fn first_field(line: &[u8]) -> Option<&[u8]> {
    let comma = find(line, b',')?;
    unquote(&line[..comma]).filter(|id| !id.is_empty())
}

#[inline]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "x86_64")]
    if (16..=128).contains(&line.len()) {
        return scanned_fields(line);
    }
    scalar_fields(line)
}

#[inline]
fn scalar_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
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

#[cfg(target_arch = "x86_64")]
fn scanned_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let (mut commas, mut quotes) = delimiter_masks(line);
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    let mut values = [b"".as_slice(); N];
    let mut start = 0;
    for value in &mut values {
        let stop = if commas == 0 { line.len() } else { commas.trailing_zeros() as usize };
        let in_field = if stop == 128 { quotes } else { quotes & ((1u128 << stop) - 1) };
        *value = if in_field == 0 {
            &line[start..stop]
        } else if stop - start >= 2 && in_field == (1u128 << start) | (1u128 << (stop - 1)) {
            &line[start + 1..stop - 1]
        } else {
            return None;
        };
        quotes ^= in_field;
        commas &= commas.wrapping_sub(1);
        start = stop + 1;
    }
    Some(values)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn delimiter_masks(line: &[u8]) -> (u128, u128) {
    #[cfg(target_feature = "avx2")]
    if line.len() >= 32 {
        return avx_delimiter_masks(line);
    }
    use std::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8};

    let mut commas = 0u128;
    let mut quotes = 0u128;
    let full = line.len() / 16 * 16;
    let remainder = line.len() - full;
    // SSE2 is available on x86-64. Each full load, including the overlapping
    // load used for the tail, stays inside the borrowed line.
    unsafe {
        let comma = _mm_set1_epi8(b',' as i8);
        let quote = _mm_set1_epi8(b'"' as i8);
        let masks = |at| {
            let block = _mm_loadu_si128(line.as_ptr().add(at).cast::<__m128i>());
            (_mm_movemask_epi8(_mm_cmpeq_epi8(block, comma)) as u128,
             _mm_movemask_epi8(_mm_cmpeq_epi8(block, quote)) as u128)
        };
        for at in (0..full).step_by(16) {
            let (comma, quote) = masks(at);
            commas |= comma << at;
            quotes |= quote << at;
        }
        if remainder != 0 {
            let (comma, quote) = masks(line.len() - 16);
            commas |= (comma >> (16 - remainder)) << full;
            quotes |= (quote >> (16 - remainder)) << full;
        }
    }
    (commas, quotes)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn avx_delimiter_masks(line: &[u8]) -> (u128, u128) {
    use std::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8};

    let mut commas = 0u128;
    let mut quotes = 0u128;
    let full = line.len() / 32 * 32;
    let remainder = line.len() - full;
    // The AVX2 path is compiled only for hosts where AVX2 is part of the
    // declared target. Both full and overlapping tail loads stay in bounds.
    unsafe {
        let comma = _mm256_set1_epi8(b',' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        let masks = |at| {
            let block = _mm256_loadu_si256(line.as_ptr().add(at).cast::<__m256i>());
            (u128::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(block, comma)) as u32),
             u128::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(block, quote)) as u32))
        };
        for at in (0..full).step_by(32) {
            let (comma, quote) = masks(at);
            commas |= comma << at;
            quotes |= quote << at;
        }
        if remainder != 0 {
            let (comma, quote) = masks(line.len() - 32);
            commas |= (comma >> (32 - remainder)) << full;
            quotes |= (quote >> (32 - remainder)) << full;
        }
    }
    (commas, quotes)
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
        for (file, mapping) in self.files.iter().enumerate() {
            let bytes = mapping.bytes();
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

    pub fn scan<'a, T: Send>(
        &'a self, threads: usize, create: impl Fn() -> T + Sync,
        parse: impl Fn(&mut T, &'a [u8]) + Sync,
    ) -> Vec<T> {
        let chunks = self.chunks();
        worker_map(threads, chunks.len(), create, |worker, index| {
            let Chunk { file, start, end } = chunks[index];
            let bytes = self.files[file].bytes();
            let mut offset = start;
            while offset < end {
                let remainder = &bytes[offset..end];
                let line_len = find(remainder, b'\n').unwrap_or(remainder.len());
                let line = &remainder[..line_len];
                parse(worker, line);
                offset += line_len + 1;
            }
        })
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
pub fn mix(state: u64, value: u64) -> u64 {
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
    fn finish(&self) -> u64 {
        // The low bits alone retain shared character patterns in fixed-size IDs.
        // Fold high bits into the bucket index before the table chooses a slot.
        self.0 ^ (self.0 >> 33)
    }
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
    if bytes.len() <= 18 {
        for &byte in bytes {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 { return None; }
            value = value * 10 + i64::from(digit);
        }
        return Some(value);
    }
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
    let leap = leap_year(year);
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
    let start_of_month = YEAR_START[year as usize] + MONTH_START[month as usize - 1];
    let days = start_of_month + day - 1 + i64::from(leap && month > 2);
    Some(days * DAY + ((hour * 60 + minute - offset) * 60 + second) * 1000)
}

const MONTH_START: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
static YEAR_START: [i64; 10_000] = year_starts();

const fn leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

const fn year_starts() -> [i64; 10_000] {
    let mut result = [0; 10_000];
    let mut year = 1;
    let mut date = -719_162; // Days from 1970-01-01 back to 0001-01-01.
    while year < result.len() {
        result[year] = date;
        date += if leap_year(year as i64) { 366 } else { 365 };
        year += 1;
    }
    result
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

#[cfg(test)]
mod tests {
    use super::{fields, scalar_fields};

    #[test]
    fn delimiter_scanner_matches_scalar() {
        let mut random = 0x1234_5678_1234_5678u64;
        let mut draw = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        for width in [2, 4, 7] {
            for _ in 0..4096 {
                let mut line = Vec::new();
                for field in 0..width {
                    if field != 0 { line.push(b','); }
                    let quoted = draw() % 3 == 0;
                    if quoted { line.push(b'"'); }
                    for _ in 0..draw() % 26 { line.push(b'a'); }
                    if quoted { line.push(b'"'); }
                }
                if !line.is_empty() && draw() % 2 == 0 {
                    let index = draw() as usize % line.len();
                    line[index] = if draw() % 2 == 0 { b',' } else { b'"' };
                }
                match width {
                    2 => assert_eq!(fields::<2>(&line), scalar_fields::<2>(&line)),
                    4 => assert_eq!(fields::<4>(&line), scalar_fields::<4>(&line)),
                    _ => assert_eq!(fields::<7>(&line), scalar_fields::<7>(&line)),
                }
            }
        }
    }
}
