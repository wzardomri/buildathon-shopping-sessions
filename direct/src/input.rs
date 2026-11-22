use crate::direct_event;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ffi::c_void;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const SHARDS: usize = 128;
pub const CHUNK_BYTES: usize = 4 << 20;
const COLD_EVENT_CHUNK_BYTES: usize = 16 << 20;
// A terminated candidate needs an ID byte, ingest byte, six commas and LF.
// A nominal cold range can extend to finish one physical row.
const _: () = assert!(COLD_EVENT_CHUNK_BYTES / 9 <= CHUNK_BYTES);

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
        Self::from_file(&file, file.metadata()?.len())
    }

    fn from_file(file: &File, len: u64) -> io::Result<Self> {
        let len = len.try_into().map_err(io::Error::other)?;
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

#[cfg(all(target_arch = "x86_64", not(target_feature = "bmi2")))]
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

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline]
fn bits_at_commas(bits: u128, commas: u128) -> u64 {
    use std::arch::x86_64::_pext_u64;

    let low = commas as u64;
    let high = (commas >> 64) as u64;
    // The scanner checks the small, fixed number of commas before this call.
    let below = unsafe { _pext_u64(bits as u64, low) };
    let above = unsafe { _pext_u64((bits >> 64) as u64, high) };
    below | (above << low.count_ones())
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn scanned_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let (commas, quotes) = delimiter_masks(line);
    fields_from_masks(line, commas, quotes)
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn csv_wrapper_bits<const N: usize>(line_len: usize, commas: u128, quotes: u128) -> Option<u64> {
    let mut wrapped = 0;
    if quotes != 0 {
        let end = 1u128 << (line_len - 1);
        let opens = quotes & ((commas << 1) | 1);
        let closes = quotes & ((commas >> 1) | end);
        // A quote can only be the first or last byte of a field. A single
        // quote cannot count as both the opening and closing byte.
        if opens | closes != quotes || opens & closes != 0 {
            return None;
        }
        // Extract the quote flags in field order, aligning opening quotes with
        // the preceding commas and closing quotes with the following commas.
        wrapped = (bits_at_commas(opens >> 1, commas) << 1) | (opens as u64 & 1);
        let closed = bits_at_commas(closes << 1, commas) | (u64::from(closes & end != 0) << (N - 1));
        if wrapped != closed {
            return None;
        }
    }
    Some(wrapped)
}

#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn fields_from_masks<const N: usize>(line: &[u8], mut commas: u128, quotes: u128) -> Option<[&[u8]; N]> {
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    let mut wrapped = csv_wrapper_bits::<N>(line.len(), commas, quotes)?;
    let mut values = [b"".as_slice(); N];
    let mut start = 0;
    for value in &mut values[..N - 1] {
        let stop = commas.trailing_zeros() as usize;
        let padding = (wrapped as usize) & 1;
        // The delimiter mask has exactly N - 1 set bits at valid, ordered
        // byte positions. If padding is one, the quote validation proved
        // the field has two distinct quotes before the stop.
        *value = unsafe { line.get_unchecked(start + padding..stop - padding) };
        wrapped >>= 1;
        commas &= commas - 1;
        start = stop + 1;
    }
    let padding = (wrapped as usize) & 1;
    // Every comma lies within the line, and the final field passed the
    // same quote validation as preceding fields.
    values[N - 1] = unsafe { line.get_unchecked(start + padding..line.len() - padding) };
    Some(values)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
fn split_event_fields(line: &[u8], commas: u128, quotes: u128) -> Option<[&[u8]; 7]> {
    let first_block = commas as u32;
    let suffix = (commas >> 32) as u64;
    if first_block.count_ones() != 2 || suffix.count_ones() != 4 {
        return event_mask_fallback(line, commas, quotes);
    }
    let mut wrapped = csv_wrapper_bits::<7>(line.len(), commas, quotes)?;
    // The first SIMD block has two delimiters and the remaining two
    // blocks have four. Decode each ordered subset in its own register.
    let without_third = suffix & (suffix - 1);
    let without_fourth = without_third & (without_third - 1);
    let stops = [
        first_block.trailing_zeros() as usize,
        (31 - first_block.leading_zeros()) as usize,
        32 + suffix.trailing_zeros() as usize,
        32 + without_third.trailing_zeros() as usize,
        32 + without_fourth.trailing_zeros() as usize,
        95 - suffix.leading_zeros() as usize,
        line.len(),
    ];
    let mut values = [b"".as_slice(); 7];
    let mut start = 0;
    for (value, stop) in values.iter_mut().zip(stops) {
        let padding = (wrapped & 1) as usize;
        // The masks came from within the row and contain exactly six
        // distinct separators. Wrapping is proven for both endpoints.
        *value = unsafe { line.get_unchecked(start + padding..stop - padding) };
        wrapped >>= 1;
        start = stop + 1;
    }
    Some(values)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
#[cold]
#[inline(never)]
fn event_mask_fallback(line: &[u8], commas: u128, quotes: u128) -> Option<[&[u8]; 7]> {
    fields_from_masks(line, commas, quotes)
}

#[inline]
pub fn event_row_fields(bytes: &[u8]) -> (usize, Option<[&[u8]; 7]>) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
    if let Some((end, commas, quotes)) = short_event_masks(bytes) {
        return (end + 1, split_event_fields(&bytes[..end], commas, quotes));
    }
    let end = find(bytes, b'\n').unwrap_or(bytes.len());
    ((end + 1).min(bytes.len()), fields(&bytes[..end]))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2"))]
#[inline]
fn short_event_masks(bytes: &[u8]) -> Option<(usize, u128, u128)> {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };
    if bytes.len() < 96 {
        return None;
    }
    // All three possible loads are in bounds. Bits at and after the
    // first LF are dropped, including delimiters from subsequent rows.
    unsafe {
        let mask = |block, delimiter| _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, delimiter)) as u32;
        let (lf, comma, quote) = (_mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b',' as i8),
                                  _mm256_set1_epi8(b'"' as i8));
        let first = _mm256_loadu_si256(bytes.as_ptr().cast::<__m256i>());
        let second = _mm256_loadu_si256(bytes.as_ptr().add(32).cast::<__m256i>());
        let low_mask = |delimiter| u64::from(mask(first, delimiter)) | (u64::from(mask(second, delimiter)) << 32);
        let low_lf = low_mask(lf);
        if low_lf != 0 {
            let end = low_lf.trailing_zeros();
            let keep_low = (1u64 << end) - 1;
            return Some((end as usize, u128::from(low_mask(comma) & keep_low),
                u128::from(low_mask(quote) & keep_low)));
        }
        let third = _mm256_loadu_si256(bytes.as_ptr().add(64).cast::<__m256i>());
        let high_lf = mask(third, lf);
        if high_lf == 0 {
            return None;
        }
        let high_end = high_lf.trailing_zeros();
        let keep_high = (1u32 << high_end) - 1;
        let commas = u128::from(low_mask(comma)) | (u128::from(mask(third, comma) & keep_high) << 64);
        let quotes = u128::from(low_mask(quote)) | (u128::from(mask(third, quote) & keep_high) << 64);
        Some((64 + high_end as usize, commas, quotes))
    }
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

fn file_waves(chunks: &[Chunk], threads: usize) -> Vec<Chunk> {
    let files: Vec<_> = chunks.chunk_by(|left, right| left.file == right.file).collect();
    let mut ready: BinaryHeap<_> = files.iter().enumerate().map(|(file, ranges)| {
        (ranges.last().unwrap().end - ranges[0].start, Reverse(file), 0)
    }).collect();
    let mut result = Vec::with_capacity(chunks.len());
    let width = threads.max(1).min(files.len());
    let mut wave = Vec::with_capacity(width);
    while !ready.is_empty() {
        for _ in 0..width {
            let Some((_, Reverse(file), index)) = ready.pop() else { break };
            let ranges = files[file];
            result.push(ranges[index]);
            if let Some(next) = ranges.get(index + 1) {
                let remaining = ranges.last().unwrap().end - next.start;
                wave.push((remaining, Reverse(file), index + 1));
            }
        }
        // Give each wave different file descriptions where available,
        // prioritizing long streams so small files can fill the tail.
        ready.extend(wave.drain(..));
    }
    result
}

pub struct Table {
    files: Vec<Mapping>,
    event_origins: Option<Vec<direct_event::Origin>>,
}

pub enum EventPlan<'a> {
    Mapped(Vec<&'a [u8]>),
    Direct(direct_event::Plan<'a>),
}

fn csv_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
    if !path.is_dir() { return Ok(vec![path.to_owned()]); }
    let mut paths = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    paths.retain(|name| name.extension().is_some_and(|ext| ext == "csv"));
    paths.sort_unstable();
    Ok(paths)
}

impl Table {
    pub fn open(path: &Path) -> io::Result<Self> {
        let paths = csv_paths(path)?;
        let files = paths.iter().map(|name| Mapping::open(name)).collect::<io::Result<Vec<_>>>()?;
        Ok(Self { files, event_origins: None })
    }

    pub fn open_events(path: &Path, threads: usize) -> io::Result<Self> {
        if !direct_event::SUPPORTED || !(2..=direct_event::MAX_WORKERS).contains(&threads) {
            return Self::open(path);
        }
        let paths = csv_paths(path)?;
        if paths.len() > direct_event::MAX_FILES {
            let files = paths.iter().map(|name| Mapping::open(name)).collect::<io::Result<Vec<_>>>()?;
            return Ok(Self { files, event_origins: None });
        }
        let mut files = Vec::with_capacity(paths.len());
        let mut origins = Vec::with_capacity(paths.len());
        for path in paths {
            let opened = File::open(&path)?;
            let metadata = opened.metadata()?;
            files.push(Mapping::from_file(&opened, metadata.len())?);
            origins.push(direct_event::Origin { path, metadata });
        }
        Ok(Self { files, event_origins: Some(origins) })
    }

    fn chunks(&self) -> Vec<Chunk> {
        self.chunks_sized(CHUNK_BYTES)
    }

    fn chunks_sized(&self, chunk_bytes: usize) -> Vec<Chunk> {
        let mut chunks = vec![];
        for (file, mapping) in self.files.iter().enumerate() {
            let bytes = mapping.bytes();
            let Some(header_end) = find(bytes, b'\n') else { continue };
            let mut start = header_end + 1;
            while start < bytes.len() {
                let limit = start.saturating_add(chunk_bytes).min(bytes.len());
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

    pub fn data_chunks(&self) -> Vec<&[u8]> {
        self.chunks().into_iter().map(|chunk| {
            &self.files[chunk.file].bytes()[chunk.start..chunk.end]
        }).collect()
    }

    pub fn event_chunks(&self, threads: usize) -> EventPlan<'_> {
        if !self.cold_across_files(threads) {
            return EventPlan::Mapped(self.data_chunks());
        }
        let direct_sources = self.event_origins.as_ref()
            .filter(|_| threads <= direct_event::MAX_WORKERS && direct_event::supports_page_size())
            .map(|origins| origins.iter().map(|origin| direct_event::Source::open(origin, COLD_EVENT_CHUNK_BYTES))
                .collect::<Vec<_>>());
        let chunks = self.chunks_sized(COLD_EVENT_CHUNK_BYTES);
        let waves = file_waves(&chunks, threads);
        eprintln!("cold event file waves  {} ranges", waves.len());
        if let Some(sources) = direct_sources {
            let eligible = sources.iter().filter(|source| source.eligible).count();
            let opened = sources.iter().filter(|source| source.opened()).count();
            if opened != 0 {
                let ranges = waves.into_iter().map(|chunk| {
                    let file = &self.files[chunk.file];
                    direct_event::Range {
                        mapped: &file.bytes()[chunk.start..chunk.end], file: chunk.file,
                        offset: chunk.start, file_len: file.len,
                    }
                }).collect();
                return EventPlan::Direct(direct_event::Plan { ranges, sources, eligible, opened });
            }
            let fallback = waves.iter().filter(|chunk| sources[chunk.file].eligible).count();
            eprintln!("cold direct events eligible={eligible} opened=0 ranges={} used=0 fallback={fallback} bytes=0",
                waves.len());
        }
        EventPlan::Mapped(waves.into_iter().map(|chunk| {
            &self.files[chunk.file].bytes()[chunk.start..chunk.end]
        }).collect())
    }

    #[cfg(target_os = "linux")]
    fn cold_across_files(&self, threads: usize) -> bool {
        const SAMPLES: usize = 16;
        let size = self.files.iter().fold(0usize, |sum, file| sum.saturating_add(file.len));
        let substantial = self.files.iter().filter(|file| file.len >= COLD_EVENT_CHUNK_BYTES)
            .take(threads).count();
        if threads < 2 || substantial < threads
            || size < COLD_EVENT_CHUNK_BYTES.saturating_mul(threads).saturating_mul(12)
        {
            return false;
        }
        unsafe extern "C" {
            fn getpagesize() -> i32;
            fn mincore(address: *mut c_void, length: usize, residency: *mut u8) -> i32;
        }
        let Ok(page) = usize::try_from(unsafe { getpagesize() }) else { return false };
        if page == 0 {
            return false;
        }
        let stride = size / SAMPLES;
        let mut next = stride / 2;
        let (mut probes, mut usable, mut cold) = (0, 0, 0);
        'files: for file in &self.files {
            while next < file.len {
                // mmap starts at a page boundary, and this offset lies
                // inside the live read-only mapping. The probe reads no bytes.
                let address = file.address.cast::<u8>().wrapping_add(next - next % page);
                let mut residency = 0u8;
                if unsafe { mincore(address.cast(), 1, &mut residency) } == 0 {
                    usable += 1;
                    cold += usize::from(residency & 1 == 0);
                }
                probes += 1;
                if probes == SAMPLES {
                    break 'files;
                }
                next = next.saturating_add(stride);
            }
            next -= file.len;
        }
        usable >= 12 && cold * 4 >= usable * 3
    }

    #[cfg(not(target_os = "linux"))]
    fn cold_across_files(&self, _: usize) -> bool { false }

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

#[inline]
fn decimal_word(digits: u64) -> u32 {
    // Little-endian byte lanes carry one digit each. Each multiply-add
    // stays within its lane before the mask retains the merged result.
    let pairs = (digits * 10 + (digits >> 8)) & 0x00ff_00ff_00ff_00ff;
    let groups = (pairs * 100 + (pairs >> 16)) & 0x0000_ffff_0000_ffff;
    (groups * 10_000 + (groups >> 32)) as u32
}

#[inline]
fn decimal_epoch(bytes: &[u8; 13]) -> Option<i64> {
    const HIGH_NIBBLES: u64 = 0xf0f0_f0f0_f0f0_f0f0;
    const ASCII_DIGITS: u64 = 0x3030_3030_3030_3030;
    const SIXES: u64 = 0x0606_0606_0606_0606;
    const TENS: u64 = 0x1010_1010_1010_1010;

    let head = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let tail = u64::from_le_bytes(bytes[5..].try_into().unwrap());
    if ((head ^ ASCII_DIGITS) | (tail ^ ASCII_DIGITS)) & HIGH_NIBBLES != 0 {
        return None;
    }
    let head = head & !HIGH_NIBBLES;
    // The two safe loads overlap by three already-validated head bytes.
    let tail = tail & !0x00ff_ffff & !HIGH_NIBBLES;
    if ((head + SIXES) | (tail + SIXES)) & TENS != 0 {
        return None;
    }
    Some(i64::from(decimal_word(head)) * 100_000 + i64::from(decimal_word(tail)))
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if let Ok(decimal) = <&[u8; 13]>::try_from(bytes) {
        return decimal_epoch(decimal);
    }
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

pub struct IngestCalendar {
    days: [([u8; 11], i64); 64],
}

const INVALID_CLOCK: u32 = 1 << 28;
const CLOCK_HOURS: [u32; 256] = clock_table(24, 3600);
const CLOCK_MINUTES: [u32; 256] = clock_table(60, 60);
const CLOCK_SECONDS: [u32; 256] = clock_table(60, 1);

const fn clock_table(limit: usize, scale: u32) -> [u32; 256] {
    let mut table = [INVALID_CLOCK; 256];
    let mut number = 0;
    while number < limit {
        table[(number / 10) | ((number % 10) << 4)] = number as u32 * scale;
        number += 1;
    }
    table
}

#[inline]
fn clock_seconds(clock: &[u8; 8]) -> Option<i64> {
    const DIGIT_HIGH: u64 = 0xf0f0_00f0_f000_f0f0;
    const FIXED_BYTES: u64 = 0x0000_ff00_00ff_0000;
    const FIXED_FORM: u64 = u64::from_le_bytes(*b"00:00:00");
    let raw = u64::from_le_bytes(*clock);
    if (raw ^ FIXED_FORM) & (DIGIT_HIGH | FIXED_BYTES) != 0 {
        return None;
    }
    let digits = raw & 0x0f0f_000f_0f00_0f0f;
    let pairs = digits | (digits >> 4);
    let seconds = CLOCK_HOURS[(pairs as u8) as usize]
        + CLOCK_MINUTES[((pairs >> 24) as u8) as usize]
        + CLOCK_SECONDS[((pairs >> 48) as u8) as usize];
    (seconds < INVALID_CLOCK).then_some(i64::from(seconds))
}

impl IngestCalendar {
    pub const DEFER_CODE: u32 = u32::MAX;

    fn bucket(prefix: &[u8; 11]) -> usize {
        (usize::from(prefix[8]) * 10 + usize::from(prefix[9]) + (usize::from(prefix[6]) << 5)) & 63
    }

    pub fn new(window_start: i64, window_end: i64) -> Self {
        let mut days = [([0; 11], 0); 64];
        let start = window_start.div_euclid(DAY);
        let end = window_end.div_euclid(DAY);
        for day in (start - 1).max(YEAR_START[1])..=(end + 5).min(start + 30) {
            if let Ok(day) = i32::try_from(day) {
                let mut label = display_day(day);
                label.push('T');
                if let Ok(prefix) = <[u8; 11]>::try_from(label.as_bytes()) {
                    days[Self::bucket(&prefix)] = (prefix, i64::from(day) * (DAY / 1000));
                }
            }
        }
        Self { days }
    }

    #[inline]
    fn matching_midnight(&self, prefix: &[u8; 11]) -> Option<i64> {
        let (expected, midnight) = &self.days[Self::bucket(prefix)];
        (prefix == expected && prefix[10] == b'T').then_some(*midnight)
    }

    #[inline]
    pub fn parse(&self, bytes: &[u8]) -> Option<(i64, u32)> {
        if let Ok(clock) = <&[u8; 20]>::try_from(bytes) {
            if clock[19] == b'Z' {
                let prefix = clock[..11].try_into().unwrap();
                if let Some(midnight) = self.matching_midnight(prefix) {
                    let seconds = midnight + clock_seconds(clock[11..19].try_into().unwrap())?;
                    let code = if seconds as u64 >= 1u64 << 31 {
                        Self::DEFER_CODE
                    } else {
                        seconds as u32
                    };
                    return Some((seconds * 1000, code));
                }
            }
        } else if let Ok(clock) = <&[u8; 25]>::try_from(bytes) {
            let prefix = clock[..11].try_into().unwrap();
            if let (sign @ (b'+' | b'-'), b':') = (clock[19], clock[22]) {
                if let Some(midnight) = self.matching_midnight(prefix) {
                    let seconds = clock_seconds(clock[11..19].try_into().unwrap())?;
                    let hour = pair(&clock[20..])?;
                    let minute = pair(&clock[23..])?;
                    if hour > 23 || minute > 59 {
                        return None;
                    }
                    let zone = (hour * 3600 + minute * 60) * if sign == b'+' { 1 } else { -1 };
                    return Some(((midnight + seconds - zone) * 1000, Self::DEFER_CODE));
                }
            }
        }
        timestamp(bytes).map(|millis| (millis, Self::DEFER_CODE))
    }
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
    use super::{DAY, IngestCalendar, Table, event_row_fields, fields, file_waves, scalar_fields, timestamp, unsigned};

    #[test]
    fn cold_file_waves_schedule_distinct_files_and_preserve_physical_rows() {
        let root = std::env::temp_dir().join(format!("shopping-cold-file-waves-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let long = "z".repeat(333);
        let inputs = [
            format!("header\na,b,c\n{long}\nlast without LF"),
            "header\nempty,,,\nleft\nright\n".to_owned(),
            "header\nsmall\n".to_owned(),
            "header\n".to_owned(),
            "unterminated header".to_owned(),
        ];
        for (index, input) in inputs.iter().enumerate() {
            std::fs::write(root.join(format!("part-{index}.csv")), input).unwrap();
        }
        let table = Table::open(&root).unwrap();
        let chunks = table.chunks_sized(8);
        for width in [1, 2, 3, 8] {
            let waves = file_waves(&chunks, width);
            assert_eq!(chunks.len(), waves.len());
            let first_wave: std::collections::HashSet<_> = waves.iter().take(width.min(3))
                .map(|chunk| chunk.file).collect();
            assert_eq!(first_wave.len(), width.min(3));
            for (file, input) in inputs.iter().enumerate() {
                let expected = input.split_once('\n').map_or(b"".as_slice(), |(_, data)| data.as_bytes());
                let actual: Vec<_> = waves.iter().filter(|chunk| chunk.file == file)
                    .flat_map(|chunk| &table.files[file].bytes()[chunk.start..chunk.end]).copied().collect();
                assert_eq!(actual, expected);
            }
        }
        drop(table);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn split_mask_preserves_separators_and_wrappers_at_block_edges() {
        for second in 28..=34 {
            for third in second + 1..=39 {
                for fifth in 62..=65 {
                    let stops = [9, second, third, 49, fifth, 72, 83];
                    for wrapped in 0..128 {
                        let mut row = [b'x'; 128];
                        let mut start = 0;
                        for (field, stop) in stops.into_iter().enumerate() {
                            row[stop] = if field == 6 { b'\n' } else { b',' };
                            if wrapped & (1 << field) != 0 && start < stop {
                                row[start] = b'"';
                                row[stop - 1] = b'"';
                            }
                            start = stop + 1;
                        }
                        let expected = scalar_fields::<7>(&row[..83]);
                        let result = event_row_fields(&row);
                        assert_eq!(result, (84, expected));
                        if let (Some(actual), Some(expected)) = (result.1, expected) {
                            for (actual, expected) in actual.into_iter().zip(expected) {
                                assert_eq!(actual.as_ptr(), expected.as_ptr());
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn fused_event_fields_preserve_all_physical_boundaries() {
        let tail = b"\"new\",\"\"\nshort,no,end-with-delimiters,,,,\n";
        for padding in 0..180 {
            for head in [b",,,,,,".as_slice(), b"\"id\",user,\"\",at,kind,p,",
                         b"x,y,\"bad,z,d,p,", b"", b"a,b,c,d,e,f,g,extra"] {
                let mut line = head.to_vec();
                line.resize(line.len() + padding, b'x');
                for corruption in [None, Some(b','), Some(b'"'), Some(b'\r')] {
                    let mut row = line.clone();
                    if let Some(value) = corruption {
                        if let Some(byte) = row.get_mut(padding / 2) {
                            *byte = value;
                        }
                    }
                    let mut input = row.clone();
                    input.push(b'\n');
                    input.extend(tail.repeat(4));
                    assert_eq!(event_row_fields(&input), (row.len() + 1, scalar_fields::<7>(&row)));
                    assert_eq!(event_row_fields(&row), (row.len(), scalar_fields::<7>(&row)));
                }
            }
        }
    }

    #[test]
    fn decimal_epoch_matches_general_digit_parser() {
        for digit in 0..=9 {
            let digits = [b'0' + digit; 13];
            for index in 0..digits.len() {
                for byte in 0..=u8::MAX {
                    let mut changed = digits;
                    changed[index] = byte;
                    assert_eq!(timestamp(&changed), unsigned(&changed));
                }
            }
        }
        let mut random = 0x9be7_4531_62d4_180fu64;
        for _ in 0..8192 {
            let mut digits = [0; 13];
            for digit in &mut digits {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                *digit = b'0' + (random % 10) as u8;
            }
            assert_eq!(timestamp(&digits), unsigned(&digits));
        }
        for digits in [
            b"000000000007".as_slice(),
            b"00000000000007",
            b"000001791071890000",
            b"9223372036854775807",
            b"9223372036854775808",
        ] {
            assert_eq!(timestamp(digits), unsigned(digits));
        }
    }

    #[test]
    fn cached_clock_table_matches_full_calendar() {
        let start = timestamp(b"2028-02-28T00:00:00Z").unwrap();
        let calendar = IngestCalendar::new(start, start + 14 * DAY);
        for hour in 0..=24 {
            for minute in 0..=60 {
                for second in 0..=60 {
                    let value = format!("2028-02-29T{hour:02}:{minute:02}:{second:02}Z");
                    let bytes = value.as_bytes();
                    assert_eq!(calendar.parse(bytes).map(|(millis, _)| millis), timestamp(bytes));
                }
            }
        }
        for date in ["2028-02-29", "2028-03-08", "2028-02-14", "2028-02-30", "1970-01-01"] {
            for clock in ["00:00:00", "19:01:59", "23:59:59", "24:00:00", "12:1;:00"] {
                for zone in ["+00:00", "-00:00", "-23:59", "+23:59", "+24:00", "+15:60", "-12=00"] {
                    let input = format!("{date}T{clock}{zone}");
                    let bytes = input.as_bytes();
                    assert_eq!(calendar.parse(bytes).map(|(millis, _)| millis), timestamp(bytes));
                }
            }
        }
        for (seed, width) in [(b"2028-02-29T09:38:57Z".as_slice(), 20),
                              (b"2028-02-29T09:38:57-15:26".as_slice(), 25)] {
            for index in 0..width {
                for byte in 0..=u8::MAX {
                    let mut changed = seed.to_vec();
                    changed[index] = byte;
                    assert_eq!(calendar.parse(&changed).map(|(millis, _)| millis), timestamp(&changed));
                }
            }
        }
        let last_inline = timestamp(b"2038-01-19T03:14:07Z").unwrap();
        let calendar = IngestCalendar::new(last_inline, last_inline + DAY);
        assert_eq!(calendar.parse(b"2038-01-19T03:14:07Z"), Some((last_inline, i32::MAX as u32)));
        assert_eq!(calendar.parse(b"2038-01-19T03:14:08Z"), Some((last_inline + 1000, IngestCalendar::DEFER_CODE)));
    }

    #[test]
    fn scanner_matches_scalar_for_empty_cells_and_endpoint_quotes() {
        for row_size in 15usize..=131 {
            for quoted in 0..128u8 {
                let Some(payload) = row_size.checked_sub(6 + 2 * quoted.count_ones() as usize) else {
                    continue;
                };
                let mut line = Vec::with_capacity(row_size);
                for field in 0..7 {
                    if field != 0 {
                        line.push(b',');
                    }
                    let wrap = quoted >> field & 1 != 0;
                    if wrap {
                        line.push(b'"');
                    }
                    // Put the payload before empty cells, so successive
                    // delimiters and quoted empties also exercise the tail.
                    if field == row_size % 7 {
                        line.resize(line.len() + payload, b'x');
                    }
                    if wrap {
                        line.push(b'"');
                    }
                }
                assert_eq!(fields::<7>(&line), scalar_fields::<7>(&line));
                for index in [0, line.len() / 2, line.len() - 1] {
                    let mut malformed = line.clone();
                    malformed[index] = b'"';
                    assert_eq!(fields::<7>(&malformed), scalar_fields::<7>(&malformed));
                }
            }
        }
    }

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
