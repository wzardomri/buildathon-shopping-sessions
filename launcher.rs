use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::{env, fs, process::Command};

const MAX_DIRECTORY_ENTRIES: usize = 1024;
const MAX_EVENT_FILES: usize = 128;
const MAX_EVENT_BYTES: u64 = 6 << 30;
const COLD_CHUNK_BYTES: u64 = 16 << 20;
const SAMPLES: usize = 16;
const MMAP_ELF: &str = "shopping-sessions-mmap";
const DIRECT_ELF: &str = "shopping-sessions-direct";

#[derive(Debug, PartialEq, Eq)]
enum Solver { Mmap, Direct }

struct EventFile {
    path: PathBuf,
    metadata: fs::Metadata,
}

fn event_files(directory: &Path) -> Option<Vec<EventFile>> {
    if !fs::symlink_metadata(directory).ok()?.is_dir() {
        return None;
    }
    let mut files = Vec::new();
    let mut bytes = 0u64;
    for (index, entry) in fs::read_dir(directory).ok()?.enumerate() {
        if index == MAX_DIRECTORY_ENTRIES {
            return None;
        }
        let entry = entry.ok()?;
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "csv") {
            continue;
        }
        if files.len() == MAX_EVENT_FILES {
            return None;
        }
        let metadata = entry.metadata().ok()?;
        if !metadata.is_file() {
            return None;
        }
        bytes = bytes.checked_add(metadata.len())?;
        if bytes > MAX_EVENT_BYTES {
            return None;
        }
        files.push(EventFile { path, metadata });
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Some(files)
}

fn event_ranges(sizes: &[u64], threads: usize) -> Option<[(usize, u64); SAMPLES]> {
    if !(2..=8).contains(&threads) || sizes.len() > MAX_EVENT_FILES {
        return None;
    }
    let bytes = sizes.iter().try_fold(0u64, |sum, &size| sum.checked_add(size))?;
    let substantial = sizes.iter().filter(|&&size| size >= COLD_CHUNK_BYTES).count();
    if bytes > MAX_EVENT_BYTES || bytes < COLD_CHUNK_BYTES * (threads as u64) * 12 || substantial < threads {
        return None;
    }

    // Same midpoint/stride on sorted concatenated range lengths as the solver.
    let stride = bytes / SAMPLES as u64;
    let mut next = stride / 2;
    let mut file = 0;
    let mut ranges = [(0, 0); SAMPLES];
    for range in &mut ranges {
        while next >= sizes[file] {
            next -= sizes[file];
            file += 1;
        }
        *range = (file, next);
        next += stride;
    }
    Some(ranges)
}

fn cold_probes(
    ranges: [(usize, u64); SAMPLES], mut probe: impl FnMut(usize, u64) -> io::Result<Option<bool>>,
) -> bool {
    let mut usable = 0;
    let mut cold = 0;
    for (file, offset) in ranges {
        match probe(file, offset) {
            Ok(Some(true)) => {
                usable += 1;
                cold += 1;
            }
            // Any observed resident sample routes to the exact scored ELF.
            // This is deliberately stricter than the direct solver's 75%
            // cold gate, and makes the common warm rejection stop early.
            Ok(Some(false)) => return false,
            // An individual unsuccessful mincore probe is unusable under
            // the solver's original threshold. Other errors abandon probing.
            Ok(None) => {}
            Err(_) => return false,
        }
    }
    usable >= 12 && cold * 4 >= usable * 3
}

#[cfg(target_os = "linux")]
fn use_direct(data: &Path, threads: usize) -> bool {
    use std::ffi::c_void;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::ptr;

    unsafe extern "C" {
        fn getpagesize() -> i32;
        fn mmap(address: *mut c_void, length: usize, protection: i32, flags: i32, fd: i32, offset: i64) -> *mut c_void;
        fn mincore(address: *mut c_void, length: usize, residency: *mut u8) -> i32;
        fn munmap(address: *mut c_void, length: usize) -> i32;
    }
    const PROT_READ: i32 = 1;
    const MAP_PRIVATE: i32 = 2;
    const O_NONBLOCK: i32 = 0x800;
    const O_NOFOLLOW: i32 = 0x20000;
    let Some(files) = event_files(&data.join("events")) else { return false };
    let sizes: Vec<_> = files.iter().map(|file| file.metadata.len()).collect();
    let Some(ranges) = event_ranges(&sizes, threads) else { return false };
    let Ok(page) = u64::try_from(unsafe { getpagesize() }) else { return false };
    if page == 0 {
        return false;
    }

    let mut opened: Option<(usize, fs::File)> = None;
    cold_probes(ranges, |index, offset| {
        if opened.as_ref().is_none_or(|(open, _)| *open != index) {
            drop(opened.take());
            let expected = &files[index];
            // Do not follow a symlink or block on a FIFO if the name changes
            // between enumeration and the read-only page mapping.
            let new = fs::OpenOptions::new().read(true).custom_flags(O_NONBLOCK | O_NOFOLLOW).open(&expected.path)?;
            let actual = new.metadata()?;
            if !actual.is_file() || (actual.dev(), actual.ino(), actual.len())
                != (expected.metadata.dev(), expected.metadata.ino(), expected.metadata.len())
            {
                return Err(io::Error::other("event metadata changed while probing"));
            }
            opened = Some((index, new));
        }
        let fd = opened.as_ref().unwrap().1.as_raw_fd();
        let offset = (offset - offset % page) as i64;
        // The kernel maps one page. The launcher queries it without reading
        // its contents, then unmaps it before proceeding to the next probe.
        let address = unsafe { mmap(ptr::null_mut(), 1, PROT_READ, MAP_PRIVATE, fd, offset) };
        if address as usize == usize::MAX {
            return Err(io::Error::last_os_error());
        }
        let mut residency = 0u8;
        let status = unsafe { mincore(address, 1, &mut residency) };
        if unsafe { munmap(address, 1) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((status == 0).then_some(residency & 1 == 0))
    })
}

#[cfg(not(target_os = "linux"))]
fn use_direct(_: &Path, _: usize) -> bool { false }

fn select(args: &[OsString], ncpu: Option<&OsStr>, probe: impl FnOnce(&Path, usize) -> bool) -> Solver {
    if let [_, data, _] = args {
        if let Some(threads) = ncpu.and_then(OsStr::to_str).and_then(|count| count.parse::<usize>().ok()) {
            if (2..=8).contains(&threads) && probe(Path::new(data), threads) {
                return Solver::Direct;
            }
        }
    }
    Solver::Mmap
}

fn exec(path: &Path, args: &[OsString]) -> io::Error {
    let mut command = Command::new(path);
    if let Some((argv0, other)) = args.split_first() {
        command.arg0(argv0).args(other);
    }
    command.exec()
}

fn main() -> io::Result<()> {
    let args: Vec<_> = env::args_os().collect();
    let exe = env::current_exe()?;
    let directory = exe.parent().ok_or_else(|| io::Error::other("launcher executable has no parent directory"))?;
    if select(&args, env::var_os("NCPU").as_deref(), use_direct) == Solver::Direct {
        // Only a failed exec returns. No solver has started in that case.
        let _ = exec(&directory.join(DIRECT_ELF), &args);
    }
    Err(exec(&directory.join(MMAP_ELF), &args))
}

#[cfg(test)]
mod launcher_tests;
