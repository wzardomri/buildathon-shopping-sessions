//! Bounded direct reads for the cold 16 MiB event wave plan.

use std::fs::{File, Metadata};
use std::path::PathBuf;

const PAGE: usize = 4096;
const ADDRESS_ALIGN: usize = 64 << 10;
const MAX_REQUEST: usize = 17 << 20;
pub const MAX_FILES: usize = 128;
pub const MAX_WORKERS: usize = 8;

pub const SUPPORTED: bool = cfg!(all(target_os = "linux", target_arch = "x86_64"));

pub struct Origin {
    pub path: PathBuf,
    pub metadata: Metadata,
}

pub struct Plan<'a> {
    pub ranges: Vec<Range<'a>>,
    pub sources: Vec<Source>,
    pub eligible: usize,
    pub opened: usize,
}

pub struct Range<'a> {
    pub mapped: &'a [u8],
    pub file: usize,
    pub offset: usize,
    pub file_len: usize,
}

impl<'a> Range<'a> {
    pub fn retain(&self, bytes: &[u8], field: &[u8]) -> &'a [u8] {
        // Full names/IDs must outlive the read buffer. The projected slice
        // borrows the original mmap at the same checked physical offset.
        let at = (field.as_ptr() as usize).checked_sub(bytes.as_ptr() as usize)
            .expect("event field starts before its direct range");
        assert_eq!(bytes.len(), self.mapped.len());
        &self.mapped[at..][..field.len()]
    }
}

pub struct Source {
    pub eligible: bool,
    fd: Option<File>,
}

impl Source {
    pub fn open(origin: &Origin, minimum: usize) -> Self {
        let eligible = origin.metadata.is_file() && origin.metadata.len() >= minimum as u64;
        let fd = if eligible { open_checked(origin) } else { None };
        Self { eligible, fd }
    }

    pub fn opened(&self) -> bool { self.fd.is_some() }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn open_checked(origin: &Origin) -> Option<File> {
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    const O_DIRECT: i32 = 0o40000;
    const O_NONBLOCK: i32 = 0o4000;
    const O_NOFOLLOW: i32 = 0o400000;
    const F_GETFL: i32 = 3;
    unsafe extern "C" { fn fcntl(fd: i32, command: i32, ...) -> i32; }

    // Source::open has already checked the original inode was regular.
    // A replacement FIFO must not block this optional open.
    let opened = OpenOptions::new().read(true).custom_flags(O_DIRECT | O_NONBLOCK | O_NOFOLLOW)
        .open(&origin.path).ok()?;
    let metadata = opened.metadata().ok()?;
    if !same_regular_file(&origin.metadata, &metadata) {
        return None;
    }
    let flags = unsafe { fcntl(opened.as_raw_fd(), F_GETFL) };
    (flags >= 0 && flags & O_DIRECT != 0).then_some(opened)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn same_regular_file(original: &Metadata, reopened: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    original.is_file() && reopened.is_file() && original.dev() == reopened.dev()
        && original.ino() == reopened.ino() && original.len() == reopened.len()
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn open_checked(_: &Origin) -> Option<File> { None }

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn supports_page_size() -> bool {
    unsafe extern "C" { fn getpagesize() -> i32; }
    (unsafe { getpagesize() }) == PAGE as i32
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
pub fn supports_page_size() -> bool { false }

#[derive(Clone, Copy)]
struct Geometry {
    offset: usize,
    skip: usize,
    required: usize,
    request: usize,
}

impl Geometry {
    fn new(offset: usize, len: usize) -> Option<Self> {
        let skip = offset % PAGE;
        let offset = offset - skip;
        let required = skip.checked_add(len)?;
        let request = required.checked_add(PAGE - 1)? / PAGE * PAGE;
        let end = offset.checked_add(request)?;
        if len == 0 || request > MAX_REQUEST || (end as u128) > i64::MAX as u128 { return None; }
        Some(Self { offset, skip, required, request })
    }

    fn accepts(self, returned: usize, file_len: usize) -> bool {
        returned >= self.required && returned <= self.request
            && (returned == self.request || self.offset + returned == file_len)
    }
}

#[derive(Default)]
pub struct AlignedBuffer {
    storage: Vec<std::mem::MaybeUninit<u8>>,
}

impl AlignedBuffer {
    pub fn read<'a>(&'a mut self, range: &Range<'_>, source: &Source) -> Option<&'a [u8]> {
        self.read_file(range, source.fd.as_ref()?)
    }

    fn read_file<'a>(&'a mut self, range: &Range<'_>, file: &File) -> Option<&'a [u8]> {
        let geometry = Geometry::new(range.offset, range.mapped.len())?;
        if self.storage.capacity() == 0 && self.storage.try_reserve_exact(MAX_REQUEST + ADDRESS_ALIGN - 1).is_err() {
            return None;
        }
        let base = self.storage.as_mut_ptr().cast::<u8>();
        let align_offset = base.align_offset(ADDRESS_ALIGN);
        if align_offset.checked_add(geometry.request).is_none_or(|end| end > self.storage.capacity()) {
            return None;
        }
        // The stride is one byte and the aligned, writable interval is
        // wholly inside the allocated capacity, even though Vec::len is 0.
        let target = unsafe { base.add(align_offset) };
        let returned = pread(file, target, geometry)?;
        // Rounded reads can end at physical EOF. Only the exact logical
        // interval is decoded, and every exposed byte must be initialized.
        if !geometry.accepts(returned, range.file_len) { return None; }
        Some(unsafe { std::slice::from_raw_parts(target.add(geometry.skip), range.mapped.len()) })
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn pread(file: &File, target: *mut u8, geometry: Geometry) -> Option<usize> {
    use std::ffi::c_void;
    use std::os::fd::AsRawFd;
    unsafe extern "C" { fn pread(fd: i32, buffer: *mut c_void, count: usize, offset: i64) -> isize; }
    for _ in 0..2 {
        // The caller supplies an aligned allocation with room for the
        // complete request and guarantees a nonnegative, representable offset.
        let result = unsafe { pread(file.as_raw_fd(), target.cast(), geometry.request, geometry.offset as i64) };
        if result >= 0 { return Some(result as usize); }
        if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted { return None; }
    }
    None
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn pread(_: &File, _: *mut u8, _: Geometry) -> Option<usize> { None }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_geometry_checks_outer_rounding_partial_eof_and_cap() {
        let unaligned = Geometry::new(PAGE + 11, PAGE + 7).unwrap();
        assert_eq!((unaligned.offset, unaligned.skip, unaligned.request, unaligned.required),
            (PAGE, 11, 2 * PAGE, PAGE + 18));
        let eof = PAGE + unaligned.required;
        assert!(unaligned.accepts(unaligned.request, eof + PAGE));
        assert!(unaligned.accepts(unaligned.required, eof));
        assert!(!unaligned.accepts(unaligned.required, eof + 1));
        assert!(!unaligned.accepts(unaligned.required - 1, eof - 1));
        assert!(!unaligned.accepts(unaligned.request + 1, eof + PAGE));
        assert_eq!(Geometry::new(0, MAX_REQUEST).unwrap().request, MAX_REQUEST);
        assert!(Geometry::new(1, MAX_REQUEST).is_none());
        assert!(Geometry::new(usize::MAX, PAGE).is_none());
        assert!(Geometry::new(0, 0).is_none());
    }

    #[test]
    fn retained_slice_survives_reusing_trimmed_storage() {
        let original = b"full id,owner-\xe6\x9d\xb1\xe4\xba\xac";
        let mut buffer = [255; 64];
        let payload = &mut buffer[7..7 + original.len()];
        payload.copy_from_slice(original);
        let range = Range { mapped: original, offset: 7, file: 0, file_len: 7 + original.len() };
        let retained_id = range.retain(payload, &payload[..7]);
        let retained_owner = range.retain(payload, &payload[8..]);
        buffer.fill(0);
        assert_eq!(retained_id, b"full id");
        assert_eq!(retained_owner, "owner-東京".as_bytes());
        assert_eq!(retained_owner.as_ptr(), original[8..].as_ptr());
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    mod linux {
        use super::*;
        use std::{fs, io};

        struct Scratch(PathBuf);
        impl Scratch {
            fn new(label: &str) -> io::Result<Self> {
                let directory = std::env::temp_dir().join(format!("shopping-direct-{label}-{}", std::process::id()));
                fs::create_dir(&directory)?;
                Ok(Self(directory))
            }
        }
        impl Drop for Scratch {
            fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
        }

        #[test]
        fn aligned_reader_accepts_only_initialized_trimmed_bytes() -> io::Result<()> {
            let scratch = Scratch::new("exact-eof")?;
            let path = scratch.0.join("file.csv");
            let contents = (0..PAGE + 119).map(|i| (i % 221) as u8).collect::<Vec<_>>();
            fs::write(&path, &contents)?;
            let file = File::open(&path)?;
            let start = PAGE + 19;
            let range = Range { mapped: &contents[start..], file: 0, offset: start, file_len: contents.len() };
            let mut buffer = AlignedBuffer::default();
            // A buffered test descriptor lets this check run even if the
            // temporary filesystem rejects O_DIRECT. The read envelope is identical.
            let bytes = buffer.read_file(&range, &file).expect("exact EOF read");
            assert_eq!(bytes, range.mapped);
            assert_eq!((bytes.as_ptr() as usize - 19) % ADDRESS_ALIGN, 0);

            let mut too_short = range;
            too_short.offset += 1;
            assert!(buffer.read_file(&too_short, &file).is_none(), "short by one must fall back");
            Ok(())
        }

        #[test]
        fn same_length_replacement_and_length_change_are_rejected() -> io::Result<()> {
            let scratch = Scratch::new("identity")?;
            let path = scratch.0.join("file.csv");
            fs::write(&path, b"original")?;
            let file = File::open(&path)?;
            let metadata = file.metadata()?;
            assert!(same_regular_file(&metadata, &File::open(&path)?.metadata()?));
            fs::rename(&path, scratch.0.join("original.csv"))?;
            fs::write(&path, b"replaced")?;
            assert!(!same_regular_file(&metadata, &File::open(&path)?.metadata()?));
            fs::write(scratch.0.join("original.csv"), b"short")?;
            assert!(!same_regular_file(&metadata, &file.metadata()?));
            Ok(())
        }

        #[test]
        fn optional_reopen_rejects_fifo_replacement() -> io::Result<()> {
            use std::ffi::{CString, c_char};
            use std::os::unix::ffi::OsStrExt;
            unsafe extern "C" { fn mkfifo(path: *const c_char, mode: u32) -> i32; }

            let scratch = Scratch::new("fifo")?;
            let path = scratch.0.join("file.csv");
            fs::write(&path, b"regular")?;
            let original = File::open(&path)?;
            let origin = Origin { path: path.clone(), metadata: original.metadata()? };
            fs::remove_file(&path)?;
            let fifo = CString::new(path.as_os_str().as_bytes()).unwrap();
            if unsafe { mkfifo(fifo.as_ptr(), 0o600) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let source = Source::open(&origin, 1);
            assert!(source.eligible);
            assert!(!source.opened());
            Ok(())
        }
    }
}
