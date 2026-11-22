use super::*;

struct Scratch(PathBuf);
impl Scratch {
    fn new(label: &str) -> io::Result<Self> {
        let path = env::temp_dir().join(format!("shopping-sessions-{label}-{}", std::process::id()));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Scratch {
    fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
}

#[test]
fn caps_and_ordered_midpoints_match_the_original_gate() {
    let threshold = COLD_CHUNK_BYTES * 12;
    let mut sizes = [0, threshold, 0, threshold, 0];
    let plan = event_ranges(&sizes, 2).unwrap();
    let stride = sizes.iter().sum::<u64>() / 16;
    for (n, &(file, offset)) in plan.iter().enumerate() {
        assert_eq!(file, if n < 8 { 1 } else { 3 });
        assert_eq!(offset, stride / 2 + (n as u64 % 8) * stride);
    }
    sizes[1] -= 1;
    assert!(event_ranges(&sizes, 2).is_none());
    let mut at_cap = [MAX_EVENT_BYTES / 8; 8];
    assert!(event_ranges(&at_cap, 8).is_some());
    at_cap[7] += 1;
    assert!(event_ranges(&at_cap, 8).is_none());
    assert!(event_ranges(&[MAX_EVENT_BYTES, 1], 2).is_none());
    assert!(event_ranges(&[u64::MAX, 1], 2).is_none());
    assert!(event_ranges(&[threshold; 2], 1).is_none());
    assert!(event_ranges(&[threshold; 2], 9).is_none());
    let mut files = [0; MAX_EVENT_FILES + 1];
    files[..2].fill(threshold);
    assert!(event_ranges(&files[..MAX_EVENT_FILES], 2).is_some());
    assert!(event_ranges(&files, 2).is_none());

    let unequal = [0, threshold + 7, COLD_CHUNK_BYTES + 1, threshold * 2 + 13, 0];
    let plan = event_ranges(&unequal, 2).unwrap();
    let stride = unequal.iter().sum::<u64>() / SAMPLES as u64;
    for (n, &(file, offset)) in plan.iter().enumerate() {
        assert!(offset < unequal[file]);
        assert_eq!(unequal[..file].iter().sum::<u64>() + offset, stride / 2 + n as u64 * stride);
    }
}

#[test]
fn partial_and_failed_probe_thresholds_are_exact() {
    let probes = |cold, warm| {
        let mut index = 0;
        cold_probes([(0, 0); SAMPLES], |_, _| {
            index += 1;
            Ok(if index <= cold { Some(true) } else if index <= cold + warm { Some(false) } else { None })
        })
    };
    assert!(probes(16, 0));
    assert!(probes(12, 0));
    assert!(!probes(12, 1));
    assert!(!probes(9, 3));
    assert!(!probes(11, 5));
    assert!(!probes(11, 0));
    assert!(!probes(8, 4));
    let mut n = 0;
    assert!(!cold_probes([(0, 0); SAMPLES], |_, _| {
        n += 1;
        if n == 3 { Err(io::Error::other("unreliable namespace")) } else { Ok(Some(true)) }
    }));
    let mut n = 0;
    assert!(!cold_probes([(0, 0); SAMPLES], |_, _| {
        n += 1;
        if n == 4 { Ok(Some(false)) } else { Ok(Some(true)) }
    }));
    assert_eq!(n, 4, "a resident sample should stop the warm preflight immediately");
}

#[test]
fn metadata_uses_sorted_csv_names_and_rejects_unreliable_namespace() -> io::Result<()> {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new("namespace")?;
    fs::write(scratch.0.join("z.csv"), b"zz")?;
    fs::write(scratch.0.join("a.csv"), b"a")?;
    fs::write(scratch.0.join("ignored.CSV"), b"ignore")?;
    let files = event_files(&scratch.0).unwrap();
    let names: Vec<_> = files.iter().map(|file| (file.path.file_name().unwrap(), file.metadata.len())).collect();
    assert_eq!(names, [(OsStr::new("a.csv"), 1), (OsStr::new("z.csv"), 2)]);
    assert!(event_files(&scratch.0.join("missing")).is_none());
    let alias = scratch.0.join("alias");
    symlink(&scratch.0, &alias)?;
    assert!(event_files(&alias).is_none());
    let wrong_type = scratch.0.join("bad.csv");
    fs::create_dir(&wrong_type)?;
    assert!(event_files(&scratch.0).is_none());
    fs::remove_dir(wrong_type)?;
    symlink(scratch.0.join("a.csv"), scratch.0.join("bad.csv"))?;
    assert!(event_files(&scratch.0).is_none());
    Ok(())
}

#[test]
fn selector_defaults_to_scored_and_only_probes_supported_invocations() {
    use std::os::unix::ffi::OsStrExt;
    let args = [OsString::from("original dispatch"), OsString::from("/some/data"), OsString::from("/some/output")];
    for ncpu in [None, Some(b"0".as_slice()), Some(b"1"), Some(b"9"), Some(b"eight"), Some(b"\xff")] {
        assert_eq!(select(&args, ncpu.map(OsStr::from_bytes), |_, _| panic!("unexpected probe")), Solver::Mmap);
    }
    for invocation in [&args[..0], &args[..2]] {
        assert_eq!(select(invocation, Some(OsStr::new("8")), |_, _| panic!("unexpected probe")), Solver::Mmap);
    }
    assert_eq!(select(&args, Some(OsStr::new("8")), |path, n| {
        assert_eq!((path, n), (Path::new("/some/data"), 8));
        true
    }), Solver::Direct);
    assert_eq!(select(&args, Some(OsStr::new("2")), |_, _| false), Solver::Mmap);
}
