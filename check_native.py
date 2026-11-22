"""Run unit tests and verify exec forwarding to locally compiled witnesses."""

from pathlib import Path
import os
import subprocess
import sys
from tempfile import TemporaryDirectory


HERE = Path(__file__).resolve().parent
WITNESS = """
use std::{env, process};
use std::os::unix::ffi::OsStrExt;
fn main() {
    println!("{}", process::id());
    println!("{}", env::current_exe().unwrap().file_name().unwrap().to_str().unwrap());
    println!("{}", env::var("DISPATCH_TEST_VALUE").unwrap());
    for arg in env::args_os() {
        for byte in arg.as_bytes() { print!("{byte:02x}"); }
        println!();
    }
    process::exit(37);
}
"""


def main() -> None:
    compiler, *options = sys.argv[1:]
    with TemporaryDirectory(prefix="shopping-dual-elf-check-") as directory:
        paths = Path(directory)
        executable = paths / "shopping-sessions"
        tests = paths / "launcher-tests"
        source = HERE / "launcher.rs"
        subprocess.run([compiler, "--edition=2024", "--test", "-O", *options,
                        str(source), "-o", str(tests)], check=True)
        subprocess.run([str(tests), "--test-threads=1"], check=True)
        subprocess.run([compiler, "--edition=2024", "-O", *options,
                        str(source), "-o", str(executable)], check=True)
        mmap = paths / "shopping-sessions-mmap"
        direct = paths / "shopping-sessions-direct"
        subprocess.run([compiler, "--edition=2024", "--crate-name", "witness", "-O", "-",
                        "-o", str(mmap)], input=WITNESS, text=True, check=True)
        os.link(mmap, direct)
        arguments = [b"arbitrary launcher argv0", os.fsencode(paths) + b"/missing-\xff-data", b"raw-out-\xfe"]
        environment = dict(os.environ, NCPU="8", DISPATCH_TEST_VALUE="untouched", PATH="/missing-command-path")
        with subprocess.Popen(arguments, executable=executable, cwd="/", env=environment,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:
            stdout, stderr = child.communicate()
            assert child.returncode == 37, (child.returncode, stderr)
            assert stdout.decode().splitlines() == [str(child.pid), "shopping-sessions-mmap", "untouched",
                                                   *(arg.hex() for arg in arguments)]
        print("exact fallback preserved PID, raw argv, environment and exit status")


if __name__ == "__main__":
    main()
