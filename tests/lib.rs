extern crate num_cpus;

use std::io::{Read, Write};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new<P: AsRef<std::path::Path>>(p: P) -> TempDir {
        let here = std::env::current_dir().unwrap();
        let p = here.join(p);
        println!("remove test repository");
        std::fs::remove_dir_all(&p).ok(); // ignore failure to remove directory: it might not exist
        println!("create {:?}", &p);
        assert!(std::fs::create_dir_all(&p).is_ok());
        TempDir(std::path::PathBuf::from(&p))
    }
    fn rq(&self, args: &[&str]) -> std::process::Output {
        let newpath = match std::env::var_os("PATH") {
            Some(paths) => {
                let mut new_paths = vec![location_of_executables()];
                for path in std::env::split_paths(&paths) {
                    new_paths.push(path);
                }
                std::env::join_paths(new_paths).unwrap()
            }
            None => {
                println!("PATH is not defined in the environment.");
                std::env::join_paths(&[std::env::current_dir().unwrap().join("target/debug")])
                    .unwrap()
            }
        };
        // println!("PATH is {:?}", &newpath);
        // println!("HOME is {:?}", &self.0);
        let s = std::process::Command::new("rq")
            .args(args)
            .env("PATH", newpath)
            .env("HOME", &self.0)
            .current_dir(&self.0)
            .output();
        println!("I am in {:?} with args {:?}", std::env::current_dir(), args);
        if !s.is_ok() {
            println!("Bad news: {:?}", s);
            println!(
                "  exists:: {:?}",
                std::path::Path::new("target/debug/fac").exists()
            );
            for x in std::path::Path::new("target/debug").read_dir().unwrap() {
                println!("  target/debug has {:?}", x);
            }
        } else {
            let s = s.unwrap();
            println!("output is:\n{}", String::from_utf8_lossy(&s.stdout));
            println!("stderr is:\n{}", String::from_utf8_lossy(&s.stderr));
            return s;
        }
        s.unwrap()
    }
    fn add_file(&self, p: &str, contents: &[u8]) {
        let absp = self.0.join(p);
        let mut f = std::fs::File::create(absp).unwrap();
        f.write(contents).unwrap();
    }
    fn expect_file(&self, p: &str, contents: &[u8]) {
        let absp = self.0.join(p);
        let mut f = std::fs::File::open(absp).unwrap();
        let mut actual_contents = Vec::new();
        f.read_to_end(&mut actual_contents).unwrap();
        while b" \n\r".contains(&actual_contents[actual_contents.len() - 1]) {
            actual_contents.pop();
        }
        let mut contents = Vec::from(contents);
        while b" \n\r".contains(&contents[contents.len() - 1]) {
            contents.pop();
        }
        assert_eq!(
            std::str::from_utf8(actual_contents.as_slice()),
            std::str::from_utf8(&contents)
        );
    }
    fn file_contains(&self, p: &str, pattern: &[u8]) {
        let absp = self.0.join(p);
        let mut f = std::fs::File::open(absp).unwrap();
        let mut contents = Vec::new();
        f.read_to_end(&mut contents).unwrap();
        for i in 0..contents.len() - pattern.len() {
            if &contents[i..i + pattern.len()] == pattern {
                println!("found {:?}", String::from_utf8_lossy(pattern));
                return;
            }
        }
        println!("no such pattern: {:?}", String::from_utf8_lossy(pattern));
        println!("in file: {:?}", std::str::from_utf8(&contents));
        panic!("could not find pattern");
    }
    fn file_does_not_contain(&self, p: &str, pattern: &[u8]) {
        let absp = self.0.join(p);
        let mut f = std::fs::File::open(absp).unwrap();
        let mut contents = Vec::new();
        f.read_to_end(&mut contents).unwrap();
        for i in 0..contents.len() - pattern.len() {
            if &contents[i..i + pattern.len()] == pattern {
                panic!("found unwanted {:?}", String::from_utf8_lossy(pattern));
            }
        }
        println!(
            "no unwanted pattern: {:?}",
            String::from_utf8_lossy(pattern)
        );
        println!("in file: {:?}", std::str::from_utf8(&contents));
    }
    fn no_such_file(&self, p: &str) {
        let absp = self.0.join(p);
        assert!(!absp.exists());
    }
    fn file_exists(&self, p: &str) {
        println!("checking for existence of {}", p);
        let absp = self.0.join(p);
        assert!(absp.exists());
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok(); // ignore errors that might happen on windows
    }
}

fn location_of_executables() -> std::path::PathBuf {
    // The key here is that this test executable is located in almost
    // the same place as the built `fac` is located.
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // chop off exe name
    path.pop(); // chop off "deps"
    path
}

#[test]
fn rq_version() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    tempdir.rq(&["--version"]);
}

#[test]
fn rq_invalid_exe() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let out = tempdir.rq(&["run", "path/to/garbage"]);
    assert!(!out.status.success());
}

#[test]
fn rq_jobname_gives_default_output() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let out = tempdir.rq(&["daemon"]);
    assert!(out.status.success());
    let out = tempdir.rq(&["run", "-J", "goodname", "echo", "hello world"]);
    assert!(out.status.success());
    std::thread::sleep(std::time::Duration::from_secs(1));
    let out = tempdir.rq(&[]);
    assert!(out.status.success());
    tempdir.file_exists("goodname.out");
}

#[test]
fn rq_max_output_enforced() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let out = tempdir.rq(&["daemon"]);
    assert!(out.status.success());
    let out = tempdir.rq(&[
        "run",
        "-J",
        "goodname",
        "--max-output=1e-6",
        "sh",
        "-c",
        "echo hello world && sleep 3 && echo goodbye world",
    ]);
    assert!(out.status.success());
    std::thread::sleep(std::time::Duration::from_secs(5));
    let out = tempdir.rq(&[]);
    assert!(out.status.success());
    tempdir.file_exists("goodname.out");
    tempdir.file_contains("goodname.out", b"created too large an output");
    tempdir.file_does_not_contain("goodname.out", b"goodbye world\n");
}

#[test]
fn rq_run_with_flags() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let out = tempdir.rq(&["run", "echo", "-n", "hello world"]);
    assert!(out.status.success());
}

#[test]
fn rq_run_with_dash_dash_flags() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let out = tempdir.rq(&["run", "--", "echo", "-n", "hello world"]);
    assert!(out.status.success());
    tempdir.no_such_file("hello world I just want to silence a warning");
}

#[test]
fn rq_restart_daemon_while_job_is_running() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let out = tempdir.rq(&["daemon"]);
    assert!(out.status.success());
    let out = tempdir.rq(&["run", "sh", "-c", "sleep 2 && echo hello world > greeting"]);
    assert!(out.status.success());
    std::thread::sleep(std::time::Duration::from_secs(1));
    let out = tempdir.rq(&["daemon"]);
    assert!(out.status.success());
    let out = tempdir.rq(&["run", "sh", "-c", "echo goodbye world > farewell"]);
    assert!(out.status.success());
    std::thread::sleep(std::time::Duration::from_secs(3));
    tempdir.file_exists("greeting");
    tempdir.file_exists("farewell");
}

#[test]
fn do_not_overload_cpu() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let cpus = num_cpus::get_physical();
    let out = tempdir.rq(&["daemon"]);
    assert!(out.status.success());
    for _ in 0..cpus {
        let out = tempdir.rq(&["run", "sleep", "10"]);
        assert!(out.status.success());
    }
    println!("This next job won't run because all the cpus are busy sleeping.");
    let out = tempdir.rq(&["run", "sh", "-c", "echo hello world > greeting"]);
    assert!(out.status.success());
    let out = tempdir.rq(&[]);
    assert!(out.status.success());
    tempdir.no_such_file("greeting");
}

#[test]
fn cancel_by_jobname() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    assert!(tempdir.rq(&["daemon"]).status.success());
    assert!(tempdir
        .rq(&[
            "run",
            "-J",
            "greet",
            "sh",
            "-c",
            "sleep 2 && echo hello > greeting"
        ])
        .status
        .success());
    assert!(tempdir
        .rq(&[
            "run",
            "-J",
            "hello",
            "sh",
            "-c",
            "sleep 2 && echo hello > hello"
        ])
        .status
        .success());
    assert!(tempdir
        .rq(&["cancel", "--job-name", "greet"])
        .status
        .success());
    assert!(tempdir.rq(&[]).status.success());
    std::thread::sleep(std::time::Duration::from_secs(3));
    tempdir.no_such_file("greeting");
    tempdir.file_exists("hello");
}

#[test]
fn cancel_all() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    assert!(tempdir.rq(&["daemon"]).status.success());
    assert!(tempdir
        .rq(&[
            "run",
            "-J",
            "greet",
            "sh",
            "-c",
            "sleep 3 && echo hello > greeting"
        ])
        .status
        .success());
    assert!(tempdir
        .rq(&[
            "run",
            "-J",
            "hello",
            "sh",
            "-c",
            "sleep 3 && echo hello > hello"
        ])
        .status
        .success());
    assert!(tempdir.rq(&["cancel", "--all"]).status.success());
    std::thread::sleep(std::time::Duration::from_secs(5));
    tempdir.no_such_file("greeting");
    tempdir.no_such_file("hello");
}

#[test]
fn cancel_waiting() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/home-{}/user", line!()));
    let cpus = num_cpus::get_physical();
    assert!(tempdir.rq(&["daemon"]).status.success());
    assert!(tempdir
        .rq(&[
            "run",
            "-J",
            "greet",
            "sh",
            "-c",
            "sleep 2 && echo hello > greeting"
        ])
        .status
        .success());
    for _ in 0..cpus - 1 {
        assert!(tempdir.rq(&["run", "sleep", "2"]).status.success());
    }
    assert!(tempdir
        .rq(&["run", "-J", "hello", "sh", "-c", "hello > hello"])
        .status
        .success());
    assert!(tempdir.rq(&["daemon"]).status.success());
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(tempdir
        .rq(&["cancel", "--all", "--waiting"])
        .status
        .success());
    std::thread::sleep(std::time::Duration::from_secs(2));
    tempdir.file_exists("greeting");
    tempdir.no_such_file("hello");
}

#[test]
fn polite_users_share_cpus() {
    let home = format!("tests/temp-homes/home-{}", line!());
    let rude = TempDir::new(&format!("{}/rude", &home));
    let polite = TempDir::new(&format!("{}/polite", &home));
    let cpus = num_cpus::get_physical();
    if num_cpus::get() <= cpus {
        println!("this tests requires hyperthreading!");
        return;
    }
    assert!(rude.rq(&["daemon", "--fg"]).status.success());
    assert!(polite.rq(&["daemon", "--fg"]).status.success());
    for _ in 0..2 * cpus {
        assert!(rude.rq(&["run", "sleep", "100"]).status.success());
    }
    assert!(rude.rq(&["daemon", "--fg"]).status.success());
    println!("This next job won't run because all the cpus are busy sleeping.");
    assert!(rude
        .rq(&["run", "sh", "-c", "echo hello world > greeting"])
        .status
        .success());
    assert!(polite
        .rq(&["run", "sh", "-c", "echo hello > greeting"])
        .status
        .success());
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(rude.rq(&["daemon", "--fg"]).status.success());
    assert!(polite.rq(&["daemon", "--fg"]).status.success());
    assert!(rude.rq(&[]).status.success());
    assert!(polite.rq(&[]).status.success());
    std::thread::sleep(std::time::Duration::from_secs(10));
    assert!(rude.rq(&[]).status.success());
    assert!(polite.rq(&[]).status.success());
    assert!(rude.rq(&["nodes"]).status.success());
    assert!(rude.rq(&["users"]).status.success());
    assert!(polite.rq(&["daemon", "--fg"]).status.success());
    rude.no_such_file("greeting");
    polite.file_exists("greeting");
}

#[test]
fn zombie_jobs_disappear() {
    let home = format!("tests/temp-homes/home-{}", line!());
    let user = TempDir::new(&format!("{}/user", &home));
    assert!(user.rq(&["daemon"]).status.success());
    assert!(user.rq(&["run", "sleep", "100"]).status.success());

    for j in user
        .0
        .join(".roundqueue/running")
        .read_dir()
        .unwrap()
        .flat_map(|r| r.ok())
    {
        std::fs::copy(
            user.0.join(".roundqueue/running").join(j.path()),
            user.0.join(".roundqueue/running").join("bogus"),
        )
        .unwrap();
    }

    assert!(user.rq(&[]).status.success());
    user.no_such_file(".roundqueue/running/bogus");
}
