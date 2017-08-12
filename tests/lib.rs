
use std::io::{Write, Read};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new<P: AsRef<std::path::Path>> (p: P) -> TempDir {
        let here = std::env::current_dir().unwrap();
        let p = here.join(p);
        println!("remove test repository");
        std::fs::remove_dir_all(&p).ok();
        println!("create {:?}", &p);
        assert!(std::fs::create_dir_all(&p).is_ok());
        TempDir(std::path::PathBuf::from(&p))
    }
    fn rq(&self, args: &[&str]) -> std::process::Output {
        let newpath =
            match std::env::var_os("PATH") {
                Some(paths) => {
                    let mut new_paths = vec![location_of_executables()];
                    for path in std::env::split_paths(&paths) {
                        new_paths.push(path);
                    }
                    std::env::join_paths(new_paths).unwrap()
                }
                None => {
                    println!("PATH is not defined in the environment.");
                    std::env::join_paths(&[std::env::current_dir().unwrap()
                                           .join("target/debug")]).unwrap()
                },
            };
        println!("PATH is {:?}", &newpath);
        println!("HOME is {:?}", &self.0);
        let s = std::process::Command::new("rq").args(args).env("PATH", newpath)
            .env("HOME", &self.0)
            .current_dir(&self.0).output();
        println!("I am in {:?} with args {:?}", std::env::current_dir(), args);
        if !s.is_ok() {
            println!("Bad news: {:?}", s);
            println!("  exists:: {:?}", std::path::Path::new("target/debug/fac").exists());
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
        while b" \n\r".contains(&actual_contents[actual_contents.len()-1]) {
            actual_contents.pop();
        }
        let mut contents = Vec::from(contents);
        while b" \n\r".contains(&contents[contents.len()-1]) {
            contents.pop();
        }
        assert_eq!(std::str::from_utf8(actual_contents.as_slice()),
                   std::str::from_utf8(&contents));
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
    let tempdir = TempDir::new(&format!("tests/temp-homes/test-{}", line!()));
    tempdir.rq(&["--version"]);
}

#[test]
fn rq_invalid_exe() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/test-{}", line!()));
    let out = tempdir.rq(&["run", "path/to/garbage"]);
    assert!(!out.status.success());
}

#[test]
fn rq_jobname_gives_default_output() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/test-{}", line!()));
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
fn rq_run_with_flags() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/test-{}", line!()));
    let out = tempdir.rq(&["run", "echo", "-n", "hello world"]);
    assert!(out.status.success());
}

#[test]
fn rq_run_with_dash_dash_flags() {
    let tempdir = TempDir::new(&format!("tests/temp-homes/test-{}", line!()));
    let out = tempdir.rq(&["run", "--", "echo", "-n", "hello world"]);
    assert!(out.status.success());
    tempdir.no_such_file("hello world I just want to silence a warning");
}