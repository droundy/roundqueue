#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;
extern crate hostname;
extern crate unix_daemonize;
extern crate num_cpus;
extern crate notify;
extern crate libc;

use notify::{Watcher};

use std::path::{Path,PathBuf};
use std::time::{SystemTime,Duration,UNIX_EPOCH};
use std::io::{Result,Write,Read};

use std::os::unix::io::{FromRawFd,IntoRawFd};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Running {
    pub started: Duration,
    pub node: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Job {
    pub home_dir: PathBuf,
    pub directory: PathBuf,
    pub command: Vec<String>,
    pub jobname: String,
    pub output: PathBuf,
    pub submitted: Duration,
    pub running: Option<Running>,
}

impl Job {
    pub fn new(cmd: Vec<String>, jobname: String, output: PathBuf) -> Result<Job> {
        Ok(Job {
            directory: std::env::current_dir()?,
            home_dir: std::env::home_dir().unwrap(),
            command: cmd,
            jobname: jobname,
            output: output,
            submitted: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            running: None,
        })
    }
    fn filename(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}.job",
                              self.submitted.as_secs(), self.submitted.subsec_nanos()))
    }
    fn filepath(&self, subdir: &Path) -> PathBuf {
        self.home_dir.join(RQ).join(subdir).join(self.filename())
    }
    fn is_running_on(&self, host: &str) -> bool {
        if let Some(ref r) = self.running {
            return r.node == host
        }
        false
    }
    fn read(fname: &Path) -> Result<Job> {
        let mut f = std::fs::File::open(fname)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        match serde_json::from_slice::<Job>(&data) {
            Ok(job) => {
                if job.command.len() == 0 {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "empty cmd?"))
                } else {
                    Ok(job)
                }
            },
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
    fn save(&self, subdir: &Path) -> Result<()> {
        let mut f = std::fs::File::create(self.filepath(subdir))?;
        f.write_all(&serde_json::to_string(self).unwrap().as_bytes())
    }
    fn change_status(&self, old_subdir: &Path, new_subdir: &Path) -> Result<()> {
        std::fs::rename(self.filepath(old_subdir), self.filepath(new_subdir))
    }
    pub fn submit(&self) -> Result<()> {
        self.ensure_directories()?;
        self.save(Path::new(WAITING))
    }
    fn ensure_directories(&self) -> Result<()> {
        std::fs::create_dir_all(&self.home_dir.join(RQ).join(WAITING))?;
        std::fs::create_dir_all(&self.home_dir.join(RQ).join(RUNNING))?;
        std::fs::create_dir_all(&self.home_dir.join(RQ).join(COMPLETED))
    }
}

const RQ: &'static str = ".roundqueue";
const RUNNING: &'static str = "RUNNING";
const WAITING: &'static str = "WAITING";
const COMPLETED: &'static str = "COMPLETED";

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Status {
    pub waiting: Vec<Job>,
    pub running: Vec<Job>,
    pub completed: Vec<Job>,
}

impl Status {
    pub fn new() -> Result<Status> {
        let mut status = Status {
            waiting: Vec::new(),
            running: Vec::new(),
            completed: Vec::new(),
        };
        for userdir in std::fs::read_dir("/home")? {
            if let Ok(userdir) = userdir {
                let rqdir = userdir.path().join(RQ);
                // eprintln!("{:?}", rqdir);
                if let Ok(rr) = rqdir.join(RUNNING).read_dir() {
                    for run in rr.flat_map(|r| r.ok()) {
                        if let Ok(j) = Job::read(&run.path()) {
                            status.running.push(j);
                        } else {
                            eprintln!("Error reading {:?}", run.path());
                        }
                    }
                }
                if let Ok(rr) = rqdir.join(WAITING).read_dir() {
                    for run in rr.flat_map(|r| r.ok()) {
                        if let Ok(j) = Job::read(&run.path()) {
                            status.waiting.push(j);
                        } else {
                            eprintln!("Error reading {:?}", run.path());
                        }
                    }
                }
                if let Ok(rr) = rqdir.join(COMPLETED).read_dir() {
                    for run in rr.flat_map(|r| r.ok()) {
                        if let Ok(j) = Job::read(&run.path()) {
                            status.completed.push(j);
                        } else {
                            eprintln!("Error reading {:?}", run.path());
                        }
                    }
                }
            }
        }
        Ok(status)
    }
    /// run_next consumes the status to enforce that we must re-read
    /// the status before attempting anything else.  This is because
    /// another node may have run something or submitted something.
    fn run_next(self, host: &str) {
        let waiting: Vec<_> =  self.waiting.iter().map(|j| j.home_dir.clone()).collect();
        if waiting.len() == 0 { return; }
        let mut next_homedir = waiting[0].clone();
        let mut least_running = 100000;
        for hd in waiting.into_iter() {
            let count = self.running.iter().filter(|j| &j.home_dir == &hd).count();
            if count < least_running {
                next_homedir = hd;
                least_running = count;
            }
        }
        let mut job = self.waiting[0].clone();
        let mut earliest_submitted = job.submitted;
        for j in self.waiting.into_iter() {
            if j.home_dir == next_homedir && j.submitted < earliest_submitted {
                earliest_submitted = j.submitted;
                job = j;
            }
        }
        println!("starting {:?}", &job.jobname);
        let f = match std::fs::OpenOptions::new()
            .append(true).open(job.directory.join(&job.output)) {
                Ok(f) => f,
                Err(e) => {
                    println!("Error creating output {:?}: {}",
                             job.directory.join(&job.output), e);
                    return;
                },
            };
        if let Err(e) = job.change_status(Path::new(WAITING), Path::new(RUNNING)) {
            println!("Unable to change status of job {} ({})", &job.jobname, e);
            return;
        }
        job.running = Some(Running {
            started: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            node: String::from(host),
        });
        if let Err(e) = job.save(Path::new(RUNNING)) {
            println!("Yikes, unable to save job? {}", e);
            std::process::exit(1);
        }
        std::thread::spawn(move || {
            let mut cmd = std::process::Command::new(&job.command[0]);
            let fd = f.into_raw_fd();
            let stderr = unsafe {
                std::process::Stdio::from_raw_fd(fd)
            };
            let stdout = unsafe {
                std::process::Stdio::from_raw_fd(fd)
            };
            cmd.args(&job.command[1..]).current_dir(&job.directory)
                .stderr(stderr)
                .stdout(stdout)
                .stdin(std::process::Stdio::null());
            match cmd.status() {
                Err(e) => {
                    println!("Error running {:?}: {}", job.command, e);
                },
                Ok(st) => {
                    println!("Done running {:?}: {}", job.jobname, st);
                }
            }
            if let Err(e) = job.change_status(Path::new(RUNNING), Path::new(COMPLETED)) {
                println!("Unable to change status of completed job {} ({})",
                         &job.jobname, e);
                return;
            }
        });
    }
}

pub fn spawn_runner() -> Result<()> {
    let home = std::env::home_dir().unwrap();
    let host = hostname::get_hostname().unwrap();

    if let Ok(pid_old) = read_pid(&home.join(RQ).join(&host)) {
        unsafe { libc::kill(pid_old, libc::SIGTERM); }
        std::thread::sleep(Duration::from_secs(2));
        unsafe { libc::kill(pid_old, libc::SIGKILL); }
    }
    let cpus = num_cpus::get_physical();
    println!("I am spawning a runner for {} with {} cpus in {:?}!",
             &host, cpus, &home);
    unix_daemonize::daemonize_redirect(
        Some(home.join(RQ).join("log")),
        Some(home.join(RQ).join("log")),
        unix_daemonize::ChdirMode::ChdirRoot).unwrap();
    write_pid(&home.join(RQ).join(&host))?;
    println!("==================\nRestarting runner!");
    let (notify_tx, notify_rx) = std::sync::mpsc::channel();
    let mut watcher =
        notify::watcher(notify_tx,
                        std::time::Duration::from_secs(1)).unwrap();
    watcher.watch(home.join(RQ).join(RUNNING),
                  notify::RecursiveMode::NonRecursive).ok();
    watcher.watch(home.join(RQ).join(WAITING),
                  notify::RecursiveMode::NonRecursive).ok();
    let mut old_status = Status::new().unwrap();
    loop {
        let status = Status::new().unwrap();
        let running = status.running.iter().filter(|j| j.is_running_on(&host)).count();
        let waiting = status.waiting.iter().count();
        if old_status != status {
            println!("Currently using {}/{} cores, with {} jobs waiting.",
                     running, cpus, waiting);
        }
        old_status = status.clone();
        if cpus > running && waiting > 0 {
            status.run_next(&host);
        }
        notify_rx.recv().unwrap();
    }
}

fn read_pid(fname: &Path) -> Result<libc::pid_t> {
    let mut f = std::fs::File::open(fname)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;
    match serde_json::from_slice::<libc::pid_t>(&data) {
        Ok(pid) => Ok(pid),
        Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    }
}

fn write_pid(fname: &Path) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let mut f = std::fs::File::create(fname)?;
    f.write_all(&serde_json::to_string(&pid).unwrap().as_bytes())
}
