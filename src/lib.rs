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
use std::collections::HashSet;

use std::os::unix::io::{FromRawFd,IntoRawFd};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RunningJob {
    pub job: Job,
    pub started: Duration,
    pub node: String,
    pub pid: u32,
}

impl RunningJob {
    pub fn duration(&self) -> Duration {
        let dur = now();
        if let Some(t) = dur.checked_sub(self.started) {
            t
        } else {
            Duration::from_secs(0)
        }
    }
    fn completed(&self) -> Result<()> {
        std::fs::rename(self.job.filepath(Path::new(RUNNING)),
                        self.job.filepath(Path::new(COMPLETED)))
    }
    pub fn cancel(&self) -> Result<()> {
        std::fs::rename(self.job.filepath(Path::new(RUNNING)),
                        self.job.filepath(Path::new(CANCELING)))
    }
    fn canceled(&self) -> Result<()> {
        std::fs::rename(self.job.filepath(Path::new(CANCELING)),
                        self.job.filepath(Path::new(CANCELED)))
    }
    fn read(fname: &Path) -> Result<RunningJob> {
        let mut f = std::fs::File::open(fname)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        match serde_json::from_slice::<RunningJob>(&data) {
            Ok(job) => {
                if job.job.command.len() == 0 {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "empty cmd?"))
                } else {
                    Ok(job)
                }
            },
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
    fn save(&self, subdir: &Path) -> Result<()> {
        let mut f = std::fs::File::create(self.job.filepath(subdir))?;
        f.write_all(&serde_json::to_string(self).unwrap().as_bytes())
    }
    pub fn kill(&self) {
        unsafe { libc::kill(self.pid as i32, libc::SIGTERM); }
        std::thread::sleep(Duration::from_secs(2));
        unsafe { libc::kill(self.pid as i32, libc::SIGKILL); }
        self.completed().ok();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Job {
    pub home_dir: PathBuf,
    pub directory: PathBuf,
    pub command: Vec<String>,
    pub jobname: String,
    pub output: PathBuf,
    pub submitted: Duration,
}

impl Job {
    pub fn new(cmd: Vec<String>, jobname: String, output: PathBuf) -> Result<Job> {
        Ok(Job {
            directory: std::env::current_dir()?,
            home_dir: std::env::home_dir().unwrap(),
            command: cmd,
            jobname: jobname,
            output: output,
            submitted: now(),
        })
    }
    pub fn cancel(&self) -> Result<()> {
        std::fs::rename(self.filepath(Path::new(WAITING)),
                        self.filepath(Path::new(CANCELED)))
    }
    pub fn wait_duration(&self) -> Duration {
        let dur = now();
        if let Some(t) = dur.checked_sub(self.submitted) {
            t
        } else {
            Duration::from_secs(0)
        }
    }
    fn filename(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}.job",
                              self.submitted.as_secs(), self.submitted.subsec_nanos()))
    }
    fn filepath(&self, subdir: &Path) -> PathBuf {
        self.home_dir.join(RQ).join(subdir).join(self.filename())
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
        ensure_directories()
    }
}

const RQ: &'static str = ".roundqueue";
const RUNNING: &'static str = "running";
const WAITING: &'static str = "waiting";
const COMPLETED: &'static str = "completed";
const CANCELED: &'static str = "canceled";
const CANCELING: &'static str = "cancel";
const POLLING_TIME: u64 = 60;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Status {
    pub homedirs_sharing_host: Vec<PathBuf>,
    pub waiting: Vec<Job>,
    pub running: Vec<RunningJob>,
    //pub completed: Vec<RunningJob>,
}

impl Status {
    pub fn new() -> Result<Status> {
        let mut status = Status {
            homedirs_sharing_host: Vec::new(),
            waiting: Vec::new(),
            running: Vec::new(),
            // completed: Vec::new(),
        };
        let root_home = PathBuf::from("/home");
        let host = hostname::get_hostname().unwrap();
        // Look for all the jobs!
        for userdir in std::fs::read_dir("/home")? {
            if let Ok(userdir) = userdir {
                let rqdir = userdir.path().join(RQ);
                // First check whether this user is running a
                // roundqueue daemon on the same host we are using.
                if let Ok(pid) = read_pid(&rqdir.join(&host)) {
                    if pid_exists(pid) {
                        status.homedirs_sharing_host.push(root_home.join(userdir.path()));
                        // println!("found homedir {:?}", root_home.join(userdir.path()));
                    } else {
                        // println!("no daemon: {:?}", root_home.join(userdir.path()));
                    }
                } else {
                    // println!("no lockfile: {:?}", root_home.join(userdir.path()));
                }
                // eprintln!("{:?}", rqdir);
                if let Ok(rr) = rqdir.join(RUNNING).read_dir() {
                    for run in rr.flat_map(|r| r.ok()) {
                        if let Ok(j) = RunningJob::read(&run.path()) {
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
                // if let Ok(rr) = rqdir.join(COMPLETED).read_dir() {
                //     for run in rr.flat_map(|r| r.ok()) {
                //         if let Ok(j) = RunningJob::read(&run.path()) {
                //             status.completed.push(j);
                //         } else {
                //             eprintln!("Error reading {:?}", run.path());
                //         }
                //     }
                // }
            }
        }
        Ok(status)
    }
    /// run_next consumes the status to enforce that we must re-read
    /// the status before attempting anything else.  This is because
    /// another node may have run something or submitted something.
    fn run_next(self, host: &str, home_dir: &Path) {
        // "waiting" is the set of jobs that are waiting to run *on
        // this host*.  Thus this ignores any waiting jobs that cannot
        // run on this host because their user does not have a daemon
        // running on this host.
        let waiting: Vec<_> =  self.waiting.iter()
            .filter(|j| self.homedirs_sharing_host.contains(&j.home_dir))
            .map(|j| j.home_dir.clone()).collect();
        if waiting.len() == 0 { return; }
        let mut next_homedir = HashSet::new();
        let mut least_running = 100000;
        for hd in waiting.into_iter() {
            let count = self.running.iter().filter(|j| &j.job.home_dir == &hd).count();
            if count < least_running {
                next_homedir.insert(hd);
                least_running = count;
            }
        }
        if !next_homedir.contains(home_dir) {
            return;
        }
        let mut job = self.waiting[0].clone();
        let mut earliest_submitted = job.submitted;
        for j in self.waiting.into_iter() {
            if next_homedir.contains(&j.home_dir) && j.submitted < earliest_submitted {
                earliest_submitted = j.submitted;
                job = j;
            }
        }
        if &job.home_dir != home_dir {
            return;
        }
        println!("starting {:?}", &job.jobname);
        let mut f = match std::fs::OpenOptions::new()
            .create(true)
            .append(true).open(job.directory.join(&job.output)) {
                Ok(f) => f,
                Err(e) => {
                    println!("Error creating output {:?}: {}",
                             job.directory.join(&job.output), e);
                    return;
                },
            };
        if let Err(e) = writeln!(f, "::::: Starting job {:?} on {}", &job.jobname, &host) {
            println!("Error writing to output {:?}: {}",
                     job.directory.join(&job.output), e);
            return;
        }
        if let Err(e) = job.change_status(Path::new(WAITING), Path::new(RUNNING)) {
            println!("Unable to change status of job {} ({})", &job.jobname, e);
            return;
        }

        // First spawn the child...
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
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                println!("Unable to spawn child: {}", e);
                // runningjob.failed().ok();
                return;
            },
        };

        let runningjob = RunningJob {
            started: now(),
            node: String::from(host),
            job: job,
            pid: child.id(),
        };
        if let Err(e) = runningjob.save(Path::new(RUNNING)) {
            println!("Yikes, unable to save job? {}", e);
            std::process::exit(1);
        }
        // let logpath = Some(home_dir.join(RQ).join(&host).with_extension("log"));
        std::thread::spawn(move || {
            match child.wait() {
                Err(e) => {
                    println!("Error running {:?}: {}", runningjob.job.command, e);
                },
                Ok(st) => {
                    if let Ok(mut f) = std::fs::OpenOptions::new().create(true)
                        .append(true).open(runningjob.job.directory.join(&runningjob.job.output))
                    {
                        writeln!(f, ":::::: Job {:?} exited with status {:?}",
                                 &runningjob.job.jobname, st.code()).ok();
                    }
                    println!("Done running {:?}: {}", runningjob.job.jobname, st);
                }
            }
            if let Err(e) = runningjob.completed() {
                println!("Unable to change status of completed job {} ({})",
                         &runningjob.job.jobname, e);
                return;
            }
        });
    }
}

pub fn spawn_runner() -> Result<()> {
    ensure_directories()?;
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
        Some(home.join(RQ).join(&host).with_extension("log")),
        Some(home.join(RQ).join(&host).with_extension("log")),
        unix_daemonize::ChdirMode::ChdirRoot).unwrap();
    write_pid(&home.join(RQ).join(&host))?;
    println!("==================\nRestarting runner!");
    let (notify_tx, notify_rx) = std::sync::mpsc::channel();
    let mut old_status = Status::new()?;
    let mut watcher =
        notify::watcher(notify_tx.clone(),
                        std::time::Duration::from_secs(1)).unwrap();
    // We watch our own user's WAITING directory, since this is the
    // only place new jobs can show up that we might want to run.
    watcher.watch(home.join(RQ).join(WAITING),
                  notify::RecursiveMode::NonRecursive).ok();
    // We watch all user RUNNING directories, since in the future they
    // may be running a job on this host, and when that job completes
    // we may want to run a job of our own.  Even if they don't
    // currently have a daemon running (and thus no jobs), they may
    // start a daemon in the future, while we are still running!
    for userdir in std::fs::read_dir("/home")? {
        if let Ok(userdir) = userdir {
            watcher.watch(userdir.path().join(RQ).join(RUNNING),
                          notify::RecursiveMode::NonRecursive).ok();
        }
    }
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(POLLING_TIME));
            // println!("Polling...");
            notify_tx.send(notify::DebouncedEvent::Rescan).unwrap();
        }
    });
    loop {
        if let Ok(rr) = home.join(RQ).join(CANCELING).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    j.kill();
                    j.canceled().ok();
                } else {
                    eprintln!("Error reading scancel {:?}", run.path());
                }
            }
        }
        let status = Status::new().unwrap();
        let running = status.running.iter().filter(|j| &j.node == &host).count();
        let waiting = status.waiting.iter().count();
        if old_status != status {
            println!("Currently using {}/{} cores, with {} jobs waiting.",
                     running, cpus, waiting);
        }
        old_status = status.clone();
        if cpus > running && waiting > 0 {
            status.run_next(&host, &home);
        }
        notify_rx.recv().unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct DaemonInfo {
    pid: libc::pid_t,
    physical_cores: usize,
    logical_cpus: usize,
    restart_time: Duration,
}

impl DaemonInfo {
    fn new() -> DaemonInfo {
        DaemonInfo {
            pid: unsafe { libc::getpid() },
            physical_cores: num_cpus::get_physical(),
            logical_cpus: num_cpus::get(),
            restart_time: now(),
        }
    }
    fn read(fname: &Path) -> Result<DaemonInfo> {
        let mut f = std::fs::File::open(fname)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        match serde_json::from_slice::<DaemonInfo>(&data) {
            Ok(dinfo) => Ok(dinfo),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
}

fn read_pid(fname: &Path) -> Result<libc::pid_t> {
    Ok(DaemonInfo::read(fname)?.pid)
}

fn write_pid(fname: &Path) -> Result<()> {
    let mut f = std::fs::File::create(fname)?;
    f.write_all(&serde_json::to_string(&DaemonInfo::new()).unwrap().as_bytes())
}

fn ensure_directories() -> Result<()> {
    let home = std::env::home_dir().unwrap();
    std::fs::create_dir_all(&home.join(RQ).join(WAITING))?;
    std::fs::create_dir_all(&home.join(RQ).join(RUNNING))?;
    std::fs::create_dir_all(&home.join(RQ).join(COMPLETED))?;
    std::fs::create_dir_all(&home.join(RQ).join(CANCELED))?;
    std::fs::create_dir_all(&home.join(RQ).join(CANCELING))
}

fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

/// Determine if a process exists with this pid.  The kill system call
/// when given a zero signal just checks if the process exists.
fn pid_exists(pid: libc::pid_t) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}
