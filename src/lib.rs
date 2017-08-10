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
        std::fs::create_dir_all(&self.home_dir.join(RQ).join(WAITING))?;
        std::fs::create_dir_all(&self.home_dir.join(RQ).join(RUNNING))?;
        std::fs::create_dir_all(&self.home_dir.join(RQ).join(COMPLETED))
    }
}

const RQ: &'static str = ".roundqueue";
const RUNNING: &'static str = "RUNNING";
const WAITING: &'static str = "WAITING";
const COMPLETED: &'static str = "COMPLETED";
const POLLING_TIME: u64 = 60;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Status {
    pub waiting: Vec<Job>,
    pub running: Vec<RunningJob>,
    //pub completed: Vec<RunningJob>,
}

impl Status {
    pub fn new() -> Result<Status> {
        let mut status = Status {
            waiting: Vec::new(),
            running: Vec::new(),
            // completed: Vec::new(),
        };
        for userdir in std::fs::read_dir("/home")? {
            if let Ok(userdir) = userdir {
                let rqdir = userdir.path().join(RQ);
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
        let waiting: Vec<_> =  self.waiting.iter().map(|j| j.home_dir.clone()).collect();
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
    watcher.watch(home.join(RQ).join(WAITING),
                  notify::RecursiveMode::NonRecursive).ok();
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

fn ensure_directories() -> Result<()> {
    let home = std::env::home_dir().unwrap();
    std::fs::create_dir_all(&home.join(RQ).join(WAITING))?;
    std::fs::create_dir_all(&home.join(RQ).join(RUNNING))?;
    std::fs::create_dir_all(&home.join(RQ).join(COMPLETED))
}

fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}
