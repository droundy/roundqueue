#[macro_use]
extern crate serde_derive;

extern crate dirs;
extern crate hostname;
extern crate libc;
extern crate notify;
extern crate num_cpus;
extern crate serde;
extern crate serde_json;
extern crate shared_child;
extern crate unix_daemonize;

mod longthreads;

use notify::Watcher;

use std::collections::HashMap;
use std::io::{Read, Result, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::os::unix::process::CommandExt;
use std::sync::{Arc, Mutex};

const GB: f64 = (1u64 << 30) as f64;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RunningJob {
    pub job: Job,
    pub started: Duration,
    pub node: String,
    pub pid: u32,
    #[serde(default)]
    pub completed: Duration,
    #[serde(default)]
    pub exit_code: Option<i32>,
}

impl RunningJob {
    pub fn duration(&self) -> Duration {
        if self.completed != std::time::Duration::from_secs(0) {
            if let Some(t) = self.completed.checked_sub(self.started) {
                t
            } else {
                Duration::from_secs(0)
            }
        } else {
            let dur = now();
            if let Some(t) = dur.checked_sub(self.started) {
                t
            } else {
                Duration::from_secs(0)
            }
        }
    }
    pub fn wait_duration(&self) -> Duration {
        self.started
            .checked_sub(self.job.submitted)
            .unwrap_or(Duration::from_secs(0))
    }
    fn completed(&self) -> Result<()> {
        if let Some(ec) = self.exit_code {
            if ec != 0 {
                std::fs::rename(
                    self.job.filepath(Path::new(RUNNING)),
                    self.job.filepath(Path::new(FAILED)),
                )?;
                let mut x = self.clone();
                x.completed = now();
                return x.save(&Path::new(FAILED));
            }
        }
        // First try renaming from CANCELING, just in case this job
        // has been cancelled right before it completed.
        if let Err(_) = std::fs::rename(
            self.job.filepath(Path::new(CANCELING)),
            self.job.filepath(Path::new(COMPLETED)),
        ) {
            // It looks like it wasn't cancelled, so let's rename from
            // RUNNING.
            std::fs::rename(
                self.job.filepath(Path::new(RUNNING)),
                self.job.filepath(Path::new(COMPLETED)),
            )?;
        }
        let mut x = self.clone();
        x.completed = now();
        x.save(&Path::new(COMPLETED))
    }
    pub fn cancel(&self) -> Result<()> {
        std::fs::rename(
            self.job.filepath(Path::new(RUNNING)),
            self.job.filepath(Path::new(CANCELING)),
        )?;
        let mut x = self.clone();
        x.completed = now();
        x.save(&Path::new(CANCELING))?;
        self.kill()
    }
    pub fn zombie(&self) -> Result<()> {
        std::fs::rename(
            self.job.filepath(Path::new(RUNNING)),
            self.job.filepath(Path::new(ZOMBIE)),
        )?;
        let mut x = self.clone();
        x.completed = now();
        x.save(&Path::new(ZOMBIE))
    }
    fn canceled(&self) -> Result<()> {
        std::fs::rename(
            self.job.filepath(Path::new(CANCELING)),
            self.job.filepath(Path::new(CANCELED)),
        )?;
        let mut x = self.clone();
        x.completed = now();
        x.save(&Path::new(CANCELED))
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
            }
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
    fn save(&self, subdir: &Path) -> Result<()> {
        let mut f = std::fs::File::create(self.job.filepath(subdir).with_extension("tmp"))?;
        f.write_all(&serde_json::to_string(self).unwrap().as_bytes())?;
        std::fs::rename(
            self.job.filepath(subdir).with_extension("tmp"),
            self.job.filepath(subdir),
        )
    }
    pub fn kill(&self) -> Result<()> {
        let host = hostname::get().unwrap().into_string().unwrap();
        if self.node != host {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("cannot kill job on {} from host {}", &self.node, &host),
            ));
        }
        if self.exists() {
            self.canceled()?;
            unsafe {
                libc::kill(self.pid as i32, libc::SIGTERM);
            }
            std::thread::sleep(Duration::from_secs(2));
            if self.exists() {
                let myself = DaemonInfo::new();
                myself.log(format!(
                    "FAILED to kill {} (pid {}) with SIGTERM",
                    self.job.jobname, self.pid
                ));
                unsafe {
                    libc::kill(self.pid as i32, libc::SIGKILL);
                }
                std::thread::sleep(Duration::from_secs(2));
                if self.exists() {
                    myself.log(format!(
                        "FAILED to kill {} (pid {}) with SIGKILL",
                        self.job.jobname, self.pid
                    ));
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "bad kill?"));
                }
            }
        }
        Ok(())
    }
    /// Check if this job exists on this host.  This checks if a
    /// process with the right pid has the right command line.  It is
    /// not perfect, but it beats just randomly killing a process with
    /// the same id if something went wrong.
    pub fn exists(&self) -> bool {
        if let Ok(mut f) = std::fs::File::open(format!("/proc/{}/environ", self.pid)) {
            let mut data = Vec::new();
            f.read_to_end(&mut data).ok();
            let goal = format!("RQ_SUBMIT_TIME={}", self.job.submitted.as_secs());
            let goal = goal.as_bytes();
            if data.windows(goal.len()).any(|w| w == goal) {
                return true;
            }
        } else {
            // This happens if the jobs is that of another user, since
            // we don't have permission to read their environment.
            return pid_exists(self.pid as i32);
        }
        false
    }
    /// Memory used by this job, returning 0 if unavailable.
    pub fn memory_in_use(&self) -> u64 {
        if let Ok(p) = procfs::process::Process::new(self.pid as libc::pid_t) {
            let page_size = procfs::page_size().unwrap() as u64;
            (p.stat.rss as u64 * page_size) as u64
        } else {
            0
        }
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
    #[serde(default)]
    pub cores: usize,
    #[serde(default)]
    pub memory_required: u64,
    #[serde(default)]
    pub max_output: Option<u64>,
    #[serde(default)]
    pub restartable: bool,
}

impl Job {
    pub fn new(
        command: Vec<String>,
        jobname: String,
        output: PathBuf,
        cores: usize,
        memory_required: u64,
        max_output: u64,
        restartable: bool,
    ) -> Result<Job> {
        Ok(Job {
            directory: std::env::current_dir()?,
            home_dir: dirs::home_dir().unwrap(),
            command,
            jobname,
            output,
            submitted: now(),
            cores,
            memory_required,
            max_output: Some(max_output),
            restartable: restartable,
        })
    }
    fn pretty_command(&self) -> String {
        let mut out = String::new();
        for c in self.command.iter() {
            if !c.contains(' ') && !c.contains('"') && !c.contains('\'') && !c.contains('\\') {
                out.push_str(c);
            } else {
                out.push_str(&format!("{:?}", c));
            }
            out.push(' ');
        }
        out.pop();
        out
    }
    pub fn cancel(&self) -> Result<()> {
        std::fs::rename(
            self.filepath(Path::new(WAITING)),
            self.filepath(Path::new(CANCELED)),
        )
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
        PathBuf::from(format!(
            "{}.{}.job",
            self.submitted.as_secs(),
            self.submitted.subsec_nanos()
        ))
    }
    fn filepath(&self, subdir: &Path) -> PathBuf {
        self.home_dir.join(RQ).join(subdir).join(self.filename())
    }
    fn read(fname: &Path) -> Result<Job> {
        let mut f = std::fs::File::open(fname)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        match serde_json::from_slice::<RunningJob>(&data) {
            Ok(rj) => {
                let job = rj.job;
                if job.command.len() == 0 {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, "empty cmd?"))
                } else {
                    Ok(job)
                }
            }
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
    /// We save the `Job` as a `RunningJob`, so we can atomically make
    /// it "running" later.
    fn save(&self, subdir: &Path) -> Result<()> {
        let mut f = std::fs::File::create(self.filepath(subdir).with_extension("tmp"))?;
        let rj = RunningJob {
            job: self.clone(),
            node: String::from("NONE"),
            pid: 0,
            started: now(),
            completed: std::time::Duration::from_secs(0),
            exit_code: None,
        };
        f.write_all(&serde_json::to_string(&rj).unwrap().as_bytes())?;
        std::fs::rename(
            &self.filepath(subdir).with_extension("tmp"),
            &self.filepath(subdir),
        )
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
const FAILED: &'static str = "failed";
const ZOMBIE: &'static str = "zombie";
const COMPLETED: &'static str = "completed";
const CANCELED: &'static str = "canceled";
const CANCELING: &'static str = "cancel";
const SHORT_TIME: std::time::Duration = std::time::Duration::from_secs(1);
const LIVE_TIME: std::time::Duration = std::time::Duration::from_secs(10 * 60);
const POLLING_TIME: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Status {
    pub homedirs_sharing_host: Vec<PathBuf>,
    pub waiting: Vec<Job>,
    pub running: Vec<RunningJob>,
    pub nodes: Vec<DaemonInfo>,
}

impl Status {
    pub fn new() -> Result<Status> {
        let mut status = Status {
            homedirs_sharing_host: Vec::new(),
            waiting: Vec::new(),
            running: Vec::new(),
            nodes: Vec::new(),
        };
        let host = hostname::get().unwrap().into_string().unwrap();
        let my_homedir = dirs::home_dir().unwrap();
        let mut root_home = my_homedir.clone();
        root_home.pop();
        let root_home = root_home;
        for node in std::fs::read_dir(my_homedir.join(RQ)) {
            for node in node.flat_map(|r| r.ok()) {
                if let Ok(dinfo) = DaemonInfo::read(&my_homedir.join(RQ).join(node.path())) {
                    status.nodes.push(dinfo);
                }
            }
        }
        // Look for all the jobs!
        for userdir in root_home.read_dir()? {
            if let Ok(userdir) = userdir {
                let rqdir = userdir.path().join(RQ);
                // First check whether this user is running a
                // roundqueue daemon on the same host we are using.
                if let Ok(pid) = read_pid(&rqdir.join(&host)) {
                    if pid_exists(pid) {
                        status
                            .homedirs_sharing_host
                            .push(root_home.join(userdir.path()));
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
                            if host == j.node && !j.exists() {
                                println!("Job {} appears to have failed!", j.job.jobname);
                                j.zombie().ok();
                            } else {
                                status.running.push(j);
                            }
                        } else {
                            eprintln!("Error reading {:?}", run.path());
                        }
                    }
                }
                for j in status.running.iter_mut().filter(|j| j.job.cores == 0) {
                    j.job.cores = num_cpus::get_physical();
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
    pub fn my_waiting_jobs() -> Vec<Job> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(WAITING).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = Job::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn my_running_jobs() -> Vec<RunningJob> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(RUNNING).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn my_completed_jobs(&self) -> Vec<RunningJob> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(COMPLETED).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn my_failed_jobs(&self) -> Vec<RunningJob> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(FAILED).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn my_zombie_jobs(&self) -> Vec<RunningJob> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(ZOMBIE).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn my_canceled_jobs(&self) -> Vec<RunningJob> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(CANCELED).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn my_canceling_jobs(&self) -> Vec<RunningJob> {
        let home = dirs::home_dir().unwrap();
        let mut out = Vec::new();
        if let Ok(rr) = home.join(".roundqueue").join(CANCELING).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    out.push(j);
                }
            }
        }
        out
    }
    pub fn has_jobname(&self, jn: &str) -> bool {
        self.waiting.iter().any(|j| j.jobname == jn)
            || self.running.iter().any(|j| j.job.jobname == jn)
    }
    /// run_next consumes the status to enforce that we must re-read
    /// the status before attempting anything else.  This is because
    /// another node may have run something or submitted something.
    fn run_next(
        self,
        host: &str,
        home_dir: &Path,
        threads: &longthreads::Threads,
        in_foreground: bool,
    ) {
        // "waiting" is the set of jobs that are waiting to run *on
        // this host*.  Thus this ignores any waiting jobs that cannot
        // run on this host because their user does not have a daemon
        // running on this host.
        let cpus = num_cpus::get_physical();
        let available_ram = if let Ok(m) = procfs::Meminfo::new() {
            if let Some(ma) = m.mem_available {
                ma
            } else {
                m.mem_total
            }
        } else {
            16 * GB as u64
        };
        let myself = DaemonInfo::new();
        // myself.log(format!("I am in run_next."));
        let waiting: Vec<_> = self
            .waiting
            .iter()
            .filter(|j| self.homedirs_sharing_host.contains(&j.home_dir))
            .filter(|j| j.cores <= cpus)
            .filter(|j| j.memory_required <= available_ram)
            .map(|j| j.home_dir.clone())
            .collect();
        if waiting.len() == 0 {
            return;
        }
        let run_counts: HashMap<_, _> = waiting
            .iter()
            .map(|hd| {
                (
                    hd.clone(),
                    self.running
                        .iter()
                        .filter(|j| &j.job.home_dir == hd)
                        .map(|j| j.job.cores)
                        .sum::<usize>(),
                )
            })
            .collect();
        let least_running = run_counts.values().cloned().min().unwrap();
        if run_counts.get(home_dir) != Some(&least_running) {
            // myself.log(format!("Not my turn 1: {:?}", run_counts));
            return;
        }
        // myself.log(format!("The running counts are: {:?}", run_counts));
        let mut job = self.waiting[0].clone();
        let mut earliest_submitted = std::time::Duration::from_secs(0xffffffffffffff);
        for j in self
            .waiting
            .into_iter()
            .filter(|j| j.cores <= cpus)
            .filter(|j| j.memory_required <= available_ram)
        {
            if run_counts.get(&j.home_dir) == Some(&least_running)
                && j.submitted < earliest_submitted
            {
                earliest_submitted = j.submitted;
                job = j;
            }
        }
        if &job.home_dir != home_dir {
            // myself.log(format!("Not my turn: {:?} should go next.", job.home_dir));
            return;
        }

        // Now I need to determine if this job is actually runnable.  It may require more
        // cores than are available.
        let status = Status::new().unwrap();
        let cores_in_use: usize = status
            .running
            .iter()
            .filter(|&j| host == j.node)
            .map(|j| j.job.cores)
            .sum();
        let mem_reserved: u64 = status
            .running
            .iter()
            .filter(|&j| host == j.node)
            .map(|j| {
                let in_use = j.memory_in_use();
                if j.job.memory_required < in_use {
                    0
                } else {
                    j.job.memory_required - in_use
                }
            })
            .sum();

        let cores_available = cpus - cores_in_use;
        if job.cores > cores_available || cores_available == 0 {
            // myself.log(format!(
            //     "Not enough cores available: {}/{} cores in use, need {}",
            //     cores_in_use, cpus, job.cores
            // ));
            return;
        }
        let memory_still_available = if available_ram > mem_reserved {
            available_ram - mem_reserved
        } else {
            0
        };
        if job.memory_required > memory_still_available {
            myself.log(format!(
                "Not enough memory available: {:.1}G memory available, with {:.1}G reserved, but I require {:.1}G",
                available_ram as f64/GB, mem_reserved as f64/GB, job.memory_required as f64/GB,
            ));
            return;
        }

        if let Err(e) = job.change_status(Path::new(WAITING), Path::new(RUNNING)) {
            myself.log(format!(
                "Unable to change status of job {} ({})",
                &job.jobname, e
            ));
            return;
        }
        myself.log(format!("starting {:?}", &job.jobname));
        let mut f = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(job.directory.join(&job.output))
        {
            Ok(f) => f,
            Err(e) => {
                myself.log(format!(
                    "Error creating output {:?}: {}",
                    job.directory.join(&job.output),
                    e
                ));
                return;
            }
        };
        if let Err(e) = writeln!(
            f,
            "::::: Starting job {:?} on {}: {}",
            &job.jobname,
            &host,
            job.pretty_command()
        ) {
            myself.log(format!(
                "Error writing to output {:?}: {}",
                job.directory.join(&job.output),
                e
            ));
            return;
        }

        if in_foreground {
            let home = dirs::home_dir().unwrap();
            let host = hostname::get().unwrap().into_string().unwrap();
            unix_daemonize::daemonize_redirect(
                Some(home.join(RQ).join(&host).with_extension("log")),
                Some(home.join(RQ).join(&host).with_extension("log")),
                unix_daemonize::ChdirMode::ChdirRoot,
            )
            .unwrap();
        }

        // First spawn the child...
        let mut cmd = std::process::Command::new(&job.command[0]);

        let fd = f.into_raw_fd();
        let stderr = unsafe { std::process::Stdio::from_raw_fd(fd) };
        let stdout = unsafe { std::process::Stdio::from_raw_fd(fd) };
        cmd.args(&job.command[1..])
            .current_dir(&job.directory)
            .env("RQ_SUBMIT_TIME", format!("{}", job.submitted.as_secs()))
            .stderr(stderr)
            .stdout(stdout)
            .stdin(std::process::Stdio::null());
        unsafe {
            cmd.pre_exec(|| {
                // make child processes always be maximally nice!
                libc::nice(19);
                Ok(())
            });
        }
        let child = match shared_child::SharedChild::spawn(&mut cmd) {
            Ok(c) => c,
            Err(e) => {
                myself.log(format!("Unable to spawn child: {}", e));
                // runningjob.failed().ok();
                return;
            }
        };

        let mut runningjob = RunningJob {
            started: now(),
            node: String::from(host),
            job: job,
            pid: child.id(),
            completed: std::time::Duration::from_secs(0),
            exit_code: None,
        };
        if runningjob.job.cores == 0 {
            // Just set the number of cores used to all of them.
            runningjob.job.cores = num_cpus::get_physical();
        }
        if let Err(e) = runningjob.save(Path::new(RUNNING)) {
            myself.log(format!("Yikes, unable to save job? {}", e));
            return;
        }
        let child = Arc::new(child);
        let child_to_kill = child.clone();
        // Now we create the thread that waits to see if the child
        // process exits, and if so marks it as completed.
        let all_done = Arc::new(Mutex::new(false));
        let all_done_setter = all_done.clone();
        let output_path = runningjob.job.directory.join(&runningjob.job.output);
        if let Some(max_output) = runningjob.job.max_output {
            // This thread will periodically check to see the size of
            // the output file, and if it is too large it will kill the
            // child.  This is intended to reduce the danger of the disk
            // being entirely filled with data written to stdout (which
            // should normally *not* be large, but frequently is due to a
            // printf in a longrunning process).
            threads.spawn(move || {
                let output_path = &output_path;
                loop {
                    // We wait a very short while before checking if
                    // the process is both still running *and* has
                    // exceeded its maximum output size in the log
                    // file.  This is short to make testing less
                    // painful.
                    std::thread::sleep(SHORT_TIME);
                    if *all_done.lock().unwrap() {
                        return;
                    }
                    if let Ok(md) = output_path.metadata() {
                        if md.len() > max_output {
                            child_to_kill.kill().ok();
                            if let Ok(mut f) = std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(output_path)
                            {
                                writeln!(
                                    f,
                                    ":::::: [{}] Job created too large an output file! {} > {}",
                                    child_to_kill.id(),
                                    md.len(),
                                    max_output
                                )
                                .ok();
                            }
                        }
                    }
                    // We wait a much longer time between checks,
                    // since we don't want to spend much time polling
                    // the output file size.
                    std::thread::sleep(LIVE_TIME);
                }
            });
        }
        threads.spawn(move || {
            match child.wait() {
                Err(e) => {
                    myself.log(format!("Error running {:?}: {}", runningjob.job.command, e));
                }
                Ok(st) => {
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(runningjob.job.directory.join(&runningjob.job.output))
                    {
                        writeln!(
                            f,
                            ":::::: [{}] Job {:?} exited with status {:?}",
                            myself.pid,
                            &runningjob.job.jobname,
                            st.code()
                        )
                        .ok();
                        runningjob.exit_code = st.code();
                    }
                    myself.log(format!("Done running {:?}: {}", runningjob.job.jobname, st));
                }
            }
            *all_done_setter.lock().unwrap() = true;
            if let Err(e) = runningjob.completed() {
                myself.log(format!(
                    "Unable to change status of completed job {} ({})",
                    &runningjob.job.jobname, e
                ));
                return;
            }
        });
    }
}

pub fn spawn_runner(in_foreground: bool, quietly: bool) -> Result<()> {
    ensure_directories()?;
    std::env::set_var("RUST_BACKTRACE", "1"); // we always want to see a backtrace if daemon crashes
    let home = dirs::home_dir().unwrap();
    let host = hostname::get().unwrap().into_string().unwrap();
    let mut root_home = home.clone();
    root_home.pop();
    let root_home = root_home;

    let cpus = num_cpus::get_physical();
    let hyperthreads = num_cpus::get();
    if !quietly {
        println!(
            "I am spawning a runner for {} with {} cpus in {:?}!",
            &host, cpus, &home
        );
    }
    if !in_foreground {
        unix_daemonize::daemonize_redirect(
            Some(home.join(RQ).join(&host).with_extension("log")),
            Some(home.join(RQ).join(&host).with_extension("log")),
            unix_daemonize::ChdirMode::ChdirRoot,
        )
        .unwrap();
    }
    DaemonInfo::write()?;
    let myself = DaemonInfo::new();
    myself.log(format!(
        "==================\nRestarting runner process {}!",
        myself.pid
    ));
    let mut old_status = Status::new()?;
    let (notify_tx, notify_rx) = std::sync::mpsc::channel();
    let notify_polling = notify_tx.clone();
    std::thread::spawn(move || {
        loop {
            // println!("Polling...");
            notify_polling.send(notify::DebouncedEvent::Rescan).unwrap();
            std::thread::sleep(POLLING_TIME);
        }
    });
    let mut watcher = notify::watcher(notify_tx.clone(), std::time::Duration::from_secs(1)).ok();
    // We watch our own user's WAITING directory, since this is the
    // only place new jobs can show up that we might want to run.
    if let Some(watcher) = &mut watcher {
        watcher
            .watch(
                home.join(RQ).join(WAITING),
                notify::RecursiveMode::NonRecursive,
            )
            .ok();
    }
    // We watch all user RUNNING directories, since in the future they
    // may be running a job on this host, and when that job completes
    // we may want to run a job of our own.  Even if they don't
    // currently have a daemon running (and thus no jobs), they may
    // start a daemon in the future, while we are still running!
    for userdir in root_home.read_dir()? {
        if let Ok(userdir) = userdir {
            if let Some(watcher) = &mut watcher {
                watcher
                    .watch(
                        userdir.path().join(RQ).join(RUNNING),
                        notify::RecursiveMode::NonRecursive,
                    )
                    .ok();
            }
        }
    }
    let threads = longthreads::Threads::new();
    loop {
        // We re-watch the relevant directories each time through the
        // loop, just in case they have been completely removed and
        // replaced, or some other error has "interrupted" our watch.
        // It's a bit wasteful, but rq is pretty wasteful, and relies
        // on the fact that we have few users, few nodes, and few
        // jobs.  This *should* ensure that we don't end up with
        // daemons that are disconnected from reality and rely on our
        // POLLING_TIME to run jobs.
        if let Some(watcher) = &mut watcher {
            watcher
                .watch(
                    home.join(RQ).join(WAITING),
                    notify::RecursiveMode::NonRecursive,
                )
                .ok();
        }
        if let Ok(rr) = home.join(RQ).join(CANCELING).read_dir() {
            for run in rr.flat_map(|r| r.ok()) {
                if let Ok(j) = RunningJob::read(&run.path()) {
                    j.kill().ok();
                } else {
                    myself.log(format!("Error reading scancel {:?}", run.path()));
                }
            }
        }
        // Now check whether we are in fact the real daemon running on
        // this node/home combination.
        let daemon_running =
            DaemonInfo::read_my_own().expect("Lock file unreadable, I should exit");
        if daemon_running.pid != myself.pid {
            myself.log(format!(
                "I {} have been replaced by {}.",
                myself.pid, daemon_running.pid
            ));
            return Ok(()); // being replaced is not an error!
        }
        DaemonInfo::write().unwrap();
        // Now check whether there is a job to be run... we begin by
        // checking if we have any waiting jobs of our own.  If not,
        // then we can quit now, because we know that we need not run
        // anything.
        if Status::my_waiting_jobs().len() == 0 {
            if in_foreground {
                println!("There are no runnable jobs.");
                return Ok(());
            }

            if let Some(watcher) = &mut watcher {
                watcher
                    .watch(
                        home.join(RQ).join(CANCELING),
                        notify::RecursiveMode::NonRecursive,
                    )
                    .ok();
            }
            notify_rx.recv().unwrap();
            continue;
        }
        for userdir in root_home.read_dir()? {
            if let Ok(userdir) = userdir {
                if let Some(watcher) = &mut watcher {
                    watcher
                        .watch(
                            userdir.path().join(RQ).join(RUNNING),
                            notify::RecursiveMode::NonRecursive,
                        )
                        .ok();
                }
            }
        }
        // We have a job we would like to run, so let us now find out
        // if it is runnable!
        let status = Status::new().unwrap();
        for j in status.running.iter().filter(|j| &j.node == &host) {
            myself.log(format!("  Job {} has {} cores", j.job.jobname, j.job.cores));
        }
        let running = status
            .running
            .iter()
            .filter(|j| &j.node == &host)
            .map(|j| j.job.cores)
            .sum();
        if old_status != status {
            myself.log(format!(
                "Currently using {}/{} cores, with {} jobs waiting.",
                running,
                cpus,
                status.waiting.len()
            ));
        }
        old_status = status.clone();
        let total_cpus: usize = status.nodes.iter().map(|di| di.physical_cores).sum();
        let total_running: usize = status
            .running
            .iter()
            .filter(|&j| status.nodes.iter().any(|d| d.hostname == j.node))
            .map(|j| j.job.cores)
            .sum();
        if cpus > running && status.waiting.len() > 0 {
            status.run_next(&host, &home, &threads, in_foreground);
        } else if status.waiting.len() > 0 && total_running >= total_cpus {
            let mut user_running_cores = HashMap::new();
            // the following ignores any jobs running on nodes
            // that are not currently up by our measure.  This
            // prevents us from concluding that other users are
            // oversubscribed based on a faulty total number of
            // CPUs.
            for j in status
                .running
                .iter()
                .filter(|&j| status.nodes.iter().any(|d| d.hostname == j.node))
            {
                let hd = j.job.home_dir.clone();
                let count = user_running_cores.get(&hd).unwrap_or(&0) + j.job.cores;
                user_running_cores.insert(hd, count);
            }
            if !user_running_cores.contains_key(&home) {
                user_running_cores.insert(home.clone(), 0);
            }
            let total_users = user_running_cores.len();
            let cpus_per_user = total_cpus / total_users;
            let fewest_running_waiting_user: usize = status
                .waiting
                .iter()
                .map(|j| user_running_cores.get(&j.home_dir).unwrap_or(&0))
                .min()
                .map(|&n| n)
                .unwrap_or(0);
            if user_running_cores[&home] > cpus_per_user
                && user_running_cores[&home] > fewest_running_waiting_user
            {
                // I should consider cancelling and resubmitting a job
                // in order to be polite.
                let my_running = Status::my_running_jobs();
                if let Some(j) = my_running
                    .into_iter()
                    .filter(|j| j.job.restartable)
                    .min_by_key(|j| j.started)
                {
                    println!("Could consider restarting {}", j.job.jobname);
                    let restart_job = j.job.clone();
                    // First try to create a restart job, then kill the existing one.
                    if restart_job.submit().is_ok() {
                        if j.kill().is_err() {
                            // If the kill failed, we should undo the restart job.
                            restart_job.cancel().ok(); // If the cancel fails, such is life, all things are sad now.
                        }
                    }
                }
            }
            if hyperthreads > running {
                // We will now decide whether to run a job using a
                // hyperthread that shares a CPU core.  We do this in
                // cases where some users are using more than their
                // fair share of the cluster, and all the cores are
                // currently busy, in order to ensure low latency for
                // all users, at the cost of slowing down some jobs.
                if user_running_cores[&home] < cpus_per_user {
                    // It is possible that we are next in line and should
                    // run using a hyperthread...
                    myself.log(format!(
                        "Thinking about using hyperthreads: {} < {}.",
                        user_running_cores[&home], cpus_per_user
                    ));
                    let politely_waiting = status
                        .waiting
                        .iter()
                        .filter(|j| user_running_cores[&j.home_dir] < cpus_per_user)
                        .count();
                    if politely_waiting > 0 {
                        myself.log(format!(
                            "    Starting a job since we have some {} waiting.",
                            politely_waiting
                        ));
                        status.run_next(&host, &home, &threads, in_foreground);
                    } else {
                        myself.log(format!("    I have no jobs waiting to run."));
                    }
                }
            }
        }
        if in_foreground {
            return Ok(());
        }
        notify_rx.recv().unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DaemonInfo {
    pub hostname: String,
    pub pid: libc::pid_t,
    pub physical_cores: usize,
    pub logical_cpus: usize,
    pub restart_time: Duration,
}

impl DaemonInfo {
    fn new() -> DaemonInfo {
        DaemonInfo {
            hostname: hostname::get().unwrap().into_string().unwrap(),
            pid: unsafe { libc::getpid() },
            physical_cores: num_cpus::get_physical(),
            logical_cpus: num_cpus::get(),
            restart_time: now(),
        }
    }
    fn read_my_own() -> Result<DaemonInfo> {
        let home = dirs::home_dir().unwrap();
        let host = hostname::get().unwrap().into_string().unwrap();
        DaemonInfo::read(&home.join(RQ).join(&host))
    }
    fn read(fname: &Path) -> Result<DaemonInfo> {
        let mut f = std::fs::File::open(fname)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        let dinfo = match serde_json::from_slice::<DaemonInfo>(&data) {
            Ok(dinfo) => dinfo,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        };
        let now = now();
        match dinfo.exists() {
            Some(true) => (), // Cool!
            Some(false) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Process does not exist",
                ));
            }
            None => {
                if dinfo.restart_time < now && now - dinfo.restart_time > LIVE_TIME {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Must have died long ago",
                    ));
                }
            }
        }
        Ok(dinfo)
    }
    fn write() -> Result<()> {
        let home = dirs::home_dir().unwrap();
        let myself = DaemonInfo::new();
        let mut f = std::fs::File::create(&home.join(RQ).join(&myself.hostname))?;
        f.write_all(&serde_json::to_string(&myself).unwrap().as_bytes())
    }
    fn log(&self, msg: String) {
        eprintln!("{}: {}", self.pid, msg);
    }
    fn exists(&self) -> Option<bool> {
        let host = hostname::get().unwrap().into_string().unwrap();
        if self.hostname != host {
            return None; // We have no clue
        }
        if let Ok(mut f) = std::fs::File::open(format!("/proc/{}/cmdline", self.pid)) {
            let mut data = Vec::new();
            f.read_to_end(&mut data).ok();
            if data.windows(b"daemon\0".len()).any(|w| w == b"daemon\0")
                || data.windows(b"restart".len()).any(|w| w == b"restart")
                || data.windows(b"run".len()).any(|w| w == b"run")
            {
                return Some(true);
            }
        }
        Some(false)
    }
}

fn read_pid(fname: &Path) -> Result<libc::pid_t> {
    Ok(DaemonInfo::read(fname)?.pid)
}

fn ensure_directories() -> Result<()> {
    let home = dirs::home_dir().unwrap();
    std::fs::create_dir_all(&home.join(RQ).join(WAITING))?;
    std::fs::create_dir_all(&home.join(RQ).join(RUNNING))?;
    std::fs::create_dir_all(&home.join(RQ).join(COMPLETED))?;
    std::fs::create_dir_all(&home.join(RQ).join(FAILED))?;
    std::fs::create_dir_all(&home.join(RQ).join(CANCELED))?;
    std::fs::create_dir_all(&home.join(RQ).join(ZOMBIE))?;
    std::fs::create_dir_all(&home.join(RQ).join(CANCELING))
}

pub fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

/// Determine if a process exists with this pid.  You can also kill
/// with signal zero to check, but that fails if the process is owned
/// by a different user.
fn pid_exists(pid: libc::pid_t) -> bool {
    std::path::Path::new(&format!("/proc/{}", pid)).exists()
}
