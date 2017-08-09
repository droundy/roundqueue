#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;

use std::path::{Path,PathBuf};
use std::time::{SystemTime,Duration,UNIX_EPOCH};
use std::io::{Result,Write,Read};
use std::ffi::OsString;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Running {
    pub started: Duration,
    pub node: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Job {
    pub home_dir: PathBuf,
    pub directory: PathBuf,
    pub command: Vec<OsString>,
    pub jobname: String,
    pub submitted: Duration,
    pub running: Option<Running>,
}

impl Job {
    fn new(cmd: Vec<OsString>, jobname: String) -> Result<Job> {
        Ok(Job {
            directory: std::env::current_dir()?,
            home_dir: std::env::home_dir().unwrap(),
            command: cmd,
            jobname: jobname,
            submitted: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            running: None,
        })
    }
    fn filename(&self) -> PathBuf {
        PathBuf::from(format!("{}-{}.job",
                              self.submitted.as_secs(), self.submitted.subsec_nanos()))
    }
    fn read(fname: &Path) -> Result<Job> {
        let mut f = std::fs::File::open(fname)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        match serde_json::from_slice::<Job>(&data) {
            Ok(job) => Ok(job),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
    fn save(&self, subdir: &Path) -> Result<()> {
        let p = self.home_dir.join(RQ).join(subdir).join(self.filename());
        let mut f = std::fs::File::create(&p)?;
        f.write_all(&serde_json::to_string(self).unwrap().as_bytes())
    }
}

const RQ: &'static str = ".roundqueue";
const RUNNING: &'static str = "RUNNING";
const WAITING: &'static str = "WAITING";
const COMPLETED: &'static str = "COMPLETED";

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
                eprintln!("{:?}", rqdir);
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
}

pub fn create_job(directory: PathBuf, command: String) {
}

pub fn running_jobs() -> Result<Vec<Job>> {
    Ok(Vec::new())
}
