#[macro_use]
extern crate clap;

extern crate roundqueue;

use std::io::Result;
use std::os::unix::fs::PermissionsExt;

fn main() {
    let m = clap::App::new("rq")
        .version(crate_version!())
        .about(crate_description!())
        .subcommand(
            clap::SubCommand::with_name("run")
                .setting(clap::AppSettings::TrailingVarArg)
                .version(crate_version!())
                .about("submit a new job")
                .arg(clap::Arg::with_name("cores")
                     .short("c")
                     .long("cpus")
                     .takes_value(true)
                     .value_name("CPUS")
                     .default_value("1")
                     .hide_default_value(true)
                     .help("the number of cores the job requires [default: 1]"))
                .arg(clap::Arg::with_name("jobname")
                     .short("J")
                     .long("job-name")
                     .takes_value(true)
                     .value_name("NAME")
                     .default_value("")
                     .hide_default_value(true)
                     .help("the name of the job"))
                .arg(clap::Arg::with_name("output")
                     .short("o")
                     .long("output")
                     .takes_value(true)
                     .value_name("OUTFILE")
                     .default_value(DEFAULT_OUTPUT)
                     .help("the file for stdout and stderr"))
                .arg(clap::Arg::with_name("verbose")
                     .long("verbose")
                     .short("v")
                     .multiple(true)
                     .help("show verbose output"))
                .arg(clap::Arg::with_name("command")
                     .index(1)
                     .multiple(true)
                     .help("command line"))
        )
        .subcommand(
            clap::SubCommand::with_name("cancel")
                .version(crate_version!())
                .about("cancel a job")
                .arg(clap::Arg::with_name("jobname")
                     .short("J")
                     .long("job-name")
                     .takes_value(true)
                     .value_name("NAME")
                     .help("the name of the job to cancel"))
                .arg(clap::Arg::with_name("waiting")
                     .long("waiting")
                     .short("w")
                     .help("only cancel jobs that have not yet started running"))
                .arg(clap::Arg::with_name("all")
                     .long("all")
                     .short("a")
                     .help("cancel all jobs"))
                .arg(clap::Arg::with_name("more-job-names")
                     .index(1)
                     .multiple(true)
                     .help("job names to cancel"))
        )
        .subcommand(
            clap::SubCommand::with_name("nodes")
                .version(crate_version!())
                .about("show node information")
        )
        .subcommand(
            clap::SubCommand::with_name("q")
                .version(crate_version!())
                .about("show the queue")
                .arg(clap::Arg::with_name("verbose")
                     .long("verbose")
                     .short("v")
                     .multiple(true)
                     .help("show verbose output"))
        )
        .subcommand(
            clap::SubCommand::with_name("daemon")
                .version(crate_version!())
                .about("spawn the runner daemon")
                .arg(clap::Arg::with_name("verbose")
                     .long("verbose")
                     .short("v")
                     .multiple(true)
                     .help("show verbose output"))
        )
        .get_matches();
    match m.subcommand() {
        ("daemon", Some(_)) => {
            do_daemon().unwrap();
        },
        ("cancel", Some(m)) => {
            let job_selected = move |j: &roundqueue::Job| -> bool {
                if m.is_present("all") {
                    return true;
                }
                if let Some(mut jn) = m.values_of("jobname") {
                    if jn.any(|jn| jn == j.jobname) { return true; }
                }
                if let Some(mut jn) = m.values_of("more-job-names") {
                    if jn.any(|jn| jn == j.jobname) { return true; }
                }
                false
            };
            let status = roundqueue::Status::new().unwrap();
            if let Some(jn) = m.values_of("jobname") {
                for x in jn.filter(|jn| !status.has_jobname(jn)) {
                    println!("No such job: {:?}", x);
                }
            }
            if let Some(jn) = m.values_of("more-job-names") {
                for x in jn.filter(|jn| !status.has_jobname(jn)) {
                    println!("No such job: {:?}", x);
                }
            }
            let mut retry = true;
            while retry {
                let status = roundqueue::Status::new().unwrap();
                retry = false;
                for j in status.waiting.iter().filter(|j| job_selected(j)) {
                    println!("canceling: W {:8} {:10} {:6} {:6} {:30}",
                             homedir_to_username(&j.home_dir),
                             "","",
                             pretty_duration(j.wait_duration()),
                             &j.jobname);
                    if j.cancel().is_err() && !m.is_present("waiting") {
                        println!("Having difficulty canceling {} ... did it just start?",
                                 &j.jobname);
                        retry = true;
                    }
                }
                if !m.is_present("waiting") {
                    for j in status.running.iter().filter(|j| job_selected(&j.job)) {
                        println!("canceling: R {:8} {:10} {:6} {:6} {:30}",
                                 homedir_to_username(&j.job.home_dir),
                                 &j.node,
                                 pretty_duration(j.duration()),
                                 pretty_duration(j.job.wait_duration()),
                                 &j.job.jobname,
                        );
                        if j.cancel().is_err() {
                            println!("error canceling {}.?", &j.job.jobname);
                        }
                    }
                }
            }
        },
        ("nodes", _) => {
            do_nodes().unwrap();
        },
        ("q", _) => {
            do_q().unwrap();
        },
        (_, None) => {
            do_q().unwrap();
        }
        ("run", Some(m)) => {
            let mut command = Vec::new();
            if let Some(c) = m.values_of("command") {
                for x in c {
                    command.push(String::from(x));
                }
            }
            let mut jn = String::from(m.value_of("jobname").unwrap());
            let output = if jn == "" {
                jn = command.join(" ");
                std::path::PathBuf::from(m.value_of("output").unwrap())
            } else {
                if m.value_of("output").unwrap() == DEFAULT_OUTPUT {
                    std::path::PathBuf::from(&jn).with_extension("out")
                } else {
                    std::path::PathBuf::from(m.value_of("output").unwrap())
                }
            };
            if !path_has(&command[0]) {
                println!("No such command: {:?}", &command[0]);
                std::process::exit(1);
            }
            let ncores = value_t!(m, "cores", usize).unwrap_or_else(|e| e.exit());
            println!("submitted {:?}", &jn);
            roundqueue::Job::new(command, jn, output, ncores).unwrap().submit().unwrap()
        },
        (x, _) => {
            eprintln!("Invalid subcommand {}!", x);
            std::process::exit(1);
        }
    }
}

fn homedir_to_username(home: &std::path::Path) -> String {
    match home.file_name() {
        Some(name) => name.to_string_lossy().into_owned(),
        None => home.to_string_lossy().into_owned(),
    }
}

fn do_q() -> Result<()> {
    let mut status = roundqueue::Status::new().unwrap();
    status.waiting.sort_by_key(|j| j.submitted);
    status.waiting.reverse();
    status.running.sort_by_key(|j| j.started);
    let home = std::env::home_dir().unwrap();
    let mut most_recent_submission = std::time::Duration::from_secs(0);
    let seconds = std::time::Duration::from_secs(5);
    println!("STATU USER {:10} {:7} {:7} {} {}",
             "NODE", "RTIME", "SUBMIT", "CPUS", "JOBNAME");
    for j in status.waiting.iter() {
        if j.home_dir == home && j.submitted > most_recent_submission {
            most_recent_submission = j.submitted;
        }
        println!("W {:>8} {:10} {:7} {:7}{:>2} {}",
                 homedir_to_username(&j.home_dir),
                 "","",
                 pretty_duration(j.wait_duration()),
                 j.cores,
                 &j.jobname);
    }
    let mut failed = status.my_failed_jobs();
    let mut completed = status.my_completed_jobs();
    failed.sort_by_key(|j| j.started);
    completed.sort_by_key(|j| j.started);
    for j in status.running.iter().chain(&failed).chain(&completed) {
        if j.job.home_dir == home && j.job.submitted > most_recent_submission {
            most_recent_submission = j.job.submitted;
        }
    }
    // Scoot backwards just a tad... the intent is that if users
    // submit a bunch of jobs with a script, they should be able to
    // see all of their completion afterwards, even if the first ones
    // finish before the last are submitted.
    for j in status.waiting.iter() {
        if j.home_dir == home && j.submitted+seconds > most_recent_submission {
            most_recent_submission = j.submitted;
        }
    }
    status.waiting.reverse();
    for j in status.running.iter().chain(&failed).chain(&completed) {
        if j.job.home_dir == home && j.job.submitted+seconds > most_recent_submission {
            most_recent_submission = j.job.submitted;
        }
    }
    most_recent_submission -= std::time::Duration::from_secs(60);
    completed.reverse();
    for j in completed.iter().filter(|j| j.completed > most_recent_submission) {
        println!("C {:>8} {:10} {:7} {:7}{:>2} {}",
                 homedir_to_username(&j.job.home_dir),
                 &j.node,
                 pretty_duration(j.duration()),
                 pretty_duration(j.job.wait_duration()),
                 j.job.cores,
                 &j.job.jobname,
        );
    }
    failed.reverse();
    for j in failed.iter().filter(|j| j.completed > most_recent_submission) {
        println!("F {:>8} {:10} {:7} {:7}{:>2} {}",
                 homedir_to_username(&j.job.home_dir),
                 &j.node,
                 pretty_duration(j.duration()),
                 pretty_duration(j.job.wait_duration()),
                 j.job.cores,
                 &j.job.jobname,
        );
    }
    status.running.reverse();
    for j in status.running.iter() {
        println!("R {:>8} {:10} {:7} {:7}{:>2} {}",
                 homedir_to_username(&j.job.home_dir),
                 &j.node,
                 pretty_duration(j.duration()),
                 pretty_duration(j.job.wait_duration()),
                 j.job.cores,
                 &j.job.jobname,
        );
    }
    Ok(())
}

fn do_nodes() -> Result<()> {
    let status = roundqueue::Status::new()?;
    println!("{:>12} {:>2}/{:<2}({})",
             "NODE", "R", "C", "H");
    for h in status.nodes.iter() {
        let running = status.running.iter().filter(|j| j.node == h.hostname).count();
        println!("{:>12} {:>2}/{:<2}({})",
                 h.hostname,
                 running,
                 h.physical_cores,
                 h.logical_cpus);
    }
    Ok(())
}

fn do_daemon() -> Result<()> {
    roundqueue::spawn_runner()
}

fn pretty_duration(time: std::time::Duration) -> String {
    let secs = time.as_secs();
    let minute = 60;
    let hour = 60*minute;
    let day = 24*hour;
    if secs < hour {
        format!("{}:{:02}", secs/minute, secs%minute)
    } else if secs < day {
        format!("{}h:{:02}m", secs/hour, (secs%hour)/minute)
    } else {
        format!("{}d-{:02}h", secs/day, (secs%day)/hour)
    }
}

const DEFAULT_OUTPUT: &'static str = "round-queue.log";

fn path_has(cmd: &str) -> bool {
    let p = std::path::PathBuf::from(cmd);
    if p.components().count() > 1 {
        // this cmd has "/" in it, so we need to look in the current
        // directory...
        is_executable(&p)
    } else {
        let key = "PATH";
        match std::env::var_os(key) {
            Some(paths) => {
                for path in std::env::split_paths(&paths) {
                    if is_executable(&path.join(&p)) {
                        return true;
                    }
                }
                false
            }
            None => {
                println!("PATH is undefined!");
                false
            }
        }
    }
}

fn is_executable(p: &std::path::Path) -> bool {
    if let Ok(md) = p.metadata() {
        (md.permissions().mode() & 1) == 1
    } else {
        false
    }
}
