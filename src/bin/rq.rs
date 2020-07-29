#[macro_use]
extern crate clap;

extern crate dirs;
extern crate hostname;
extern crate roundqueue;

use std::io::Result;
use std::os::unix::fs::PermissionsExt;

const VERSION: &str = git_version::git_version!(
    args = ["--always", "--tags", "--dirty"],
    cargo_prefix = "cargo-"
);

fn main() {
    let m = clap::App::new("rq")
        .version(VERSION)
        .about(crate_description!())
        .arg(
            clap::Arg::with_name("user")
                .short("u")
                .long("user")
                .takes_value(true)
                .value_name("USER")
                .help("show only jobs of USER"),
        )
        .arg(
            clap::Arg::with_name("mine")
                .long("mine")
                .help("show only my own jobs"),
        )
        .subcommand(
            clap::SubCommand::with_name("run")
                .setting(clap::AppSettings::TrailingVarArg)
                .version(VERSION)
                .about("submit a new job")
                .arg(
                    clap::Arg::with_name("cores")
                        .short("c")
                        .long("cpus")
                        .takes_value(true)
                        .value_name("CPUS")
                        .default_value("1")
                        .hide_default_value(true)
                        .help("the number of cores the job requires [default: 1]"),
                )
                .arg(
                    clap::Arg::with_name("max-output")
                        .long("max-output")
                        .takes_value(true)
                        .value_name("MB")
                        .default_value("1")
                        .hide_default_value(true)
                        .help("the maximum size of the output file [default: 1MB]"),
                )
                .arg(
                    clap::Arg::with_name("restart")
                        .long("restart")
                        .short("R")
                        .help("this job can be restarted"),
                )
                .arg(
                    clap::Arg::with_name("jobname")
                        .short("J")
                        .long("job-name")
                        .takes_value(true)
                        .value_name("NAME")
                        .default_value("")
                        .hide_default_value(true)
                        .help("the name of the job"),
                )
                .arg(
                    clap::Arg::with_name("output")
                        .short("o")
                        .long("output")
                        .takes_value(true)
                        .value_name("OUTFILE")
                        .default_value(DEFAULT_OUTPUT)
                        .help("the file for stdout and stderr"),
                )
                .arg(
                    clap::Arg::with_name("verbose")
                        .long("verbose")
                        .short("v")
                        .multiple(true)
                        .help("show verbose output"),
                )
                .arg(
                    clap::Arg::with_name("command")
                        .index(1)
                        .multiple(true)
                        .help("command line"),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("cancel")
                .version(VERSION)
                .about("cancel a job")
                .arg(
                    clap::Arg::with_name("jobname")
                        .short("J")
                        .long("job-name")
                        .takes_value(true)
                        .value_name("NAME")
                        .help("the name of the job to cancel"),
                )
                .arg(
                    clap::Arg::with_name("waiting")
                        .long("waiting")
                        .short("w")
                        .help("only cancel jobs that have not yet started running"),
                )
                .arg(
                    clap::Arg::with_name("all")
                        .long("all")
                        .short("a")
                        .help("cancel all jobs"),
                )
                .arg(
                    clap::Arg::with_name("more-job-names")
                        .index(1)
                        .multiple(true)
                        .help("job names to cancel"),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("restart")
                .version(VERSION)
                .about("restart a job")
                .arg(
                    clap::Arg::with_name("jobname")
                        .short("J")
                        .long("job-name")
                        .takes_value(true)
                        .value_name("NAME")
                        .help("the name of the job to restart"),
                )
                .arg(
                    clap::Arg::with_name("all")
                        .long("all")
                        .short("a")
                        .help("restart all jobs"),
                )
                .arg(
                    clap::Arg::with_name("more-job-names")
                        .index(1)
                        .multiple(true)
                        .help("job names to restart"),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("nodes")
                .version(VERSION)
                .about("show node information"),
        )
        .subcommand(
            clap::SubCommand::with_name("users")
                .version(VERSION)
                .about("show information about users"),
        )
        .subcommand(
            clap::SubCommand::with_name("q")
                .version(VERSION)
                .about("show the queue")
                .arg(
                    clap::Arg::with_name("verbose")
                        .long("verbose")
                        .short("v")
                        .multiple(true)
                        .help("show verbose output"),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("daemon")
                .version(VERSION)
                .about("spawn the runner daemon")
                .arg(
                    clap::Arg::with_name("verbose")
                        .long("verbose")
                        .short("v")
                        .multiple(true)
                        .help("show verbose output"),
                )
                .arg(
                    clap::Arg::with_name("fg")
                        .long("fg")
                        .help("Briefly run the daemon in the foreground (for testing)."),
                ),
        )
        .get_matches();
    match m.subcommand() {
        ("daemon", Some(m)) => {
            roundqueue::spawn_runner(m.is_present("fg"), false).unwrap();
        }
        ("cancel", Some(m)) => {
            let job_selected = move |j: &roundqueue::Job| -> bool {
                if m.is_present("all") {
                    return true;
                }
                if let Some(mut jn) = m.values_of("jobname") {
                    if jn.any(|jn| jn == j.jobname) {
                        return true;
                    }
                }
                if let Some(mut jn) = m.values_of("more-job-names") {
                    if jn.any(|jn| jn == j.jobname) {
                        return true;
                    }
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
                    println!(
                        "canceling: W {:8} {:10} {:6} {:6} {:30}",
                        homedir_to_username(&j.home_dir),
                        "",
                        "",
                        pretty_duration(j.wait_duration()),
                        &j.jobname
                    );
                    if j.cancel().is_err() && !m.is_present("waiting") {
                        println!(
                            "Having difficulty canceling {} ... did it just start?",
                            &j.jobname
                        );
                        retry = true;
                    }
                }
                if !m.is_present("waiting") {
                    for j in status.running.iter().filter(|j| job_selected(&j.job)) {
                        println!(
                            "canceling: R {:8} {:10} {:6} {:6} {:30}",
                            homedir_to_username(&j.job.home_dir),
                            &j.node,
                            pretty_duration(j.duration()),
                            pretty_duration(j.wait_duration()),
                            &j.job.jobname,
                        );
                        if let Err(e) = j.cancel() {
                            println!("error canceling {}: {}", &j.job.jobname, e);
                        }
                    }
                }
            }
        }
        ("restart", Some(m)) => {
            let job_selected = move |j: &roundqueue::Job| -> bool {
                if m.is_present("all") {
                    return true;
                }
                if let Some(mut jn) = m.values_of("jobname") {
                    if jn.any(|jn| jn == j.jobname) {
                        return true;
                    }
                }
                if let Some(mut jn) = m.values_of("more-job-names") {
                    if jn.any(|jn| jn == j.jobname) {
                        return true;
                    }
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
                for j in status.running.iter().filter(|j| job_selected(&j.job)) {
                    if !j.job.restartable {
                        println!("job {} is not restartable.", j.job.jobname);
                    } else {
                        println!(
                            "restarting: R {:8} {:10} {:6} {:6} {:30}",
                            homedir_to_username(&j.job.home_dir),
                            &j.node,
                            pretty_duration(j.duration()),
                            pretty_duration(j.wait_duration()),
                            &j.job.jobname,
                        );
                        if let Err(e) = j.cancel() {
                            println!("error restarting {}: {}", &j.job.jobname, e);
                        }
                        // Now we resubmit the job.
                        let mut newj = j.job.clone();
                        newj.submitted = roundqueue::now();
                        newj.submit().unwrap();
                    }
                }
            }
            roundqueue::spawn_runner(false, true).unwrap();
        }
        ("nodes", _) => {
            do_nodes().unwrap();
        }
        ("users", _) => {
            do_users().unwrap();
        }
        ("q", _) => {
            if m.is_present("mine") {
                let home = dirs::home_dir().unwrap();
                do_q(|j| &j.home_dir == &home).unwrap()
            } else {
                match m.value_of("user") {
                    None => do_q(|_| true).unwrap(),
                    Some(user) => do_q(|j| homedir_to_username(&j.home_dir) == user).unwrap(),
                }
            }
        }
        (_, None) => {
            if m.is_present("mine") {
                let home = dirs::home_dir().unwrap();
                do_q(|j| &j.home_dir == &home).unwrap()
            } else {
                match m.value_of("user") {
                    None => do_q(|_| true).unwrap(),
                    Some(user) => do_q(|j| homedir_to_username(&j.home_dir) == user).unwrap(),
                }
            }
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
                    // handle properly job names that contain "."
                    let p = std::path::PathBuf::from(&jn);
                    let mut ext = std::ffi::OsString::from("out");
                    if let Some(extension) = p.extension() {
                        ext = std::ffi::OsString::from(extension);
                        ext.push(".out");
                    }
                    p.with_extension(ext)
                } else {
                    std::path::PathBuf::from(m.value_of("output").unwrap())
                }
            };
            if !path_has(&command[0]) {
                println!("No such command: {:?}", &command[0]);
                std::process::exit(1);
            }
            let ncores = value_t!(m, "cores", usize).unwrap_or_else(|e| e.exit());
            let max_output_mb = value_t!(m, "max-output", f64).unwrap_or_else(|e| e.exit());
            let max_output = (max_output_mb * ((1 << 20) as f64)) as u64;
            if let Ok(len) = std::fs::metadata(&output).map(|x| x.len()) {
                if len >= max_output {
                    println!(
                        "Output file {:?} already exceeds maximum size of {} MB, exiting.",
                        output, max_output_mb
                    );
                    std::process::exit(1);
                }
            }
            println!("submitted {:?}", &jn);
            roundqueue::Job::new(
                command,
                jn,
                output,
                ncores,
                max_output,
                m.is_present("restart"),
            )
            .unwrap()
            .submit()
            .unwrap();
            roundqueue::spawn_runner(false, true).unwrap();
        }
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

fn do_q<F>(want_to_see: F) -> Result<()>
where
    F: Fn(&roundqueue::Job) -> bool,
{
    let mut status = roundqueue::Status::new().unwrap();
    status.waiting.sort_by_key(|j| j.submitted);
    status.waiting.reverse();
    status.running.sort_by_key(|j| j.started);
    let home = dirs::home_dir().unwrap();
    let mut most_recent_submission = std::time::Duration::from_secs(0);
    let seconds = std::time::Duration::from_secs(5);
    println!(
        "STATU USER {:10} {:7} {:7} {} {}",
        "NODE", "RTIME", "WAIT", "CPUS", "JOBNAME"
    );
    for j in status.waiting.iter() {
        if j.home_dir == home && j.submitted > most_recent_submission {
            most_recent_submission = j.submitted;
        }
        let juser = homedir_to_username(&j.home_dir);
        if want_to_see(&j) {
            println!(
                "W {:>8} {:10} {:7} {:7}{:>2} {}",
                juser,
                "",
                "",
                pretty_duration(j.wait_duration()),
                j.cores,
                &j.jobname
            );
        }
    }
    let mut failed = status.my_failed_jobs();
    let mut completed = status.my_completed_jobs();
    let mut canceled = status.my_canceled_jobs();
    let mut zombie = status.my_zombie_jobs();
    let mut canceling = status.my_canceling_jobs();
    failed.sort_by_key(|j| j.started);
    completed.sort_by_key(|j| j.started);
    zombie.sort_by_key(|j| j.started);
    canceled.sort_by_key(|j| j.started);
    canceling.sort_by_key(|j| j.started);
    for j in status
        .running
        .iter()
        .chain(&failed)
        .chain(&completed)
        .chain(&canceled)
        .chain(&canceling)
    {
        if j.job.home_dir == home && j.job.submitted > most_recent_submission {
            most_recent_submission = j.job.submitted;
        }
    }
    // Scoot backwards just a tad... the intent is that if users
    // submit a bunch of jobs with a script, they should be able to
    // see all of their completion afterwards, even if the first ones
    // finish before the last are submitted.
    for j in status.waiting.iter() {
        if j.home_dir == home && j.submitted + seconds > most_recent_submission {
            most_recent_submission = j.submitted;
        }
    }
    status.waiting.reverse();
    for j in status.running.iter().chain(&failed).chain(&completed) {
        if j.job.home_dir == home && j.job.submitted + seconds > most_recent_submission {
            most_recent_submission = j.job.submitted;
        }
    }
    if most_recent_submission > std::time::Duration::from_secs(60) {
        most_recent_submission -= std::time::Duration::from_secs(60);
    }
    completed.reverse();
    for j in completed
        .iter()
        .filter(|j| j.completed > most_recent_submission)
    {
        if want_to_see(&j.job) {
            println!(
                "C {:>8} {:10} {:7} {:7}{:>2} {}",
                homedir_to_username(&j.job.home_dir),
                &j.node,
                pretty_duration(j.duration()),
                pretty_duration(j.wait_duration()),
                j.job.cores,
                &j.job.jobname,
            );
        }
    }
    canceled.reverse();
    for j in canceled
        .iter()
        .filter(|j| j.completed >= most_recent_submission)
    {
        if want_to_see(&j.job) {
            println!(
                "X {:>8} {:10} {:7} {:7}{:>2} {}",
                homedir_to_username(&j.job.home_dir),
                &j.node,
                pretty_duration(j.duration()),
                pretty_duration(j.wait_duration()),
                j.job.cores,
                &j.job.jobname,
            );
        }
    }
    canceling.reverse();
    for j in canceling
        .iter()
        .filter(|j| j.completed >= most_recent_submission)
    {
        if want_to_see(&j.job) {
            println!(
                "RX{:>8} {:10} {:7} {:7}{:>2} {}",
                homedir_to_username(&j.job.home_dir),
                &j.node,
                pretty_duration(j.duration()),
                pretty_duration(j.wait_duration()),
                j.job.cores,
                &j.job.jobname,
            );
        }
    }
    failed.reverse();
    for j in failed
        .iter()
        .filter(|j| j.completed > most_recent_submission)
    {
        if want_to_see(&j.job) {
            println!(
                "F {:>8} {:10} {:7} {:7}{:>2} {}",
                homedir_to_username(&j.job.home_dir),
                &j.node,
                pretty_duration(j.duration()),
                pretty_duration(j.wait_duration()),
                j.job.cores,
                &j.job.jobname,
            );
        }
    }
    zombie.reverse();
    for j in zombie
        .iter()
        .filter(|j| j.completed > most_recent_submission)
    {
        if want_to_see(&j.job) {
            println!(
                "Z {:>8} {:10} {:7} {:7}{:>2} {}",
                homedir_to_username(&j.job.home_dir),
                &j.node,
                pretty_duration(j.duration()),
                pretty_duration(j.wait_duration()),
                j.job.cores,
                &j.job.jobname,
            );
        }
    }
    status.running.reverse();
    for j in status.running.iter() {
        if want_to_see(&j.job) {
            if Some(j.node.clone()) == hostname::get().unwrap().into_string().ok() && !j.exists() {
                println!(
                    "r {:>8} {:10} {:7} {:7}{:>2} {}",
                    homedir_to_username(&j.job.home_dir),
                    &j.node,
                    pretty_duration(j.duration()),
                    pretty_duration(j.wait_duration()),
                    j.job.cores,
                    &j.job.jobname
                );
            } else {
                println!(
                    "R {:>8} {:10} {:7} {:7}{:>2} {}",
                    homedir_to_username(&j.job.home_dir),
                    &j.node,
                    pretty_duration(j.duration()),
                    pretty_duration(j.wait_duration()),
                    j.job.cores,
                    &j.job.jobname
                );
            }
        }
    }
    Ok(())
}

fn do_nodes() -> Result<()> {
    let status = roundqueue::Status::new()?;
    println!("{:>12} {:>2}/{:<2}({})", "NODE", "R", "C", "H");
    let mut total_running = 0;
    let mut total_physical = 0;
    let mut total_logical = 0;
    for h in status.nodes.iter() {
        let running = status
            .running
            .iter()
            .filter(|j| j.node == h.hostname)
            .count();
        total_running += running;
        total_physical += h.physical_cores;
        total_logical += h.logical_cpus;
        println!(
            "{:>12} {:>2}/{:<2}({})",
            h.hostname, running, h.physical_cores, h.logical_cpus
        );
    }
    println!("{:>12} {:>2} {:<2} {}", "--------", "--", "--", "--");
    println!(
        "{:>12} {:>2}/{:<2}({})",
        "TOTAL", total_running, total_physical, total_logical
    );
    Ok(())
}

fn do_users() -> Result<()> {
    let status = roundqueue::Status::new()?;
    println!("{:>12} {:>2}/{:<2}({})", "USER", "R", "C", "W");
    let mut total_running = 0;
    let mut total_cpus = 0;
    for h in status.nodes.iter() {
        total_cpus += h.physical_cores;
    }
    let mut total_waiting = 0;
    let mut users: Vec<String> = Vec::new();
    for j in status.running.iter() {
        let u = homedir_to_username(&j.job.home_dir);
        if !users.contains(&u) {
            users.push(u);
        }
    }
    for j in status.waiting.iter() {
        let u = homedir_to_username(&j.home_dir);
        if !users.contains(&u) {
            users.push(u);
        }
    }
    for u in users {
        let running = status
            .running
            .iter()
            .filter(|j| homedir_to_username(&j.job.home_dir) == u)
            .count();
        let waiting = status
            .waiting
            .iter()
            .filter(|j| homedir_to_username(&j.home_dir) == u)
            .count();
        total_running += running;
        total_waiting += waiting;
        println!("{:>12} {:>2}/{:<2}({})", u, running, total_cpus, waiting);
    }
    println!("{:>12} {:>2} {:<2} {}", "--------", "--", "--", "--");
    println!(
        "{:>12} {:>2}/{:<2}({})",
        "TOTAL", total_running, total_cpus, total_waiting
    );
    Ok(())
}

fn pretty_duration(time: std::time::Duration) -> String {
    let secs = time.as_secs();
    let minute = 60;
    let hour = 60 * minute;
    let day = 24 * hour;
    if secs < hour {
        format!("{}:{:02}", secs / minute, secs % minute)
    } else if secs < day {
        format!("{}h:{:02}m", secs / hour, (secs % hour) / minute)
    } else {
        format!("{}d-{:02}h", secs / day, (secs % day) / hour)
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
