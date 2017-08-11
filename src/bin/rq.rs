extern crate clap;

extern crate roundqueue;

use std::io::Result;

fn main() {
    let m = clap::App::new("rq")
        .subcommand(
            clap::SubCommand::with_name("run")
                .about("submit a new job")
                .arg(clap::Arg::with_name("cores")
                     .short("c")
                     .long("cpus")
                     .takes_value(true)
                     .value_name("CPUS")
                     .default_value("1")
                     .hide_default_value(true)
                     .help("the number of cores the job requires"))
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
                     .default_value("round-queue.log")
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
                .about("cancel a job")
                .arg(clap::Arg::with_name("jobname")
                     .short("J")
                     .long("job-name")
                     .takes_value(true)
                     .value_name("NAME")
                     .help("the name of the job to cancel"))
        )
        .subcommand(
            clap::SubCommand::with_name("nodes")
                .about("show node information")
        )
        .subcommand(
            clap::SubCommand::with_name("q")
                .about("show the queue")
                .arg(clap::Arg::with_name("verbose")
                     .long("verbose")
                     .short("v")
                     .multiple(true)
                     .help("show verbose output"))
        )
        .subcommand(
            clap::SubCommand::with_name("daemon")
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
            let status = roundqueue::Status::new().unwrap();
            if let Some(jn) = m.value_of("jobname") {
                let mut retry = true;
                while retry {
                    retry = false;
                    for j in status.waiting.iter()
                        .filter(|j| j.jobname == jn) {
                        println!("W {:8} {:10} {:6} {:6} {:30}",
                                 homedir_to_username(&j.home_dir),
                                 "","",
                                 pretty_duration(j.wait_duration()),
                                 &j.jobname);
                        if j.cancel().is_err() {
                            println!("difficulty canceling {} ... did it just start?", &j.jobname);
                        }
                    }
                    for j in status.running.iter()
                        .filter(|j| j.job.jobname == jn)
                    {
                        println!("R {:8} {:10} {:6} {:6} {:30}",
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
            } else {
                println!("hello world");
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
            if jn == "" {
                jn = command.join(" ");
            }
            println!("submitted {:?}", &jn);
            roundqueue::Job::new(command, jn,
                                 std::path::PathBuf::from(m.value_of("output").unwrap())
            ).unwrap().submit().unwrap()
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
    status.running.reverse();
    for j in status.waiting.iter() {
        println!("W {:8} {:10} {:6} {:6} {:30}",
                 homedir_to_username(&j.home_dir),
                 "","",
                 pretty_duration(j.wait_duration()),
                 &j.jobname);
    }
    for j in status.running.iter() {
        println!("R {:8} {:10} {:6} {:6} {:30}",
                 homedir_to_username(&j.job.home_dir),
                 &j.node,
                 pretty_duration(j.duration()),
                 pretty_duration(j.job.wait_duration()),
                 &j.job.jobname,
        );
    }
    Ok(())
}

fn do_nodes() -> Result<()> {
    let status = roundqueue::Status::new()?;
    println!("{:>12} {:>2}/{:<2} {:5}",
             "NODE", "R", "C", "HYPER");
    for h in status.nodes.iter() {
        let running = status.running.iter().filter(|j| j.node == h.hostname).count();
        println!("{:>12} {:>2}/{:<2} {:5}",
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
    if secs < 60*60 {
        format!("{}:{:02}", secs/60, secs%60)
    } else {
        format!("{}h:{:02}m", secs/60/60, (secs/60)%60)
    }
}
