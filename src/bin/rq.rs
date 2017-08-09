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
        ("q", _) => {
            do_q().unwrap();
        },
        ("daemon", Some(_)) => {
            do_daemon().unwrap();
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

fn do_q() -> Result<()> {
    let status = roundqueue::Status::new().unwrap();
    for j in status.waiting.iter() {
        println!("W {:?} {}", &j.home_dir, &j.jobname);
    }
    for j in status.running.iter() {
        println!("R {:?} {}", &j.home_dir, &j.jobname);
    }
    Ok(())
}

fn do_daemon() -> Result<()> {
    roundqueue::spawn_runner()
}
