extern crate roundqueue;
extern crate clap;

use std::ffi::OsString;

fn main() {
    let m = clap::App::new("run")
        .about("run a command")
        .arg(clap::Arg::with_name("jobs")
             .short("j")
             .long("jobs")
             .takes_value(true)
             .value_name("JOBS")
             .default_value("0")
             .hide_default_value(true)
             .help("the number of jobs to run simultaneously"))
        .arg(clap::Arg::with_name("clean")
             .short("c")
             .long("clean")
             .help("remove all traces of built files"))
        .arg(clap::Arg::with_name("dry")
             .long("dry")
             .help("dry run (don't do any building!)"))
        .arg(clap::Arg::with_name("verbose")
             .long("verbose")
             .short("v")
             .multiple(true)
             .help("show verbose output"))
        .arg(clap::Arg::with_name("command")
             .index(1)
             .multiple(true)
             .help("command line"))
        .get_matches();
    let mut command = Vec::new();
    if let Some(c) = m.values_of_os("command") {
        for x in c {
            command.push(OsString::from(x));
        }
    }
    println!("Command: {:?}", &command);
    roundqueue::Job::new(command, String::from("testing")).ok();
}
