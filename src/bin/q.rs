extern crate roundqueue;

fn main() {
    let status = roundqueue::Status::new().unwrap();
    for j in status.waiting.iter() {
        println!("W {:?} {:?}", &j.home_dir, &j.command);
    }
    for j in status.running.iter() {
        println!("R {:?} {:?}", &j.home_dir, &j.command);
    }
}
