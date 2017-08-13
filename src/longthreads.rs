use std::sync::{Arc, Mutex, Condvar};
use std;

/// A pool of threads that will wait until they are completed.
/// Specifically, the drop method of `Threads` will wait until all of
/// its children are complete.  They are, however, *detached* threads,
/// which means that they will be cleaned up *as* they complete, and
/// there is no need to join them, even if this is a very long-lived
/// process, and the drop method is never called.
pub struct Threads {
    count: Arc<(Mutex<usize>, Condvar)>,
}

impl Threads {
    pub fn new() -> Threads {
        Threads {
            count: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }
    pub fn spawn<F, T>(&self, f: F)
        where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        {
            let mut count = self.count.0.lock().unwrap();
            *count += 1;
        }
        let countclone = self.count.clone();
        std::thread::spawn(move || {
            f();
            let &(ref lock, ref cvar) = &*countclone;
            let mut count = lock.lock().unwrap();
            *count -= 1;
            if *count == 0 {
                cvar.notify_one();
            }
        });
    }
}

impl Drop for Threads {
    fn drop(&mut self) {
        let &(ref lock, ref cvar) = &*self.count;
        let mut count = lock.lock().unwrap();
        while *count > 0 {
            count = cvar.wait(count).unwrap();
        }
    }
}
