#![cfg(loom)]

use loom::sync::{Arc, Mutex};
use loom::thread;

#[derive(Debug)]
struct View {
    generation: u64,
}

struct ViewHolder {
    inner: Mutex<Arc<View>>,
}

impl ViewHolder {
    fn new(view: Arc<View>) -> Self {
        Self {
            inner: Mutex::new(view),
        }
    }

    fn load(&self) -> Arc<View> {
        let inner = self.inner.lock().expect("view lock is not poisoned");
        Arc::clone(&inner)
    }

    fn store(&self, view: Arc<View>) {
        let mut inner = self.inner.lock().expect("view lock is not poisoned");
        *inner = view;
    }
}

#[test]
fn loom_dynamic_view_generation_monotonic() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let holder = Arc::new(ViewHolder::new(Arc::new(View { generation: 0 })));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let reader_holder = Arc::clone(&holder);
            readers.push(thread::spawn(move || {
                let first_generation = reader_holder.load().generation;
                let second_generation = reader_holder.load().generation;
                assert!(second_generation >= first_generation);
            }));
        }

        holder.store(Arc::new(View { generation: 1 }));
        for reader in readers {
            reader.join().expect("reader completes");
        }
    });
}
