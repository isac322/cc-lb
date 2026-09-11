#![cfg(loom)]

use loom::sync::atomic::{AtomicI64, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

#[test]
fn loom_limit_engine_rolling_drop_refunds_full_reservations() {
    loom::model(|| {
        let counter = Arc::new(RollingCounter::new(100));
        let mut handles = Vec::new();

        for _ in 0..2 {
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                let reservation = counter.reserve(1).expect("reservation succeeds");
                drop(reservation);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.remaining(), 100);
    });
}

#[test]
fn loom_limit_engine_rolling_reconcile_keeps_actual_totals() {
    loom::model(|| {
        let counter = Arc::new(RollingCounter::new(1_000));
        let mut handles = Vec::new();

        for _ in 0..2 {
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                let reservation = counter.reserve(150).expect("reservation succeeds");
                reservation.reconcile(15);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.remaining(), 970);
    });
}

#[test]
fn loom_limit_engine_rolling_drop_vs_reconcile_single_owner_wins() {
    loom::model(|| {
        let counter = Arc::new(RollingCounter::new(1_000));
        let reservation = counter.reserve(150).expect("reservation succeeds");
        let slot = Arc::new(Mutex::new(Some(reservation)));

        let drop_slot = Arc::clone(&slot);
        let drop_handle = thread::spawn(move || {
            let reservation = drop_slot.lock().unwrap().take();
            drop(reservation);
        });

        let reconcile_slot = Arc::clone(&slot);
        let reconcile_handle = thread::spawn(move || {
            if let Some(reservation) = reconcile_slot.lock().unwrap().take() {
                reservation.reconcile(15);
            }
        });

        drop_handle.join().unwrap();
        reconcile_handle.join().unwrap();

        let remaining = counter.remaining();
        assert!(matches!(remaining, 1_000 | 985));
    });
}

struct RollingCounter {
    remaining: Arc<AtomicI64>,
}

impl RollingCounter {
    fn new(capacity: i64) -> Self {
        Self {
            remaining: Arc::new(AtomicI64::new(capacity)),
        }
    }

    fn reserve(&self, amount: i64) -> Result<Reservation, ()> {
        let mut observed = self.remaining.load(Ordering::Acquire);
        loop {
            if observed < amount {
                return Err(());
            }
            match self.remaining.compare_exchange(
                observed,
                observed - amount,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Reservation {
                        remaining: Arc::clone(&self.remaining),
                        reserved: amount,
                        settled: false,
                    });
                }
                Err(actual) => observed = actual,
            }
        }
    }

    fn remaining(&self) -> i64 {
        self.remaining.load(Ordering::Acquire)
    }
}

struct Reservation {
    remaining: Arc<AtomicI64>,
    reserved: i64,
    settled: bool,
}

impl Reservation {
    fn reconcile(mut self, actual: i64) {
        self.remaining
            .fetch_add(self.reserved - actual, Ordering::AcqRel);
        self.settled = true;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.settled {
            self.remaining.fetch_add(self.reserved, Ordering::AcqRel);
        }
    }
}
