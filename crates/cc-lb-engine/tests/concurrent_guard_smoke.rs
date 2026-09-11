use std::sync::{Arc, Barrier};

use cc_lb_engine::api_keys::concurrent_guard::{ConcurrencyRejected, KeyConcurrencyManager};

#[test]
fn cap_two_third_reject() {
    let manager = KeyConcurrencyManager::new();
    let key_id = "key-cap-two";

    let first = manager.try_acquire(key_id, 2).expect("first acquire");
    let second = manager.try_acquire(key_id, 2).expect("second acquire");

    assert_eq!(manager.current(key_id), 2);
    assert!(matches!(
        manager.try_acquire(key_id, 2),
        Err(ConcurrencyRejected::Limit(2))
    ));

    drop(first);
    drop(second);
    assert_eq!(manager.current(key_id), 0);
}

#[test]
fn drop_releases() {
    let manager = KeyConcurrencyManager::new();
    let key_id = "key-drop-release";

    let guard = manager.try_acquire(key_id, 1).expect("acquire slot");
    assert_eq!(manager.current(key_id), 1);

    drop(guard);
    assert_eq!(manager.current(key_id), 0);

    let reacquired = manager.try_acquire(key_id, 1).expect("reacquire slot");
    assert_eq!(manager.current(key_id), 1);
    drop(reacquired);
    assert_eq!(manager.current(key_id), 0);
}

// tier-allow(multi-thread): os-thread claim
#[test]
fn concurrent_try_acquire_allows_exactly_cap() {
    const THREADS: usize = 100;
    const CAP: u32 = 10;

    let manager = KeyConcurrencyManager::new();
    let key_id = "key-thread-cap";

    std::thread::scope(|scope| {
        let start = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let start = Arc::clone(&start);
            let manager = &manager;
            handles.push(scope.spawn(move || {
                start.wait();
                manager.try_acquire(key_id, CAP)
            }));
        }

        let mut guards = Vec::new();
        let mut rejected = 0;

        for handle in handles {
            match handle.join().expect("thread joins") {
                Ok(guard) => guards.push(guard),
                Err(ConcurrencyRejected::Limit(limit)) => {
                    assert_eq!(limit, CAP);
                    rejected += 1;
                }
            }
        }

        println!("accepted={}, rejected={rejected}", guards.len());
        assert_eq!(guards.len(), CAP as usize);
        assert_eq!(rejected, THREADS - CAP as usize);
        assert_eq!(manager.current(key_id), CAP);

        drop(guards);
    });

    assert_eq!(manager.current(key_id), 0);
}

// tier-allow(multi-thread): os-thread claim
#[test]
fn concurrent_drop_returns_to_zero() {
    const THREADS: usize = 100;

    let manager = KeyConcurrencyManager::new();
    let key_id = "key-thread-drop";

    std::thread::scope(|scope| {
        let start = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let start = Arc::clone(&start);
            let manager = &manager;
            handles.push(scope.spawn(move || {
                start.wait();
                let guard = manager
                    .try_acquire(key_id, THREADS as u32)
                    .expect("acquire slot");
                drop(guard);
            }));
        }

        for handle in handles {
            handle.join().expect("thread joins");
        }
    });

    assert_eq!(manager.current(key_id), 0);
}
