#![cfg(loom)]

use cc_lb_loom_tests::api_keys::concurrent_guard::KeyConcurrencyManager;
use loom::sync::Arc;
use loom::thread;

#[test]
fn loom_key_concurrent_cap_two_allows_exactly_two() {
    loom::model(|| {
        let cm = Arc::new(KeyConcurrencyManager::new());
        let seed = cm.try_acquire("k", 2).expect("seed key counter");
        drop(seed);
        assert_eq!(cm.current("k"), 0);
        let mut handles = Vec::new();

        for _ in 0..3 {
            let cm = Arc::clone(&cm);
            handles.push(thread::spawn(move || cm.try_acquire("k", 2)));
        }

        let mut acquired = Vec::new();
        let mut rejected = 0;
        for handle in handles {
            match handle.join().unwrap() {
                Ok(guard) => acquired.push(guard),
                Err(_) => rejected += 1,
            }
        }

        assert_eq!(acquired.len(), 2);
        assert_eq!(rejected, 1);
        drop(acquired);
        assert_eq!(cm.current("k"), 0);
    });
}

#[test]
fn loom_key_concurrent_drop_two_guards_concurrently() {
    loom::model(|| {
        let cm = Arc::new(KeyConcurrencyManager::new());
        let first = cm.try_acquire("k", 2).expect("first guard");
        let second = cm.try_acquire("k", 2).expect("second guard");

        let first_drop = thread::spawn(move || drop(first));
        let second_drop = thread::spawn(move || drop(second));

        first_drop.join().unwrap();
        second_drop.join().unwrap();
        assert_eq!(cm.current("k"), 0);
    });
}
