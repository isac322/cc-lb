#![cfg(loom)]

use cc_lb_loom_tests::single_flight::RefreshState;
use loom::sync::Arc;
use loom::thread;

#[test]
fn single_flight_exact_one_token_endpoint_call() {
    loom::model(|| {
        let state = Arc::new(RefreshState::new());
        let mut handles = Vec::new();

        for _ in 0..3 {
            let state = Arc::clone(&state);
            handles.push(thread::spawn(move || state.on_unauthorized(0)));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(state.token_calls(), 1);
    });
}
