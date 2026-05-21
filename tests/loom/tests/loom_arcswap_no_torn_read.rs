#![cfg(loom)]

use cc_lb_loom_tests::arcswap_cert::{Cert, CertStore};
use loom::sync::Arc;
use loom::thread;

#[test]
fn arcswap_cert_no_torn_read() {
    loom::model(|| {
        let store = Arc::new(CertStore::new(Arc::new(Cert { id: 7 })));
        let mut handles = Vec::new();

        for _ in 0..2 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let snapshot = store.load();
                let id = snapshot.id;
                thread::yield_now();
                assert_eq!(snapshot.id, id);
                assert!(id == 7 || id == 8);
            }));
        }

        let writer = {
            let store = Arc::clone(&store);
            thread::spawn(move || store.store_next())
        };

        for handle in handles {
            handle.join().unwrap();
        }
        writer.join().unwrap();

        assert_eq!(store.load().id, 8);
    });
}
