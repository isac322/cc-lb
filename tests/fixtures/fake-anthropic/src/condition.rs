use tokio::sync::Notify;

pub(crate) async fn wait_for_condition(predicate: impl FnMut() -> bool, notify: &Notify) {
    let mut predicate = predicate;
    loop {
        let notified = notify.notified();
        if predicate() {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::task::{Context, Poll, Waker};

    use super::wait_for_condition;
    use tokio::sync::Notify;

    #[test]
    fn condition_wait_lost_wakeup_regression() {
        let notify = Notify::new();
        let ready = Cell::new(false);
        let mut future = std::pin::pin!(wait_for_condition(
            || {
                let observed_ready = ready.get();
                if !observed_ready {
                    ready.set(true);
                    notify.notify_waiters();
                }
                observed_ready
            },
            &notify,
        ));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }

    #[test]
    fn condition_wait_returns_when_predicate_is_already_ready() {
        let notify = Notify::new();
        let ready = Cell::new(true);
        let mut future = std::pin::pin!(wait_for_condition(|| ready.get(), &notify));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }

    #[test]
    fn condition_wait_rechecks_after_spurious_wake() {
        let notify = Notify::new();
        let ready = Cell::new(false);
        let mut future = std::pin::pin!(wait_for_condition(|| ready.get(), &notify));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        notify.notify_waiters();
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        ready.set(true);
        notify.notify_waiters();
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }

    #[test]
    fn condition_wait_broadcast_wakes_all_waiters() {
        let notify = Notify::new();
        let ready = Cell::new(false);
        let mut first = std::pin::pin!(wait_for_condition(|| ready.get(), &notify));
        let mut second = std::pin::pin!(wait_for_condition(|| ready.get(), &notify));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));
        ready.set(true);
        notify.notify_waiters();
        assert!(matches!(first.as_mut().poll(&mut context), Poll::Ready(())));
        assert!(matches!(
            second.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
    }
}
