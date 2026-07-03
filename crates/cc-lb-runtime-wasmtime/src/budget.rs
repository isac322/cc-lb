use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::WasmtimeRuntimeError;

#[derive(Debug)]
pub struct StoreBudget {
    limit: u32,
    in_flight: AtomicU32,
}

impl StoreBudget {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            in_flight: AtomicU32::new(0),
        }
    }

    pub fn try_acquire(self: &Arc<Self>) -> Result<StorePermit, WasmtimeRuntimeError> {
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.limit {
                return Err(WasmtimeRuntimeError::PoolSaturated { resource: "stores" });
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(StorePermit {
                        budget: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

#[derive(Debug)]
pub struct StorePermit {
    budget: Arc<StoreBudget>,
}

impl Drop for StorePermit {
    fn drop(&mut self) {
        self.budget.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_budget_rejects_second_concurrent_store() {
        let budget = Arc::new(StoreBudget::new(1));
        let _permit = budget.try_acquire().expect("first store enters");

        let err = budget
            .try_acquire()
            .expect_err("second concurrent store saturates budget");

        assert!(matches!(
            err,
            WasmtimeRuntimeError::PoolSaturated { resource: "stores" }
        ));
    }

    #[test]
    fn store_budget_releases_capacity_on_drop() {
        let budget = Arc::new(StoreBudget::new(1));
        let permit = budget.try_acquire().expect("first store enters");
        drop(permit);

        budget.try_acquire().expect("dropped permit releases slot");
    }
}
