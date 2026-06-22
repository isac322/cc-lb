#[path = "scheduler_lazy_vs_proactive/common.rs"]
mod common;
#[path = "scheduler_lazy_vs_proactive/fake.rs"]
mod fake;
#[path = "scheduler_lazy_vs_proactive/scenario.rs"]
mod scenario;
#[path = "scheduler_lazy_vs_proactive/worker.rs"]
mod worker;

#[cfg(feature = "postgres")]
#[path = "scheduler_lazy_vs_proactive/postgres.rs"]
mod postgres;
#[cfg(feature = "sqlite")]
#[path = "scheduler_lazy_vs_proactive/sqlite.rs"]
mod sqlite;
