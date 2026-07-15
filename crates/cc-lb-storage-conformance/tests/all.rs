#[path = "support/request_event_quota.rs"]
mod request_event_quota_support;

#[cfg(all(feature = "sqlite", feature = "postgres"))]
#[path = "managed_keys_cross_backend.rs"]
mod managed_keys_cross_backend;
#[cfg(feature = "postgres")]
#[path = "managed_keys_postgres.rs"]
mod managed_keys_postgres;
#[path = "quota_aggregate_parity.rs"]
mod quota_aggregate_parity;
#[cfg(feature = "postgres")]
#[path = "storage_roundtrips_postgres.rs"]
mod storage_roundtrips_postgres;
#[cfg(feature = "sqlite")]
#[path = "storage_roundtrips_sqlite.rs"]
mod storage_roundtrips_sqlite;
