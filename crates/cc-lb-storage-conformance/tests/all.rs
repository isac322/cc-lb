#![allow(non_snake_case)]

#[path = "support/request_event_quota.rs"]
mod request_event_quota_support;

#[path = "storage_roundtrips_fake.rs"]
mod storage_roundtrips_fake;
#[cfg(feature = "postgres")]
#[path = "storage_roundtrips_postgres.rs"]
mod storage_roundtrips_postgres;
#[cfg(feature = "sqlite")]
#[path = "storage_roundtrips_sqlite.rs"]
mod storage_roundtrips_sqlite;
#[cfg(feature = "postgres")]
#[path = "postgres_fixture.rs"]
mod t3_postgres__fixture;
#[cfg(feature = "postgres")]
#[path = "managed_keys_postgres.rs"]
mod t3_postgres__managed_keys;
#[cfg(all(feature = "sqlite", feature = "postgres"))]
#[path = "managed_keys_cross_backend.rs"]
mod t3_postgres__managed_keys_cross_backend;
#[path = "quota_aggregate_parity.rs"]
mod t3_postgres__quota_aggregate_parity;
