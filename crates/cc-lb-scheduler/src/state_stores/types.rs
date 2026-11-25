use sqlx::{Database, Pool};
use uuid::Uuid;

macro_rules! define_store {
    ($name:ident) => {
        #[derive(Debug)]
        pub struct $name<Db: Database> {
            pub(super) pool: Pool<Db>,
            pub(super) schema: Option<&'static str>,
        }

        impl<Db: Database> Clone for $name<Db> {
            fn clone(&self) -> Self {
                Self {
                    pool: self.pool.clone(),
                    schema: self.schema,
                }
            }
        }

        impl<Db: Database> $name<Db> {
            pub fn new(pool: Pool<Db>) -> Self {
                Self { pool, schema: None }
            }

            pub fn with_scheduler_schema(pool: Pool<Db>) -> Self {
                Self {
                    pool,
                    schema: Some("cc_lb_scheduler"),
                }
            }
        }
    };
}

define_store!(OAuthUsagePollCursorsStore);
define_store!(AnthropicCompatEtagsStore);
define_store!(PriceCatalogVersionsStore);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthUsagePollCursor {
    pub upstream_id: Uuid,
    pub last_observed_at_unix_secs: Option<u64>,
    pub last_status: Option<i32>,
    pub attempt_count: u32,
}

impl OAuthUsagePollCursor {
    pub fn new(upstream_id: Uuid) -> Self {
        Self {
            upstream_id,
            last_observed_at_unix_secs: None,
            last_status: None,
            attempt_count: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnthropicCompatEtag {
    pub key: String,
    pub last_applied_at_unix_secs: u64,
    pub last_value_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PriceCatalogVersion {
    pub source: String,
    pub fingerprint: String,
    pub fetched_at_unix_secs: u64,
}
