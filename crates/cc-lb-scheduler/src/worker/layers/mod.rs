mod entity;
mod singleton;

pub use entity::EntityWorker;
pub use singleton::{SingletonWorker, build_singleton_worker};

pub(in crate::worker) use entity::build_backend_entity_worker;
