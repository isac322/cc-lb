use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cc_lb_storage_api::PriceCatalogCache;

use crate::loader::LoaderError;

pub(crate) async fn put_storage_snapshot(
    storage: Arc<dyn PriceCatalogCache>,
    bytes: Vec<u8>,
    fetched_at_ms: u64,
) -> Result<(), LoaderError> {
    storage
        .put_price_snapshot(&bytes, fetched_at_ms)
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))
}

pub(crate) async fn read_disk_cache(cache_path: PathBuf) -> Result<Option<Vec<u8>>, LoaderError> {
    match tokio::fs::read(&cache_path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LoaderError::Storage(format!(
            "read {}: {error}",
            cache_path.display()
        ))),
    }
}

pub(crate) async fn write_disk_cache(cache_path: PathBuf, bytes: &[u8]) -> Result<(), LoaderError> {
    if let Some(parent) = cache_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| LoaderError::Storage(error.to_string()))?;
    }

    let tmp_path = tmp_cache_path(&cache_path);
    tokio::fs::write(&tmp_path, bytes)
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))?;
    tokio::fs::rename(&tmp_path, &cache_path)
        .await
        .map_err(|error| LoaderError::Storage(error.to_string()))
}

fn tmp_cache_path(cache_path: &Path) -> PathBuf {
    let mut tmp = OsString::from(cache_path.as_os_str());
    tmp.push(".tmp");
    PathBuf::from(tmp)
}
