use bincode::{config, serde as bincode_serde};
use serde::{Deserialize, Serialize};

use crate::{Storage, StorageError, PRICE_CATALOG_V1};

const PRICE_CATALOG_ROW_KEY: &str = "litellm_snapshot";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceSnapshot {
    pub json_bytes: Vec<u8>,
    pub fetched_at_ms: u64,
}

impl Storage {
    pub fn put_price_snapshot(
        &self,
        json_bytes: &[u8],
        fetched_at_ms: u64,
    ) -> Result<(), StorageError> {
        let snapshot = PriceSnapshot {
            json_bytes: json_bytes.to_vec(),
            fetched_at_ms,
        };
        let encoded = bincode_serde::encode_to_vec(snapshot, config::standard())?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PRICE_CATALOG_V1)?;
            table.insert(PRICE_CATALOG_ROW_KEY, encoded.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_price_snapshot(&self) -> Result<Option<PriceSnapshot>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PRICE_CATALOG_V1)?;
        let Some(encoded) = table
            .get(PRICE_CATALOG_ROW_KEY)?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let (snapshot, _): (PriceSnapshot, usize) =
            bincode_serde::decode_from_slice(encoded.as_slice(), config::standard())?;
        Ok(Some(snapshot))
    }
}
