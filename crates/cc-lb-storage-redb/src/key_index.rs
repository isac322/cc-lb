use crate::{Storage, StorageError, KEY_INDEX_BY_HASH_V1};

impl Storage {
    pub fn put_key_index(
        &self,
        write_txn: &redb::WriteTransaction,
        index_hash: &[u8; 32],
        composite_row_key: &[u8],
    ) -> Result<(), StorageError> {
        let value = self.encrypt_value(index_hash.as_slice(), composite_row_key)?;

        {
            let mut table = write_txn.open_table(KEY_INDEX_BY_HASH_V1)?;
            table.insert(index_hash.as_slice(), value.as_slice())?;
        }

        Ok(())
    }

    pub fn get_composite_by_index(
        &self,
        index_hash: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(KEY_INDEX_BY_HASH_V1)?;
        let Some(ciphertext) = table
            .get(index_hash.as_slice())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let composite_row_key = self.decrypt_value(index_hash.as_slice(), &ciphertext)?;
        Ok(Some(composite_row_key))
    }

    pub fn remove_key_index(
        &self,
        write_txn: &redb::WriteTransaction,
        index_hash: &[u8; 32],
    ) -> Result<(), StorageError> {
        {
            let mut table = write_txn.open_table(KEY_INDEX_BY_HASH_V1)?;
            table.remove(index_hash.as_slice())?;
        }

        Ok(())
    }
}
