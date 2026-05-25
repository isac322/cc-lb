use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CONFIG_DRAFT_V1, CONFIG_HISTORY_V1, RedbStorage, StorageError};

const CONFIG_DRAFT_KEY: &str = "draft";
const CONFIG_HISTORY_LIMIT: usize = 50;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConfigDraftState {
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation_error: Option<String>,
    pub saved_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySummary {
    pub upstreams: usize,
    pub principals: usize,
    pub plugin_count: usize,
    pub tls_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub revision: u64,
    pub config_toml: String,
    pub applied_at_unix_secs: u64,
    pub summary: HistorySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredHistoryEntry {
    config_toml: String,
    applied_at_unix_secs: u64,
    summary: HistorySummary,
}

impl RedbStorage {
    pub fn get_config_draft(&self) -> Result<ConfigDraftState, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CONFIG_DRAFT_V1)?;
        match table.get(CONFIG_DRAFT_KEY)? {
            Some(stored) => Ok(serde_json::from_slice(stored.value())?),
            None => Ok(ConfigDraftState::default()),
        }
    }

    pub fn put_config_draft(
        &self,
        mut new: ConfigDraftState,
        expected_revision: u64,
    ) -> Result<u64, StorageError> {
        let write_txn = self.db.begin_write()?;
        let current = {
            let table = write_txn.open_table(CONFIG_DRAFT_V1)?;
            read_config_draft_from_table(&table)?
        };
        if current.revision != expected_revision {
            return Err(StorageError::StaleDraftRevision {
                current: current.revision,
            });
        }

        let revision = current
            .revision
            .checked_add(1)
            .ok_or(StorageError::ConfigRevisionOverflow)?;
        new.revision = revision;
        new.last_validated_revision = None;
        new.last_validation_error = None;

        let payload = serde_json::to_vec(&new)?;
        {
            let mut table = write_txn.open_table(CONFIG_DRAFT_V1)?;
            table.insert(CONFIG_DRAFT_KEY, payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(revision)
    }

    pub fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> Result<(), StorageError> {
        let write_txn = self.db.begin_write()?;
        let mut state = {
            let table = write_txn.open_table(CONFIG_DRAFT_V1)?;
            read_config_draft_from_table(&table)?
        };
        if state.revision != revision {
            return Err(StorageError::StaleDraftRevision {
                current: state.revision,
            });
        }

        match error {
            Some(error) => {
                state.last_validation_error = Some(error);
            }
            None => {
                state.last_validated_revision = Some(revision);
                state.last_validation_error = None;
            }
        }

        let payload = serde_json::to_vec(&state)?;
        {
            let mut table = write_txn.open_table(CONFIG_DRAFT_V1)?;
            table.insert(CONFIG_DRAFT_KEY, payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn append_config_history(
        &self,
        revision: u64,
        config_toml: String,
        applied_at_unix_secs: u64,
        summary: HistorySummary,
    ) -> Result<(), StorageError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(CONFIG_HISTORY_V1)?;
            let stored = StoredHistoryEntry {
                config_toml,
                applied_at_unix_secs,
                summary,
            };
            let payload = serde_json::to_vec(&stored)?;
            table.insert(revision, payload.as_slice())?;

            let mut revisions = Vec::new();
            for row in table.iter()? {
                let (key, _) = row?;
                revisions.push(key.value());
            }
            let prune_count = revisions.len().saturating_sub(CONFIG_HISTORY_LIMIT);
            for old_revision in revisions.into_iter().take(prune_count) {
                table.remove(old_revision)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn list_config_history(&self, limit: usize) -> Result<Vec<HistoryEntry>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CONFIG_HISTORY_V1)?;
        let mut entries = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            entries.push(decode_history_entry(key.value(), value.value())?);
        }
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.revision));
        entries.truncate(limit);
        Ok(entries)
    }

    pub fn get_config_history(&self, revision: u64) -> Result<Option<HistoryEntry>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CONFIG_HISTORY_V1)?;
        match table.get(revision)? {
            Some(stored) => Ok(Some(decode_history_entry(revision, stored.value())?)),
            None => Ok(None),
        }
    }
}

fn read_config_draft_from_table(
    table: &redb::Table<'_, &str, &[u8]>,
) -> Result<ConfigDraftState, StorageError> {
    match table.get(CONFIG_DRAFT_KEY)? {
        Some(stored) => Ok(serde_json::from_slice(stored.value())?),
        None => Ok(ConfigDraftState::default()),
    }
}

fn decode_history_entry(revision: u64, value: &[u8]) -> Result<HistoryEntry, StorageError> {
    let stored: StoredHistoryEntry = serde_json::from_slice(value)?;
    Ok(HistoryEntry {
        revision,
        config_toml: stored.config_toml,
        applied_at_unix_secs: stored.applied_at_unix_secs,
        summary: stored.summary,
    })
}
