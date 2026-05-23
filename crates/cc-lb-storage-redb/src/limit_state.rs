use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::{RedbStorage, StorageError, PRINCIPAL_LIMIT_STATES_V1};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitKind {
    Requests,
    Tokens,
    InputTokens,
    OutputTokens,
}

impl PrincipalLimitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Tokens => "tokens",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalLimitIdentityKind {
    Account,
    Credential,
    Unobserved,
}

impl PrincipalLimitIdentityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Credential => "credential",
            Self::Unobserved => "unobserved",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalLimitState {
    pub principal_id: String,
    pub identity_kind: PrincipalLimitIdentityKind,
    pub identity_value: Option<String>,
    pub account_observed: bool,
    pub window: String,
    pub kind: PrincipalLimitKind,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
    pub stored_at_unix_secs: u64,
}

pub fn principal_limit_state_key(
    principal_id: &str,
    identity_kind: PrincipalLimitIdentityKind,
    identity_value: Option<&str>,
    window: &str,
    kind: PrincipalLimitKind,
) -> Vec<u8> {
    let mut key = Vec::new();
    push_segment(&mut key, "v1");
    push_segment(&mut key, principal_id);
    push_segment(&mut key, identity_kind.as_str());
    push_segment(
        &mut key,
        identity_value
            .filter(|_| identity_kind != PrincipalLimitIdentityKind::Unobserved)
            .unwrap_or(""),
    );
    push_segment(&mut key, window);
    push_segment(&mut key, kind.as_str());
    key
}

impl RedbStorage {
    pub fn put_principal_limit_state(
        &self,
        state: &PrincipalLimitState,
    ) -> Result<(), StorageError> {
        let key = state.key();
        let payload = serde_json::to_vec(state)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PRINCIPAL_LIMIT_STATES_V1)?;
            table.insert(key.as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_principal_limit_state(
        &self,
        principal_id: &str,
        identity_kind: PrincipalLimitIdentityKind,
        identity_value: Option<&str>,
        window: &str,
        kind: PrincipalLimitKind,
    ) -> Result<Option<PrincipalLimitState>, StorageError> {
        let key =
            principal_limit_state_key(principal_id, identity_kind, identity_value, window, kind);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PRINCIPAL_LIMIT_STATES_V1)?;
        let Some(value) = table
            .get(key.as_slice())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&value)?))
    }

    pub fn list_principal_limit_states(
        &self,
        principal_id: &str,
    ) -> Result<Vec<PrincipalLimitState>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PRINCIPAL_LIMIT_STATES_V1)?;
        let mut states = Vec::new();
        for row in table.iter()? {
            let (_, value) = row?;
            let state: PrincipalLimitState = serde_json::from_slice(value.value())?;
            if state.principal_id == principal_id {
                states.push(state);
            }
        }
        Ok(states)
    }
}

impl PrincipalLimitState {
    fn key(&self) -> Vec<u8> {
        principal_limit_state_key(
            &self.principal_id,
            self.identity_kind,
            self.identity_value.as_deref(),
            &self.window,
            self.kind,
        )
    }
}

fn push_segment(key: &mut Vec<u8>, value: &str) {
    key.extend_from_slice(&(value.len() as u64).to_be_bytes());
    key.extend_from_slice(value.as_bytes());
}
