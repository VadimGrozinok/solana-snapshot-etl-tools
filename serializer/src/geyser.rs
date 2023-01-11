//! Queue configuration for Solana Geyser plugins
use log::warn;
use serde::{Deserialize, Serialize};
pub use solana_program::pubkey::Pubkey;
use solana_program::{
    hash::Hash, instruction::CompiledInstruction, message::v0::MessageAddressTableLookup,
};
use solana_sdk::signature::Signature;
use solana_transaction_status::{TransactionStatusMeta, UiTransactionStatusMeta};
use std::borrow::Cow;

/// Message data for an account update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    /// The account's public key
    pub key: Pubkey,
    /// The lamport balance of the account
    pub lamports: u64,
    /// The Solana program controlling this account
    pub owner: Pubkey,
    /// True if the account's data is an executable smart contract
    pub executable: bool,
    /// The next epoch for which this account will owe rent
    pub rent_epoch: u64,
    /// The binary data stored on this account
    pub data: Vec<u8>,
    /// Monotonic-increasing counter for sequencing on-chain writes
    pub write_version: u64,
    /// The slot in which this account was updated
    pub slot: u64,
    /// True if this update was triggered by a validator startup
    pub is_startup: bool,
}

/// Transaction message header
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

impl From<solana_program::message::MessageHeader> for MessageHeader {
    fn from(mh: solana_program::message::MessageHeader) -> Self {
        Self {
            num_required_signatures: mh.num_required_signatures,
            num_readonly_signed_accounts: mh.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: mh.num_readonly_unsigned_accounts,
        }
    }
}

/// Legacy message for sanitized transaction
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyMessage {
    pub header: MessageHeader,
    pub account_keys: Vec<Pubkey>,
    pub recent_blockhash: Hash,
    pub instructions: Vec<CompiledInstruction>,
}

impl From<solana_program::message::legacy::Message> for LegacyMessage {
    fn from(lm: solana_program::message::legacy::Message) -> Self {
        Self {
            header: lm.header.into(),
            account_keys: lm.account_keys,
            recent_blockhash: lm.recent_blockhash,
            instructions: lm.instructions,
        }
    }
}

/// Message v0 for sanitized transaction
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageV0 {
    pub header: MessageHeader,
    pub account_keys: Vec<Pubkey>,
    pub recent_blockhash: Hash,
    pub instructions: Vec<CompiledInstruction>,
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

impl From<Cow<'_, solana_program::message::v0::Message>> for MessageV0 {
    fn from(mv0: Cow<solana_program::message::v0::Message>) -> Self {
        Self {
            header: mv0.header.into(),
            account_keys: mv0.account_keys.clone(),
            recent_blockhash: mv0.recent_blockhash,
            instructions: mv0.instructions.clone(),
            address_table_lookups: mv0.address_table_lookups.clone(),
        }
    }
}

/// Represent loaded addresses
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedAddresses {
    pub writable: Vec<Pubkey>,
    pub readonly: Vec<Pubkey>,
}

impl From<Cow<'_, solana_program::message::v0::LoadedAddresses>> for LoadedAddresses {
    fn from(la: Cow<solana_program::message::v0::LoadedAddresses>) -> Self {
        Self {
            writable: la.writable.clone(),
            readonly: la.readonly.clone(),
        }
    }
}

/// Loaded message(v0) for sanitized transaction
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedMessageV0 {
    pub message: MessageV0,
    pub loaded_addresses: LoadedAddresses,
}

impl<'a> From<solana_program::message::v0::LoadedMessage<'a>> for LoadedMessageV0 {
    fn from(lm: solana_program::message::v0::LoadedMessage) -> Self {
        Self {
            message: lm.message.into(),
            loaded_addresses: lm.loaded_addresses.into(),
        }
    }
}

/// Sanitized message of a transaction
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SanitizedMessage {
    Legacy(LegacyMessage),
    V0(LoadedMessageV0),
}

impl From<solana_program::message::SanitizedMessage> for SanitizedMessage {
    fn from(sm: solana_program::message::SanitizedMessage) -> Self {
        match sm {
            solana_program::message::SanitizedMessage::Legacy(legacy) => {
                SanitizedMessage::Legacy(legacy.into())
            }
            solana_program::message::SanitizedMessage::V0(v0) => SanitizedMessage::V0(v0.into()),
        }
    }
}

/// Sanitized transaction
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizedTransaction {
    pub message: SanitizedMessage,
    pub message_hash: Hash,
    pub is_simple_vote_tx: bool,
    pub signatures: Vec<Signature>,
}

impl From<solana_sdk::transaction::SanitizedTransaction> for SanitizedTransaction {
    fn from(st: solana_sdk::transaction::SanitizedTransaction) -> Self {
        Self {
            message: st.message().clone().into(),
            message_hash: *st.message_hash(),
            is_simple_vote_tx: st.is_simple_vote_transaction(),
            signatures: st.signatures().into(),
        }
    }
}

/// Message data for raw Metdata
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataNotify {
    pub slot: u64,
    pub blockhash: String,
    pub rewards: String,
    pub block_time: i64,
    pub block_height: u64,
}

impl MetadataNotify {
    /// Creates new MetadataNotify from ReplicaBlockInfo
    pub fn new_from_replica_block_info(
        rbi: &solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfo,
    ) -> Self {
        let rewards = serde_json::to_string(rbi.rewards).unwrap_or_else(|err| {
            warn!("Rewards field was given default value, err: {:#?}", err);
            "".to_string()
        });

        let block_time = rbi.block_time.unwrap_or_else(|| {
            warn!("Block time field was given default value");
            0
        });

        let block_height = rbi.block_height.unwrap_or_else(|| {
            warn!("Block height field was given default value");
            0
        });

        Self {
            slot: rbi.slot,
            blockhash: rbi.blockhash.to_string(),
            rewards,
            block_time,
            block_height,
        }
    }
}

/// Message data for MetadataOffChain
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NftOffChainDataNotify {
    pub pubkey: String,
    pub uri: String,
    pub slot: u64,
    pub is_startup: bool,
}

/// Message data for raw transaction
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct TransactionNotify {
    pub signature: Signature,
    pub is_vote: bool,
    pub slot: u64,
    pub transaction: SanitizedTransaction,
    pub transaction_meta: TransactionStatusMeta,
}

impl TransactionNotify {
    /// Creates new `TransactionNotify` from `ReplicaTransactionInfo`
    pub fn new_from_replica_transaction_info(
        rti: &solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfo,
        slot: u64,
    ) -> Self {
        Self {
            signature: *rti.signature,
            is_vote: rti.is_vote,
            slot,
            transaction: rti.transaction.clone().into(),
            transaction_meta: rti.transaction_status_meta.clone(),
        }
    }
}

/// Message data for raw transaction
///
/// Can be serialized with Serde
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionNotifyWithSerde {
    pub signature: Signature,
    pub is_vote: bool,
    pub slot: u64,
    pub transaction: SanitizedTransaction,
    pub transaction_meta: UiTransactionStatusMeta,
}

impl TransactionNotifyWithSerde {
    /// Creates new `TransactionNotifyWithSerde` from `TransactionNotify`
    pub fn new_from_transaction_notify(tx_notify: TransactionNotify) -> Self {
        let transaction_meta = UiTransactionStatusMeta::from(tx_notify.transaction_meta);

        Self {
            signature: tx_notify.signature,
            is_vote: tx_notify.is_vote,
            slot: tx_notify.slot,
            transaction: tx_notify.transaction,
            transaction_meta,
        }
    }
}

impl<'a>
    From<(
        solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfo<'a>,
        u64,
    )> for TransactionNotify
{
    fn from(
        payload: (
            solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfo<'a>,
            u64,
        ),
    ) -> Self {
        let (rti, slot) = payload;

        Self {
            signature: *rti.signature,
            is_vote: rti.is_vote,
            slot,
            transaction: rti.transaction.clone().into(),
            transaction_meta: rti.transaction_status_meta.clone(),
        }
    }
}

impl<'a>
    From<(
        &solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfo<'a>,
        u64,
    )> for TransactionNotify
{
    fn from(
        payload: (
            &solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfo<'a>,
            u64,
        ),
    ) -> Self {
        let (rti, slot) = payload;

        Self {
            signature: *rti.signature,
            is_vote: rti.is_vote,
            slot,
            transaction: rti.transaction.clone().into(),
            transaction_meta: rti.transaction_status_meta.clone(),
        }
    }
}
