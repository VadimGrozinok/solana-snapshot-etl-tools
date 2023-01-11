//! Serde serialization module
use crate::geyser::{
    AccountUpdate, MetadataNotify, NftOffChainDataNotify, TransactionNotify,
    TransactionNotifyWithSerde,
};

use super::Serialization;

/// Struct which implements Serde serialization for accounts, block metadata and transactions data
#[derive(Debug, Clone, Copy)]
pub struct SerdeSerialization {}

impl Serialization for SerdeSerialization {
    fn serialize_account(&self, account: &AccountUpdate) -> Vec<u8> {
        let json = serde_json::to_string(account).unwrap().as_bytes().to_vec();

        json
    }

    fn serialize_metadata(&self, metadata: &MetadataNotify) -> Vec<u8> {
        let json = serde_json::to_string(metadata).unwrap().as_bytes().to_vec();

        json
    }

    fn serialize_transaction(&self, transaction: &TransactionNotify) -> Vec<u8> {
        let transaction_notify_with_serde =
            TransactionNotifyWithSerde::new_from_transaction_notify(transaction.clone());

        let json = serde_json::to_string(&transaction_notify_with_serde)
            .unwrap()
            .as_bytes()
            .to_vec();

        json
    }

    fn serialize_nft_off_chain_data(&self, nft_off_chain_data: &NftOffChainDataNotify) -> Vec<u8> {
        let json = serde_json::to_string(nft_off_chain_data)
            .unwrap()
            .as_bytes()
            .to_vec();

        json
    }

    fn serialize_finalized_slot(&self, slot: &u64) -> Vec<u8> {
        let json = serde_json::to_string(&slot).unwrap().as_bytes().to_vec();

        json
    }
}
