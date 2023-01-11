use crate::geyser::{AccountUpdate, MetadataNotify, NftOffChainDataNotify, TransactionNotify};

#[cfg(feature = "flatbuffers")]
pub mod flatbuffer;
#[cfg(feature = "serde")]
pub mod serde_serialisation;

pub mod geyser;

cfg_if::cfg_if! {
    if #[cfg(feature = "flatbuffers")] {
        pub type Serializer = flatbuffer::FlatBufferSerialization;
    } else if #[cfg(feature = "serde")] {
        pub type Serializer = serde_serialization::SerdeSerialization;
    }
}

/// Interface for data serialization implementations
pub trait Serialization {
    ///
    fn serialize_account(&self, account: &AccountUpdate) -> Vec<u8>;
    ///
    fn serialize_metadata(&self, metadata: &MetadataNotify) -> Vec<u8>;
    ///
    fn serialize_nft_off_chain_data(&self, off_chain_data: &NftOffChainDataNotify) -> Vec<u8>;
    ///
    fn serialize_transaction(&self, transaction: &TransactionNotify) -> Vec<u8>;
    //
    fn serialize_finalized_slot(&self, slot: &u64) -> Vec<u8>;
}
