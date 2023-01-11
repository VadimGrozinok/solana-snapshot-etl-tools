use serializer::geyser::{AccountUpdate, MetadataNotify, NftOffChainDataNotify, TransactionNotify};

/// A message transmitted by a Geyser plugin
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    /// Indicates an account should be updated
    AccountUpdate(AccountUpdate),
    /// Indicates raw metadata should be updated
    MetadataNotify(MetadataNotify),
    /// Indicates raw transaction should be updated
    TransactionNotify(TransactionNotify),
    /// Indicates Metaplex Metadata acc should be updated
    NftOffChainDataNotify(NftOffChainDataNotify),
    /// Indicates finalized slot
    FinalizedSlotNotify(u64),
}

#[derive(Debug, Clone, Copy)]
pub enum ExchangeType {
    Account,
    Transaction,
    Metadata,
    NftData,
    Slot,
}
