use crate::{
    config::{KafkaTopics, Producer},
    types::{ExchangeType, Message},
};
use rdkafka::{
    config::FromClientConfig,
    error::KafkaResult,
    producer::{BaseRecord, ThreadedProducer},
    ClientConfig,
};
use serializer::Serialization;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPluginError as PluginError, Result as PluginResult,
};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub struct Sender<S> {
    conf: HashMap<String, String>,
    producer: RwLock<Producer>,
    topics: KafkaTopics,
    serializer: S,
}

impl<S: Serialization> Sender<S> {
    pub async fn new(
        kafka_conf: HashMap<String, String>,
        kafka_topics: KafkaTopics,
        serializer: S,
    ) -> PluginResult<Self> {
        let producer = Self::create_producer(&kafka_conf)
            .await
            .map_err(|e| PluginError::Custom(Box::new(e)))?;

        Ok(Self {
            conf: kafka_conf,
            producer: RwLock::new(producer),
            topics: kafka_topics,
            serializer,
        })
    }

    async fn create_producer(kafka_conf: &HashMap<String, String>) -> KafkaResult<Producer> {
        let mut config = ClientConfig::new();
        for (k, v) in kafka_conf.iter() {
            config.set(k, v);
        }
        ThreadedProducer::from_config(&config)
    }

    pub async fn send(&self, msg: Message, exchange_type: ExchangeType) {
        #[inline]
        fn log_err<E: std::fmt::Debug>(err: E) {
            log::error!("{:?}", err);
        }

        let prod = self.producer.read().await;

        let data = match msg {
            Message::AccountUpdate(account) => self.serializer.serialize_account(&account),
            Message::TransactionNotify(transaction) => {
                self.serializer.serialize_transaction(&transaction)
            }
            Message::MetadataNotify(metadata) => self.serializer.serialize_metadata(&metadata),
            Message::NftOffChainDataNotify(off_chain_data) => self
                .serializer
                .serialize_nft_off_chain_data(&off_chain_data),
            Message::FinalizedSlotNotify(slot) => self.serializer.serialize_finalized_slot(&slot),
        };

        // TODO: process errors
        match exchange_type {
            ExchangeType::Account => {
                let record = BaseRecord::<Vec<u8>, _>::to(&self.topics.accounts).payload(&data);
                prod.send(record).map(|_| ()).map_err(log_err);
            }
            ExchangeType::Transaction => {
                let record = BaseRecord::<Vec<u8>, _>::to(&self.topics.transactions).payload(&data);
                prod.send(record).map(|_| ()).map_err(log_err);
            }
            ExchangeType::Metadata => {
                let record =
                    BaseRecord::<Vec<u8>, _>::to(&self.topics.block_metadata).payload(&data);
                prod.send(record).map(|_| ()).map_err(log_err);
            }
            ExchangeType::NftData => {
                let record =
                    BaseRecord::<Vec<u8>, _>::to(&self.topics.nft_off_chain_data).payload(&data);
                prod.send(record).map(|_| ()).map_err(log_err);
            }
            ExchangeType::Slot => {
                let record =
                    BaseRecord::<Vec<u8>, _>::to(&self.topics.finalized_slots).payload(&data);
                prod.send(record).map(|_| ()).map_err(log_err);
            }
        }
    }
}
