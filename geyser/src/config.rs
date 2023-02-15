use crate::{
    prelude::*,
    selectors::{AccountSelector, TransactionSelector},
};
use hashbrown::HashSet;
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    kafka: HashMap<String, String>,
    kafka_topics: KafkaTopics,
    jobs: Jobs,

    accounts: Accounts,

    transaction_programs: HashSet<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopics {
    pub accounts: String,
    pub transactions: String,
    pub block_metadata: String,
    pub nft_off_chain_data: String,
    pub finalized_slots: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Jobs {
    pub limit: usize,

    #[serde(default)]
    pub blocking: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Accounts {
    pub owners: HashSet<String>,

    /// Filter for changing how to interpret the `is_startup` flag.
    ///
    /// This option has three states:
    ///  - `None`: Ignore the `is_startup` flag and send all updates.
    ///  - `Some(true)`: Only send updates when `is_startup` is `true`.
    ///  - `Some(false)`: Only send updates when `is_startup` is `false`.
    pub startup: Option<bool>,

    /// Filter for deletion events.
    ///
    /// This option has two states:
    /// - `false`: Ignore deletion events.
    /// - `true`: Send deletion events.
    pub deletion: bool,
}

impl Config {
    pub fn read(path: &str) -> Result<Self> {
        let f = std::fs::File::open(path).context("Failed to open config file")?;
        let cfg = serde_json::from_reader(f).context("Failed to parse config file")?;

        Ok(cfg)
    }

    pub fn into_parts(
        self,
    ) -> Result<(
        HashMap<String, String>,
        KafkaTopics,
        Jobs,
        AccountSelector,
        TransactionSelector,
    )> {
        let Self {
            kafka,
            kafka_topics,
            jobs,
            accounts,
            transaction_programs: instruction_programs,
        } = self;

        let acct =
            AccountSelector::from_config(accounts).context("Failed to create account selector")?;
        let ins = TransactionSelector::from_config(instruction_programs)
            .context("Failed to create instruction selector")?;

        Ok((kafka, kafka_topics, jobs, acct, ins))
    }
}

pub type Producer = ThreadedProducer<DefaultProducerContext>;
