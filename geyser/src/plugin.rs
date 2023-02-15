use crate::{
    config::Config,
    interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfo, ReplicaAccountInfoV2,
        ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
        Result,
    },
    prelude::*,
    selectors::{AccountSelector, TransactionSelector},
    sender::Sender,
    types::{ExchangeType, Message},
};
use serializer::geyser::{AccountUpdate, MetadataNotify, NftOffChainDataNotify, TransactionNotify};
use serializer::Serializer;
use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use std::{
    env,
    fmt::{Debug, Formatter},
    str,
    sync::Arc,
};

const UNINIT: &str = "RabbitMQ plugin not initialized yet!";

// not to import whole mpl_metadata crate🙃
const MPL_METADATA: [u8; 32] = [
    11, 112, 101, 177, 227, 209, 124, 69, 56, 157, 82, 127, 107, 4, 195, 205, 88, 184, 108, 115,
    26, 160, 253, 181, 73, 182, 209, 188, 3, 248, 41, 70,
];

#[inline]
fn custom_err<E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>>(
    e: E,
) -> GeyserPluginError {
    GeyserPluginError::Custom(e.into())
}

pub(crate) struct Inner {
    rt: tokio::runtime::Runtime,
    producer: Sender<Serializer>,
    acct_sel: AccountSelector,
    tx_sel: TransactionSelector,
}

impl Inner {
    pub fn spawn<F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(Arc<Self>) -> F,
    ) {
        self.rt.spawn(f(Arc::clone(self)));
    }
}

/// An instance of the plugin
#[derive(Default)]
#[repr(transparent)]
pub struct GeyserPluginRabbitMq(Option<Arc<Inner>>);

impl GeyserPluginRabbitMq {
    fn expect_inner(&self) -> &Arc<Inner> {
        self.0.as_ref().expect(UNINIT)
    }

    #[inline]
    fn with_inner<T>(
        &self,
        uninit: impl FnOnce() -> GeyserPluginError,
        f: impl FnOnce(&Arc<Inner>) -> anyhow::Result<T>,
    ) -> Result<T> {
        match self.0 {
            Some(ref inner) => f(inner).map_err(custom_err),
            None => Err(uninit()),
        }
    }
}

impl Debug for GeyserPluginRabbitMq {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for GeyserPluginRabbitMq {
    fn name(&self) -> &'static str {
        "GeyserPluginRabbitMq"
    }

    fn on_load(&mut self, cfg: &str) -> Result<()> {
        solana_logger::setup_with_default("info");

        {
            let ver = env!("CARGO_PKG_VERSION");
            let git = option_env!("META_GIT_HEAD");

            {
                use std::fmt::Write;

                let mut s = format!("v{}", ver);

                if let Some(git) = git {
                    write!(s, "+git.{}", git).unwrap();
                }
            }
        }

        let (kafka_conf, kafka_topics, jobs, acct_sel, ins_sel) = Config::read(cfg)
            .and_then(Config::into_parts)
            .map_err(custom_err)?;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("geyser-rabbitmq")
            .worker_threads(jobs.limit)
            .max_blocking_threads(jobs.blocking.unwrap_or(jobs.limit))
            .build()
            .map_err(custom_err)?;

        let producer = rt.block_on(async {
            let producer = Sender::new(kafka_conf, kafka_topics, Serializer {})
                .await
                .map_err(custom_err)?;

            Result::<_>::Ok(producer)
        })?;

        self.0 = Some(Arc::new(Inner {
            rt,
            producer,
            acct_sel,
            tx_sel: ins_sel,
        }));

        Ok(())
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::AccountsUpdateError { msg: UNINIT.into() },
            |this| {
                match account {
                    ReplicaAccountInfoVersions::V0_0_1(acct) => {
                        if !this.acct_sel.is_selected(acct, is_startup) {
                            return Ok(());
                        }

                        let ReplicaAccountInfo {
                            pubkey,
                            lamports,
                            owner,
                            executable,
                            rent_epoch,
                            data,
                            write_version,
                        } = *acct;

                        let key = Pubkey::new_from_array(pubkey.try_into()?);
                        let owner = Pubkey::new_from_array(owner.try_into()?);
                        let data = data.to_owned();

                        let msg_data = AccountUpdate {
                            key,
                            lamports,
                            owner,
                            executable,
                            rent_epoch,
                            data,
                            write_version,
                            slot,
                            is_startup,
                        };

                        this.spawn(|this| async move {
                            this.producer
                                .send(Message::AccountUpdate(msg_data), ExchangeType::Account)
                                .await;

                            Ok(())
                        });

                        // 4 == MetadataV1 account
                        if this.acct_sel.with_offchain()
                            && acct.owner == MPL_METADATA.as_ref()
                            && acct.data[0] == 4
                        {
                            // key, update auth pubkey, mint pubkey, name, symbol
                            let start = 1 + 32 + 32 + 4 + 32 + 4 + 10 + 4;
                            let end = start + 200;
                            let metadata_uri_bytes = acct.data[start..end].as_ref();
                            let uri = str::from_utf8(metadata_uri_bytes);

                            if let Ok(uri) = uri {
                                let msg_data = NftOffChainDataNotify {
                                    pubkey: key.to_string(),
                                    uri: uri.to_string(),
                                    slot,
                                    is_startup,
                                };

                                this.spawn(|this| async move {
                                    this.producer
                                        .send(
                                            Message::NftOffChainDataNotify(msg_data),
                                            ExchangeType::NftData,
                                        )
                                        .await;

                                    Ok(())
                                });
                            }
                        }
                    }
                    ReplicaAccountInfoVersions::V0_0_2(acct) => {
                        if !this.acct_sel.is_selected_2(acct, is_startup) {
                            return Ok(());
                        }

                        let ReplicaAccountInfoV2 {
                            pubkey,
                            lamports,
                            owner,
                            executable,
                            rent_epoch,
                            data,
                            write_version,
                            txn_signature,
                        } = *acct;

                        let key = Pubkey::new_from_array(pubkey.try_into()?);
                        let owner = Pubkey::new_from_array(owner.try_into()?);
                        let data = data.to_owned();

                        let msg_data = AccountUpdate {
                            key,
                            lamports,
                            owner,
                            executable,
                            rent_epoch,
                            data,
                            write_version,
                            slot,
                            is_startup,
                        };

                        this.spawn(|this| async move {
                            this.producer
                                .send(Message::AccountUpdate(msg_data), ExchangeType::Account)
                                .await;

                            Ok(())
                        });

                        // 4 == MetadataV1 account
                        if this.acct_sel.with_offchain()
                            && acct.owner == MPL_METADATA.as_ref()
                            && acct.data[0] == 4
                        {
                            // key, update auth pubkey, mint pubkey, name, symbol
                            let start = 1 + 32 + 32 + 4 + 32 + 4 + 10 + 4;
                            let end = start + 200;
                            let metadata_uri_bytes = acct.data[start..end].as_ref();
                            let uri = str::from_utf8(metadata_uri_bytes);

                            if let Ok(uri) = uri {
                                let msg_data = NftOffChainDataNotify {
                                    pubkey: key.to_string(),
                                    uri: uri.to_string(),
                                    slot,
                                    is_startup,
                                };

                                this.spawn(|this| async move {
                                    this.producer
                                        .send(
                                            Message::NftOffChainDataNotify(msg_data),
                                            ExchangeType::NftData,
                                        )
                                        .await;

                                    Ok(())
                                });
                            }
                        }
                    }
                };

                Ok(())
            },
        )
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::SlotStatusUpdateError { msg: UNINIT.into() },
            |this| {
                if let SlotStatus::Rooted = status {
                    this.spawn(|this| async move {
                        this.producer
                            .send(Message::FinalizedSlotNotify(slot), ExchangeType::Slot)
                            .await;

                        Ok(())
                    });
                }

                Ok(())
            },
        )
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                match transaction {
                    ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                        if matches!(tx.transaction_status_meta.status, Err(..)) {
                            return Ok(());
                        }

                        if !this
                            .tx_sel
                            .is_selected_in_range(tx.transaction.message().account_keys().iter())
                        {
                            return Ok(());
                        }

                        let msg_data =
                            TransactionNotify::new_from_replica_transaction_info(tx, slot);

                        this.spawn(|this| async move {
                            this.producer
                                .send(
                                    Message::TransactionNotify(msg_data),
                                    ExchangeType::Transaction,
                                )
                                .await;

                            Ok(())
                        });
                    }
                    ReplicaTransactionInfoVersions::V0_0_2(tx) => {}
                }

                Ok(())
            },
        )
    }

    fn notify_block_metadata(&mut self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                match block_info {
                    ReplicaBlockInfoVersions::V0_0_1(block_info) => {
                        let msg_data = MetadataNotify::new_from_replica_block_info(block_info);

                        this.spawn(|this| async move {
                            this.producer
                                .send(Message::MetadataNotify(msg_data), ExchangeType::Metadata)
                                .await;

                            Ok(())
                        });
                    }
                }

                Ok(())
            },
        )
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        let this = self.expect_inner();
        !this.tx_sel.is_empty()
    }
}
