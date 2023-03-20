use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::append_vec_iter;
use spl_token::state::Account;
use solana_sdk::program_pack::Pack;
use std::collections::HashMap;
use crate::mpl_metadata::Metadata;
use borsh::BorshDeserialize;
use std::io::Stdout;
use std::fs::File;
use std::rc::Rc;

const METADATA_PROGRAM_ID: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

pub(crate) struct CollectionDumper {
    accounts_spinner: ProgressBar,
    writer: csv::Writer<File>,
    accounts_count: u64,
    collection_id: String,
    metadata_mints: Vec<String>,
    token_owners: HashMap<String, String>,  // mint, owner
    collection_owners: HashMap<String, u64>, // owner, amount
}

#[derive(Serialize)]
struct Record {
    pubkey: String,
    owner: String,
    data_len: u64,
    lamports: u64,
}

impl CollectionDumper {
    pub(crate) fn new(collection_id: String) -> Self {
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        let writer = csv::Writer::from_path("./holders.csv").unwrap();

        Self {
            accounts_spinner,
            writer,
            accounts_count: 0,
            collection_id,
            metadata_mints: Vec::new(),
            token_owners: HashMap::new(),
            collection_owners: HashMap::new(),
        }
    }

    pub(crate) fn dump_append_vec(&mut self, append_vec: AppendVec) {
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            self.dump_account(account);
        }
    }

    pub(crate) fn dump_account(&mut self, account: StoredAccountMeta) {
        let owner = account.meta.pubkey.to_string();

        if owner == METADATA_PROGRAM_ID && account.data[0] == 4 {
            let mut data_peek = account.data;
            let metadata = Metadata::deserialize(&mut data_peek).unwrap();

            if let Some(collection) = metadata.collection {
                if collection.key.to_string() == self.collection_id {
                    self.metadata_mints.push(metadata.mint.to_string());
                }
            }

            if let Some(creators) = metadata.data.creators {
                let first_one = creators[0].address.to_string();
                if first_one == self.collection_id {
                    self.metadata_mints.push(metadata.mint.to_string());
                }
            }
        } else if owner == TOKEN_PROGRAM_ID {
            let res = Account::unpack(account.data);
            if res.is_ok() {
                let acc = res.unwrap();
                
                self.token_owners.insert(acc.mint.to_string(), acc.owner.to_string());
            }
        }

        self.accounts_count += 1;
        if self.accounts_count % 1024 == 0 {
            self.accounts_spinner.set_position(self.accounts_count);
        }
    }

    pub(crate) fn identify_owners(&mut self) {
        for mint in &self.metadata_mints {
            if let Some(owner) = self.token_owners.get(mint) {
                if let Some(amount) = self.collection_owners.get_mut(owner) {
                    *amount += 1;
                } else {
                    self.collection_owners.insert(owner.to_string(), 1);
                }
            }
        }
    }

    pub(crate) fn dump_owners(&mut self) {
        for (owner, amount) in &self.collection_owners {
            self.writer.write_record(&[owner, &*amount.to_string()]).unwrap();
        }

        self.writer.flush().unwrap();
    }
}

impl Drop for CollectionDumper {
    fn drop(&mut self) {
        self.accounts_spinner.finish();
    }
}
