use super::config::Accounts;
use crate::{interface::{ReplicaAccountInfo, ReplicaAccountInfoV2}, prelude::*};
use hashbrown::HashSet;

#[derive(Debug)]
pub struct AccountSelector {
    owners: HashSet<[u8; 32]>,
    startup: Option<bool>,
    deletion: bool,
    all_accounts: bool,
}

impl AccountSelector {
    pub fn from_config(config: Accounts) -> Result<Self> {
        let Accounts {
            owners,
            startup,
            deletion,
            all_accounts,
        } = config;

        let owners = owners
            .into_iter()
            .map(|s| s.parse().map(Pubkey::to_bytes))
            .collect::<Result<_, _>>()
            .context("Failed to parse account owner keys")?;

        Ok(Self {
            owners,
            startup,
            deletion,
            all_accounts,
        })
    }

    #[inline]
    pub fn is_selected(&self, acct: &ReplicaAccountInfo, is_startup: bool) -> bool {
        if self.all_accounts {
            return true;
        }

        if self.deletion
            && acct.lamports == 0
            && acct.data.is_empty()
            && acct.owner == solana_program::system_program::id().to_bytes()
        {
            return true;
        }

        // TODO: change it because now it loads only sturtup accounts
        self.startup.map_or(true, |s| is_startup == s) && self.owners.contains(acct.owner)
    }

    #[inline]
    pub fn is_selected_2(&self, acct: &ReplicaAccountInfoV2, is_startup: bool) -> bool {
        if self.all_accounts {
            return true;
        }

        if self.deletion
            && acct.lamports == 0
            && acct.data.is_empty()
            && acct.owner == solana_program::system_program::id().to_bytes()
        {
            return true;
        }

        // TODO: change it because now it loads only sturtup accounts
        self.startup.map_or(true, |s| is_startup == s) && self.owners.contains(acct.owner)
    }
}

#[derive(Debug)]
pub struct TransactionSelector {
    programs: HashSet<Pubkey>,
}

impl TransactionSelector {
    pub fn from_config(programs: HashSet<String>) -> Result<Self> {
        let programs = programs
            .into_iter()
            .map(|s| s.parse())
            .collect::<Result<_, _>>()
            .context("Failed to parse instruction program keys")?;

        Ok(Self { programs })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.programs.is_empty()
    }

    #[allow(dead_code)]
    #[inline]
    pub fn is_selected(&self, pgm: &Pubkey) -> bool {
        self.programs.contains(pgm)
    }

    pub fn is_selected_in_range<'a, I>(&self, pgms: I) -> bool
    where
        I: Iterator<Item = &'a Pubkey>,
    {
        for pgm in pgms {
            if self.is_selected(pgm) {
                return true;
            }
        }

        false
    }
}
