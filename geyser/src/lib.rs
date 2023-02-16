//! Solana Geyser plugin adapter
//! transport

pub(crate) use solana_geyser_plugin_interface::geyser_plugin_interface as interface;
pub(crate) mod prelude {
    pub use std::result::Result as StdResult;

    pub use anyhow::{anyhow, bail, Context, Error};
    pub use log::{debug, error, info, trace, warn};
    pub use solana_program::pubkey::Pubkey;

    pub type Result<T, E = Error> = StdResult<T, E>;
}
pub(crate) mod config;
mod plugin;
pub(crate) mod selectors;
pub(crate) mod sender;
pub(crate) mod types;

pub use plugin::GeyserPluginRabbitMq;

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// Construct a new instance of the plugin.
///
/// # Safety
/// This function is only safe if called by a Solana Geyser plugin manager
/// conformant to the plugin interface.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn interface::GeyserPlugin {
    Box::into_raw(Box::new(GeyserPluginRabbitMq::default()))
}
