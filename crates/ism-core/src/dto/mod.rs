//! Wire types. These are the source of truth for `docs/client-api.md` and for the TypeScript
//! copies in `web/src/api/generated` (`make gen-api` runs the ts-rs `export_bindings_*` tests).

pub mod admin;
pub mod client;

pub use admin::*;
pub use client::*;
