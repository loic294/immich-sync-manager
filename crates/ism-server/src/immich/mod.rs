//! Immich v3.4.1 adapters: the HTTP API (preferred) and narrow guarded database writes.

pub mod api;
pub mod pg;
pub mod registry;

/// The only Immich version whose database contract has been verified.
pub const SUPPORTED_VERSION: &str = "3.4.1";
/// The last schema migration of Immich v3.4.1.
pub const SUPPORTED_MIGRATION: &str = "1792568637987-AddAssetOwnerIdUpdateIdIndex";
