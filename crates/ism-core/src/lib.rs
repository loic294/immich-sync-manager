//! Pure domain logic and wire types of Immich Sync Manager. Nothing here performs IO, reads the
//! clock or touches the network: callers pass `now`, directory listings and stored state in.

pub mod checksum;
pub mod dto;
pub mod maintenance;
pub mod mapping;
pub mod paths;
pub mod schedule;
pub mod sidecar;
pub mod version;
