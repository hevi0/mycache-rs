use crate::peernode::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(crate) struct Config {
    pub id: String,
    pub ip: String,
    pub port: String,
    pub seeds: Vec<Seed>
}