use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PeerNode {
    pub id: String,
    pub ip: String,
    pub port: String,
    pub version: u64,
    pub generation: u32,
    pub healthcheck: u64,
    pub error_count: u8
}

pub type PeerList = Vec<PeerNode>;
pub type PeerMap = HashMap<String, PeerNode>;

#[derive(Deserialize, Serialize)]
pub struct Seed {
    pub id: String,
    pub ip: String,
    pub port: String
}