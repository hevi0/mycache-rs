use crate::common::*;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub type NodeVersions = Vec<u64>;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PeerNode {
    pub id: IdType,
    pub ip: String,
    pub port: String,
    pub version: u64,
    pub generation: u32,
    pub healthcheck: u64,
    pub error_count: u8
}

pub type PeerList = Vec<PeerNode>;
pub type PeerMap = HashMap<IdType, PeerNode>;

#[derive(Deserialize, Serialize)]
pub struct Seed {
    pub id: IdType,
    pub ip: String,
    pub port: String
}