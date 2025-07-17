use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json;

use crate::config::*;
use crate::connection::PeerUpdate;
use crate::peernode::*;

#[derive(Deserialize, Serialize, Clone)]
pub struct State {
    pub id: String,
    pub ip: String,
    pub port: String,
    pub version: u64,
    pub generation: u32,
    pub peermap: PeerMap
}

impl State {

    pub fn save(&mut self, f: &PathBuf) {
    
        let result = serde_json::to_string(self);

        match result {
            Ok(output_json) =>  {
                if let Err(e) = std::fs::write(f, output_json) {
                    eprintln!("Error saving state {}", e.to_string()); 
                }
            }
            Err(e) => {
                eprintln!("Could not serialize state: {:?}", &e); 
            }
        }
    }

    pub fn diff_peerlist(&self, peer: &PeerUpdate) -> (PeerList, PeerList) {
        let max_capacity = peer.peerlist.len() + self.peermap.len();
        let mut notinpeer: Vec<PeerNode> = Vec::with_capacity(max_capacity);
        let mut notinself: Vec<PeerNode> = Vec::with_capacity(max_capacity);

        let mut aspeermap: HashMap<&str, &PeerNode> = HashMap::with_capacity(peer.peerlist.len());
        for p in &peer.peerlist {
            aspeermap.insert(&p.id, &p);
        }

        for k in self.peermap.keys() {

            let kstr = k.as_str();
            if !aspeermap.contains_key(kstr) {
                notinpeer.push(self.peermap[k].clone());
            } else if aspeermap[kstr].version < self.peermap[kstr].version {
                notinpeer.push(self.peermap[k].clone());
            }
        }
        
        for k in aspeermap.keys() {
            if !self.peermap.contains_key(*k) {
                notinself.push(aspeermap[k].clone())
            } else if self.peermap[*k].version < aspeermap[k].version {
                notinself.push(aspeermap[k].clone());
            }
        }
        if !self.peermap.contains_key(&peer.id) {
            notinself.push(PeerNode {
                id: peer.id.clone(),
                ip: peer.ip.clone(),
                port: peer.port.clone(),
                version: peer.version,
                generation: peer.generation,
                healthcheck: 0
            });
        }

        (notinpeer, notinself)
    }

    /// Merge peerlist (which includes updates and new peers)
    pub fn merge_peerlist(&mut self, peerlist: &PeerList) -> u32 {
        let mut new_peer_counter: u32 = 0; // only count NEW peers, not updates
        for p in peerlist {
            if p.id != self.id {
                if !self.peermap.contains_key(&p.id) {
                    new_peer_counter += 1;
                }
                self.peermap.insert(p.id.clone(), p.clone());
            }
        }
        new_peer_counter
    }

    // Config is consumed, should it?
    pub fn from_config(config: &Config) -> State {
        let mut s = State {
            id: config.id.clone(),
            ip: config.ip.clone(),
            port: config.port.clone(),
            version: 0,
            generation: 0,
            peermap: HashMap::new()
        };

        for p in &config.seeds {
            s.peermap.insert(p.id.clone(), PeerNode {
                id: p.id.clone(),
                ip: p.ip.clone(),
                port: p.port.clone(),
                version: 0,
                generation: 0,
                healthcheck: 0,
            });
        }

        s
    }

    // Config is consumed, should it?
    pub fn merge_config(&mut self, config: &Config) {
        self.id = config.id.clone();
        self.ip = config.ip.clone();
        self.port = config.port.clone();
        
        for p in &config.seeds {
            self.peermap.insert(p.id.clone(), PeerNode {
                id: p.id.clone(),
                ip: p.ip.clone(),
                port: p.port.clone(),
                version: 0,
                generation: 0,
                healthcheck: 0,
            });
        }
    }

    // allow for caching/memoizing the peer list
    pub fn peerlist(&self) -> Vec<&PeerNode> { 
        self.peermap.values().collect()
    }

}

pub fn diff_peerlist(peer1: &PeerUpdate, peer2: &PeerUpdate) -> (PeerList, PeerList) {
    let max_capacity = peer2.peerlist.len() + peer1.peerlist.len();
    let mut notinpeer2: Vec<PeerNode> = Vec::with_capacity(max_capacity);
    let mut notinpeer1: Vec<PeerNode> = Vec::with_capacity(max_capacity);

    let mut peer1map: HashMap<&str, &PeerNode> = HashMap::with_capacity(peer1.peerlist.len());
    for p in &peer1.peerlist {
        peer1map.insert(&p.id, &p);
    }

    let mut peer2map: HashMap<&str, &PeerNode> = HashMap::with_capacity(peer2.peerlist.len());
    for p in &peer2.peerlist {
        peer2map.insert(&p.id, &p);
    }

    for k in peer1map.keys() {

        if !peer2map.contains_key(*k) {
            notinpeer2.push(peer1map[k].clone());
        } else if peer2map[*k].version < peer1map[k].version {
            notinpeer2.push(peer1map[k].clone());
        }
    }
    
    for k in peer2map.keys() {
        if !peer1map.contains_key(*k) {
            notinpeer1.push(peer2map[k].clone())
        } else if peer1map[*k].version < peer2map[k].version {
            notinpeer1.push(peer2map[k].clone());
        }
    }

    if !peer1map.contains_key(peer2.id.as_str()) {
        notinpeer1.push(PeerNode {
            id: peer2.id.clone(),
            ip: peer2.ip.clone(),
            port: peer2.port.clone(),
            version: peer2.version,
            generation: peer2.generation,
            healthcheck: 0
        });
    }

    if !peer2map.contains_key(peer1.id.as_str()) {
        notinpeer2.push(PeerNode {
            id: peer1.id.clone(),
            ip: peer1.ip.clone(),
            port: peer1.port.clone(),
            version: peer1.version,
            generation: peer1.generation,
            healthcheck: 0
        });
    }

    (notinpeer1, notinpeer2)
}