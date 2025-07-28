use std::cmp::max;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json;

use crate::config::*;
use crate::connection::PeerUpdate;
use crate::peernode::*;
use crate::common::*;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct State {
    pub id: IdType,
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

    pub fn max_known_id(&self) -> IdType {
        let mut curr_max = self.id;
        for k in self.peermap.keys() {
            curr_max = max(curr_max, *k);
        }

        curr_max
    }

    pub fn diff_versions(&self, offset: usize, nodes: &Vec<u64>) -> (Vec<IdType>, Vec<IdType>) {
        let mut notinself: Vec<IdType> = Vec::with_capacity(nodes.len());
        let mut notinpeer: Vec<IdType> = Vec::with_capacity(nodes.len());
        for i in 0..nodes.len() {
            let id = (i + offset) as IdType;
            
            if !self.peermap.contains_key(&id) {
                notinself.push(id);
            } else if self.peermap[&id].version > nodes[i] {
                notinpeer.push(id);
            } else if self.peermap[&id].version < nodes[i] {
                notinself.push(id);
            }
        }

        // We need to also take into account that this
        // node has more peers beyond the ones known by the
        // other node that is talking to this one.
        // Add these peers to the notinpeer.
        let n = self.max_known_id() as usize;
        if nodes.len() <= n {
            for id in nodes.len()..n {
                notinpeer.push(id as u16);
            }
        }

        (notinself, notinpeer)
    }

    pub fn diff_peerlist(&self, peer: &PeerUpdate) -> (PeerList, PeerList) {
        let max_capacity = peer.peerlist.len() + self.peermap.len();
        let mut notinpeer: Vec<PeerNode> = Vec::with_capacity(max_capacity);
        let mut notinself: Vec<PeerNode> = Vec::with_capacity(max_capacity);

        let mut aspeermap: HashMap<&IdType, &PeerNode> = HashMap::with_capacity(peer.peerlist.len());
        for p in &peer.peerlist {
            aspeermap.insert(&p.id, &p);
        }

        for k in self.peermap.keys() {

            if !aspeermap.contains_key(k) {
                notinpeer.push(self.peermap[k].clone());
            } else if aspeermap[k].version < self.peermap[k].version {
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
                id: peer.id,
                ip: peer.ip.clone(),
                port: peer.port.clone(),
                version: peer.version,
                generation: peer.generation,
                healthcheck: 0,
                error_count: 0
            });
        }

        (notinpeer, notinself)
    }

    /// Merge peerlist (which includes updates and new peers)
    pub fn merge_peerlist(&mut self, peerlist: &PeerList) -> u32 {

        println!("Merging {:?}", peerlist);
        let mut new_peer_counter: u32 = 0; // only count NEW peers, not updates
        for p in peerlist {
            if p.id != self.id {
                if !self.peermap.contains_key(&p.id) {
                    new_peer_counter += 1;
                    self.peermap.insert(p.id, p.clone());
                    println!("New peer merged");

                } else if self.peermap[&p.id].version < p.version {
                    let peernode = self.peermap.get_mut(&p.id).unwrap();
                    peernode.id = p.id;
                    peernode.ip = p.ip.clone();
                    peernode.port = p.port.clone();
                    peernode.version = p.version;
                    peernode.generation = p.generation;
                    peernode.error_count = 0; // New version of this node, means we should probably retry connections
                    println!("Peer update merged");
                    // Not updating the healthcheck,
                    // this belong to this node's view
                }
            }
        }
        new_peer_counter
    }

    pub fn from_config(config: &Config) -> State {
        let mut s = State {
            id: config.id,
            ip: config.ip.clone(),
            port: config.port.clone(),
            version: 0,
            generation: 0,
            peermap: HashMap::new()
        };

        for p in &config.seeds {
            s.peermap.insert(p.id, PeerNode {
                id: p.id,
                ip: p.ip.clone(),
                port: p.port.clone(),
                version: 0,
                generation: 0,
                healthcheck: 0,
                error_count: 0
            });
        }

        s
    }

    pub fn merge_config(&mut self, config: &Config) {
        self.id = config.id;
        self.ip = config.ip.clone();
        self.port = config.port.clone();
        
        for p in &config.seeds {
            
            if self.peermap.contains_key(&p.id) {
                if let Some(peer) = self.peermap.get_mut(&p.id) {
                    peer.id = p.id.clone();
                    peer.ip = p.ip.clone();
                    peer.port = p.port.clone();
                }
            } else {
                self.peermap.insert(p.id.clone(), PeerNode {
                    id: p.id.clone(),
                    ip: p.ip.clone(),
                    port: p.port.clone(),
                    version: 0,
                    generation: 0,
                    healthcheck: 0,
                    error_count: 0
                });
            }
        }
    }

}