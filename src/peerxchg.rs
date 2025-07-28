use crate::common::*;
use crate::connection::*;
use crate::node::*;
use crate::peernode::*;

use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;

impl Node {

    fn select_gossipees(&self) -> Vec<PeerNode> {
        let (live_gossipees, dead_gossipees) = {
            let locked_state = self.state.lock().unwrap();
            let mut live_gossipees: Vec<PeerNode> = Vec::with_capacity(locked_state.peermap.len());
            let mut dead_gossipees: Vec<PeerNode> = Vec::with_capacity(locked_state.peermap.len());
            println!("============");
            println!("{:#?}", locked_state);
            println!("============");

            for p in locked_state.peermap.values() {
                if p.error_count < 3 {
                    live_gossipees.push(p.clone());
                } else {
                    dead_gossipees.push(p.clone());
                }
            }

            (live_gossipees, dead_gossipees)
        };

        let gossipees = {
            // Every once in a while, try some "dead" peers
            if dead_gossipees.len() > 0 && rand::random::<f32>() >= f32::min(0.1, 1.0/(live_gossipees.len() + dead_gossipees.len()+1) as f32) {
                dead_gossipees
            }
            else if live_gossipees.len() == 0 {
                vec![]
            } else {
                let gossipee_idx: usize = (rand::random::<u32>() % (live_gossipees.len() as u32)) as usize;
                vec![live_gossipees[gossipee_idx].clone()]
            }
        };

        gossipees

    }

    fn build_init(&self) -> NodeVersions {
        let locked_state = self.state.lock().unwrap();

        let max_known_id = locked_state.max_known_id();

        let mut node_versions = NodeVersions {
            id: self.config.id,
            offset: 0,
            versions: vec![0; (max_known_id as usize) + 1]
        };

        node_versions.versions[self.config.id as usize] = locked_state.version;
        for p in locked_state.peermap.values() {
            node_versions.versions[p.id as usize] = p.version;
        }

        node_versions
    }

    fn build_update(&self, notinpeer: &Vec<IdType>, notinself: &Vec<IdType>) -> PeerUpdate {
        let locked_state = self.state.lock().unwrap();
        let mut peerlist: PeerList = vec![];
        for id in notinpeer {
            if id == &locked_state.id {
                peerlist.push(PeerNode {
                    id: locked_state.id,
                    ip: locked_state.ip.clone(),
                    port: locked_state.port.clone(),
                    version: locked_state.version,
                    generation: locked_state.generation,
                    healthcheck: 0,
                    error_count: 0
                })
            }
            else if locked_state.peermap.contains_key(&id) {
                peerlist.push(locked_state.peermap[&id].clone())
            }
        }

        PeerUpdate {
            id: locked_state.id,
            ip: locked_state.ip.clone(),
            port: locked_state.port.clone(),
            version: locked_state.version,
            generation: locked_state.generation,
            peerlist: peerlist,
            peerreq: notinself.clone()
        }
    }

    ///
    /// Check a `PeerConnResult`, which is returned everytime one
    /// calls a Connection's readframe or writeframe method.
    /// Returns true if there is an error, and false otherwise.
    /// Additionally, it handles incrementing the error_count
    /// that is kept track of for each peer.
    /// 
    /// `peer_id` should be the other node that this node is talking to.
    /// 
    /// This is useful for the common pattern where we want to branch off
    /// to handle an error and shortcut the normal logic flow, which
    /// happens frequently with async comms.
    fn detect_peerxchg_error<T>(&self, peer_id: &IdType, result: &PeerConnResult<T>)-> bool {
        match result {
            Err(PeerError::ConnectionResetError) | Err(PeerError::ReadStreamError) | Err(PeerError::WriteStreamError) => {
                let mut state = self.state.lock().unwrap();
                if let Some(peer) = state.peermap.get_mut(&peer_id) {
                    peer.error_count += 1;
                    state.save(&self.state_file);
                }
                return true;
            }
            Err(_) => {
                return true;
            }
            Ok(_) => {
                return false;
            }
        }
        
    }

    ///
    /// Update state and persist to storage.
    /// 
    fn update_state(&self, update: &PeerUpdate) {
        let mut locked_state = self.state.lock().unwrap();
        if locked_state.merge_peerlist(&update.peerlist) > 0 {
            locked_state.version += 1;
        }
        
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
        if let Some(peer) = locked_state.peermap.get_mut(&update.id) {
            peer.healthcheck = now;
            peer.error_count = 0; // successful exchange, so reset errors
            peer.version = update.version;
            peer.generation = update.generation;
        }
        
        locked_state.save(&self.state_file);
    }

    ///
    /// Initiate the peerxchg operation between
    /// nodes.
    pub(crate) async fn peerxchg_initiator(&self) {
        // prepare peer versions list and Init msg

        let gossipees = self.select_gossipees();
        let node_versions = self.build_init();
        
        if gossipees.len() < 1 {
            return;
        }

        let gossipee = &gossipees[0];

        let result = PeerError::convert_io_error(TcpStream::connect(format!("{}:{}", gossipee.ip, gossipee.port)).await);
        if self.detect_peerxchg_error(&gossipee.id, &result) {
            return;
        }

        let Ok(stream) = result else {
            return;
        };

        let mut conn = Connection::new(stream);

        // send Init msg
        let result = conn.write_frame(&Frame::Init(node_versions)).await;
        if self.detect_peerxchg_error(&gossipee.id, &result) {
            return;
        }

        // recv Update msg and update state
        let result = conn.read_frame().await;
        if self.detect_peerxchg_error(&gossipee.id, &result) {
            return;
        }

        let Ok(Some(Frame::Update(update))) = result else 
        {
            return;
        };
        
        self.update_state(&update);
        let myupdate = self.build_update(&update.peerreq, &vec![]);

        let result = conn.write_frame(&Frame::Update(myupdate)).await;
        if self.detect_peerxchg_error(&gossipee.id, &result) {
            return;
        }
        
    }

    ///
    /// Handle the incoming peerxchg init msg
    /// and complete the conversation with the
    /// calling node.
    pub(crate) async fn peerxchg_handler(&self, init: NodeVersions, conn: &mut Connection) {
        
        let (notinself, notinpeer) = {
            let locked_state = self.state.lock().unwrap();
            locked_state.diff_versions(0, &init.versions)
        };

        // send Update msg
        let myupdate = self.build_update(&notinpeer, &notinself);
        let result = conn.write_frame(&Frame::Update(myupdate)).await;
        if self.detect_peerxchg_error(&init.id, &result) {
            return;
        }

        // recv Update msg and update state
        let result = conn.read_frame().await;
        if self.detect_peerxchg_error(&init.id, &result) {
            return;
        }

        let Ok(Some(Frame::Update(update))) = result else 
        {
            return;
        };

        self.update_state(&update);

    }
}