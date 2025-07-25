use crate::chash::*;
use crate::common::*;
use crate::config::*;
use crate::connection::*;
use crate::node;
use crate::peernode::*;
use crate::state::*;
use crate::shutdown::*;

use std::env;
use std::path::PathBuf;
use std::process::exit;
use std::time::{Duration, SystemTime};
use std::vec::Vec;
use std::fs::File;
use std::io::{BufReader};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

async fn handle_incoming_gossip(node: &Node, shared_state: &Arc<Mutex<State>>, stream: TcpStream) -> Result<()> {
    let mut conn = Connection::new(stream);

    let maybe_frame = conn.read_frame().await?;
    
    match maybe_frame {
        Some(Frame::Update(update)) => {
            println!("Unexpected Update message");
        }
        Some(Frame::Init(nodes)) => {
            println!("Received Init from {}, {} versions", nodes.id, nodes.versions.len());
            let myupdate = {
                let mut state = shared_state.lock().unwrap();

                let (notinself, notinpeer) = state.diff_versions(nodes.offset, &nodes.versions);
                
                let mut peerlist: Vec<PeerNode> = Vec::with_capacity(notinpeer.len());
                for id in notinpeer {
                    peerlist.push(state.peermap[&id].clone());
                }

                let myupdate = PeerUpdate {
                    id: state.id,
                    ip: state.ip.clone(),
                    port: state.port.clone(),
                    version: state.version,
                    generation: state.generation,
                    peerlist: peerlist,
                    peerreq: notinself.iter().filter(|id| {
                        **id != state.id
                    }).cloned().collect()
                };
                myupdate
                
            };
            println!("Sending update to {}, {:?} peers", nodes.id, myupdate);
            if let Err(e) = conn.write_frame(&Frame::Update(myupdate)).await {
                match e {
                    PeerError::ConnectionResetError | PeerError::ReadStreamError | PeerError::WriteStreamError => {
                        let mut state = shared_state.lock().unwrap();
                        if let Some(peer) = state.peermap.get_mut(&nodes.id) {
                            peer.error_count += 1;
                            state.save(&node.state_file);
                        }
                        println!("Error in connection to peer {}", nodes.id);
                    }
                    _ => {}
                }
                return Ok(());
            }

            match conn.read_frame().await {
                Ok(Some(Frame::Update(peerupdate))) => {
                    println!("Received Update from {}, {} peers", peerupdate.id, peerupdate.peerlist.len());
                    {
                        let mut state = shared_state.lock().unwrap();
                        if state.merge_peerlist(&peerupdate.peerlist) > 0 {
                            state.version += 1;
                        }
                        let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
                        if let Some(peer) = state.peermap.get_mut(&peerupdate.id) {
                            peer.healthcheck = now;
                            peer.error_count = 0; // successful exchange, so reset errors
                            peer.version = peerupdate.version;
                            peer.generation = peerupdate.generation;
                        }
                        state.save(&node.state_file);
                    }

                    // Add everything in notinself to the consistent-hash
                    {
                        let mut chash = node.chash.lock().unwrap();
                        for p in &peerupdate.peerlist {
                            chash.add_node(&p.id);
                        }
                    }
                },
                Ok(Some(Frame::Init(_))) => {
                    println!("Unexpected Init message received");
                },
                Ok(None) => {
                    println!("Unexpected empty message received");
                },
                Err(_) => {
                }
            }
        }
        _ => {
            println!("Not a frame, but not an error either")
        }
    }
    Ok(())
}

async fn send_gossip2(conn: &mut Connection, node_versions: NodeVersions, peer: &PeerNode) -> PeerConnResult<Option<PeerUpdate>> {
    
    let frame = Frame::Init(node_versions);
    
    match conn.write_frame(&frame).await? {
        () => {
            match conn.read_frame().await? {
                Some(Frame::Update(resp_update)) => {
                    return Ok(Some(resp_update));
                },
                Some(Frame::Init(_)) => {
                    println!("Unexpected Init message received");
                    return Ok(None);
                },
                None => {
                    println!("Unexpected empty message received");
                    return Ok(None);
                }
            }
        }
    }
    
    
}

async fn send_update(conn: &mut Connection, update: PeerUpdate) -> PeerConnResult<()> {

    let frame = Frame::Update(update);
    
    conn.write_frame(&frame).await?;

    Ok(()) 
}

fn load_state(state_file: &PathBuf) -> Result<State> {
    let bufreader = BufReader::new(File::open(&state_file)?);
    let state: State = serde_json::from_reader(bufreader)?;

    return Ok(state)

}

fn load_config(config_file: &PathBuf) -> Result<Config> {
    let bufreader = BufReader::new(File::open(&config_file)?);
    
    let config: Config = serde_json::from_reader(bufreader)?;

    return Ok(config)
}

pub struct Node {
    pub state_file: PathBuf,
    pub config_file: PathBuf,
    
    pub config: Config,
    pub state: Arc<Mutex<State>>,
    pub shutdown: Shutdown,
    pub chash: Arc<Mutex<Chash>>,
}

impl Node {
    pub fn new(id: IdType, data_path: Option<String>, shutdown: Shutdown) -> Node {


        let path: PathBuf = match &data_path {
            Some(path) => {
                PathBuf::new().join(path)
            }
            None => {
                env::current_dir().unwrap_or_else(|err| {
                    println!("Could not find/open config for {}: {:?}", id, err);
                    exit(1);
                })
            }
        };
        let config_file = path.join(format!("node{}.config.json", id));
        let state_file = path.join(format!("node{}.state.json", id));

        let config = load_config(&config_file).unwrap_or_else(|err| {
            println!("Could not find/open config for {}: {:?}", id, err);
            exit(1);
        });

        let mut state: State = {
            let result = load_state(&state_file);
            
            match result {
                Ok(mut s) => {
                    // Merge state with latest config
                    s.merge_config(&config);
                    s.save(&state_file);
                    s
                },
                Err(_) => {
                    let mut new_state = State::from_config(&config);
                    new_state.save(&state_file);

                    new_state
                }
            }
        };

        let mut node = Node {
            config_file: config_file,
            state_file: state_file,
            config: config,
            state: Arc::new(Mutex::new(state)),
            shutdown: shutdown,
            chash: Arc::new(Mutex::new(Chash::new(vec![452, 70821937, 12, 462308])))
        };

        {
            let mut chash = node.chash.lock().unwrap();
            chash.add_node(&node.config.id);
        }

        node

    }

    /// Listens for gossip messages from other nodes.
    pub async fn listenloop(&self) {
        let state_clone = self.state.clone();

        // Ok, we've (re)started the server loop. Now's a pretty good
        // time to increment the generation counter.
        {
            let mut state = state_clone.lock().unwrap();
            state.generation += 1;
            state.version += 1;

            // Since we've restarted, its best to give every potential
            // peer a clean slate and attempt to reconnect.
            for mut p in state.peermap.values_mut() {
                p.error_count = 0;
            }
            state.save(&self.state_file);
        }

        let shutdown_clone = self.shutdown.clone();

        let listenaddr = format!("{}:{}", self.config.ip, self.config.port);
        let result = TcpListener::bind(listenaddr).await;

        match result {
            Ok(mut listener) => {
                loop {
                    tokio::select! {

                        result = listener.accept() => {
                            // This result will just catch any errors that occur on the stream,
                            // print a message about it, but continue the loop.
                            if let Ok(stream) = result {
                                if let Err(e) = handle_incoming_gossip(&self, &state_clone, stream.0).await {
                                    println!("Error handling client connection: {:?}", e);
                                    
                                }
                            }
                            
                        },
                        val = shutdown_clone.wait_shutdown() => {
                            if val {
                                println!("Stopping server");
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error setting up listener: {:?}", e);
            }
        }
    }

    /// Periodically sends messages to random nodes
    pub async fn gossiploop2(&self) {
        let shutdown_clone = self.shutdown.clone();
        let state_clone = self.state.clone();
        loop {
            
            if shutdown_clone.check_shutdown() {
                println!("Stopping client");
                return;
            }
            tokio::time::sleep(Duration::from_millis(2000)).await;

            let (peerlist, version, generation, gossipees, max_known_id) = {
                let state = state_clone.lock().unwrap();
                let peerlist: Vec<PeerNode> = state.peermap.values().cloned().collect();

                println!("============");
                println!("{:#?}", state);
                println!("============");

                let mut live_gossipees: Vec<PeerNode> = Vec::with_capacity(state.peermap.len());
                let mut dead_gossipees: Vec<PeerNode> = Vec::with_capacity(state.peermap.len());

                for p in state.peermap.values() {
                    if p.error_count < 3 {
                        live_gossipees.push(p.clone());
                    } else {
                        dead_gossipees.push(p.clone());
                    }
                }

                let gossipees = {
                    // Every once in a while, try some "dead" peers
                    if dead_gossipees.len() > 0 && rand::random::<f32>() >= f32::min(0.1, 1.0/(state.peermap.len()+1) as f32) {
                        dead_gossipees
                    }
                    else if live_gossipees.len() == 0 {
                        vec![]
                    } else {
                        let gossipee_idx: usize = (rand::random::<u32>() % (live_gossipees.len() as u32)) as usize;
                        vec![live_gossipees[gossipee_idx].clone()]
                    }
                };

                let max_known_id = state.max_known_id();

                (peerlist, state.version, state.generation, gossipees, max_known_id)
            };

            if gossipees.len() == 0 {
                break;
            }

            let mut node_versions = NodeVersions {
                id: self.config.id,
                offset: 0,
                versions: vec![0; (max_known_id as usize) + 1]
            };

            node_versions.versions[self.config.id as usize] = version;
            for p in &peerlist {
                node_versions.versions[p.id as usize] = p.version;
            }

            let gossipee = &gossipees[0];

            if let Ok(stream) = TcpStream::connect(format!("{}:{}", gossipee.ip, gossipee.port)).await {

                let mut conn = Connection::new(stream);
                let result = send_gossip2(&mut conn, node_versions, &gossipee).await;
                match result {
                    Err(PeerError::ConnectionResetError) | Err(PeerError::WriteStreamError) | Err(PeerError::ReadStreamError) => {
                        let mut state = state_clone.lock().unwrap();
                        if let Some(peer) = state.peermap.get_mut(&gossipee.id) {
                            peer.error_count += 1;
                            state.save(&self.state_file);
                        }
                        println!("Error in connection to peer {}", format!("{}:{}", gossipee.ip, gossipee.port));
                    }
                    Err(PeerError::NonConnectionError) => {
                        println!("Error parsing or serializing/deserializing data");
                    }
                    Ok(Some(resp_update)) => {
                        let myupdate = {

                            let mut state = state_clone.lock().unwrap();
                            if state.merge_peerlist(&peerlist) > 0 {
                                state.version += 1;
                            }
                            
                            let now = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
                            if let Some(peer) = state.peermap.get_mut(&gossipee.id) {
                                peer.healthcheck = now;
                                peer.error_count = 0; // successful exchange, so reset errors
                                peer.version = resp_update.version;
                                peer.generation = resp_update.generation;
                            }
                            
                            state.save(&self.state_file);

                            let mut notinpeer: PeerList = Vec::with_capacity(resp_update.peerreq.len());
                            for id in resp_update.peerreq {
                                if id == state.id {
                                    notinpeer.push(PeerNode {
                                        id: state.id,
                                        ip: state.ip.clone(),
                                        port: state.port.clone(),
                                        version: state.version,
                                        generation: state.generation,
                                        healthcheck: 0,
                                        error_count: 0
                                    })
                                }
                                else if state.peermap.contains_key(&id) {
                                    notinpeer.push(state.peermap[&id].clone())
                                }
                            }

                            let myupdate = PeerUpdate {
                                id: state.id,
                                ip: state.ip.clone(),
                                port: state.port.clone(),
                                version: state.version,
                                generation: state.generation,
                                peerlist: notinpeer,
                                peerreq: vec![]
                            };
                            myupdate
                        };

                        match send_update(&mut conn, myupdate).await {
                            Err(PeerError::ConnectionResetError) | Err(PeerError::WriteStreamError) | Err(PeerError::ReadStreamError) => {
                                let mut state = state_clone.lock().unwrap();
                                if let Some(peer) = state.peermap.get_mut(&gossipee.id) {
                                    peer.error_count += 1;
                                    state.save(&self.state_file);
                                }
                                println!("Error in connection to peer {}", format!("{}:{}", gossipee.ip, gossipee.port));
                            }
                            Err(PeerError::NonConnectionError) => {
                                println!("Error parsing or serializing/deserializing data");
                            }
                            _ => {}
                        }

                        // Add everything in notinself to the consistent-hash
                        {
                            let mut chash = self.chash.lock().unwrap();
                            for p in &resp_update.peerlist {
                                chash.add_node(&p.id);
                            }
                        }


                    }
                    _ => {

                    }
                }
            } else {
                let mut state = state_clone.lock().unwrap();
                if let Some(peer) = state.peermap.get_mut(&gossipee.id) {
                    peer.error_count += 1;
                    state.save(&self.state_file);
                }
                println!("Error in connection to peer {}", format!("{}:{}", gossipee.ip, gossipee.port));
            }
        }
    }
}