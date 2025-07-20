use crate::chash::*;
use crate::common::*;
use crate::config::*;
use crate::connection::*;
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
            
            let new_frame = {
                let mut state = shared_state.lock().unwrap();

                let (notinpeer, notinself) = state.diff_peerlist(&update);

                if state.merge_peerlist(&notinself) > 0 {
                    state.version += 1;
                }
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
                if let Some(peer) = state.peermap.get_mut(&update.id) {
                    peer.healthcheck = now;
                    peer.error_count = 0; // successful exchange, so reset errors
                    peer.version = update.version;
                    peer.generation = update.generation;
                }
                
                state.save(&node.state_file);

                let frame = Frame::Update(PeerUpdate {
                    id: state.id.clone(),
                    ip: state.ip.clone(),
                    port: state.port.clone(),
                    version: state.version,
                    generation: state.generation,
                    peerlist: notinpeer
                });

                frame
            };

            if let Err(PeerConnError(msg)) = conn.write_frame(&new_frame).await {
                // Writing to the peer stream failed, so we should note this.
                // It may mean we have lost connection to the peer.
                let mut state = shared_state.lock().unwrap();
                if let Some(peer) = state.peermap.get_mut(&update.id) {
                    peer.error_count += 1;
                    state.save(&node.state_file);
                }
            }

        }

        _ => {
            println!("Not a frame, but not an error either")
        }
    }
    Ok(())
}

async fn send_gossip2(update: PeerUpdate, peer: &PeerNode) -> PeerConnResult<Option<(PeerUpdate, PeerList, PeerList)>> {
    
    if let Ok(stream) = TcpStream::connect(format!("{}:{}", peer.ip, peer.port)).await {

        let mut conn = Connection::new(stream);

        let frame = Frame::Update(update);
        
        match conn.write_frame(&frame).await {
            Ok(()) => {
                match conn.read_frame().await {
                    Ok(Some(Frame::Update(resp_update))) => {
                        let Frame::Update(u) = frame;
                        let (notinself, notinpeer) = diff_peerlist(&u, &resp_update);
                        return Ok(Some((resp_update, notinself, notinpeer)));
                    },
                    Ok(None) => {
                        return Ok(None);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    } else {
        Err(PeerConnError("Error connecting to peer stream".to_string()))
    }
    
    
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
    pub chash: Chash
}

impl Node {
    pub fn new(id: String, data_path: Option<String>, shutdown: Shutdown) -> Node {


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
        let config_file = path.join(format!("{}.config.json", id));
        let state_file = path.join(format!("{}.state.json", id));

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

        Node {
            config_file: config_file,
            state_file: state_file,
            config: config,
            state: Arc::new(Mutex::new(state)),
            shutdown: shutdown,
            chash: Chash::new(vec![452, 70821937, 12, 462308])
        }
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
    pub async fn gossiploop(&self) {
        let shutdown_clone = self.shutdown.clone();
        let state_clone = self.state.clone();
        loop {
            
            if shutdown_clone.check_shutdown() {
                println!("Stopping client");
                return;
            }
            tokio::time::sleep(Duration::from_millis(2000)).await;

            let (peerlist, version, generation, gossipees) = {
                let state = state_clone.lock().unwrap();
                let peerlist: Vec<PeerNode> = state.peermap.values().cloned().collect();

                println!("============");
                println!("{:?}", peerlist);

                let live_gossipees: Vec<PeerNode> = state.peermap.values().cloned().filter(|p| {
                    p.error_count < 3
                }).collect();

                let gossipees = {
                    if live_gossipees.len() == 0 {
                        vec![]
                    } else {
                        let gossipee_idx: usize = (rand::random::<u32>() % (live_gossipees.len() as u32)) as usize;
                        vec![live_gossipees[gossipee_idx].clone()]
                    }
                };

                (peerlist, state.version, state.generation, gossipees)
            };

            if gossipees.len() == 0 {
                break;
            }
        

            let update = PeerUpdate {
                id: self.config.id.clone(),
                ip: self.config.ip.clone(),
                port: self.config.port.clone(),
                version: version,
                generation: generation,
                peerlist: peerlist
            };

            let gossipee = &gossipees[0];
            let result = send_gossip2(update, &gossipee).await;
            match result {
                Err(e) => {
                    let mut state = state_clone.lock().unwrap();
                    if let Some(peer) = state.peermap.get_mut(&gossipee.id) {
                        peer.error_count += 1;
                        state.save(&self.state_file);
                    }
                    println!("Error in connection to peer {} {:?}", format!("{}:{}", gossipee.ip, gossipee.port), e);
                }
                Ok(Some((resp_update, notinself, notinpeer))) => {
                    let mut state = state_clone.lock().unwrap();
                    if state.merge_peerlist(&notinself) > 0 {
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
                }
                _ => {

                }
            }
        }
    }
}