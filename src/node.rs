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
            
            match shared_state.lock() {

                Ok(mut state) => {
                    let (notinpeer, notinself) = state.diff_peerlist(&update);

                    if state.merge_peerlist(&notinself) > 0 {
                        state.version += 1;
                    }
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
                    if let Some(peer) = state.peermap.get_mut(&update.id) {
                        peer.healthcheck = now;
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

                    conn.write_frame(&frame).await?;

                }

                _ => {

                }
            }
        }
        _ => {
            println!("Not a frame, but not an error either")
        }
    }
    Ok(())
}

async fn send_gossip(node: &Node, state: &mut State, peer: &PeerNode) -> Result<()> {

    let mut stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port)).await?;

    let mut conn = Connection::new(stream);

    let update = Frame::Update(PeerUpdate {
        id: state.id.clone(),
        ip: state.ip.clone(),
        port: state.port.clone(),
        version: state.version,
        generation: state.generation,
        peerlist: state.peermap.values().cloned().collect()
    });
    
    conn.write_frame(&update).await?;

    if let Some(Frame::Update(resp_update)) = conn.read_frame().await? {
        println!("Received response");
        
        let (notinpeer, notinself) = state.diff_peerlist(&resp_update);
        if state.merge_peerlist(&notinself) > 0 {
            state.version += 1;
        }
        
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
        if let Some(peer) = state.peermap.get_mut(&peer.id) {
            peer.healthcheck = now;
            peer.version = resp_update.version;
            peer.generation = resp_update.generation;
        }
        
        state.save(&node.state_file);
    }
    
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
    pub shutdown: Shutdown
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
            shutdown: shutdown
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

            if let Ok(mut state) = state_clone.lock() {
                
                let peerlist = state.peerlist();
                println!("{:?}", peerlist);

                let peer_idx: usize = (rand::random::<u32>() % (peerlist.len() as u32)) as usize;
                let peer = peerlist[peer_idx].clone();
                
                println!("connecting to peer");
                let result = send_gossip(&self, &mut state, &peer).await;
                match result {
                    Err(e) => {
                        println!("Error in connection to peer {} {:?}", format!("{}:{}", peer.ip, peer.port), e);
                    }
                    _ => {

                    }
                }
            }
    
            tokio::time::sleep(Duration::from_millis(2000)).await;
        }
    }
}