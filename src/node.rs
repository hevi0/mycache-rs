use crate::chash::*;
use crate::common::*;
use crate::config::*;
use crate::connection::*;
use crate::state::*;
use crate::shutdown::*;

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::exit;
use std::time::Duration;
use std::fs::File;
use std::io::{BufReader};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

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
    pub kv_store: Arc<Mutex<HashMap<String, String>>>
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
            chash: Arc::new(Mutex::new(Chash::new(vec![452, 70821937, 12, 462308]))),
            kv_store: Arc::new(Mutex::new(HashMap::new()))
        };

        {
            let mut chash = node.chash.lock().unwrap();
            chash.add_node(&node.config.id);
        }

        node

    }

    pub fn process_update(&self, update: &PeerUpdate) {
        let mut locked_chash = self.chash.lock().unwrap();

        locked_chash.add_node(&update.id);
        for p in &update.peerlist {
            locked_chash.add_node(&p.id);
        }

        println!("============");
        println!("{:#?}", &locked_chash);
        println!("============");

    }

    /// Listens for gossip protocol messages from other nodes.
    pub async fn listenloop(&'static self) {
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
                                let conn = Connection::new(stream.0);
                                tokio::spawn(async move {
                                    self.handle_incoming(conn).await;
                                });
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

                // Cleanup remaining work, or wait for any threads to finish?
            }
            Err(e) => {
                println!("Error setting up listener: {:?}", e);
            }
        }
    }

    ///
    /// This method is intended to multiplex different
    /// kinds of messages/frames coming to our public-facing
    /// TCP listener.
    async fn handle_incoming(&'static self, mut conn: Connection) {

        let Ok(maybe_frame) = conn.read_frame().await else {
            return;
        };
        
        // Handle various kinds of messages here...
        match maybe_frame {
            Some(Frame::Init(init)) => {
                self.peerxchg_handler(init, &mut conn).await;
            }

            Some(Frame::GetVal(k)) => {
                // - Check consistent-hash for appropriate node
                // - if it's this node, return it from the local table
                // - Otherwise, lookup correct node(s)
                // - and forward request
                let node_ids = {
                    let locked_chash = self.chash.lock().unwrap();
                    let node_ids = locked_chash.calculate_vnodes(k.clone());
                    node_ids
                };

                if node_ids.contains(&self.config.id) {
                    let maybe_val = {
                        let locked_kv_store = self.kv_store.lock().unwrap();
                        if let Some(v) = locked_kv_store.get(&k) {
                            Some(v.clone())
                        } else {
                            None
                        }
                    };

                    let Some(v) = maybe_val else {
                        let _ = conn.write_frame(&Frame::GetValReply(None)).await;
                        return;
                    };

                    let _ = conn.write_frame(&Frame::GetValReply(Some((k, v.clone(), self.config.id)))).await;
                    return;
                }
                
            }
            Some(Frame::SetVal((k, v))) => {
                // - Check consistent-hash for appropriate node
                // - if it's this node, update it in the local table
                // - also, lookup other backup node(s)
                // - and forward request
            }
            _ => {
                println!("Not an expected frame, but not an error either")
            }
        }
    }

    /// Periodically sends peerxchg messages to random nodes
    /// This is an anti-entropy strategy to keep all nodes
    /// in the system up-to-date on all other nodes available.
    pub async fn peerxchg_gossip_loop(&self) {
        let shutdown_clone = self.shutdown.clone();
        loop {
            
            if shutdown_clone.check_shutdown() {
                println!("Stopping client");
                return;
            }
            tokio::time::sleep(Duration::from_millis(2000)).await;

            self.peerxchg_initiator().await;
        }
    }
}