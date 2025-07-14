
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::Vec;
use std::collections::{HashMap};
use std::fs::File;
use std::io::{BufReader, Cursor, Error};
use std::process::exit;
use std::time::SystemTime;

use bytes::{buf::Reader, Buf};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;

use serde::{Deserialize, Serialize};
use serde_json;

use std::env;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Deserialize, Serialize, Clone, Debug)]
struct PeerNode {
    id: String,
    ip: String,
    port: String,
    version: u64,
    generation: u32,
    healthcheck: u64
}

type PeerList = Vec<PeerNode>;
type PeerMap = HashMap<String, PeerNode>;

#[derive(Deserialize, Serialize)]
struct Seed {
    id: String,
    ip: String,
    port: String
}

#[derive(Deserialize, Serialize)]
struct Config {
    id: String,
    ip: String,
    port: String,
    seeds: Vec<Seed>
}

#[derive(Deserialize, Serialize, Clone)]
struct State {
    id: String,
    ip: String,
    port: String,
    version: u64,
    generation: u32,
    peermap: PeerMap
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

    pub fn diff_peerlist(&self, peerlist: &PeerList) -> (PeerList, PeerList) {
        let max_capacity = peerlist.len() + self.peermap.len();
        let mut notinpeer: Vec<PeerNode> = Vec::with_capacity(max_capacity);
        let mut notinself: Vec<PeerNode> = Vec::with_capacity(max_capacity);

        let mut aspeermap: HashMap<&str, &PeerNode> = HashMap::with_capacity(peerlist.len());
        for p in peerlist {
            aspeermap.insert(&p.id, &p);
        }


        for k in self.peermap.keys() {
            if !aspeermap.contains_key(k.as_str()) {
                notinpeer.push(self.peermap[k].clone())
            }
        }
        
        for k in aspeermap.keys() {
            if !self.peermap.contains_key(*k) {
                notinself.push(aspeermap[k].clone())
            }
        }

        (notinpeer, notinself)
    }

    pub fn merge_peerlist(&mut self, peerlist: &PeerList) {
        for p in peerlist {
            if !self.peermap.contains_key(&p.id) && p.id != self.id {
                self.peermap.insert(p.id.clone(), p.clone());
            }
        }
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

    pub fn set_peer(&mut self, id: String, peer: PeerNode) {
        self.peermap.insert(id, peer);
    }

    pub fn get_peer(&mut self, id: String) -> Option<&mut PeerNode> {
        return self.peermap.get_mut(&id)
    }

    // allow for caching/memoizing the peer list
    pub fn peerlist(&self) -> Vec<&PeerNode> { 
        self.peermap.values().collect()
    }

}

#[derive(Deserialize, Serialize, Debug)]
struct PeerUpdate {
    id: String,
    version: u64,
    generation: u32,
    peerlist: PeerList
}

#[derive(Deserialize, Serialize, Debug)]
enum Frame {
    Update(PeerUpdate)    
}



use bytes::BytesMut;
struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(tcpstream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(tcpstream),
            buffer: BytesMut::with_capacity(4096)
        }
    }

    pub fn check_frame(&self, cur: &[u8]) -> Result<bool> {
        if cur.len() < 5 {
            return Ok(false);
        }

        match &cur[0] {
            152 => { // 'j'
                
                let len = u32::from_be_bytes(cur[1..5].try_into()?);

                // There's enough bytes in the buffer to parse
                if cur.len() - 5 <= (len as usize) {
                    return Ok(true)
                }
            }
            _ => {
                return Err(String::from("Invalid frame").into())
            }
        }

        Ok(false)
    }
    pub async fn parse_frame(&mut self) -> Result<Option<Frame>> {

        let frame_ok = self.check_frame(&self.buffer)?;
        
        if frame_ok {
            let end = 5 + u32::from_be_bytes(self.buffer[1..5].try_into()?) as usize;
            let update: PeerUpdate = serde_json::from_slice(&self.buffer[5..end])?;
            self.buffer.advance(end);

            return Ok(Some(Frame::Update(update)));
        }

        
        Ok(None)
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {

        // Keep reading chunks of data from the stream,
        // until an error or a frame is completely read
        loop {
            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }

            // Read more from stream, a 0 indicates end-of-stream
            if 0 == self.stream.read_buf(&mut self.buffer).await? {

                // Nothing left to read, just return empty
                if self.buffer.is_empty() {
                    println!("Nothing to read from buffer");
                    return Ok(None);
                }

                // If the buffer isn't empty, the connection to the
                // other party was broken somehow
                return Err("Connection reset by peer".into());
                    
            }
        }
    }

    pub async fn write_frame(&mut self, f: &Frame) -> Result<()> {
        match f {
            Frame::Update(data) => {

                let datastr: String = serde_json::to_string::<PeerUpdate>(data)?;
                let databytes = datastr.as_bytes();

                self.stream.write_u8(152).await?;
                self.stream.write_u32(databytes.len() as u32).await?;
                self.stream.write_all(databytes).await?;
            }
        }
        self.stream.flush().await?;
        Ok(())
    }

}

async fn handle_conn(node: &Node, shared_state: &Arc<Mutex<State>>, stream: TcpStream) -> Result<()> {
    let mut conn = Connection::new(stream);
    let maybe_frame = conn.read_frame().await?;
    
    match maybe_frame {
        Some(Frame::Update(update)) => {
            

            match shared_state.lock() {

                Ok(mut state) => {
                    let (notinpeer, notinself) = state.diff_peerlist(&update.peerlist);
                    
                    let frame = Frame::Update(PeerUpdate {
                        id: state.id.clone(),
                        version: state.version,
                        generation: state.generation,
                        peerlist: notinpeer
                    });

                    conn.write_frame(&frame).await?;

                    state.merge_peerlist(&notinself);
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
                    if let Some(peer) = state.peermap.get_mut(&update.id) {
                        peer.healthcheck = now;
                    }

                    state.version += 1;
                    state.save(&node.state_file);

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

#[derive(Clone, Debug)]
struct Shutdown {
    _shutdown: Arc<Mutex<bool>>
}

impl Shutdown {

    fn new() -> Self {
        Shutdown {
            _shutdown: Arc::new(Mutex::new(false))
        }
    }

    fn shutdown(&self) {
        let result = self._shutdown.lock();
        
        match result {
            Ok(mut guard) => {
                *guard = true;
            },
            Err(e) => {
                println!("Cannot gracefully shutdown from ctrl-c input. Exiting.");
                println!("{:?}", e);
                exit(1);
            }
        }
    }

    async fn wait_shutdown(&self) -> bool {
        loop {
            let result = self._shutdown.try_lock();
            if let Ok(guard) = result {
                if *guard {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn check_shutdown(&self) -> bool {
        let result = self._shutdown.try_lock();
        if let Ok(guard) = result {
            if *guard {
                return true;
            }
        }
        false
    }
}


async fn peerconnect(node: &Node, state: &mut State, peer: &PeerNode) -> Result<()> {

    let mut stream = TcpStream::connect(format!("{}:{}", peer.ip, peer.port)).await?;

    let mut conn = Connection::new(stream);

    let update = Frame::Update(PeerUpdate {
        id: state.id.clone(),
        version: state.version,
        generation: state.generation,
        peerlist: state.peermap.values().cloned().collect()
    });
    
    conn.write_frame(&update).await?;

    if let Some(Frame::Update(resp_update)) = conn.read_frame().await? {
        println!("Received response");
        
        let (notinpeer, notinself) = state.diff_peerlist(&resp_update.peerlist);
        state.merge_peerlist(&notinself);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|e| Duration::from_secs(0)).as_secs();
        if let Some(peer) = state.peermap.get_mut(&peer.id) {
            peer.healthcheck = now;
        }
        state.version += 1;
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

struct Node {
    state_file: PathBuf,
    config_file: PathBuf,
    
    config: Config,
    state: Arc<Mutex<State>>,
    shutdown: Shutdown
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

    pub async fn serverloop(&self) {
        let state_clone = self.state.clone();
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
                                if let Err(e) = handle_conn(&self, &state_clone, stream.0).await {
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

    async fn clientloop(&self) {
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
                let result = peerconnect(&self, &mut state, &peer).await;
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


#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        
        println!("Missing id argument");
        exit(1);
    }
    let id = &args[1];
    let shutdown= Shutdown::new();
    let shutdown_clone = shutdown.clone();

    let join = tokio::spawn(async move {
        let _ = signal::ctrl_c().await;

        shutdown_clone.shutdown();
    });

    let node = Node::new(id.clone(), None, shutdown);

    // run async jobs on the same task
    let _ = tokio::join!(node.serverloop(), node.clientloop());
    join.await.unwrap();
    println!("Exiting...");

}