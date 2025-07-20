
use std::path::PathBuf;

use std::time::Duration;
use std::vec::Vec;
use std::fs::File;
use std::io::{BufReader};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use serde_json;

use std::env;

mod chash;

mod common;
use common::Result;

mod connection;
use connection::*;

mod peernode;
use peernode::*;

mod config;
use config::*;

mod state;
use state::*;

mod shutdown;
use shutdown::*;

mod node;
use node::*;


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
    let _ = tokio::join!(node.listenloop(), node.gossiploop());
    join.await.unwrap();
    println!("Exiting...");

}