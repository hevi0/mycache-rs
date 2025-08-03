

use std::vec::Vec;

use std::process::exit;

use tokio::signal;

use std::env;

mod chash;
mod common;
mod config;
mod connection;
mod kvstore;
mod node;
mod peernode;
mod peerxchg;
mod state;
mod shutdown;

use common::*;
use connection::*;
use peernode::*;
use config::*;
use state::*;
use shutdown::*;
use node::*;
use peerxchg::*;


#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        
        println!("Missing id argument");
        exit(1);
    }
    let id: IdType = args[1].parse().unwrap();
    let shutdown= Shutdown::new();
    let shutdown_clone = shutdown.clone();

    let join = tokio::spawn(async move {
        let _ = signal::ctrl_c().await;

        shutdown_clone.shutdown();
    });

    // node needs to have static lifetime so that it can be
    // allowed to be referenced in threads.
    let node: &'static mut Node = Box::leak(Box::new(Node::new(id, None, shutdown)));

    // run async jobs on the same task
    let _ = tokio::join!(node.listenloop(), node.peerxchg_gossip_loop());

    join.await.unwrap();
    println!("Exiting...");

}