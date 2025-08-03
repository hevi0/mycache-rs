use mycache_rs::common::Result;

use std::error::Error;
use std::io::{self, Write};

use tokio::net::TcpListener;

fn make_set_request() -> Result<()> {
    Ok(())
}

fn make_get_request() -> Result<()> {
    Ok(())

}

fn main() {
    loop {
        print!("Enter a string >> ");
        let _ = io::stdout().flush();
        let mut line = String::new();

        if let Err(_) = std::io::stdin().read_line(&mut line) {
            continue;
        }

        let Some((command, keyval))= line.split_once(' ') else {
            continue;
        };

        match command {
            "get" => {
                println!("{} {}", command, keyval);
            }
            "set" => {
                let Some((key, val)) = keyval.split_once(' ') else {
                    continue;
                };
                
                println!("{} {} -> {}", command, key, val );  
            }
            _ => {
                continue;
            }
        }
    }
}