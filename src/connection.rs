use crate::common::*;
use crate::node::Node;
use crate::peernode::*;

use serde::{Deserialize, Serialize};
use serde_json;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpStream};

use bytes::{Buf, BytesMut};



pub(crate) struct Connection {
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
            151 => {
                let len = u32::from_be_bytes(cur[1..5].try_into()?);

                // There's enough bytes in the buffer to parse
                if cur.len() - 5 <= (len as usize) {
                    return Ok(true)
                }
            }
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
            let t = &self.buffer[0];
            if *t == 151 {
                let end = 5 + u32::from_be_bytes(self.buffer[1..5].try_into()?) as usize;
                let node_versions: NodeVersions = serde_json::from_slice(&self.buffer[5..end])?;
                self.buffer.advance(end);

                return Ok(Some(Frame::Init(node_versions)));
            }

            if *t == 152 {
                let end = 5 + u32::from_be_bytes(self.buffer[1..5].try_into()?) as usize;
                let update: PeerUpdate = serde_json::from_slice(&self.buffer[5..end])?;
                self.buffer.advance(end);

                return Ok(Some(Frame::Update(update)));
            }
            
        }

        
        Ok(None)
    }

    pub async fn read_frame(&mut self) -> PeerConnResult<Option<Frame>> {

        // Keep reading chunks of data from the stream,
        // until an error or a frame is completely read
        loop {
            let result = self.parse_frame().await;
            if let Err(e) = &result{
                println!("Error reading from buffer");
                return Err(PeerError::NonConnectionError);
            }

            if let Some(frame) = result.unwrap() {
                return Ok(Some(frame));
            }

            // Read more from stream, a 0 indicates end-of-stream
            match self.stream.read_buf(&mut self.buffer).await {
                Ok(num_bytes) => {
                    if 0 == num_bytes {

                        // Nothing left to read, just return empty
                        if self.buffer.is_empty() {
                            println!("Nothing to read from buffer");
                            return Ok(None);
                        }

                        // If the buffer isn't empty, the connection to the
                        // other party was broken somehow
                        //return Err(PeerError::ConnError(PeerConnError("Connection reset by peer".to_string())));
                        return Err(PeerError::ConnectionResetError)       
                    }
                }
                Err(e) => {
                    return Err(PeerError::ReadStreamError);
                }
            }
        }
    }

    pub async fn write_frame(&mut self, f: &Frame) -> PeerConnResult<()> {
        let mut peer_write_failed = false;
        match f {
            Frame::Update(data) => {

                match serde_json::to_string::<PeerUpdate>(data) {
                    Ok(datastr) => {
                        let databytes = datastr.as_bytes();

                        if let Err(e) = self.stream.write_u8(152).await {
                            peer_write_failed = true;
                        }
                        if let Err(e) = self.stream.write_u32(databytes.len() as u32).await {
                            peer_write_failed = true;
                        }
                        if let Err(e) = self.stream.write_all(databytes).await {
                            peer_write_failed = true;
                        }
                    }
                    Err(e) => {
                        println!("Error serializing frame: {:?}", e);
                        return Ok(());
                    }
                }
                
            },
            Frame::Init(data) => {
                match serde_json::to_string::<NodeVersions>(data) {
                    Ok(datastr) => {
                        let databytes = datastr.as_bytes();

                        if let Err(e) = self.stream.write_u8(151).await {
                            peer_write_failed = true;
                        }
                        if let Err(e) = self.stream.write_u32(databytes.len() as u32).await {
                            peer_write_failed = true;
                        }
                        if let Err(e) = self.stream.write_all(databytes).await {
                            peer_write_failed = true;
                        }
                    }
                    Err(e) => {
                        println!("Error serializing frame: {:?}", e);
                        return Ok(());
                    }
                }
            }
        }

        if let Err(e) = self.stream.flush().await {
            peer_write_failed = true;
        }
        
        if peer_write_failed {
            Err(PeerError::WriteStreamError)
        } else {
            Ok(())
        }
    }

}

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct PeerUpdate {
    pub id: IdType,
    pub ip: String,
    pub port: String,
    pub version: u64,
    pub generation: u32,
    pub peerlist: PeerList,
    pub peerreq: Vec<IdType>
}

///
/// Compact-ish representation of message that holds version for
/// a certain number of nodes after an offset.
/// This allows for a back-and-forth conversation to occur
/// between nodes in chunks.
#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct NodeVersions {
    pub id: IdType,
    pub offset: usize,
    pub versions: Vec<u64>
}

#[derive(Deserialize, Serialize, Debug)]
pub(crate) enum Frame {
    Update(PeerUpdate),
    Init(NodeVersions)

}