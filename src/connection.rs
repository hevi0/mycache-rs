use crate::common::*;
use crate::peernode::*;

use bytes::{Buf, BytesMut};

use serde::{Deserialize, Serialize};
use serde_json;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpStream};


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

        if cur[0] < 151 || cur[0] > 158 {
            return Ok(false);
        }

        let len = u32::from_be_bytes(cur[1..5].try_into()?);

        // There's enough bytes in the buffer to parse
        if cur.len() - 5 <= (len as usize) {
            return Ok(true)
        }

        /*
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
                // frame is invalid
                return Err(Error::from(ErrorKind::InvalidData).into())
            }
        }
        */

        Ok(false)
    }


    pub async fn parse_frame(&mut self) -> Result<Option<Frame>> {

        let frame_ok = self.check_frame(&self.buffer)?;
        
        if frame_ok {
            let t = &self.buffer[0];
            let end = 5 + u32::from_be_bytes(self.buffer[1..5].try_into()?) as usize;

            let frame = {
                    
                if *t == 151 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::Init(d))

                } else if *t == 152 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::Update(d))
                } else if *t == 153 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::SetVal(d))
                } else if *t == 154 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::SetValReply(d))
                } else if *t == 155 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::GetVal(d))
                } else if *t == 156 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::GetValReply(d))
                } else if *t == 157 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::ClientGetVal(d))
                } else if *t == 158 {
                    let Ok(d) = serde_json::from_slice(&self.buffer[5..end]) else {
                        return Ok(None);
                    };
                    Some(Frame::ClientSetVal(d))
                } else {
                    None
                }
            };

            self.buffer.advance(end);
            
            return Ok(frame);
            
        }

        
        Ok(None)
    }

    pub async fn read_frame(&mut self) -> PeerConnResult<Option<Frame>> {

        // Keep reading chunks of data from the stream,
        // until an error or a frame is completely read
        loop {

            // We need to limit the lifetime of the Result<T, dyn Error>
            // type since it isn't Send. By not being Send, it cannot
            // exist across await boundaries when handled by threads
            // since at any await tasks could be moved to another
            // thread.
            // Alternatively we could specify Send, but that makes
            // the Result type less generic. OR, use a different
            // error type for parse_frame()
            {
                let result = self.parse_frame().await;
                if let Err(e) = &result{
                    println!("Error reading from buffer");
                    return Err(PeerError::NonConnectionError);
                }

                if let Some(frame) = result.unwrap() {
                    return Ok(Some(frame));
                }
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

        let prefix = prefix(f);

        match f {
            Frame::Update(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::Init(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::GetVal(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::GetValReply(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::SetVal(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::SetValReply(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::ClientGetVal(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
                }
            }
            Frame::ClientSetVal(data) => {
                if let Err(e) = self.write_stream(data, prefix).await {
                    peer_write_failed = true;
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

    async fn write_stream<T: Serialize>(&mut self, data: &T, prefix: u8 ) -> PeerConnResult<()> {
        match serde_json::to_string::<T>(data) {
            Ok(datastr) => {
                let databytes = datastr.as_bytes();

                let Ok(_) = self.stream.write_u8(prefix).await else {
                    return Err(PeerError::WriteStreamError.into());
                };

                let Ok(_) = self.stream.write_u32(databytes.len() as u32).await else {
                    return Err(PeerError::WriteStreamError.into());
                };

                let Ok(_)= self.stream.write_all(databytes).await else {
                    return Err(PeerError::WriteStreamError.into());
                };

                return Ok(())
            }
            Err(e) => {
                println!("Error serializing frame: {:?}", e);
                return Ok(());
            }
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
    Init(NodeVersions),
    SetVal((String, String)),
    SetValReply(Option<(String, String, IdType)>),
    GetVal(String),
    GetValReply(Option<(String, String, IdType)>),
    ClientGetVal(String),
    ClientSetVal((String, String))

}

pub fn prefix(frame: &Frame) -> u8 {
    match frame {
        Frame::Update(_) => {
            152
        }
        Frame::Init(_) => {
            151
        }
        Frame::SetVal(_) => {
            153
        }
        Frame::SetValReply(_) => {
            154
        }
        Frame::GetVal(_) => {
            155
        }
        Frame::GetValReply(_) => {
            156
        }
        Frame::ClientGetVal(_) => {
            157
        }
        Frame::ClientSetVal(_) => {
            158
        }
        
    }
}

