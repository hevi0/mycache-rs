use crate::common::*;
use crate::connection::*;
use crate::node::*;

use tokio::net::TcpStream;

impl Node {

    pub(crate) async fn forward_kv_request(&mut self, node_id: IdType, frame: Frame) -> Result<(String, String, IdType)> {
        let (ip, port) = {
            let locked_state = self.state.lock().unwrap();
            
            (locked_state.peermap[&node_id].ip.clone(), locked_state.peermap[&node_id].port.clone())
        };

        let Ok(stream) = TcpStream::connect(format!("{}:{}", ip, port)).await else {

            // Increment error_count for peer
            return Err(PeerError::OtherError.into());
        };

        let mut conn = Connection::new(stream);

        conn.write_frame(&frame).await?;


        match conn.read_frame().await {
            // Quorum not necessary since an in-memory cache 
            // requires speed foremost.
            Ok(Some(Frame::GetValReply(Some((k, v, peer_id))))) => {
                return Ok((k, v, peer_id));
                
            }

            Ok(Some(Frame::SetValReply(Some((k, v, peer_id))))) => {
                return Ok((k, v, peer_id));
            }

            Ok(_) => {
                return Err(PeerError::NonConnectionError.into());
            }

            Err(e) => {
                return Err(e.into());
            }
        }

    }

    pub(crate) async fn set_kv(&mut self, k: String, v: String) -> Result<()> {
        let node_ids = {
            let locked_chash = self.chash.lock().unwrap();
            locked_chash.calculate_vnodes(&k)
        };

        for node_id in node_ids {
            if node_id == self.config.id {
                let mut locked_kv = self.kv_store.lock().unwrap();
                locked_kv.insert(k.clone(), v.clone());
            }
            else {
                self.forward_kv_request(node_id, Frame::SetVal((k.clone(), v.clone()))).await?;
            }

        }
        
        Ok(())
    }
}