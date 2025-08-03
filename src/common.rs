use std::fmt;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub type IdType = u16;
pub type PeerConnResult<T> = std::result::Result<T, PeerError>;

pub enum PeerError {
    /// Connection Reset
    ConnectionResetError,

    /// An error trying to read from the stream
    ReadStreamError,

    /// An error trying to write to the stream
    WriteStreamError,

    // Another reason the connection failed
    OtherError,

    /// Connection is working, but something else went wrong.
    /// This should not count toward the error_count between peers.
    /// Check frame parsing or serialization logic.
    NonConnectionError
}

impl PeerError {
    pub fn convert_io_error<T>(result: std::result::Result<T, std::io::Error>) -> PeerConnResult<T> {
        match result {
            Ok(contents) => {
                Ok(contents)
            }
            Err(e) => {
                Err(PeerError::OtherError)
            }
        }
    }
}

impl std::error::Error for PeerError {

}

impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl fmt::Debug for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

