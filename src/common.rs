use std::fmt;
use std::error::Error;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(crate) type PeerConnResult<T> = std::result::Result<T, PeerConnError>;

pub(crate) struct PeerConnError(pub String);

impl PeerConnError {
    pub fn new(msg: &str) -> Self {
        PeerConnError(msg.to_string())
    }
}

impl Error for PeerConnError {
    
}

impl fmt::Display for PeerConnError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // user-facing output
    }
}

impl fmt::Debug for PeerConnError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}