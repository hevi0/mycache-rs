use std::fmt;
use std::error::Error;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(crate) type PeerConnResult<T> = std::result::Result<T, PeerError>;

pub(crate) type IdType = u16;

pub(crate) struct PeerConnError(pub String);

pub(crate) struct OtherError(pub String);

pub(crate) enum PeerError {
    /// Connection Reset
    ConnectionResetError,

    /// An error trying to read from the stream
    ReadStreamError,

    /// An error trying to write to the stream
    WriteStreamError,

    /// Connection is working, but something else went wrong.
    /// This should not count toward the error_count between peers.
    /// Check frame parsing or serialization logic.
    NonConnectionError
}

impl Error for PeerError {

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