use std::process::exit;

use std::sync::{Arc, Mutex};
use std::time::Duration;


/// Encapsulate logic for communicating shutdown
/// between long-running async tasks.
#[derive(Clone, Debug)]
pub struct Shutdown {
    _shutdown: Arc<Mutex<bool>>
}

impl Shutdown {

    pub fn new() -> Self {
        Shutdown {
            _shutdown: Arc::new(Mutex::new(false))
        }
    }

    ///
    /// Let every task that is checking, or awaiting,
    /// know that we are shutting down.
    pub fn shutdown(&self) {
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

    /// Await for shutdown asynchronously
    pub async fn wait_shutdown(&self) -> bool {
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

    /// An immediate check for shutdown flag.
    /// This logic should not block, but immediately return the flag's value.
    pub fn check_shutdown(&self) -> bool {
        let result = self._shutdown.try_lock();
        if let Ok(guard) = result {
            if *guard {
                return true;
            }
        }
        false
    }
}