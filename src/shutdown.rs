use std::process::exit;

use std::sync::{Arc, Mutex};
use std::time::Duration;


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