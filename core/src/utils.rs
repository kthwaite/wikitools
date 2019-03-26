use std::time::Instant;
use log::info;

/// Simple timer for logging task duration.
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Create a new Timer.
    pub fn new() -> Self {
        Timer {
            start: Instant::now(),
        }
    }

    /// Print the time since the timer was last reset, then reset the timer.
    pub fn finish(&mut self) {
        info!("Done in {} seconds", self.start.elapsed().as_secs());
        self.reset();
    }

    /// Reset the timer.
    pub fn reset(&mut self) {
        self.start = Instant::now();
    }
}