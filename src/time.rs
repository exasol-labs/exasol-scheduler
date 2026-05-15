use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone)]
pub struct FakeClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl FakeClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    pub fn set_now(&self, now: DateTime<Utc>) {
        if let Ok(mut guard) = self.now.lock() {
            *guard = now;
        }
    }

    pub fn advance(&self, duration: std::time::Duration) {
        if let Ok(mut guard) = self.now.lock() {
            if let Ok(delta) = chrono::Duration::from_std(duration) {
                *guard += delta;
            }
        }
    }

    pub fn set(&self, now: DateTime<Utc>) {
        self.set_now(now);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| Utc::now())
    }
}
