//! Pluggable handler trait and dispatch registry.

use async_trait::async_trait;
use tracing::warn;

use crate::events::DecodedEvent;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("sqlite handler failed: {0}")]
    Sqlite(#[from] sqlx::Error),
    /// Escape hatch for future handlers that produce their own errors.
    #[allow(dead_code)]
    #[error("handler failed: {0}")]
    Other(String),
}

#[async_trait]
pub trait Handler: Send + Sync {
    fn name(&self) -> &'static str;
    async fn handle(&self, event: &DecodedEvent) -> Result<(), HandlerError>;
}

/// Runs every registered handler for each event. Per-handler failures are
/// logged and never prevent the remaining handlers from running.
#[derive(Default)]
pub struct Registry {
    handlers: Vec<Box<dyn Handler>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, handler: Box<dyn Handler>) {
        self.handlers.push(handler);
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub async fn dispatch(&self, event: &DecodedEvent) {
        for handler in &self.handlers {
            if let Err(err) = handler.handle(event).await {
                warn!(
                    handler = handler.name(),
                    error = %err,
                    topic = %event.topic,
                    "handler failed"
                );
            }
        }
    }
}
