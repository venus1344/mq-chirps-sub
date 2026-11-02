//! Stdout handler: logs a structured summary of each event.

use async_trait::async_trait;
use tracing::info;

use crate::events::DecodedEvent;
use crate::handler::{Handler, HandlerError};

pub struct StdoutHandler;

#[async_trait]
impl Handler for StdoutHandler {
    fn name(&self) -> &'static str {
        "stdout"
    }

    async fn handle(&self, event: &DecodedEvent) -> Result<(), HandlerError> {
        info!(
            application_id = %event.application_id,
            dev_eui = %event.dev_eui,
            event_type = %event.event_type.as_str(),
            time = %event.time.as_deref().unwrap_or("-"),
            f_port = ?event.f_port,
            deduplication_id = %event.deduplication_id.as_deref().unwrap_or("-"),
            "received device event"
        );
        Ok(())
    }
}
