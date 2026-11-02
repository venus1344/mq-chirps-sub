//! SQLite handler: persists each event to a local SQLite database.

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::events::DecodedEvent;
use crate::handler::{Handler, HandlerError};

pub struct SqliteHandler {
    pool: SqlitePool,
}

impl SqliteHandler {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Handler for SqliteHandler {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    async fn handle(&self, event: &DecodedEvent) -> Result<(), HandlerError> {
        let event_time = event
            .time
            .as_deref()
            .and_then(parse_event_time)
            .map(|dt| dt.to_rfc3339());

        let data_b64 = event.data_b64.clone();
        let f_port = event.f_port.map(i64::from);

        sqlx::query(
            r#"
            INSERT INTO events (
                event_time, application_id, dev_eui, event_type,
                deduplication_id, f_port, data_b64, topic, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event_time)
        .bind(&event.application_id)
        .bind(&event.dev_eui)
        .bind(event.event_type.as_str())
        .bind(&event.deduplication_id)
        .bind(f_port)
        .bind(data_b64)
        .bind(&event.topic)
        .bind(event.raw.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

/// Normalize the payload `time` string into a chrono DateTime. ChirpStack emits
/// RFC3339 with nanoseconds (e.g. `2022-07-18T09:34:15.775023242+00:00`). If
/// parsing fails we store NULL for `event_time` rather than dropping the row.
fn parse_event_time(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::events::{self, EventType};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::Row;

    async fn in_memory_pool() -> SqlitePool {
        let options = SqliteConnectOptions::new().filename(":memory:");
        // `:memory:` databases are per-connection, so pin the pool to a single
        // connection so migration and queries share the same database.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("pool");
        db::migrate(&pool).await.expect("migrate");
        pool
    }

    #[tokio::test]
    async fn persists_event_row() {
        let pool = in_memory_pool().await;
        let handler = SqliteHandler::new(pool.clone());

        let json = r#"{
            "deduplicationId": "3ac7e3c4-4401-4b8d-9386-a5c902f9202d",
            "time": "2022-07-18T09:34:15.775023242+00:00",
            "deviceInfo": { "devEui": "0101010101010101" },
            "devAddr": "00189440",
            "dr": 1,
            "fPort": 1,
            "data": "qg=="
        }"#;
        let topic = "application/app-123/device/0101010101010101/event/up";
        let event = events::decode(topic, EventType::Up, json.as_bytes()).expect("decode");

        handler.handle(&event).await.expect("handle");

        let row = sqlx::query(
            "SELECT application_id, dev_eui, event_type, event_time, f_port, data_b64, topic FROM events",
        )
        .fetch_one(&pool)
        .await
        .expect("row");

        assert_eq!(row.get::<String, _>("application_id"), "app-123");
        assert_eq!(row.get::<String, _>("dev_eui"), "0101010101010101");
        assert_eq!(row.get::<String, _>("event_type"), "up");
        assert_eq!(
            row.get::<String, _>("event_time"),
            "2022-07-18T09:34:15.775023242+00:00"
        );
        assert_eq!(row.get::<i64, _>("f_port"), 1);
        assert_eq!(row.get::<String, _>("data_b64"), "qg==");
        assert_eq!(
            row.get::<String, _>("topic"),
            "application/app-123/device/0101010101010101/event/up"
        );
    }
}
