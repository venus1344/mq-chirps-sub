//! MQTT client: connection, TLS, subscription, and the receive loop.

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use rumqttc::tokio_rustls::rustls::{self, RootCertStore};
use rumqttc::{
    AsyncClient, Event, MqttOptions, Packet, Publish, QoS, TlsConfiguration, Transport,
};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::events::{self, DecodedEvent, EventType};
use crate::handler::Registry;
use crate::topic;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("TLS setup failed: {0}")]
    Tls(String),
    #[error("MQTT client error: {0}")]
    Mqtt(#[from] rumqttc::ClientError),
    #[error("signal handling setup failed: {0}")]
    Signal(#[from] std::io::Error),
}

/// Build MQTT options from config: credentials, keep-alive, and TLS transport
/// when enabled. Shared by the subscriber and the downlink publisher.
pub(crate) fn build_mqtt_options(config: &Config) -> Result<MqttOptions, ClientError> {
    let mut options = MqttOptions::new(&config.client_id, &config.broker_host, config.broker_port);
    options.set_clean_session(true);
    options.set_keep_alive(Duration::from_secs(60));

    if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        options.set_credentials(user, pass);
    }

    if config.broker_tls {
        let client_config = build_tls_config(config)?;
        options.set_transport(Transport::Tls(TlsConfiguration::Rustls(Arc::new(
            client_config,
        ))));
    }

    Ok(options)
}

/// Subscribe to every configured topic filter in a single MQTT Subscribe.
async fn subscribe_all(
    client: &AsyncClient,
    topics: &[String],
    qos: QoS,
) -> Result<(), ClientError> {
    let filters: Vec<rumqttc::SubscribeFilter> = topics
        .iter()
        .cloned()
        .map(|path| rumqttc::SubscribeFilter::new(path, qos))
        .collect();
    client.subscribe_many(filters).await?;
    Ok(())
}

/// Run the subscriber until shutdown. Re-subscribes on every `ConnAck` so the
/// subscriptions survive broker reconnects.
pub async fn run(config: Config, registry: Arc<Registry>) -> Result<(), ClientError> {
    let qos = qos_from_u8(config.qos);
    let options = build_mqtt_options(&config)?;

    let (client, mut eventloop) = AsyncClient::new(options, 10);
    subscribe_all(&client, &config.topics, qos).await?;
    info!(
        topics = ?config.topics,
        broker = %format!("{}:{}", config.broker_host, config.broker_port),
        tls = config.broker_tls,
        "subscriber started"
    );

    loop {
        tokio::select! {
            sig = shutdown_signal() => {
                let sig = sig?;
                info!(signal = sig, "shutdown signal received; disconnecting");
                let _ = client.disconnect().await;
                return Ok(());
            }
            event = eventloop.poll() => {
                match event {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        info!("connected to broker; (re)subscribing");
                        if let Err(err) = subscribe_all(&client, &config.topics, qos).await {
                            warn!(error = %err, "subscribe after ConnAck failed");
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        handle_publish(&publish, &registry).await;
                    }
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        warn!("broker sent disconnect; waiting for reconnect");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Ok(_) => {
                        debug!("ignoring other MQTT event");
                    }
                    Err(err) => {
                        warn!(error = %err, "connection error; waiting for reconnect");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }
    }
}

async fn handle_publish(publish: &Publish, registry: &Arc<Registry>) {
    let topic_str = &publish.topic;

    let parsed = match topic::parse(topic_str) {
        Ok(parsed) => parsed,
        Err(err) => {
            warn!(topic = %topic_str, error = %err, "skipping message with unparseable topic");
            return;
        }
    };

    let event_type = match parsed.event_type.parse::<EventType>() {
        Ok(et) => et,
        Err(err) => {
            warn!(topic = %topic_str, error = %err, "skipping message with unknown event type");
            return;
        }
    };

    let event: DecodedEvent = match events::decode(topic_str, event_type, &publish.payload) {
        Ok(event) => event,
        Err(err) => {
            warn!(topic = %topic_str, error = %err, "skipping undecodable message");
            return;
        }
    };

    registry.dispatch(&event).await;
}

fn qos_from_u8(q: u8) -> QoS {
    match q {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

/// Resolve the first shutdown signal to arrive: SIGINT (Ctrl-C) or SIGTERM.
#[cfg(unix)]
async fn shutdown_signal() -> Result<&'static str, std::io::Error> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            res?;
            Ok("SIGINT")
        }
        _ = sigterm.recv() => Ok("SIGTERM"),
    }
}

/// Resolve the first shutdown signal to arrive (SIGINT only on non-Unix).
#[cfg(not(unix))]
async fn shutdown_signal() -> Result<&'static str, std::io::Error> {
    tokio::signal::ctrl_c().await?;
    Ok("SIGINT")
}

/// Warn at startup when the mTLS client private key is readable by group or
/// others — such a key lets other local users impersonate this client.
#[cfg(unix)]
fn warn_on_world_readable_key(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            warn!(
                path = %path.display(),
                mode = format!("{mode:o}"),
                "client private key is readable by group/others; run `chmod 600` on it"
            );
        }
        // The subsequent read will surface any metadata error with proper context.
    }
}

/// No-op on non-Unix platforms.
#[cfg(not(unix))]
fn warn_on_world_readable_key(_path: &std::path::Path) {}


/// Build a rustls `ClientConfig`: custom CA file when provided, otherwise the
/// platform root store; optional client certificate/key for mTLS.
fn build_tls_config(config: &Config) -> Result<rustls::ClientConfig, ClientError> {
    use rustls_pemfile as pemfile;

    let mut root_store = RootCertStore::empty();

    if let Some(path) = &config.client_key {
        warn_on_world_readable_key(path);
    }

    match &config.ca_cert {
        Some(path) => {
            let pem = std::fs::read(path)
                .map_err(|e| ClientError::Tls(format!("read CA cert {}: {e}", path.display())))?;
            let certs: Vec<_> = pemfile::certs(&mut Cursor::new(&pem))
                .collect::<Result<_, _>>()
                .map_err(|e| ClientError::Tls(format!("parse CA cert: {e}")))?;
            for cert in certs {
                root_store
                    .add(cert)
                    .map_err(|e| ClientError::Tls(format!("add CA cert: {e}")))?;
            }
        }
        None => {
            let result = rustls_native_certs::load_native_certs();
            for cert in result.certs {
                root_store
                    .add(cert)
                    .map_err(|e| ClientError::Tls(format!("add native cert: {e}")))?;
            }
        }
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    let client_config = match (&config.client_cert, &config.client_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = std::fs::read(cert_path).map_err(|e| {
                ClientError::Tls(format!("read client cert {}: {e}", cert_path.display()))
            })?;
            let certs: Vec<_> = pemfile::certs(&mut Cursor::new(&cert_pem))
                .collect::<Result<_, _>>()
                .map_err(|e| ClientError::Tls(format!("parse client cert: {e}")))?;

            let key_pem = std::fs::read(key_path).map_err(|e| {
                ClientError::Tls(format!("read client key {}: {e}", key_path.display()))
            })?;
            let key = pemfile::private_key(&mut Cursor::new(&key_pem))
                .map_err(|e| ClientError::Tls(format!("parse client key: {e}")))?
                .ok_or_else(|| ClientError::Tls("no private key found in client key file".into()))?;

            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| ClientError::Tls(format!("build client auth: {e}")))?
        }
        _ => builder.with_no_client_auth(),
    };

    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::handlers::sqlite::SqliteHandler;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::Row;

    /// Exercises the full publish-handling glue (topic parse → event type →
    /// decode → dispatch) without needing a live broker.
    #[tokio::test]
    async fn handle_publish_dispatches_to_handlers() {
        let options = SqliteConnectOptions::new().filename(":memory:");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("pool");
        db::migrate(&pool).await.expect("migrate");

        let mut registry = Registry::new();
        registry.register(Box::new(SqliteHandler::new(pool.clone())));
        let registry = Arc::new(registry);

        let json = r#"{
            "deduplicationId": "3ac7e3c4-4401-4b8d-9386-a5c902f9202d",
            "time": "2022-07-18T09:34:15.775023242+00:00",
            "deviceInfo": { "devEui": "0101010101010101" },
            "fPort": 2,
            "data": "qg=="
        }"#;
        let publish = Publish::new(
            "application/app-123/device/0101010101010101/event/up",
            QoS::AtMostOnce,
            json.as_bytes().to_vec(),
        );

        handle_publish(&publish, &registry).await;

        let row = sqlx::query(
            "SELECT application_id, dev_eui, event_type, f_port, data_b64 FROM events",
        )
        .fetch_one(&pool)
        .await
        .expect("row inserted");

        assert_eq!(row.get::<String, _>("application_id"), "app-123");
        assert_eq!(row.get::<String, _>("dev_eui"), "0101010101010101");
        assert_eq!(row.get::<String, _>("event_type"), "up");
        assert_eq!(row.get::<i64, _>("f_port"), 2);
        assert_eq!(row.get::<String, _>("data_b64"), "qg==");
    }

    /// A message with an unparseable topic is skipped without crashing.
    #[tokio::test]
    async fn handle_publish_skips_bad_topic() {
        let mut registry = Registry::new();
        registry.register(Box::new(crate::handlers::stdout::StdoutHandler));
        let registry = Arc::new(registry);

        let publish = Publish::new("not/a/valid/topic", QoS::AtMostOnce, b"{}".to_vec());
        handle_publish(&publish, &registry).await;
        // Reaching here without panic is the assertion.
    }
}
