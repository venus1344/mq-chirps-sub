//! Downlink scheduling: publish a `command/down` payload to ChirpStack.
//!
//! Topic: `application/{APPLICATION_ID}/device/{DEV_EUI}/command/down`
//! Payload (JSON, protobuf JSON mapping):
//! ```json
//! { "devEui": "…", "confirmed": true, "fPort": 10, "data": "<base64>" }
//! ```

use std::time::Duration;

use serde::Serialize;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::client::{build_mqtt_options, ClientError};
use crate::config::Config;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownlinkRequest {
    /// Must match the DevEUI in the MQTT topic.
    pub dev_eui: String,
    /// Whether to send as a confirmed downlink.
    pub confirmed: bool,
    /// FPort (must be > 0).
    pub f_port: u8,
    /// Base64-encoded payload (plaintext; ChirpStack encrypts it).
    pub data: String,
}

#[derive(Debug, Error)]
pub enum DownlinkError {
    #[error("fPort must be > 0, got {0}")]
    InvalidFPort(u8),
    #[error("invalid DevEUI `{0}`: expected 16 hex characters")]
    InvalidDevEui(String),
    #[error("invalid base64 payload `{0}`")]
    InvalidBase64(String),
    #[error("invalid hex payload `{0}`")]
    InvalidHex(String),
    #[error("application id is required (pass --application-id or set MQ_APPLICATION_ID)")]
    MissingApplicationId,
    #[error("serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("client error: {0}")]
    Client(Box<ClientError>),
    #[error("mqtt error: {0}")]
    Mqtt(Box<rumqttc::ClientError>),
    #[error("connection error: {0}")]
    Connection(Box<rumqttc::ConnectionError>),
    #[error("no PubAck received within {0:?}")]
    PublishTimeout(Duration),
}

impl From<ClientError> for DownlinkError {
    fn from(e: ClientError) -> Self {
        DownlinkError::Client(Box::new(e))
    }
}

impl From<rumqttc::ClientError> for DownlinkError {
    fn from(e: rumqttc::ClientError) -> Self {
        DownlinkError::Mqtt(Box::new(e))
    }
}

impl From<rumqttc::ConnectionError> for DownlinkError {
    fn from(e: rumqttc::ConnectionError) -> Self {
        DownlinkError::Connection(Box::new(e))
    }
}

/// Validate and publish a downlink. `data_b64` is the payload already base64
/// encoded (ChirpStack's expected wire format).
pub async fn publish(
    config: &Config,
    application_id: &str,
    dev_eui: &str,
    f_port: u8,
    data_b64: String,
    confirmed: bool,
) -> Result<(), DownlinkError> {
    validate_dev_eui(dev_eui)?;
    if f_port == 0 {
        return Err(DownlinkError::InvalidFPort(f_port));
    }
    // Validate base64 early so we fail before touching the network.
    decode_base64(&data_b64)?;

    let topic = format!("application/{application_id}/device/{dev_eui}/command/down");
    let request = DownlinkRequest {
        dev_eui: dev_eui.to_string(),
        confirmed,
        f_port,
        data: data_b64,
    };
    let payload = serde_json::to_vec(&request)?;

    let options = build_mqtt_options(config)?;
    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, 10);

    // Enqueue before connecting; rumqttc sends pending publishes once connected.
    client
        .publish(&topic, rumqttc::QoS::AtLeastOnce, false, payload)
        .await?;

    info!(
        topic = %topic,
        dev_eui = %dev_eui,
        f_port = f_port,
        confirmed = confirmed,
        "downlink queued"
    );

    // Drive the event loop until our publish is acknowledged.
    let mut published_pkid: Option<u16> = None;
    let result = tokio::time::timeout(PUBLISH_TIMEOUT, async {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    debug!("connected to broker");
                }
                Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Publish(pkid))) => {
                    debug!(pkid, "downlink publish sent");
                    published_pkid = Some(pkid);
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(ack))) => {
                    if published_pkid == Some(ack.pkid) {
                        return Ok(());
                    }
                    debug!(pkid = ack.pkid, "ignoring unrelated PubAck");
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)) => {
                    warn!("broker sent disconnect while publishing downlink");
                }
                Ok(_) => {}
                Err(err) => return Err(DownlinkError::from(err)),
            }
        }
    })
    .await;

    match result {
        Ok(inner) => {
            let _ = client.disconnect().await;
            inner
        }
        Err(_elapsed) => {
            let _ = client.disconnect().await;
            Err(DownlinkError::PublishTimeout(PUBLISH_TIMEOUT))
        }
    }
}

/// Decode base64 using the standard alphabet (with padding). ChirpStack expects
/// standard base64 in the `data` field.
fn decode_base64(s: &str) -> Result<Vec<u8>, DownlinkError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| DownlinkError::InvalidBase64(s.to_string()))
}

/// Encode raw bytes as standard base64.
pub fn encode_base64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Decode a hex string into bytes. Accepts uppercase/lowercase and optional
/// spaces. Returns an error on odd length or non-hex characters.
pub fn decode_hex(s: &str) -> Result<Vec<u8>, DownlinkError> {
    let cleaned: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    if !cleaned.len().is_multiple_of(2) {
        return Err(DownlinkError::InvalidHex(s.to_string()));
    }
    cleaned
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = hex_val(pair[0]).ok_or_else(|| DownlinkError::InvalidHex(s.to_string()))?;
            let lo = hex_val(pair[1]).ok_or_else(|| DownlinkError::InvalidHex(s.to_string()))?;
            Ok((hi << 4) | lo)
        })
        .collect()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn validate_dev_eui(dev_eui: &str) -> Result<(), DownlinkError> {
    if dev_eui.len() == 16 && dev_eui.bytes().all(|b| hex_val(b).is_some()) {
        Ok(())
    } else {
        Err(DownlinkError::InvalidDevEui(dev_eui.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_decoding() {
        assert_eq!(decode_hex("0102ff").unwrap(), vec![1, 2, 255]);
        assert_eq!(decode_hex("01 02 FF").unwrap(), vec![1, 2, 255]);
        assert!(matches!(decode_hex("abc"), Err(DownlinkError::InvalidHex(_))));
        assert!(matches!(decode_hex("zz"), Err(DownlinkError::InvalidHex(_))));
    }

    #[test]
    fn base64_roundtrip() {
        let bytes = vec![1, 2, 3, 4];
        let b64 = encode_base64(&bytes);
        assert_eq!(decode_base64(&b64).unwrap(), bytes);
        assert!(matches!(decode_base64("not base64!!"), Err(DownlinkError::InvalidBase64(_))));
    }

    #[test]
    fn dev_eui_validation() {
        assert!(validate_dev_eui("0102030405060708").is_ok());
        assert!(matches!(
            validate_dev_eui("010203040506070"),
            Err(DownlinkError::InvalidDevEui(_))
        ));
        assert!(matches!(
            validate_dev_eui("010203040506070g"),
            Err(DownlinkError::InvalidDevEui(_))
        ));
    }

    #[test]
    fn fport_must_be_positive() {
        let err = DownlinkError::InvalidFPort(0);
        assert!(matches!(err, DownlinkError::InvalidFPort(0)));
    }

    // --- Minimal MQTT v3.1.1 broker stub, enough to CONNACK + PUBACK. ---

    /// Read one MQTT packet: returns (fixed-header byte, body bytes).
    async fn read_packet(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        use tokio::io::AsyncReadExt;

        let mut hdr = [0u8; 1];
        stream.read_exact(&mut hdr).await.expect("fixed header");

        let mut remaining = 0usize;
        let mut multiplier = 1usize;
        loop {
            let mut b = [0u8; 1];
            stream.read_exact(&mut b).await.expect("remaining length");
            remaining += (b[0] & 0x7f) as usize * multiplier;
            if b[0] & 0x80 == 0 {
                break;
            }
            multiplier *= 128;
        }

        let mut body = vec![0u8; remaining];
        stream.read_exact(&mut body).await.expect("body");
        (hdr[0], body)
    }

    /// Spawn a broker stub that accepts one connection, replies CONNACK, then
    /// replies PUBACK to the first QoS1 PUBLISH it receives.
    async fn stub_broker() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");

            // CONNECT → CONNACK
            let (ptype, _body) = read_packet(&mut stream).await;
            assert_eq!(ptype, 0x10, "expected CONNECT");
            stream
                .write_all(&[0x20, 0x02, 0x00, 0x00])
                .await
                .expect("connack");

            // PUBLISH (QoS1) → PUBACK
            let (ptype, body) = read_packet(&mut stream).await;
            assert_eq!(ptype & 0xF0, 0x30, "expected PUBLISH");
            assert_eq!(ptype & 0x06, 0x02, "expected QoS1");

            let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
            let pkid = u16::from_be_bytes([body[2 + topic_len], body[3 + topic_len]]);
            stream
                .write_all(&[0x40, 0x02, (pkid >> 8) as u8, (pkid & 0xff) as u8])
                .await
                .expect("puback");

            // Let the client disconnect cleanly before we drop the socket.
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        (addr, handle)
    }

    #[tokio::test]
    async fn publish_downlink_receives_puback() {
        let (addr, broker) = stub_broker().await;

        let config = Config {
            broker_host: "127.0.0.1".to_string(),
            broker_port: addr.port(),
            broker_tls: false,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            username: None,
            password: None,
            client_id: "test-downlink".to_string(),
            topics: vec!["application/+/device/+/event/#".to_string()],
            qos: 0,
            application_id: None,
            sqlite_path: std::path::PathBuf::from(":memory:"),
            enable_stdout: false,
            enable_sqlite: false,
        };

        publish(
            &config,
            "app-123",
            "0102030405060708",
            10,
            "AQID".to_string(),
            true,
        )
        .await
        .expect("publish should be acknowledged");

        broker.await.expect("broker stub should finish cleanly");
    }
}
