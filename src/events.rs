//! Typed ChirpStack event models (JSON encoding) and decoding.
//!
//! The event type is carried in the MQTT topic, so `decode` takes the topic's
//! event type and deserializes the payload into the matching struct.

// The typed data model is the public surface consumed by handlers; not every
// field is read by the built-in stdout/sqlite handlers.
#![allow(dead_code)]

use std::str::FromStr;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Up,
    Status,
    Join,
    Ack,
    TxAck,
    Log,
    Location,
    Integration,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Up => "up",
            EventType::Status => "status",
            EventType::Join => "join",
            EventType::Ack => "ack",
            EventType::TxAck => "txack",
            EventType::Log => "log",
            EventType::Location => "location",
            EventType::Integration => "integration",
        }
    }
}

impl FromStr for EventType {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "up" => Ok(EventType::Up),
            "status" => Ok(EventType::Status),
            "join" => Ok(EventType::Join),
            "ack" => Ok(EventType::Ack),
            "txack" => Ok(EventType::TxAck),
            "log" => Ok(EventType::Log),
            "location" => Ok(EventType::Location),
            "integration" => Ok(EventType::Integration),
            other => Err(DecodeError::UnknownEventType(other.to_string())),
        }
    }
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("unknown event type `{0}`")]
    UnknownEventType(String),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Shared device info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInfo {
    pub tenant_id: Option<String>,
    pub tenant_name: Option<String>,
    pub application_id: Option<String>,
    pub application_name: Option<String>,
    pub device_profile_id: Option<String>,
    pub device_profile_name: Option<String>,
    pub device_name: Option<String>,
    pub dev_eui: Option<String>,
    #[serde(default)]
    pub tags: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Per-event payloads
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UplinkEvent {
    pub deduplication_id: Option<String>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub dev_addr: Option<String>,
    pub dr: Option<u8>,
    pub f_port: Option<u8>,
    pub data: Option<String>,
    #[serde(default)]
    pub rx_info: Vec<RxInfo>,
    pub tx_info: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RxInfo {
    pub gateway_id: Option<String>,
    pub uplink_id: Option<u64>,
    pub rssi: Option<i32>,
    pub snr: Option<f64>,
    pub context: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusEvent {
    pub deduplication_id: Option<String>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub margin: Option<i32>,
    pub battery_level: Option<f64>,
    pub battery_level_unavailable: Option<bool>,
    pub external_power_source: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinEvent {
    pub deduplication_id: Option<String>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub dev_addr: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AckEvent {
    pub deduplication_id: Option<String>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub queue_item_id: Option<String>,
    pub acknowledged: Option<bool>,
    pub f_cnt_down: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxAckEvent {
    pub downlink_id: Option<u64>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub queue_item_id: Option<String>,
    pub f_cnt_down: Option<u32>,
    pub gateway_id: Option<String>,
    pub tx_info: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEvent {
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub level: Option<String>,
    pub code: Option<String>,
    pub description: Option<String>,
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocationEvent {
    pub deduplication_id: Option<String>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub location: Option<Location>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationEvent {
    pub deduplication_id: Option<String>,
    pub time: Option<String>,
    pub device_info: Option<DeviceInfo>,
    pub integration_name: Option<String>,
    pub event_type: Option<String>,
    pub object: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub enum EventPayload {
    Up(UplinkEvent),
    Status(StatusEvent),
    Join(JoinEvent),
    Ack(AckEvent),
    TxAck(TxAckEvent),
    Log(LogEvent),
    Location(LocationEvent),
    Integration(IntegrationEvent),
}

// ---------------------------------------------------------------------------
// Decoded event — what handlers receive
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DecodedEvent {
    pub topic: String,
    pub application_id: String,
    pub dev_eui: String,
    pub event_type: EventType,
    /// Event `time` field from the payload, if present.
    pub time: Option<String>,
    /// Top-level `deduplicationId`, if present.
    pub deduplication_id: Option<String>,
    /// Uplink FPort (up events only).
    pub f_port: Option<u8>,
    /// Uplink payload, base64 as sent (up events only).
    pub data_b64: Option<String>,
    pub payload: EventPayload,
    /// Full original JSON document.
    pub raw: serde_json::Value,
}

/// Decode a raw MQTT payload for the given topic and event type.
pub fn decode(
    topic: &str,
    event_type: EventType,
    bytes: &[u8],
) -> Result<DecodedEvent, DecodeError> {
    let raw: serde_json::Value = serde_json::from_slice(bytes)?;

    let time = raw
        .get("time")
        .and_then(|v| v.as_str())
        .map(String::from);
    let deduplication_id = raw
        .get("deduplicationId")
        .and_then(|v| v.as_str())
        .map(String::from);

    // Topic already validated by topic::parse; split again just to recover ids.
    let segments: Vec<&str> = topic.split('/').collect();
    let application_id = segments.get(1).copied().unwrap_or("").to_string();
    let dev_eui = segments.get(3).copied().unwrap_or("").to_string();

    let payload = match event_type {
        EventType::Up => EventPayload::Up(serde_json::from_value(raw.clone())?),
        EventType::Status => EventPayload::Status(serde_json::from_value(raw.clone())?),
        EventType::Join => EventPayload::Join(serde_json::from_value(raw.clone())?),
        EventType::Ack => EventPayload::Ack(serde_json::from_value(raw.clone())?),
        EventType::TxAck => EventPayload::TxAck(serde_json::from_value(raw.clone())?),
        EventType::Log => EventPayload::Log(serde_json::from_value(raw.clone())?),
        EventType::Location => EventPayload::Location(serde_json::from_value(raw.clone())?),
        EventType::Integration => EventPayload::Integration(serde_json::from_value(raw.clone())?),
    };

    let (f_port, data_b64) = match &payload {
        EventPayload::Up(up) => (up.f_port, up.data.clone()),
        _ => (None, None),
    };

    Ok(DecodedEvent {
        topic: topic.to_string(),
        application_id,
        dev_eui,
        event_type,
        time,
        deduplication_id,
        f_port,
        data_b64,
        payload,
        raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "application/17c82e96-be03-4f38-aef3-f83d48582d97/device/0101010101010101/event/up";

    #[test]
    fn decodes_up_event_from_docs() {
        let json = r#"{
            "deduplicationId": "3ac7e3c4-4401-4b8d-9386-a5c902f9202d",
            "time": "2022-07-18T09:34:15.775023242+00:00",
            "deviceInfo": {
                "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
                "tenantName": "ChirpStack",
                "applicationId": "17c82e96-be03-4f38-aef3-f83d48582d97",
                "applicationName": "Test application",
                "deviceProfileId": "14855bf7-d10d-4aee-b618-ebfcb64dc7ad",
                "deviceProfileName": "Test device-profile",
                "deviceName": "Test device",
                "devEui": "0101010101010101",
                "tags": { "key": "value" }
            },
            "devAddr": "00189440",
            "dr": 1,
            "fPort": 1,
            "data": "qg==",
            "rxInfo": [{
                "gatewayId": "0016c001f153a14c",
                "uplinkId": 4217106255,
                "rssi": -36,
                "snr": 10.5,
                "context": "E3OWOQ==",
                "metadata": { "region_name": "eu868", "region_common_name": "EU868" }
            }],
            "txInfo": {
                "frequency": 867100000,
                "modulation": { "lora": { "bandwidth": 125000, "spreadingFactor": 11, "codeRate": "CR_4_5" } }
            }
        }"#;

        let ev = decode(TOPIC, EventType::Up, json.as_bytes()).expect("decode");
        assert_eq!(ev.f_port, Some(1));
        assert_eq!(ev.data_b64.as_deref(), Some("qg=="));
        assert_eq!(ev.application_id, "17c82e96-be03-4f38-aef3-f83d48582d97");
        assert_eq!(ev.dev_eui, "0101010101010101");
        match &ev.payload {
            EventPayload::Up(up) => {
                assert_eq!(up.rx_info.len(), 1);
                assert_eq!(up.rx_info[0].rssi, Some(-36));
            }
            other => panic!("expected Up, got {other:?}"),
        }
    }

    #[test]
    fn decodes_log_event_without_dedup() {
        let json = r#"{
            "time": "2022-07-18T09:39:57.311414029+00:00",
            "deviceInfo": { "devEui": "0101010101010101" },
            "level": "ERROR",
            "code": "UPLINK_MIC",
            "description": "MIC of join-request is invalid",
            "context": { "deduplication_id": "4e91db13-5ce3-4c8c-ab0f-dfc626cf0771" }
        }"#;
        let ev = decode(
            "application/a/device/0101010101010101/event/log",
            EventType::Log,
            json.as_bytes(),
        )
        .expect("decode");
        assert!(ev.deduplication_id.is_none());
        match &ev.payload {
            EventPayload::Log(log) => assert_eq!(log.code.as_deref(), Some("UPLINK_MIC")),
            other => panic!("expected Log, got {other:?}"),
        }
    }

    #[test]
    fn unknown_event_type_is_rejected() {
        let err = "nope".parse::<EventType>().unwrap_err();
        assert!(matches!(err, DecodeError::UnknownEventType(_)));
    }

    #[test]
    fn invalid_json_is_rejected() {
        assert!(matches!(
            decode(TOPIC, EventType::Up, b"not json"),
            Err(DecodeError::Json(_))
        ));
    }
}
