//! MQTT topic parsing.
//!
//! ChirpStack publishes device events on:
//! `application/{APPLICATION_ID}/device/{DEV_EUI}/event/{EVENT}`.
//! The event type is only available from the topic, not the JSON body.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTopic {
    pub application_id: String,
    pub dev_eui: String,
    pub event_type: String,
}

#[derive(Debug, Error)]
pub enum TopicError {
    #[error("topic does not have the expected `application/…/device/…/event/…` shape: `{0}`")]
    UnexpectedShape(String),
    #[error("topic segment is empty: `{0}`")]
    EmptySegment(String),
}

/// Parse a ChirpStack event topic into its parts.
///
/// Expected shape: `application/{APPLICATION_ID}/device/{DEV_EUI}/event/{EVENT}`
/// where `{EVENT}` is one of the documented event types (or a wildcard, which
/// callers should handle before decoding).
pub fn parse(topic: &str) -> Result<ParsedTopic, TopicError> {
    let segments: Vec<&str> = topic.split('/').collect();

    // application / APPLICATION_ID / device / DEV_EUI / event / EVENT
    if segments.len() != 6
        || segments[0] != "application"
        || segments[2] != "device"
        || segments[4] != "event"
    {
        return Err(TopicError::UnexpectedShape(topic.to_string()));
    }

    let application_id = segments[1];
    let dev_eui = segments[3];
    let event_type = segments[5];

    if application_id.is_empty() || application_id == "+" || application_id == "#" {
        return Err(TopicError::EmptySegment(topic.to_string()));
    }
    if dev_eui.is_empty() || dev_eui == "+" || dev_eui == "#" {
        return Err(TopicError::EmptySegment(topic.to_string()));
    }
    if event_type.is_empty() || event_type == "+" || event_type == "#" {
        return Err(TopicError::EmptySegment(topic.to_string()));
    }

    Ok(ParsedTopic {
        application_id: application_id.to_string(),
        dev_eui: dev_eui.to_string(),
        event_type: event_type.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_topic() {
        let t = parse("application/17c82e96-be03-4f38-aef3-f83d48582d97/device/0101010101010101/event/up")
            .expect("valid topic");
        assert_eq!(t.application_id, "17c82e96-be03-4f38-aef3-f83d48582d97");
        assert_eq!(t.dev_eui, "0101010101010101");
        assert_eq!(t.event_type, "up");
    }

    #[test]
    fn parses_all_event_types() {
        for ev in ["up", "status", "join", "ack", "txack", "log", "location", "integration"] {
            let topic = format!("application/app-1/device/dev-1/event/{ev}");
            assert_eq!(parse(&topic).unwrap().event_type, ev);
        }
    }

    #[test]
    fn rejects_wrong_shape() {
        assert!(matches!(
            parse("application/app-1/device/dev-1/up"),
            Err(TopicError::UnexpectedShape(_))
        ));
        assert!(matches!(
            parse("other/app-1/device/dev-1/event/up"),
            Err(TopicError::UnexpectedShape(_))
        ));
        assert!(matches!(
            parse("application/app-1/device/dev-1/event/up/extra"),
            Err(TopicError::UnexpectedShape(_))
        ));
    }

    #[test]
    fn rejects_wildcards_in_ids() {
        assert!(matches!(
            parse("application/+/device/dev-1/event/up"),
            Err(TopicError::EmptySegment(_))
        ));
        assert!(matches!(
            parse("application/app-1/device/#/event/up"),
            Err(TopicError::EmptySegment(_))
        ));
    }

    #[test]
    fn topics_are_case_sensitive() {
        assert!(matches!(
            parse("Application/app-1/device/dev-1/event/up"),
            Err(TopicError::UnexpectedShape(_))
        ));
    }
}
