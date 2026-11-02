# Runbook — ChirpStack MQTT Subscriber

How to build, configure, and run the `mq-subscriber` service.

## Prerequisites

- Rust toolchain (stable, edition 2024): <https://rustup.rs/>
- Access to a ChirpStack instance with the MQTT integration enabled
  ([docs](https://www.chirpstack.io/docs/chirpstack/integrations/mqtt.html))
- Optional, for a local end-to-end test: Docker (Mosquitto) or `mosquitto_pub`

## Build

```bash
cargo build --release
```

The binary is `target/release/mq-subscriber`.

## Configure

All settings come from environment variables. Copy the example and edit it:

```bash
cp .env.example .env
# edit .env
```

The service loads `.env` automatically in development (via `dotenvy`); in
production, export the same variables in the process environment. See
[.env.example](../.env.example) for every variable and its default. The ones
you will most likely change:

| Variable | Meaning |
|----------|---------|
| `MQ_BROKER_HOST` / `MQ_BROKER_PORT` | ChirpStack broker address (typically `8883` with TLS) |
| `MQ_BROKER_TLS` | `true` to use TLS |
| `MQ_CA_CERT` | Path to the broker CA cert; omit to use the system root store |
| `MQ_USERNAME` / `MQ_PASSWORD` | Broker credentials (required by ChirpStack's default broker) |
| `MQ_TOPIC` / `MQ_TOPICS` | Subscription filter(s). `MQ_TOPICS` is a comma-separated list and takes precedence; `MQ_TOPIC` is a single filter. Default `application/+/device/+/event/#` |
| `MQ_SQLITE_PATH` | SQLite file for the persistence handler |
| `RUST_LOG` | Log level, e.g. `mq_subscriber=debug` |

To receive only uplinks for one application:

```bash
MQ_TOPIC="application/APPLICATION_ID/device/+/event/up"
```

(`APPLICATION_ID` is the application UUID, **not** the AppEUI/JoinEUI.)

To listen to several specific applications, list their filters in `MQ_TOPICS`
(comma-separated). Each filter matches all devices and event types of that
application:

```bash
MQ_TOPICS="application/2ad6bbd4-7ee9-408f-8948-3d32cf3dba06/device/+/event/#,application/9f0e1234-abcd-.../device/+/event/#"
```

To listen to *all* applications, use a single `+` wildcard for the application
level:

```bash
MQ_TOPICS="application/+/device/+/event/#"
```

## Run

```bash
./target/release/mq-subscriber
```

Stop it with `Ctrl-C` (`SIGINT`) or `SIGTERM`; both trigger a graceful
disconnect.

## What it does

1. Connects to the broker (TLS + optional client cert auth) and subscribes to
   `MQ_TOPIC`.
2. Re-subscribes automatically after every reconnect (on each `ConnAck`).
3. Parses each incoming topic into `APPLICATION_ID`, `DEV_EUI`, and event type.
4. Decodes the JSON payload into typed structs for the 8 event types:
   `up`, `status`, `join`, `ack`, `txack`, `log`, `location`, `integration`.
5. Dispatches each event to the enabled handlers:
   - **stdout** — logs a structured summary.
   - **sqlite** — inserts a row into the `events` table.

Malformed topics or undecodable payloads are logged and skipped; they never
crash the process.

## Sending a downlink

The same binary can publish a downlink command to ChirpStack and exit. The
downlink is published to
`application/{APPLICATION_ID}/device/{DEV_EUI}/command/down` as JSON, and the
command waits for the broker's `PUBACK` before exiting.

```bash
# Payload as base64 (matches ChirpStack's `data` field)
mq-subscriber downlink \
  --application-id 2ad6bbd4-7ee9-408f-8948-3d32cf3dba06 \
  --dev-eui 0102030405060708 \
  --fport 10 \
  --data "AQID"

# Same payload, written as hex (converted to base64 for you)
mq-subscriber downlink \
  --dev-eui 0102030405060708 \
  --fport 10 \
  --data-hex "010203" \
  --confirmed
```

- `--application-id` defaults to the `MQ_APPLICATION_ID` environment variable.
- `--data` (base64) and `--data-hex` are mutually exclusive; provide exactly one.
- `--confirmed` sends a confirmed downlink; its `ack`/`txack` events arrive on
  the regular event stream when the subscriber is running.
- `fPort` must be > 0; `--dev-eui` must be 16 hex characters.

## SQLite schema

`events` — one row per device event, with the full JSON in `payload_json` plus
extracted columns (`application_id`, `dev_eui`, `event_type`, `event_time`,
`deduplication_id`, `f_port`, `data_b64`) and indexes on
`(dev_eui, event_time)` and `(event_type, event_time)`.

Inspect with:

```bash
sqlite3 events.db "SELECT event_type, dev_eui, event_time FROM events ORDER BY id DESC LIMIT 10;"
```

## Local end-to-end test with Mosquitto

Requires Docker (or a local `mosquitto` install).

```bash
# 1. Start a broker
docker run -d --rm --name mq-broker -p 1883:1883 eclipse-mosquitto

# 2. Run the subscriber (plain TCP against the local broker)
MQ_BROKER_HOST=127.0.0.1 MQ_BROKER_PORT=1883 MQ_BROKER_TLS=false \
MQ_SQLITE_PATH=events.db RUST_LOG=mq_subscriber=info \
  ./target/release/mq-subscriber

# 3. In another terminal, publish a sample uplink event
docker exec mq-broker mosquitto_pub \
  -t "application/app-123/device/0101010101010101/event/up" \
  -m '{"deduplicationId":"3ac7e3c4-4401-4b8d-9386-a5c902f9202d","time":"2022-07-18T09:34:15.775023242+00:00","deviceInfo":{"devEui":"0101010101010101"},"devAddr":"00189440","fPort":1,"data":"qg=="}'

# 4. The subscriber logs the event and persists it to events.db
```

## Tests

```bash
cargo test        # unit + integration (17 tests)
cargo clippy --all-targets
```
