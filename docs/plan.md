# ChirpStack MQTT Subscriber — Implementation Plan

A Rust service that subscribes to a [ChirpStack](https://www.chirpstack.io/docs/chirpstack/integrations/mqtt.html)
MQTT integration, decodes the JSON event payloads, and fans them out to pluggable
handlers (stdout logging and SQLite persistence in the first version).

## 1. Goal & Scope

**In scope (v1)**

- Connect to a ChirpStack MQTT broker (optionally over TLS, with username/password auth).
- Subscribe to the device event topic: `application/{APPLICATION_ID}/device/{DEV_EUI}/event/{EVENT}`.
- Automatically re-subscribe after reconnect / connection loss.
- Parse the topic to recover `APPLICATION_ID`, `DEV_EUI`, and event type.
- Decode the JSON payload into typed Rust structs (JSON encoding only).
- Dispatch decoded events to a pluggable set of handlers:
  - **stdout handler** — log a compact, structured summary of each event.
  - **sqlite handler** — persist each event to a local SQLite database.
- Graceful shutdown on `SIGINT` / `SIGTERM`.

**Out of scope (future extensions)**

- Protobuf binary encoding.
- HTTP forwarding, other persistence backends (Postgres, ClickHouse, …).
- Metrics / health endpoints.

## 2. Protocol Facts (from ChirpStack docs)

These facts drive the design and were taken from the official docs on
[2026-11-02](https://www.chirpstack.io/docs/chirpstack/integrations/mqtt.html).

- **Upstream event topic:** `application/APPLICATION_ID/device/DEV_EUI/event/EVENT`
  - `APPLICATION_ID` is the application UUID (from the API/web-interface), **not** AppEUI/JoinEUI.
  - Topics are case-sensitive.
  - `+` matches one topic level; `#` matches multiple levels.
- **Event types:** `up`, `status`, `join`, `ack`, `txack`, `log`, `location`, `integration`.
- **Encoding:** JSON (Protocol Buffers JSON mapping) or raw Protobuf. **We use JSON only.**
- **Downlink topic**: `application/APPLICATION_ID/device/DEV_EUI/command/down`
  (implemented via the `downlink` subcommand, see the runbook).

### Event payload shapes

All device events carry a shared `deviceInfo` block:

```jsonc
{
  "deviceInfo": {
    "tenantId": "…", "tenantName": "…",
    "applicationId": "…", "applicationName": "…",
    "deviceProfileId": "…", "deviceProfileName": "…",
    "deviceName": "…", "devEui": "0101010101010101",
    "tags": { "key": "value" }
  },
  "time": "2022-07-18T09:34:15.775023242+00:00"
}
```

Per-type extra fields (used for typed models, see §5):

| Event | Extra fields |
|-------|--------------|
| `up` | `deduplicationId`, `devAddr`, `dr`, `fPort`, `data` (base64), `rxInfo[]`, `txInfo` |
| `status` | `deduplicationId`, `margin`, `batteryLevel` |
| `join` | `deduplicationId`, `devAddr` |
| `ack` | `deduplicationId`, `queueItemId`, `acknowledged`, `fCntDown` |
| `txack` | `downlinkId`, `queueItemId`, `fCntDown`, `gatewayId`, `txInfo` |
| `log` | `level`, `code`, `description`, `context` |
| `location` | `deduplicationId`, `location` { `latitude`, `longitude`, `altitude` } |
| `integration` | `deduplicationId`, `integrationName`, `eventType`, `object` |

The **event type is not encoded in the payload** — it comes from the topic's
`EVENT` segment. We therefore decide the target struct from the topic, not from
the JSON body.

## 3. Technology Choices

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Language / runtime | Rust, `tokio` (full) | Async I/O for MQTT + SQLite + shutdown |
| MQTT client | `rumqttc` | Pure Rust, async API, TLS support (rustls/native-tls), active maintenance |
| JSON | `serde` + `serde_json` | Standard, typed deserialization |
| Logging | `tracing` + `tracing-subscriber` | Structured, filterable via `RUST_LOG`, goes to stdout |
| SQLite | `sqlx` (sqlite, `runtime-tokio`) | Async pool, compile-time query checking optional |
| Errors | `thiserror` | Ergonomic typed errors |
| Config | Environment variables (`std::env` + optional `dotenvy` for local `.env`) | Chosen by user; 12-factor friendly |
| TLS | `rumqttc` with `use-rustls` | TLS with optional CA cert, client cert/key |
| Misc | `base64`, `chrono` | Decode payload `data`; normalize timestamps for storage |

Dependency sketch (exact versions resolved at `cargo add` time):

```text
tokio         = { version = "1", features = ["full"] }
rumqttc       = { version = "0.24", features = ["use-rustls"] }
serde         = { version = "1", features = ["derive"] }
serde_json    = "1"
thiserror     = "2"
tracing       = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
sqlx          = { version = "0.8", features = ["runtime-tokio", "sqlite"] }
base64        = "0.22"
chrono        = { version = "0.4", features = ["serde"] }
dotenvy       = "0.15"
```

## 4. Configuration (environment variables)

All configuration is read from the environment. A `.env` file is supported in
development via `dotenvy` (not committed).

| Variable | Default | Description |
|----------|---------|-------------|
| `MQ_BROKER_HOST` | `localhost` | Broker hostname/IP |
| `MQ_BROKER_PORT` | `1883` | Broker port (typically `8883` for TLS) |
| `MQ_BROKER_TLS` | `true` | Enable TLS |
| `MQ_CA_CERT` | — | Path to CA certificate (PEM). Optional: use system roots when unset |
| `MQ_CLIENT_CERT` | — | Path to client certificate (PEM). Optional (mTLS) |
| `MQ_CLIENT_KEY` | — | Path to client private key (PEM). Optional (mTLS) |
| `MQ_USERNAME` | — | Broker username (optional) |
| `MQ_PASSWORD` | — | Broker password (optional) |
| `MQ_CLIENT_ID` | `mq-subscriber-<rand>` | MQTT client id |
| `MQ_TOPICS` | — | Comma-separated subscription filters; takes precedence over `MQ_TOPIC` |
| `MQ_TOPIC` | `application/+/device/+/event/#` | Single subscription filter (fallback when `MQ_TOPICS` unset) |
| `MQ_QOS` | `0` | Subscription QoS |
| `MQ_APPLICATION_ID` | — | Application ID used as the default for the `downlink` subcommand |
| `MQ_SQLITE_PATH` | `events.db` | SQLite file for the sqlite handler |
| `MQ_ENABLE_STDOUT` | `true` | Enable stdout handler |
| `MQ_ENABLE_SQLITE` | `true` | Enable sqlite handler |
| `RUST_LOG` | `mq_subscriber=info` | tracing filter |

## 5. Module Layout

```
mq/                        (project root, binary crate `mq-subscriber`)
├── Cargo.toml
├── .env.example           # documented env vars (no secrets)
├── docs/                  # all project documents live here
│   └── plan.md
└── src/
    ├── main.rs            # clap CLI: `subscribe` (default) + `downlink` subcommands
    ├── config.rs          # Config + from_env() with validation
    ├── client.rs          # MQTT connect, subscribe, event loop, reconnect
    ├── downlink.rs        # DownlinkRequest + publish (waits for PubAck)
    ├── topic.rs           # parse topic → ParsedTopic { application_id, dev_eui, event_type }
    ├── events.rs          # EventType enum + typed event structs + DecodedEvent
    ├── handler.rs         # Handler trait + registry/dispatch
    ├── handlers/
    │   ├── mod.rs
    │   ├── stdout.rs      # StdoutHandler (tracing-based)
    │   └── sqlite.rs      # SqliteHandler (sqlx pool)
    └── db.rs              # sqlite pool init + schema migration
```

### Key types

```rust
// events.rs
pub enum EventType { Up, Status, Join, Ack, TxAck, Log, Location, Integration }

pub struct DecodedEvent {
    pub application_id: String,   // from topic
    pub dev_eui: String,          // from topic
    pub event_type: EventType,    // from topic
    pub payload: EventPayload,    // typed union below
    pub raw: serde_json::Value,   // original JSON (for sqlite, debugging)
}

pub enum EventPayload {
    Up(UplinkEvent), Status(StatusEvent), Join(JoinEvent), Ack(AckEvent),
    TxAck(TxAckEvent), Log(LogEvent), Location(LocationEvent), Integration(IntegrationEvent),
}
```

```rust
// handler.rs — async trait, dispatched in parallel for a single event
#[async_trait::async_trait]
pub trait Handler: Send + Sync {
    fn name(&self) -> &'static str;
    async fn handle(&self, event: &DecodedEvent) -> Result<(), HandlerError>;
}
```

The registry runs every enabled handler for each event (logging errors per
handler without dropping the message). `async_trait` is pulled in transitively
by `sqlx` but is listed explicitly.

## 6. MQTT Lifecycle & Reconnection

1. Build `MqttOptions` from config: host/port, client id, clean session,
   credentials, TLS (CA cert and/or client cert/key when provided).
2. Create `(AsyncClient, EventLoop)` and subscribe to `MQ_TOPIC` at `MQ_QOS`.
3. Loop over `EventLoop::poll()`:
   - `Event::Incoming(Packet::ConnAck)` → (re)subscribe. This covers both the
     initial connect and automatic reconnects, because `rumqttc`'s internal
     thread re-establishes the TCP/TLS session and emits a fresh `ConnAck`.
   - `Event::Incoming(Packet::Publish(p))` → handle message (§7).
   - `Event::Incoming(Packet::Disconnect)` / poll error → log and continue
     polling; the background thread retries with backoff.
4. Shutdown: on `ctrl_c()` / `SIGTERM`, disconnect the client and drain handlers.

## 7. Message Handling Flow

```
MQTT Publish
  → topic::parse(topic)                      // application_id, dev_eui, EventType
  → serde_json::from_slice::<…>(payload)     // into typed struct by EventType
  → DecodedEvent { application_id, dev_eui, event_type, payload, raw }
  → handler::dispatch(event)                 // fan out to all enabled handlers
       ├── StdoutHandler  → tracing::info!(…) structured summary
       └── SqliteHandler  → INSERT INTO events …
```

Unknown/undecodable payloads are logged as errors with the raw bytes but do not
crash the loop (per-message isolation). Topic parsing failures are likewise
logged and skipped.

## 8. SQLite Schema

```sql
CREATE TABLE IF NOT EXISTS events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at       TEXT NOT NULL,           -- UTC, when we stored it
    event_time        TEXT,                    -- event "time" field from payload
    application_id    TEXT NOT NULL,
    dev_eui           TEXT NOT NULL,
    event_type        TEXT NOT NULL,           -- up|status|join|ack|txack|log|location|integration
    deduplication_id  TEXT,
    f_port            INTEGER,                 -- uplink only
    data_b64          TEXT,                    -- uplink payload, base64 as sent
    topic             TEXT NOT NULL,
    payload_json      TEXT NOT NULL            -- full original JSON
);

CREATE INDEX IF NOT EXISTS idx_events_device_time
    ON events (dev_eui, event_time);
CREATE INDEX IF NOT EXISTS idx_events_type_time
    ON events (event_type, event_time);
```

- `received_at` is generated at insert time; `event_time` is parsed from the payload.
- `deduplication_id` is stored for all events that carry it. V1 keeps an index
  only; enforcing uniqueness (and thus idempotent replay) is a follow-up because
  some events (e.g. `log`) report the dedup id in `context.deduplication_id`
  rather than a top-level field.
- Schema is applied idempotently at startup with `CREATE TABLE IF NOT EXISTS`.

## 9. Error Handling

- `config::ConfigError` — missing/invalid env values (fail fast at startup).
- `client::ClientError` — connect/auth/TLS failures; retried by the MQTT loop.
- `events::DecodeError` — JSON deserialization failure; logged, message skipped.
- `handler::HandlerError` — per-handler failure; logged, other handlers still run.
- The process exits non-zero only on unrecoverable startup errors (config, TLS
  material, sqlite open/migration). Runtime message errors never kill the process.

## 10. Testing Strategy

1. **Unit — topic parsing:** all eight event topics plus malformed/extra-level
   topics and case-sensitivity.
2. **Unit — event decoding:** fixtures copied from the ChirpStack docs (one JSON
   per event type) deserialize into the typed structs; round-trip `raw`.
3. **Unit — config:** env var parsing, defaults, invalid TLS combinations.
4. **Unit — sqlite handler:** in-memory `sqlite::memory:` DB, insert a decoded
   event, assert row contents.
5. **Integration (optional, local):** `docker run -p 1883:1883 eclipse-mosquitto`,
   publish sample events with `mosquitto_pub`, assert the subscriber logs/persists
   them. Documented as a manual step rather than CI in v1.

## 11. Milestones

| # | Milestone | Deliverable / exit criteria |
|---|-----------|------------------------------|
| M1 | Scaffold | `cargo new` binary, `Cargo.toml` deps, `src/main.rs` prints config; `.env.example` |
| M2 | Config | `config.rs` reads all env vars, validates, unit tests |
| M3 | MQTT connect | `client.rs` connects (TLS + auth), subscribes, re-subscribes on `ConnAck`, logs raw topics |
| M4 | Typed decode | `topic.rs`, `events.rs`, decode all 8 event types from doc fixtures |
| M5 | Handlers | `handler.rs` registry + stdout handler |
| M6 | Persistence | `db.rs` + sqlite handler, schema migration, unit tests |
| M7 | Hardening & docs | graceful shutdown, error isolation, README + run instructions |

Each milestone is independently buildable (`cargo build` / `cargo test` green).

## 12. Future Work

- Protobuf encoding behind a feature flag.
- Multiple topics / topic filter list.
- Deduplication (unique constraint on `deduplication_id` + `application_id`).
- More handlers (HTTP webhook, Postgres, ClickHouse) behind a feature registry.
- Metrics (Prometheus) and a `/healthz` endpoint.
