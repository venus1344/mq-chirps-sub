# Security Review — ChirpStack MQTT Subscriber

- **Date:** 2026-11-02
- **Scope:** Full source review of the Rust MQTT subscriber + downlink sender
  in this repository (`src/`, `.env.example`, `.gitignore`), plus an assessment
  of on-disk secret/key material (`certs/`, `.env`, `events.db`).
- **Method:** Read-only review by the `security-review` agent; findings verified
  against source and file-system evidence.

## Summary

One MEDIUM-severity issue was found: the mTLS client private key and other
sensitive files are stored with world-readable permissions. No code-level,
exploitable vulnerabilities (SQL injection, TLS verification bypass, command
injection, path traversal, unsafe deserialization) were identified.

## Findings

| # | Severity | File | Lines | Vulnerability | Confidence |
|---|----------|------|-------|---------------|------------|
| 1 | 🟡 MEDIUM | `certs/tls.key` (also `.env`, `events.db`) | — | mTLS client private key is world-readable (0644), allowing any local user to impersonate the client to the MQTT broker; `.env` (broker credentials) and `events.db` (decoded event data) are likewise 0644 | 9/10 |

### Finding 1 — World-readable private key and secret files

`certs/tls.key` has permissions `0644` (`-rw-r--r--`), meaning any local user
or process on the host can read the private key and impersonate this client to
the ChirpStack MQTT broker — e.g. subscribe to the device event stream or
publish forged `command/down` messages (subject only to the broker's ACLs).

The key is read at runtime by `build_tls_config` in `src/client.rs`
(`std::fs::read(key_path)` → `pemfile::private_key(...)` →
`with_client_auth_cert`). The code performs no permission check on the file.

The same concern applies to:

- `.env` — holds `MQ_USERNAME`/`MQ_PASSWORD` broker credentials (0644).
- `events.db` — SQLite database containing decoded device event data (0644).

Version control is **not** the exposure here: `.gitignore` correctly excludes
`.env` and `/certs`. The exposure is local, on-disk readability by other users.

#### Remediation

Restrict permissions on the sensitive files:

```bash
chmod 600 certs/tls.key .env events.db
```

Optionally, have the program verify key-file permissions at startup and warn or
refuse when the client key is group- or world-readable (a future hardening
step).

## Verified clean

The following areas were checked and found to be free of exploitable issues:

| Area | Result |
|------|--------|
| SQL injection | Safe — all SQLite writes use `sqlx` bound parameters (`?`), never string interpolation. |
| TLS verification | Safe — `build_tls_config` uses rustls' default `WebPkiServerVerifier`; hostname verification is enforced against `MQ_BROKER_HOST`; no `dangerous()` or custom verifier is used. `MQ_BROKER_TLS` defaults to `true` (cleartext is an explicit, documented opt-in). |
| Secrets in logs | Safe — credentials/keys are never logged; tracing fields expose only topic, DevEUI, event metadata. |
| Command/path injection | Safe — no shell execution; SQLite/TLS file paths come only from local configuration, not network input. |
| Untrusted JSON parsing | Safe — `serde_json` enforces a recursion limit; malformed payloads are logged and skipped without crashing. |
| Topic parsing | Safe — bounded, split-based parsing with explicit shape validation and wildcard rejection in concrete topics. |
| Downlink input validation | Safe — DevEUI (16 hex), FPort (> 0), and hex/base64 payloads are validated before any network I/O. |
| Dependencies | Clean — all pinned versions in `Cargo.lock` are at or above the patched versions for every applicable RustSec advisory (rustls 0.23.46, ring 0.17.14, tokio 1.54.1, sqlx 0.9.0, rumqttc 0.25.1, clap 4.6.7). |

## Residual risks / hardening recommendations (non-blocking)

- **Local trust assumption:** the `downlink` CLI is gated only by OS-level
  access to the machine and the broker's credentials/ACLs. If multiple
  untrusted users share the host, consider moving credentials to a
  more-restricted location (e.g. `0600` files owned by a dedicated service
  account) or adding broker-side ACLs.
- **Unbounded persistence:** `events.db` grows without limit. Add a retention
  policy or rotation if the subscriber runs long-term.
- **No rate limiting/backpressure beyond MQTT QoS:** extremely high message
  rates could exhaust storage. Monitor and, if needed, add backpressure.
