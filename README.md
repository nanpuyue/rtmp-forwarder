# rtmp-forwarder

[中文说明](./README.zh-CN.md)

A lightweight RTMP ingest proxy and forwarding service written in Rust.

`rtmp-forwarder` accepts one live RTMP publishing stream, optionally relays to the original destination, and forwards the stream to multiple RTMP upstream targets. It also provides a built-in web dashboard and live playback endpoints.

## Features

- RTMP ingest server (`listen_addr`, default `127.0.0.1:1935`)
- Optional publisher authentication via `server.app` and `server.stream_key`
- Multi-destination RTMP forwarding with per-target enable/disable
- Relay mode implemented as a dedicated forwarder connection (separate RTMP handshake/publish)
- SOCKS5 proxy-mode ingest for stream interception workflows (client can publish to the proxy/listen address)
- Original destination capture from SOCKS5 requests
- Original destination capture on Linux via REDIRECT/DNAT (`SO_ORIGINAL_DST` / `IP6T_SO_ORIGINAL_DST`)
- Built-in web dashboard for runtime config management
- Forwarder targets can be managed directly in the web UI
- Original destination can be viewed in the web UI and added as a forwarder target
- Relay target can reuse the captured original destination
- Optional Basic Auth for dashboard and API
- HTTP-FLV live playback endpoint (`/live/stream.flv`)
- Optional HLS playlist output (`/live/stream.m3u8`)
- Single active publishing stream design (supports one live stream at a time)

## Requirements

- Rust toolchain with Edition 2024 support
- Cargo

## Startup Behavior

- On first startup, the app writes a config file automatically if it does not exist yet.
- Default config path is `config.json`.
- A custom config path can be provided via `-c` / `--config`.

## CLI Options

```text
-l, --listen <ADDR>   RTMP listen address (default: 127.0.0.1:1935)
-d, --dest <URL>      Destination RTMP URL (repeatable)
-r, --relay <ADDR>    Relay host[:port] override
-w, --web <ADDR>      Web dashboard address (default: 127.0.0.1:8080)
-c, --config <FILE>   Config file path (default: config.json)
    --log <LEVEL>     Log level (info, debug, ...)
    --hls             Enable HLS endpoints
```

* CLI arguments have higher priority than values loaded from the config file, and the resolved startup values are saved back to the config file at startup.
* Providing `--dest` will replace the `forwarders` list in the config file at startup.
* HLS playlist endpoint: `GET /live/stream.m3u8` (available when `--hls` is enabled).

## Configuration

Default config file: `config.json` (or custom path from `-c/--config`).

### Not Configurable from Web UI

- `server.listen_addr`: RTMP ingest listen address.
- `server.app`: optional. If set, only publishers with matching RTMP app are accepted.
- `server.stream_key`: optional. If set, only publishers with matching stream key are accepted.
- `web.addr`: web dashboard listen address.
- `web.username` / `web.password`: enable optional Basic Auth when either value is set.
- `log_level`: default logging filter.

### Configurable from Web UI

- `forwarders`
- `relay_addr`
- `relay_enabled`

Relay behavior: same forwarding mechanism as normal forwarders (independent outgoing RTMP session). By default it derives destination/app/stream context from the current publisher session.

## Original Destination & Relay

- In SOCKS5 proxy mode, publishers can use the service address as the proxy endpoint and the service extracts the original RTMP destination from SOCKS5 CONNECT data.
- On Linux, the service can also read original destination from REDIRECT/DNAT traffic (`SO_ORIGINAL_DST` / `IP6T_SO_ORIGINAL_DST`).
- Current original destination is exposed in the web UI and can be added directly as a forwarder target.
- Relay mode is not a transparent pass-through tunnel; it creates its own outgoing RTMP connection (same as a forwarder).
- In relay mode, destination can come from captured original address (or `relay_addr` override), while publish context is derived from the current publisher stream metadata.

## Logging

- Config file: `log_level`
- CLI override: `--log`
- Environment override: `RUST_LOG`

`RUST_LOG` has highest priority.

## Security Notes

- Do not commit production stream keys, relay targets, or credentials.
- HTTP Basic Auth is supported for the web dashboard/API, but Basic Auth alone is weak over plaintext transport.
- If external access is required, run this service behind TLS (for example via a reverse proxy) and proper network controls.
- This codebase has not been security audited. Strongly prefer deploying only in trusted environments (LAN/internal network or behind a firewall).

## License

This project is licensed under the Apache License 2.0. See `LICENSE` for details.

## AI Disclaimer

Parts of this project were generated or assisted by AI tools. The code may contain defects, security issues, or incomplete logic. You are responsible for reviewing, testing, and validating suitability for your environment before use. The authors and contributors provide this project \"as is\" and disclaim liability for any direct or indirect damages resulting from its use.
