# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-06-21

Initial public release of Murmur — a peer-to-peer swarm coordination runtime.

### Added

- **murmur-core**: Foundational types — `NodeId`, `ManifestId`, `ChunkId`, `Node`, `Manifest`, `ChunkMeta`, `DorEvent`, `TaskKind`, cluster configuration, and error types.
- **murmur-net**: QUIC-based peer transport with TLS, mDNS zero-configuration discovery, and `PeerConnection` abstraction.
- **murmur-storage**: Chunk-level filesystem storage with `ChunkStore` (write, read, verify) and `ManifestStore` for manifest persistence.
- **murmur-overlay**: `OverlayStateTable` — distributed cluster state tracking nodes, coordinator, election terms, and chunk ownership.
- **murmur-coordinator**: Leader election with epoch management and `CoordinatorLifecycle` history tracking.
- **murmur-scheduler**: Bandwidth-weighted chunk scheduling via `dispense_batch()` with sliding-window assignment.
- **murmur-proto**: Protobuf wire format for gRPC control plane (`MurmurControl` service).
- **murmur-api**: Embeddable runtime API (`DorRuntime`) with command/event channels — the integration surface for building platform SDKs.
- **murmur-daemon**: Full daemon binary with P2P mesh, gRPC control server, SOCKS5 proxy, bonded download orchestration, and WAN fetch workers.
- **murmur-cli**: Command-line client for interacting with the daemon — `status`, `seed`, `list`, `get`, `bonded-fetch`, `stop`.
- Multi-WAN bonding: aggregate bandwidth across multiple network interfaces for accelerated downloads.
- BLAKE3 chunk verification on every receive.
- HTTP Range request aggregation for efficient bonded fetches.
- CI with `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, and release builds.

### Infrastructure

- Dual license: MIT OR Apache-2.0.
- GitHub Actions CI on stable Rust.
- Cross-platform release workflow (Linux x86_64/aarch64, macOS x86_64/aarch64).
- Architecture documentation (`ARCHITECTURE.md`) and contributor guide (`CONTRIBUTING.md`).

[0.1.0]: https://github.com/drasta-one/murmur/releases/tag/v0.1.0
