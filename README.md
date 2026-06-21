# Murmur

**Peer-to-peer swarm coordination for resilient file distribution.**

Murmur is a runtime for coordinating large file transfers across a swarm of
peers. It handles automatic discovery, cryptographic verification, intelligent
scheduling, and multi-link bonding — so you ship data fast without a central
server.

Built in Rust. No runtime dependencies. Embeddable anywhere.

## Features

- **mDNS peer discovery** — zero-configuration LAN detection via multicast DNS
- **BLAKE3 chunk verification** — every chunk is integrity-checked on receive
- **Bandwidth-weighted scheduling** — faster links carry proportionally more data
- **Leader election** — lightweight coordinator election with epoch management
- **Multi-WAN bonding** — aggregate bandwidth across multiple network interfaces
- **Overlay State Table** — consistent distributed view of cluster topology
- **Protobuf wire format** — compact, versioned, language-neutral protocol
- **Embeddable API** — FFI surface for building iOS, Android, WASM, and desktop SDKs

## Architecture

Murmur is organized as a Cargo workspace of focused crates:

```
                         ┌──────────────┐
                         │  murmur-cli  │
                         └──────┬───────┘
                                │
                         ┌──────┴───────┐
                         │ murmur-daemon │
                         └──────┬───────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
       ┌──────┴───────┐ ┌──────┴───────┐ ┌───────┴──────┐
       │  murmur-api  │ │murmur-coordi-│ │murmur-schedu-│
       │              │ │    nator     │ │     ler      │
       └──────┬───────┘ └──────┬───────┘ └───────┬──────┘
              │                │                 │
              │         ┌──────┴───────┐         │
              │         │murmur-overlay│         │
              │         └──────┬───────┘         │
              │                │                 │
       ┌──────┴────────────────┼─────────────────┴──────┐
       │                       │                        │
┌──────┴───────┐ ┌─────────────┴──┐ ┌───────────────────┴┐
│ murmur-net   │ │ murmur-storage │ │   murmur-proto     │
└──────┬───────┘ └───────┬────────┘ └────────┬───────────┘
       │                 │                   │
       └─────────────────┼───────────────────┘
                         │
                  ┌──────┴───────┐
                  │  murmur-core │
                  └──────────────┘
```

| Crate | Purpose |
|---|---|
| `murmur-core` | Shared types: `NodeId`, `ChunkId`, `Manifest`, `Task`, `Link`, `Config` |
| `murmur-net` | mDNS discovery and TCP transport |
| `murmur-storage` | Chunk I/O, manifest persistence, BLAKE3 verification |
| `murmur-proto` | Protobuf/gRPC message definitions |
| `murmur-overlay` | Overlay State Table and topology management |
| `murmur-coordinator` | Leader election, epoch management, cluster lifecycle |
| `murmur-scheduler` | Chunk scheduling with bandwidth-weighted assignment |
| `murmur-api` | Public FFI and event API for embedders |
| `murmur-daemon` | Composable node runtime |
| `murmur-cli` | Reference command-line interface |

## Quick Start

### Pre-built Binaries

Download the latest release for your platform from
[GitHub Releases](https://github.com/drasta-one/murmur/releases).

Extract and run:

```sh
tar xzf murmur-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
cd murmur-v0.1.0-x86_64-unknown-linux-gnu

# Start the daemon
./murmur-daemon --rpc-port 9090
```

### Using the CLI

```sh
# Check node status
murmur-cli status

# Seed a file into the swarm
murmur-cli seed ./dataset.tar.zst

# List known manifests
murmur-cli list

# Download a file by manifest ID
murmur-cli get <manifest-id> ./output-file

# Bonded download — aggregate bandwidth from all peers
murmur-cli bonded-fetch https://example.com/large-file.bin ./output.bin

# Stop the daemon
murmur-cli stop
```

## Building Platform Bindings

`murmur-api` exposes a C-compatible FFI surface and an event stream suitable
for building native SDKs. Embedders can target any platform Rust compiles to:

| Target | Approach |
|---|---|
| **iOS / macOS** | Build `murmur-api` as a static library (`staticlib`), generate a C header with `cbindgen`, consume from Swift via a bridging header. |
| **Android** | Build as a shared library (`cdylib`) for `aarch64-linux-android` / `armv7-linux-androideabi`, call via JNI or use `uniffi` to generate Kotlin bindings. |
| **WASM** | Compile with `wasm32-unknown-unknown` target, expose bindings via `wasm-bindgen`. Transport must be adapted to WebRTC or WebSocket. |
| **Desktop (C/C++)** | Link the static or shared library directly; use the generated C header. |
| **Flutter / React Native** | Use the platform-appropriate approach above, wrapped in a Dart FFI plugin or a Native Module. |

Example cross-compilation for Android:

```sh
rustup target add aarch64-linux-android
cargo build -p murmur-api --target aarch64-linux-android --release
```

## Platform Support

| Platform | Status |
|---|---|
| Linux (x86_64, aarch64) | Supported |
| macOS (x86_64, aarch64) | Supported |
| Windows (x86_64) | Supported |
| iOS | Via `murmur-api` FFI |
| Android | Via `murmur-api` FFI |
| WASM | Experimental |

## Building from Source

```sh
git clone https://github.com/drasta-one/murmur.git
cd murmur
cargo build --release
```

Requires Rust (latest stable) and a protobuf compiler (`protoc`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions, coding
standards, and PR guidelines.

## License

Licensed under either of

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
