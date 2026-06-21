# Contributing to Murmur

Thanks for your interest in contributing. This document covers everything you
need to get started.

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Rust | stable (latest) | Install via [rustup](https://rustup.rs/) |
| `protoc` | 3.x+ | Protobuf compiler, required by `murmur-proto` |
| Git | any | |

On Arch-based systems:

```sh
sudo pacman -S protobuf
rustup toolchain install stable
```

On Debian/Ubuntu:

```sh
sudo apt install protobuf-compiler
```

On macOS:

```sh
brew install protobuf
```

## Building

```sh
cargo build
```

To build in release mode:

```sh
cargo build --release
```

## Testing

Run the full test suite:

```sh
cargo test
```

Run tests for a specific crate:

```sh
cargo test -p murmur-scheduler
```

## Linting

All PRs must pass formatting and lint checks:

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
```

Fix formatting automatically:

```sh
cargo fmt --all
```

## Adding a New Transport Backend

Murmur's networking layer is defined in `murmur-net`. To add a new transport
(e.g., QUIC, WebSocket, Bluetooth):

1. **Define the transport trait implementation.** Look at the existing TCP
   transport in `murmur-net/src/transport/` for the interface contract. Your
   backend must implement connection establishment, framed message sending,
   and framed message receiving.

2. **Register the backend.** Add your transport as a feature-gated module in
   `murmur-net/Cargo.toml` and wire it into the transport factory.

3. **Update discovery if needed.** If your transport requires a different
   discovery mechanism (e.g., BLE scanning instead of mDNS), implement the
   `Discovery` trait alongside it.

4. **Add integration tests.** Place them in `murmur-net/tests/` and ensure
   they run in CI. Use loopback or mock peers to avoid flaky network tests.

5. **Document it.** Update `ARCHITECTURE.md` and the crate-level docs.

## Pull Request Guidelines

- **One concern per PR.** Keep changes focused. Refactors and features go in
  separate PRs.
- **Write tests.** New functionality needs tests. Bug fixes need a regression
  test.
- **Run CI locally** before pushing: `cargo fmt --check && cargo clippy -- -D warnings && cargo test`.
- **Describe the change** in the PR body. Link to relevant issues.
- **Keep commits clean.** Squash fixups before requesting review.

## Commit Messages

Use conventional-style messages:

```
feat(scheduler): add jitter to chunk assignment
fix(storage): handle partial writes on flush
docs: update architecture diagram
```

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
Be respectful. Be constructive. Assume good intent.

## License

By contributing, you agree that your contributions will be licensed under the
same dual license as the project: MIT OR Apache-2.0.
