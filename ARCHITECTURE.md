# Architecture

This document describes the internal architecture of Murmur for contributors
and integrators who need to understand how the system works.

## Crate Dependency Graph

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

**Bottom-up summary:**

- `murmur-core` defines shared types used everywhere: `NodeId`, `ChunkId`,
  `Manifest`, `Task`, `Link`, and `Config`.
- `murmur-proto` contains the Protobuf/gRPC definitions and generated code for
  the wire protocol.
- `murmur-net` handles peer discovery (mDNS) and the transport layer (TCP).
- `murmur-storage` manages on-disk chunk I/O, manifest persistence, and BLAKE3
  integrity verification.
- `murmur-overlay` maintains the Overlay State Table — a consistent,
  distributed view of which nodes are alive, what they hold, and their link
  capacities.
- `murmur-coordinator` runs leader election and epoch management. The elected
  coordinator assigns work to the swarm.
- `murmur-scheduler` implements bandwidth-weighted chunk assignment. It
  decides which peer sends which chunk over which link.
- `murmur-api` exposes a stable C FFI and event stream for embedders building
  platform SDKs.
- `murmur-daemon` composes all of the above into a runnable node process.
- `murmur-cli` is a thin reference CLI that drives the daemon.

## How a File Transfer Works

End-to-end flow for distributing a file across the swarm:

```
 Sender                    Coordinator               Receivers
   │                            │                        │
   │  1. Chunk file locally     │                        │
   │  2. Compute BLAKE3 hashes  │                        │
   │  3. Build Manifest         │                        │
   │                            │                        │
   │──── PublishManifest ───────>│                        │
   │                            │  4. Validate manifest  │
   │                            │  5. Query overlay for  │
   │                            │     peer capacities    │
   │                            │  6. Run scheduler:     │
   │                            │     assign chunks to   │
   │                            │     (peer, link) pairs │
   │                            │                        │
   │                            │──── AssignChunks ──────>│
   │                            │                        │
   │<──── RequestChunk(id) ─────┼────────────────────────│
   │                            │                        │
   │───── ChunkData(id,bytes) ──┼───────────────────────>│
   │                            │                        │
   │                            │  7. Receiver verifies  │
   │                            │     BLAKE3 hash        │
   │                            │  8. Receiver ACKs      │
   │                            │                        │
   │                            │<──── ChunkAck(id) ─────│
   │                            │                        │
   │                            │  9. Track completion   │
   │                            │ 10. Reassign on failure│
   │                            │                        │
   │                            │──── TransferComplete ──>│
```

### Step-by-step

1. **Chunking.** The sender splits the input file into fixed-size chunks
   (default: 1 MiB) and computes a BLAKE3 hash for each chunk.

2. **Manifest creation.** A `Manifest` is built containing the file metadata,
   ordered list of `ChunkId`s, and their hashes. The manifest itself is
   identified by the BLAKE3 hash of its serialized form.

3. **Manifest publication.** The sender publishes the manifest to the current
   coordinator via the `PublishManifest` RPC.

4. **Scheduling.** The coordinator queries the Overlay State Table for live
   peers and their measured link bandwidths. The scheduler assigns chunks to
   `(source_peer, destination_peer, link)` triples, weighted by available
   bandwidth.

5. **Chunk transfer.** Receivers request assigned chunks from source peers.
   Data flows over TCP (or another registered transport). Each received chunk
   is immediately verified against its expected BLAKE3 hash.

6. **Acknowledgment.** Verified chunks are acknowledged back to the
   coordinator, which tracks global transfer progress.

7. **Failure handling.** If a peer goes offline or a chunk fails verification,
   the coordinator reassigns the affected chunks to other available peers in
   the next scheduling epoch.

8. **Completion.** When all chunks are acknowledged, the coordinator
   broadcasts `TransferComplete`.

## Key Abstractions

### Node

A `Node` is a single participant in the swarm, identified by a unique
`NodeId` (128-bit random identifier). Each node runs:

- A transport listener for incoming connections
- An mDNS responder/querier for discovery
- A storage engine for local chunk management
- An overlay agent that heartbeats to the cluster

### Coordinator

One node in the swarm is elected coordinator via a lightweight leader election
protocol. The coordinator:

- Manages epoch numbers (monotonically increasing, scoped to coordinator term)
- Receives manifest publications
- Runs the scheduler to produce chunk assignments
- Tracks transfer progress and handles reassignment

If the coordinator fails, a new election occurs. The new coordinator
reconstructs state from peer reports.

### Scheduler

The scheduler takes as input:

- A set of chunks to distribute
- The Overlay State Table (peer liveness + link bandwidths)
- Current transfer progress

It produces a set of `Task` assignments: `(chunk_id, source, destination, link)`.
Assignment is proportional to measured bandwidth — a 10 Gbps link gets
roughly 10x the chunks of a 1 Gbps link.

The scheduler runs once per epoch. Epochs are short (default: 1 second) to
react quickly to topology changes.

### Overlay State Table

The overlay is a distributed data structure replicated across all peers. It
tracks:

| Field | Description |
|---|---|
| `node_id` | Unique peer identifier |
| `links` | Active network links with measured bandwidth |
| `chunks_held` | Set of chunk IDs this peer has locally |
| `last_heartbeat` | Timestamp of last liveness proof |
| `role` | `Coordinator` or `Participant` |

Peers update the overlay via periodic heartbeats. The coordinator uses it as
the ground truth for scheduling decisions.

## Extension Points

Murmur is designed to be extended without forking:

### Custom Transport

Implement the transport trait in `murmur-net` to add protocols like QUIC,
WebSocket, or Bluetooth. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
step-by-step guide.

### Custom Scheduler Strategy

The scheduler in `murmur-scheduler` is trait-based. You can implement an
alternative strategy (e.g., latency-optimized, locality-aware) by implementing
the `SchedulerStrategy` trait and registering it with the daemon.

### Storage Backend

`murmur-storage` abstracts chunk persistence behind a trait. The default
backend uses the local filesystem. You can implement backends for object
stores (S3, GCS), in-memory caches, or database-backed storage.

### Embedder API

`murmur-api` provides the FFI boundary for platform integrations. Embedders
receive an event stream of transfer progress, peer changes, and errors. You
can build reactive UIs or orchestration systems on top of this stream.

### Discovery Mechanism

The `Discovery` trait in `murmur-net` allows plugging in alternatives to mDNS:
static peer lists, DHT-based discovery, cloud service registries, or
Bluetooth LE scanning.
