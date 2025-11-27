# raftz

[![Zig support](https://img.shields.io/badge/Zig-≥0.15.2-color?logo=zig&color=%23f3ab20)](https://ziglang.org/download/)
[![Release](https://img.shields.io/github/v/release/burdzwastaken/raftz)](https://github.com/burdzwastaken/raftz/releases)
[![CI Status](https://img.shields.io/github/actions/workflow/status/burdzwastaken/raftz/ci.yml)](https://github.com/burdzwastaken/raftz/actions)

Implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Zig

## Features

- **Leader Election** - Automatic leader election with randomized timeouts for split-vote prevention
- **Log Replication** - Reliable log replication across cluster nodes with consistency guarantees
- **Persistence** - Durable state storage with snapshot support for efficient recovery
- **Pluggable State Machines** - Bring your own state machine or use the built-in key-value store
- **Network Transport** - TCP-based RPC communication between cluster nodes
- **Pre-vote Optimization** - Reduces election disruptions from partitioned nodes
- **ReadIndex Protocol** - Linearizable reads with heartbeat confirmation and follower caching

## Quick Start

### Installation

```bash
zig fetch --save https://github.com/burdzwastaken/raftz/archive/refs/tags/v0.0.1.tar.gz
```

### Basic Usage

```zig
const raft = @import("raftz");

// initialize state machine (built in k/v store)
var kv = raft.KvStore.init(allocator);
defer kv.deinit();

// configure cluster
const servers = [_]raft.ServerId{ 1, 2, 3 };
const cluster = raft.ClusterConfig{ .servers = &servers };

// create node with persistence
var storage = try raft.Storage.init(allocator, "data_dir");
defer storage.deinit();

var node = try raft.Node.init(
    allocator,
    .{ .id = 1 },
    cluster,
    kv.stateMachine(),
    &storage,
);
defer node.deinit();

// setup network transport
var transport = raft.Transport.init(allocator, 1000);
defer transport.deinit();

try transport.listen(.{ .host = "127.0.0.1", .port = 5001 });
try transport.addPeer(2, .{ .host = "127.0.0.1", .port = 5002 });

// start server
var server = raft.Server.init(allocator, &node, &transport);
try server.start();
defer server.stop();
```

## Development

```bash
# dev shell
nix develop

# build
zig build

# tests
zig build test

# fmt
zig fmt --check src/ examples/
```

## Examples

```bash
# simple node & RPC mocks
zig build run-simple

# client with log replication
zig build run-client

# full cluster with leader election and failover
zig build run-cluster
```

## TODO

### Zig Improvements
- [ ] Migrate to new Zig reader/writer I/O interfacez

### Advanced Raft Features
- [ ] Dynamic membership changes (addServer/removeServer RPCs)
- [ ] Leadership transfer / graceful handoff
- [ ] Non-voting members (learners)
- [ ] Witness members
- [ ] Client request dedup'n (idempotent updates)
- [ ] Request batching/pipelining

### Enhanced Testing
- [ ] Network partition and recovery tests
- [ ] Snapshot and log compaction tests
- [ ] Cluster membership change tests
- [ ] Performance and stress tests
- [ ] Concurrent client request handling tests
- [ ] Byzantine fault tolerance edge cases

## Credits

Thanks to Diego Ongaro and John Ousterhout for the excellent resources at [https://raft.github.io/](https://raft.github.io/). Also a shout out to the many Raft implementations in the wild for inspiration!

## License

MIT
