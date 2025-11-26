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

## Quick Start

### Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .raftz = .{
        .url = "https://github.com/burdzwastaken/raftz/archive/refs/tags/v0.0.1.tar.gz",
        .hash = "<hash>",
    },
},
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

### Core Improvements
- [ ] Test suite for Raft implementation
- [ ] Documentation
- [ ] Migrate to new Zig IO interface

### Advanced Raft Features
- [ ] Dynamic membership changes (addServer/removeServer RPCs)
- [ ] Pre-vote optimization to reduce election disruptions
- [ ] ReadIndex protocol for linearizable read-only queries
- [ ] Leadership transfer for graceful handoff
- [ ] Non-voting members (learners)
- [ ] Witness members
- [ ] Client request deduplication (idempotent updates)
- [ ] Request batching and pipelining

## License

MIT
