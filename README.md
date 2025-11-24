# raftz

Implementation of the [Raft](https://raft.github.io/raft.pdf) distributed consensus algorithm in Zig

## Features

- Leader election with randomized timeouts
- Log replication across cluster nodes
- Persistence with snapshot support
- Pluggable state machine interface
- Network transport

## Building

```bash
zig build

zig build test

zig build run-client  # client with replication
zig build run-cluster # cluster with leader election
zig build run-simple  # simple RPC example
```

## Usage

```zig
const raft = @import("raftz");

// k/v state machine
var kv = raft.KvStore.init(allocator);
defer kv.deinit();

// cluster config
const servers = [_]raft.ServerId{ 1, 2, 3 };
const cluster = raft.ClusterConfig{ .servers = &servers };

// node with optional persistence
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

// network transport
var transport = raft.Transport.init(allocator, 1000);
defer transport.deinit();

try transport.listen(.{ .host = "127.0.0.1", .port = 5001 });
try transport.addPeer(2, .{ .host = "127.0.0.1", .port = 5002 });

// server loop
var server = raft.Server.init(allocator, &node, &transport);
try server.start();
defer server.stop();
```

See `examples/` for more complete flows

## License

MIT
