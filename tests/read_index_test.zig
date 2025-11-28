const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const ServerId = raftz.ServerId;
const LogIndex = raftz.LogIndex;
const AppendEntriesRequest = raftz.AppendEntriesRequest;
const ClusterConfig = raftz.ClusterConfig;
const KvStore = raftz.KvStore;

test "ReadIndex: leader can request read index" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var node = try Node.init(
        allocator,
        .{ .id = 1 },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    try std.testing.expectError(error.NotLeader, node.requestReadIndex());

    try node.becomeLeader();

    const read_id = try node.requestReadIndex();
    try std.testing.expect(read_id > 0);
}

test "ReadIndex: pending reads confirmed by heartbeat acks" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var node = try Node.init(
        allocator,
        .{ .id = 1 },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    try node.becomeLeader();

    const read_id = try node.requestReadIndex();
    _ = read_id;

    try std.testing.expect(node.getConfirmedReadIndex() == null);

    node.ackPendingReads();
    try std.testing.expect(node.getConfirmedReadIndex() == null);

    node.ackPendingReads();
    const confirmed = node.getConfirmedReadIndex();
    try std.testing.expect(confirmed != null);
    try std.testing.expectEqual(@as(LogIndex, 0), confirmed.?);
}

test "ReadIndex: timeout cleanup for stale reads" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var node = try Node.init(
        allocator,
        .{
            .id = 1,
            .read_timeout = 100,
        },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    try node.becomeLeader();

    _ = try node.requestReadIndex();

    {
        node.mutex.lock();
        defer node.mutex.unlock();
        const leader_state = &(node.leader orelse unreachable);
        try std.testing.expectEqual(@as(usize, 1), leader_state.pending_reads.items.len);
    }

    std.Thread.sleep(150 * std.time.ns_per_ms);

    node.cleanupExpiredReads();

    {
        node.mutex.lock();
        defer node.mutex.unlock();
        const leader_state = &(node.leader orelse unreachable);
        try std.testing.expectEqual(@as(usize, 0), leader_state.pending_reads.items.len);
    }
}

test "ReadIndex: follower caches read index from heartbeats" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var follower = try Node.init(
        allocator,
        .{ .id = 2 },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer follower.deinit();

    try std.testing.expect(follower.getCachedReadIndex() == null);

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]raftz.LogEntry{},
        .leader_commit = 5,
   };

    _ = try follower.handleAppendEntries(request);

    const cached = follower.getCachedReadIndex();
    try std.testing.expect(cached != null);
    try std.testing.expectEqual(@as(LogIndex, 5), cached.?);
}

test "ReadIndex: follower cache expires" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var follower = try Node.init(
        allocator,
        .{
            .id = 2,
            .heartbeat_interval = 50,
        },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer follower.deinit();

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]raftz.LogEntry{},
        .leader_commit = 5,
   };

    _ = try follower.handleAppendEntries(request);

    try std.testing.expect(follower.getCachedReadIndex() != null);

    std.Thread.sleep(150 * std.time.ns_per_ms);

    try std.testing.expect(follower.getCachedReadIndex() == null);
}

test "ReadIndex: multiple pending reads tracked correctly" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var node = try Node.init(
        allocator,
        .{ .id = 1 },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    try node.becomeLeader();

    const read_id_1 = try node.requestReadIndex();
    const read_id_2 = try node.requestReadIndex();
    const read_id_3 = try node.requestReadIndex();

    try std.testing.expect(read_id_1 != read_id_2);
    try std.testing.expect(read_id_2 != read_id_3);

    {
        node.mutex.lock();
        defer node.mutex.unlock();
        const leader_state = &(node.leader orelse unreachable);
        try std.testing.expectEqual(@as(usize, 3), leader_state.pending_reads.items.len);
    }

    node.ackPendingReads();
    node.ackPendingReads();

    _ = node.getConfirmedReadIndex();

    {
        node.mutex.lock();
        defer node.mutex.unlock();
        const leader_state = &(node.leader orelse unreachable);
        try std.testing.expectEqual(@as(usize, 0), leader_state.pending_reads.items.len);
    }
}

test "ReadIndex: leader doesn't use cache" {
    const allocator = std.testing.allocator;

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var node = try Node.init(
        allocator,
        .{ .id = 1 },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    try node.becomeLeader();

    try std.testing.expect(node.getCachedReadIndex() == null);
}
