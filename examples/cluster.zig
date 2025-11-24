const std = @import("std");
const raft = @import("raftz");
const common = @import("common.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Raft Cluster Demo\n", .{});

    const data_dirs = [_][]const u8{ "raft_data_1", "raft_data_2", "raft_data_3" };
    common.cleanupDataDirs(&data_dirs);

    const servers = [_]raft.ServerId{ 1, 2, 3 };
    const cluster = raft.ClusterConfig{ .servers = &servers };

    var storage1 = try raft.Storage.init(allocator, "raft_data_1");
    defer storage1.deinit();
    var kv1 = raft.KvStore.init(allocator);
    defer kv1.deinit();
    var node1 = try raft.Node.init(
        allocator,
        .{
            .id = 1,
            .election_timeout_min = 150,
            .election_timeout_max = 300,
        },
        cluster,
        kv1.stateMachine(),
        &storage1,
    );
    defer node1.deinit();

    var storage2 = try raft.Storage.init(allocator, "raft_data_2");
    defer storage2.deinit();
    var kv2 = raft.KvStore.init(allocator);
    defer kv2.deinit();
    var node2 = try raft.Node.init(
        allocator,
        .{
            .id = 2,
            .election_timeout_min = 150,
            .election_timeout_max = 300,
        },
        cluster,
        kv2.stateMachine(),
        &storage2,
    );
    defer node2.deinit();

    var storage3 = try raft.Storage.init(allocator, "raft_data_3");
    defer storage3.deinit();
    var kv3 = raft.KvStore.init(allocator);
    defer kv3.deinit();
    var node3 = try raft.Node.init(
        allocator,
        .{
            .id = 3,
            .election_timeout_min = 150,
            .election_timeout_max = 300,
        },
        cluster,
        kv3.stateMachine(),
        &storage3,
    );
    defer node3.deinit();

    var transport1 = raft.Transport.init(allocator, 100);
    defer transport1.deinit();

    var transport2 = raft.Transport.init(allocator, 100);
    defer transport2.deinit();

    var transport3 = raft.Transport.init(allocator, 100);
    defer transport3.deinit();

    try transport1.listen(.{ .host = "127.0.0.1", .port = 5001 });
    try transport1.addPeer(2, .{ .host = "127.0.0.1", .port = 5002 });
    try transport1.addPeer(3, .{ .host = "127.0.0.1", .port = 5003 });

    try transport2.listen(.{ .host = "127.0.0.1", .port = 5002 });
    try transport2.addPeer(1, .{ .host = "127.0.0.1", .port = 5001 });
    try transport2.addPeer(3, .{ .host = "127.0.0.1", .port = 5003 });

    try transport3.listen(.{ .host = "127.0.0.1", .port = 5003 });
    try transport3.addPeer(1, .{ .host = "127.0.0.1", .port = 5001 });
    try transport3.addPeer(2, .{ .host = "127.0.0.1", .port = 5002 });

    var server1 = raft.Server.init(allocator, &node1, &transport1);
    var server2 = raft.Server.init(allocator, &node2, &transport2);
    var server3 = raft.Server.init(allocator, &node3, &transport3);

    std.debug.print("Starting servers...\n", .{});
    try server1.start();
    try server2.start();
    try server3.start();

    std.debug.print("\nCluster running. Waiting for leader election...\n", .{});
    std.Thread.sleep(2 * std.time.ns_per_s);

    var leader_node: ?*raft.Node = null;
    var leader_id: raft.ServerId = 0;
    if (node1.getRole() == .leader) {
        leader_node = &node1;
        leader_id = 1;
    }
    if (node2.getRole() == .leader) {
        leader_node = &node2;
        leader_id = 2;
    }
    if (node3.getRole() == .leader) {
        leader_node = &node3;
        leader_id = 3;
    }

    if (leader_node) |leader| {
        std.debug.print("\nInitial Leader: Node {d}\n", .{leader_id});

        std.debug.print("\nSubmitting commands to leader...\n", .{});
        const commands = [_][]const u8{
            "SET key1 value1",
            "SET key2 value2",
            "SET key3 value3",
        };

        for (commands) |cmd| {
            const index = try leader.submitCommand(cmd);
            std.debug.print("  Submitted: '{s}' at index {d}\n", .{ cmd, index });
        }

        std.debug.print("\nWaiting for replication...\n", .{});
        std.Thread.sleep(1 * std.time.ns_per_s);

        std.debug.print("\nCluster Status (Before Snapshot)\n", .{});
        common.printNodeStatus(&node1);
        common.printNodeStatus(&node2);
        common.printNodeStatus(&node3);

        std.debug.print("\nCreating snapshot on leader...\n", .{});
        try leader.createSnapshot();
        std.debug.print("Snapshot created successfully\n", .{});

        std.debug.print("\nCluster Status (After Snapshot)\n", .{});
        common.printNodeStatus(&node1);
        common.printNodeStatus(&node2);
        common.printNodeStatus(&node3);

        std.debug.print("\nSimulating Leader Failure\n", .{});
        std.debug.print("Stopping Node {d} (current leader)...\n\n", .{leader_id});

        if (leader_id == 1) {
            server1.stop();
            std.Thread.sleep(100 * std.time.ns_per_ms);
        } else if (leader_id == 2) {
            server2.stop();
            std.Thread.sleep(100 * std.time.ns_per_ms);
        } else if (leader_id == 3) {
            server3.stop();
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }

        std.debug.print("Waiting for new leader election...\n", .{});
        std.Thread.sleep(2 * std.time.ns_per_s);

        var new_leader_id: raft.ServerId = 0;
        if (leader_id != 1 and node1.getRole() == .leader) new_leader_id = 1;
        if (leader_id != 2 and node2.getRole() == .leader) new_leader_id = 2;
        if (leader_id != 3 and node3.getRole() == .leader) new_leader_id = 3;

        if (new_leader_id > 0) {
            std.debug.print("\nNew Leader Elected: Node {d}\n", .{new_leader_id});

            std.debug.print("\nCluster Status (After Failover)\n", .{});
            if (leader_id != 1) common.printNodeStatus(&node1);
            if (leader_id != 2) common.printNodeStatus(&node2);
            if (leader_id != 3) common.printNodeStatus(&node3);
            std.debug.print("Node {d}: STOPPED\n", .{leader_id});

            std.debug.print("\nSubmitting new command to new leader...\n", .{});
            const new_leader = if (new_leader_id == 1) &node1 else if (new_leader_id == 2) &node2 else &node3;
            const new_index = try new_leader.submitCommand("SET key4 value4");
            std.debug.print("  Submitted: 'SET key4 value4' at index {d}\n", .{new_index});

            std.debug.print("\nWaiting for replication...\n", .{});
            std.Thread.sleep(1 * std.time.ns_per_s);

            std.debug.print("\nFinal Cluster Status\n", .{});
            if (leader_id != 1) common.printNodeStatus(&node1);
            if (leader_id != 2) common.printNodeStatus(&node2);
            if (leader_id != 3) common.printNodeStatus(&node3);
            std.debug.print("Node {d}: STOPPED\n", .{leader_id});

            std.debug.print("\nLeader Failover Successful!\n", .{});
        } else {
            std.debug.print("\nNo new leader elected\n", .{});
        }
    } else {
        std.debug.print("\nNo initial leader elected\n", .{});
    }

    std.debug.print("\nStopping remaining servers...\n", .{});
    if (leader_id != 1) {
        server1.stop();
    }
    if (leader_id != 2) {
        server2.stop();
    }
    if (leader_id != 3) {
        server3.stop();
    }

    server1.deinit();
    server2.deinit();
    server3.deinit();

    std.debug.print("Demo completed.\n", .{});
}
