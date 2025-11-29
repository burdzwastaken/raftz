const std = @import("std");
const raft = @import("raftz");
const common = @import("common.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Raft Client Demo\n", .{});

    const data_dirs = [_][]const u8{ "demo_data_1", "demo_data_2", "demo_data_3" };
    common.cleanupDataDirs(&data_dirs);

    const servers = [_]raft.ServerId{ 1, 2, 3 };
    const cluster = raft.ClusterConfig.simple(&servers);

    var storage1 = try raft.Storage.init(allocator, "demo_data_1");
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

    var storage2 = try raft.Storage.init(allocator, "demo_data_2");
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

    var storage3 = try raft.Storage.init(allocator, "demo_data_3");
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

    try transport1.listen(.{ .host = "127.0.0.1", .port = 6001 });
    try transport1.addPeer(2, .{ .host = "127.0.0.1", .port = 6002 });
    try transport1.addPeer(3, .{ .host = "127.0.0.1", .port = 6003 });

    try transport2.listen(.{ .host = "127.0.0.1", .port = 6002 });
    try transport2.addPeer(1, .{ .host = "127.0.0.1", .port = 6001 });
    try transport2.addPeer(3, .{ .host = "127.0.0.1", .port = 6003 });

    try transport3.listen(.{ .host = "127.0.0.1", .port = 6003 });
    try transport3.addPeer(1, .{ .host = "127.0.0.1", .port = 6001 });
    try transport3.addPeer(2, .{ .host = "127.0.0.1", .port = 6002 });

    var server1 = raft.Server.init(allocator, &node1, &transport1);
    var server2 = raft.Server.init(allocator, &node2, &transport2);
    var server3 = raft.Server.init(allocator, &node3, &transport3);

    std.debug.print("Starting cluster...\n\n", .{});
    try server1.start();
    try server2.start();
    try server3.start();

    std.debug.print("Waiting for leader election...\n", .{});
    std.Thread.sleep(2 * std.time.ns_per_s);

    var leader_node: ?*raft.Node = null;
    if (node1.getRole() == .leader) leader_node = &node1;
    if (node2.getRole() == .leader) leader_node = &node2;
    if (node3.getRole() == .leader) leader_node = &node3;

    if (leader_node) |leader| {
        std.debug.print("\nLeader elected: Node {d}\n\n", .{leader.config.id});

        std.debug.print("Sending commands to leader...\n", .{});

        const commands = [_][]const u8{
            "SET name Alice",
            "SET age 30",
            "SET city Boston",
        };

        for (commands) |cmd| {
            const index = try leader.submitCommand(cmd);
            std.debug.print("Submitted: '{s}' at index {d}\n", .{ cmd, index });
        }

        std.debug.print("\nWaiting for replication...\n", .{});
        std.Thread.sleep(2 * std.time.ns_per_s);

        std.debug.print("\nCluster state\n", .{});
        common.printNodeStatus(&node1);
        common.printNodeStatus(&node2);
        common.printNodeStatus(&node3);

        std.debug.print("\nState machine contents (Node 1)\n", .{});
        common.printKvStore(&kv1);

        std.debug.print("\nState machine contents (Node 2)\n", .{});
        common.printKvStore(&kv2);

        std.debug.print("\nState machine contents (Node 3)\n", .{});
        common.printKvStore(&kv3);

        std.debug.print("\nAll nodes have replicated state!\n", .{});
    } else {
        std.debug.print("\nNo leader elected yet\n", .{});
    }

    server1.stop();
    server2.stop();
    server3.stop();

    std.Thread.sleep(500 * std.time.ns_per_ms);

    server1.deinit();
    server2.deinit();
    server3.deinit();
}
