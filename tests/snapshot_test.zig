//! Snapshot and Log Compaction Tests

const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const ServerId = raftz.ServerId;
const Term = raftz.Term;
const LogIndex = raftz.LogIndex;
const LogEntry = raftz.LogEntry;
const AppendEntriesRequest = raftz.AppendEntriesRequest;
const ClusterConfig = raftz.ClusterConfig;
const KvStore = raftz.KvStore;
const Storage = raftz.Storage;
const rpc = raftz.rpc;

test "Snapshot: Basic snapshot creation" {
    const allocator = std.testing.allocator;

    const dir_path = "test_snapshot_basic";
    std.fs.cwd().makeDir(dir_path) catch {};
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    var storage = try Storage.init(allocator, dir_path);
    defer storage.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.simple(&servers);

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    var node = try Node.init(
        allocator,
        test_utils.testConfig(1),
        cluster,
        kv.stateMachine(),
        &storage,
    );
    defer node.deinit();

    try node.becomeLeader();

    _ = try node.submitCommand("SET x 1");
    _ = try node.submitCommand("SET y 2");
    _ = try node.submitCommand("SET z 3");

    node.mutex.lock();
    node.volatile_state.commit_index = 3;
    node.mutex.unlock();

    try node.applyCommitted();

    node.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 3), node.volatile_state.last_applied);
    node.mutex.unlock();

    try node.createSnapshot();

    const snapshot = try storage.loadSnapshot();
    try std.testing.expect(snapshot != null);
    defer allocator.free(snapshot.?.data);

    try std.testing.expectEqual(@as(LogIndex, 3), snapshot.?.last_index);
}

test "Snapshot: InstallSnapshot from leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    leader.mutex.lock();
    _ = try leader.log.append(1, "cmd1");
    _ = try leader.log.append(1, "cmd2");
    _ = try leader.log.append(1, "cmd3");
    leader.volatile_state.commit_index = 3;
    leader.volatile_state.last_applied = 3;
    leader.mutex.unlock();

    const snapshot_data = "state_machine_snapshot_data";

    const install_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 3,
        .last_included_term = 1,
        .offset = 0,
        .data = snapshot_data,
        .done = true,
    };

    const response = try follower.handleInstallSnapshot(install_req);
    try std.testing.expectEqual(@as(Term, 1), response.term);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 3), follower.volatile_state.last_applied);
    try std.testing.expectEqual(@as(LogIndex, 3), follower.volatile_state.commit_index);
    follower.mutex.unlock();
}

test "Snapshot: Reject stale term snapshot" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    follower.mutex.lock();
    follower.persistent.current_term = 5;
    follower.mutex.unlock();

    const install_req = rpc.InstallSnapshotRequest{
        .term = 3, // stale term
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 3,
        .offset = 0,
        .data = "snapshot_data",
        .done = true,
    };

    const response = try follower.handleInstallSnapshot(install_req);

    try std.testing.expectEqual(@as(Term, 5), response.term);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 0), follower.volatile_state.last_applied);
    follower.mutex.unlock();
}

test "Snapshot: Invalid request handling" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    const invalid_term_req = rpc.InstallSnapshotRequest{
        .term = 0,
        .leader_id = 1,
        .last_included_index = 3,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
    };

    var response = try follower.handleInstallSnapshot(invalid_term_req);
    try std.testing.expectEqual(@as(Term, 0), response.term);

    const invalid_leader_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 0,
        .last_included_index = 3,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
    };

    response = try follower.handleInstallSnapshot(invalid_leader_req);
    try std.testing.expectEqual(@as(Term, 0), response.term);

    const invalid_index_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 0,
        .last_included_term = 1,
        .offset = 0,
        .data = "data",
        .done = true,
    };

    response = try follower.handleInstallSnapshot(invalid_index_req);
    try std.testing.expectEqual(@as(Term, 0), response.term);

    const empty_data_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 3,
        .last_included_term = 1,
        .offset = 0,
        .data = "",
        .done = true,
    };

    response = try follower.handleInstallSnapshot(empty_data_req);
    try std.testing.expectEqual(@as(Term, 0), response.term);
}

test "Snapshot: Log trimmed after snapshot" {
    const allocator = std.testing.allocator;

    const dir_path = "test_snapshot_trim";
    std.fs.cwd().makeDir(dir_path) catch {};
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    var storage = try Storage.init(allocator, dir_path);
    defer storage.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.simple(&servers);

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    var node = try Node.init(
        allocator,
        test_utils.testConfig(1),
        cluster,
        kv.stateMachine(),
        &storage,
    );
    defer node.deinit();

    try node.becomeLeader();

    _ = try node.submitCommand("cmd1");
    _ = try node.submitCommand("cmd2");
    _ = try node.submitCommand("cmd3");
    _ = try node.submitCommand("cmd4");
    _ = try node.submitCommand("cmd5");

    node.mutex.lock();
    const initial_log_len = node.log.entries.items.len;
    node.volatile_state.commit_index = 5;
    node.mutex.unlock();

    try std.testing.expectEqual(@as(usize, 5), initial_log_len);

    try node.applyCommitted();
    try node.createSnapshot();

    node.mutex.lock();
    const final_log_len = node.log.entries.items.len;
    node.mutex.unlock();

    try std.testing.expect(final_log_len < initial_log_len);
}

test "Snapshot: Follower updates leadership on snapshot" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    follower.mutex.lock();
    try std.testing.expectEqual(@as(?ServerId, null), follower.current_leader);
    follower.mutex.unlock();

    const install_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "snapshot_data",
        .done = true,
    };

    _ = try follower.handleInstallSnapshot(install_req);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(?ServerId, 1), follower.current_leader);
    follower.mutex.unlock();
}

test "Snapshot: Resets heartbeat timer on snapshot" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    cluster.tick(200);

    const before_snapshot = follower.isElectionTimeout();

    const install_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 3,
        .last_included_term = 1,
        .offset = 0,
        .data = "snapshot_data",
        .done = true,
    };

    _ = try follower.handleInstallSnapshot(install_req);

    const after_snapshot = follower.isElectionTimeout();

    if (before_snapshot) {
        try std.testing.expect(!after_snapshot);
    }
}

test "Snapshot: Higher term causes step down" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const node = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));

    try node.startElection();
    try node.becomeLeader();

    try std.testing.expectEqual(raftz.Role.leader, node.getRole());
    try std.testing.expectEqual(@as(Term, 1), node.getCurrentTerm());

    const install_req = rpc.InstallSnapshotRequest{
        .term = 5,
        .leader_id = 2,
        .last_included_index = 10,
        .last_included_term = 5,
        .offset = 0,
        .data = "snapshot_data",
        .done = true,
    };

    _ = try node.handleInstallSnapshot(install_req);

    try std.testing.expectEqual(raftz.Role.follower, node.getRole());
    try std.testing.expectEqual(@as(Term, 5), node.getCurrentTerm());
}

test "Snapshot: Load snapshot on restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_snapshot_load";
    std.fs.cwd().makeDir(dir_path) catch {};
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    var storage = try Storage.init(allocator, dir_path);
    defer storage.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.simple(&servers);

    {
        var kv = KvStore.init(allocator);
        defer kv.deinit();

        var node = try Node.init(
            allocator,
            test_utils.testConfig(1),
            cluster,
            kv.stateMachine(),
            &storage,
        );
        defer node.deinit();

        try node.becomeLeader();

        _ = try node.submitCommand("SET key1 value1");
        _ = try node.submitCommand("SET key2 value2");

        node.mutex.lock();
        node.volatile_state.commit_index = 2;
        node.mutex.unlock();

        try node.applyCommitted();
        try node.createSnapshot();
    }

    {
        var kv2 = KvStore.init(allocator);
        defer kv2.deinit();

        var node = try Node.init(
            allocator,
            test_utils.testConfig(1),
            cluster,
            kv2.stateMachine(),
            &storage,
        );
        defer node.deinit();

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.last_applied);
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.commit_index);
        node.mutex.unlock();
    }
}

test "Snapshot: Follower with existing log handles InstallSnapshot" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    follower.mutex.lock();
    _ = try follower.log.append(1, "old_cmd1");
    _ = try follower.log.append(1, "old_cmd2");
    try std.testing.expectEqual(@as(LogIndex, 2), follower.log.lastIndex());
    follower.mutex.unlock();

    const install_req = rpc.InstallSnapshotRequest{
        .term = 2,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 2,
        .offset = 0,
        .data = "new_snapshot_data",
        .done = true,
    };

    _ = try follower.handleInstallSnapshot(install_req);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 5), follower.volatile_state.last_applied);
    try std.testing.expectEqual(@as(LogIndex, 5), follower.volatile_state.commit_index);
    follower.mutex.unlock();
}

test "Snapshot: Partial snapshot (done=false) handling" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    const partial_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 10,
        .last_included_term = 1,
        .offset = 0,
        .data = "partial_data",
        .done = false, // NOT done - more chunks coming
    };

    const response = try follower.handleInstallSnapshot(partial_req);
    try std.testing.expectEqual(@as(Term, 1), response.term);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 0), follower.volatile_state.last_applied);
    follower.mutex.unlock();
}

test "Snapshot: SendInstallSnapshot through cluster" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const install_req = rpc.InstallSnapshotRequest{
        .term = 1,
        .leader_id = 1,
        .last_included_index = 5,
        .last_included_term = 1,
        .offset = 0,
        .data = "snapshot",
        .done = true,
    };

    const resp2 = cluster.sendInstallSnapshot(1, 2, install_req);
    try std.testing.expect(resp2 != null);
    try std.testing.expectEqual(@as(Term, 1), resp2.?.term);

    const node2 = cluster.getNode(2).?;
    node2.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 5), node2.volatile_state.last_applied);
    node2.mutex.unlock();
}
