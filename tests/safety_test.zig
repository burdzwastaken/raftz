//! Safety Property Tests

const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const ServerId = raftz.ServerId;
const Term = raftz.Term;
const LogIndex = raftz.LogIndex;
const LogEntry = raftz.LogEntry;
const AppendEntriesRequest = raftz.AppendEntriesRequest;
const RequestVoteRequest = raftz.RequestVoteRequest;
const Storage = raftz.Storage;
const ClusterConfig = raftz.ClusterConfig;
const KvStore = raftz.KvStore;

test "Safety: Committed entry present in new leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    const node3 = try cluster.addNode(test_utils.testConfig(3));

    try node1.becomeLeader();
    _ = try node1.submitCommand("cmd1");

    const entries = [_]LogEntry{
        LogEntry.command(1, 1, "cmd1"),
    };

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    _ = try node2.handleAppendEntries(request);

    node1.mutex.lock();
    node1.volatile_state.commit_index = 1;
    node1.mutex.unlock();

    try node2.startElection();

    node2.mutex.lock();
    const last_log_index = node2.log.lastIndex();
    const last_log_term = node2.log.lastTerm();
    node2.mutex.unlock();

    try std.testing.expectEqual(@as(LogIndex, 1), last_log_index);
    try std.testing.expectEqual(@as(Term, 1), last_log_term);

    const vote_req = RequestVoteRequest{
        .term = 2,
        .candidate_id = 2,
        .last_log_index = last_log_index,
        .last_log_term = last_log_term,
    };

    const vote_resp = node3.handleRequestVote(vote_req);
    try std.testing.expect(vote_resp.vote_granted);
}

test "Safety: Candidate with incomplete log cannot win" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    const node3 = try cluster.addNode(test_utils.testConfig(3));

    node1.mutex.lock();
    _ = try node1.log.append(1, "cmd1");
    _ = try node1.log.append(1, "cmd2");
    node1.mutex.unlock();

    node2.mutex.lock();
    _ = try node2.log.append(1, "cmd1");
    _ = try node2.log.append(1, "cmd2");
    node2.mutex.unlock();

    try node3.startElection();

    const vote_req = RequestVoteRequest{
        .term = 2,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp1 = node1.handleRequestVote(vote_req);
    const resp2 = node2.handleRequestVote(vote_req);

    try std.testing.expect(!resp1.vote_granted);
    try std.testing.expect(!resp2.vote_granted);
}

test "Safety: No conflicting entries applied" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));

    node1.mutex.lock();
    _ = try node1.log.append(1, "SET x 1");
    node1.volatile_state.commit_index = 1;
    node1.mutex.unlock();

    node2.mutex.lock();
    _ = try node2.log.append(1, "SET x 1");
    node2.volatile_state.commit_index = 1;
    node2.mutex.unlock();

    try node1.applyCommitted();
    try node2.applyCommitted();

    node1.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node1.volatile_state.last_applied);
    node1.mutex.unlock();

    node2.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node2.volatile_state.last_applied);
    node2.mutex.unlock();
}

test "Safety: Committed entries from previous terms" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 5);
    defer cluster.deinit();

    const s1 = try cluster.addNode(test_utils.testConfig(1));
    const s2 = try cluster.addNode(test_utils.testConfig(2));
    const s3 = try cluster.addNode(test_utils.testConfig(3));
    _ = try cluster.addNode(test_utils.testConfig(4));
    const s5 = try cluster.addNode(test_utils.testConfig(5));

    s1.mutex.lock();
    _ = try s1.log.append(1, "cmd1");
    _ = try s1.log.append(2, "cmd2");
    s1.persistent.current_term = 2;
    s1.mutex.unlock();

    s2.mutex.lock();
    _ = try s2.log.append(1, "cmd1");
    _ = try s2.log.append(2, "cmd2");
    s2.mutex.unlock();

    s3.mutex.lock();
    _ = try s3.log.append(1, "cmd1");
    s3.mutex.unlock();

    s5.mutex.lock();
    _ = try s5.log.append(1, "cmd1");
    _ = try s5.log.append(3, "different_cmd");
    s5.persistent.current_term = 3;
    s5.mutex.unlock();

    try s1.startElection();
    try s1.startElection();
    try s1.becomeLeader();

    try std.testing.expectEqual(@as(Term, 4), s1.getCurrentTerm());

    s1.mutex.lock();
    const commit_before = s1.volatile_state.commit_index;
    s1.mutex.unlock();

    try std.testing.expect(commit_before < 2);

    const idx = try s1.submitCommand("cmd_term4");
    try std.testing.expectEqual(@as(LogIndex, 3), idx);
}

test "Safety: Log matching property" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));

    node1.mutex.lock();
    _ = try node1.log.append(1, "cmd1");
    _ = try node1.log.append(1, "cmd2");
    _ = try node1.log.append(2, "cmd3");
    node1.mutex.unlock();

    node2.mutex.lock();
    _ = try node2.log.append(1, "cmd1");
    _ = try node2.log.append(1, "cmd2");
    _ = try node2.log.append(2, "cmd3");
    node2.mutex.unlock();

    node1.mutex.lock();
    node2.mutex.lock();

    const entry1_3 = node1.log.get(3).?;
    const entry2_3 = node2.log.get(3).?;

    if (entry1_3.term == entry2_3.term and entry1_3.index == entry2_3.index) {
        for (1..3) |i| {
            const e1 = node1.log.get(@intCast(i)).?;
            const e2 = node2.log.get(@intCast(i)).?;
            try std.testing.expectEqual(e1.term, e2.term);
            try std.testing.expectEqualStrings(switch (e1.data) {
                .command => |cmd| cmd,
                .configuration => "",
            }, switch (e2.data) {
                .command => |cmd| cmd,
                .configuration => "",
            });
        }
    }

    node2.mutex.unlock();
    node1.mutex.unlock();
}

test "Safety: Only committed entries are applied" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 1);
    defer cluster.deinit();

    const node = try cluster.addNode(test_utils.testConfig(1));

    node.mutex.lock();
    _ = try node.log.append(1, "cmd1");
    _ = try node.log.append(1, "cmd2");
    node.volatile_state.commit_index = 1;
    node.mutex.unlock();

    try node.applyCommitted();

    node.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node.volatile_state.last_applied);
    node.mutex.unlock();

    node.mutex.lock();
    node.volatile_state.commit_index = 2;
    node.mutex.unlock();

    try node.applyCommitted();

    node.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.last_applied);
    node.mutex.unlock();
}

test "Safety: Term monotonically increases" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 1);
    defer cluster.deinit();

    const node = try cluster.addNode(test_utils.testConfig(1));

    var previous_term: Term = 0;

    for (0..10) |_| {
        try node.startElection();
        const current_term = node.getCurrentTerm();
        try std.testing.expect(current_term > previous_term);
        previous_term = current_term;
    }
}

test "Safety: Leader never overwrites own log" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 1);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    try leader.becomeLeader();

    _ = try leader.submitCommand("cmd1");
    _ = try leader.submitCommand("cmd2");

    leader.mutex.lock();
    const entry1 = leader.log.get(1).?;
    const cmd1_copy = try leader.allocator.dupe(u8, switch (entry1.data) {
        .command => |cmd| cmd,
        .configuration => "",
    });
    defer leader.allocator.free(cmd1_copy);
    const term1 = entry1.term;
    leader.mutex.unlock();

    _ = try leader.submitCommand("cmd3");
    _ = try leader.submitCommand("cmd4");

    leader.mutex.lock();
    const entry1_after = leader.log.get(1).?;
    try std.testing.expectEqual(term1, entry1_after.term);
    try std.testing.expectEqualStrings(cmd1_copy, switch (entry1_after.data) {
        .command => |cmd| cmd,
        .configuration => "",
    });
    leader.mutex.unlock();
}

test "Safety: Applied entries persist across restarts" {
    const allocator = std.testing.allocator;

    const dir_path = "test_safety_persist";
    std.fs.cwd().makeDir(dir_path) catch {};
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    var storage = try Storage.init(allocator, dir_path);
    defer storage.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.single(&servers);

    var kv = KvStore.init(allocator);
    defer kv.deinit();

    {
        var node = try Node.init(
            allocator,
            test_utils.testConfig(1),
            cluster,
            kv.stateMachine(),
            &storage,
        );
        defer node.deinit();

        node.mutex.lock();
        _ = try node.log.append(1, "SET x 100");
        node.volatile_state.commit_index = 1;
        node.mutex.unlock();

        try node.applyCommitted();

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 1), node.volatile_state.last_applied);
        node.mutex.unlock();
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
        try std.testing.expectEqual(@as(LogIndex, 1), node.volatile_state.last_applied);
        node.mutex.unlock();
    }
}
