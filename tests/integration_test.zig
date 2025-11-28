//! Integration Tests

const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const rpc = raftz.rpc;

test "Integration: RPC routing respects partitions" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();

    const vote_req = raftz.RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp1 = cluster.sendRequestVote(1, 2, vote_req);
    try std.testing.expect(resp1 != null);
    try std.testing.expect(resp1.?.vote_granted);

    try cluster.isolateNode(2);

    const resp2 = cluster.sendRequestVote(1, 2, vote_req);
    try std.testing.expect(resp2 == null);

    const resp3 = cluster.sendRequestVote(1, 3, vote_req);
    try std.testing.expect(resp3 != null);
}

test "Integration: Broadcast respects partitions" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 5);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));
    _ = try cluster.addNode(test_utils.testConfig(4));
    _ = try cluster.addNode(test_utils.testConfig(5));

    try node1.startElection();

    const vote_req = raftz.RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    var responses = cluster.broadcastRequestVote(1, vote_req);
    defer responses.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), responses.items.len);

    const partition1 = [_]raftz.ServerId{ 1, 2 };
    const partition2 = [_]raftz.ServerId{ 3, 4, 5 };
    const partitions = [_][]const raftz.ServerId{ &partition1, &partition2 };
    try cluster.partition(&partitions);

    var partitioned_responses = cluster.broadcastRequestVote(1, vote_req);
    defer partitioned_responses.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), partitioned_responses.items.len);
}

test "Integration: AppendEntries routing" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.becomeLeader();

    const entries = [_]raftz.LogEntry{
        raftz.LogEntry.command(1, 1, "cmd1"),
    };

    const append_req = raftz.AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const resp1 = cluster.sendAppendEntries(1, 2, append_req);
    try std.testing.expect(resp1 != null);
    try std.testing.expect(resp1.?.success);

    var responses = cluster.broadcastAppendEntries(1, append_req);
    defer responses.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), responses.items.len);
}
