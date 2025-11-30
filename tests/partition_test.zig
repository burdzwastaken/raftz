//! Network Partition and Recovery Tests

const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const Role = raftz.Role;
const ServerId = raftz.ServerId;
const Term = raftz.Term;
const LogIndex = raftz.LogIndex;
const LogEntry = raftz.LogEntry;
const AppendEntriesRequest = raftz.AppendEntriesRequest;
const RequestVoteRequest = raftz.RequestVoteRequest;

test "Partition: Leader isolated loses leadership after timeout" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 5);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));
    _ = try cluster.addNode(test_utils.testConfig(4));
    _ = try cluster.addNode(test_utils.testConfig(5));

    try node1.startElection();

    const vote_req = RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    _ = cluster.sendRequestVote(1, 2, vote_req);
    _ = cluster.sendRequestVote(1, 3, vote_req);

    try node1.becomeLeader();
    try std.testing.expectEqual(Role.leader, node1.getRole());

    try cluster.isolateNode(1);

    try std.testing.expect(!cluster.canCommunicate(1, 2));
    try std.testing.expect(!cluster.canCommunicate(1, 3));
    try std.testing.expect(cluster.canCommunicate(2, 3));

    cluster.tick(350);

    try node2.startElection();
    const new_term = node2.getCurrentTerm();
    try std.testing.expect(new_term > 1);

    const vote_req2 = RequestVoteRequest{
        .term = new_term,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp3 = cluster.sendRequestVote(2, 3, vote_req2);
    try std.testing.expect(resp3 != null);
    try std.testing.expect(resp3.?.vote_granted);

    try node2.becomeLeader();
    try std.testing.expectEqual(Role.leader, node2.getRole());

    try std.testing.expectEqual(Role.leader, node1.getRole());
    try std.testing.expectEqual(Role.leader, node2.getRole());

    cluster.healPartitions();

    const heartbeat = AppendEntriesRequest{
        .term = new_term,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    _ = try node1.handleAppendEntries(heartbeat);

    try std.testing.expectEqual(Role.follower, node1.getRole());
    try std.testing.expectEqual(new_term, node1.getCurrentTerm());
}

test "Partition: Minority partition cannot elect leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 5);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));
    _ = try cluster.addNode(test_utils.testConfig(4));
    _ = try cluster.addNode(test_utils.testConfig(5));

    const partition1 = [_]ServerId{ 1, 2 };
    const partition2 = [_]ServerId{ 3, 4, 5 };
    const partitions = [_][]const ServerId{ &partition1, &partition2 };
    try cluster.partition(&partitions);

    try node1.startElection();

    const vote_req = RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    var responses = cluster.broadcastRequestVote(1, vote_req);
    defer responses.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), responses.items.len);

    try std.testing.expectEqual(Role.candidate, node1.getRole());

    try node2.startElection();
    try std.testing.expectEqual(Role.candidate, node2.getRole());
}

test "Partition: Majority partition continues progress" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 5);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    const node3 = try cluster.addNode(test_utils.testConfig(3));
    const node4 = try cluster.addNode(test_utils.testConfig(4));
    const node5 = try cluster.addNode(test_utils.testConfig(5));

    try node1.startElection();
    try node1.becomeLeader();

    const partition1 = [_]ServerId{ 1, 2 };
    const partition2 = [_]ServerId{ 3, 4, 5 };
    const partitions = [_][]const ServerId{ &partition1, &partition2 };
    try cluster.partition(&partitions);

    cluster.tick(350);
    try node3.startElection();

    const vote_req = RequestVoteRequest{
        .term = 2,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp4 = cluster.sendRequestVote(3, 4, vote_req);
    const resp5 = cluster.sendRequestVote(3, 5, vote_req);
    try std.testing.expect(resp4 != null and resp4.?.vote_granted);
    try std.testing.expect(resp5 != null and resp5.?.vote_granted);

    try node3.becomeLeader();
    try std.testing.expectEqual(Role.leader, node3.getRole());

    _ = try node3.submitCommand("cmd1");

    const entries = [_]LogEntry{
        LogEntry.command(2, 1, "cmd1"),
    };

    const append_req = AppendEntriesRequest{
        .term = 2,
        .leader_id = 3,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const resp_4 = cluster.sendAppendEntries(3, 4, append_req);
    const resp_5 = cluster.sendAppendEntries(3, 5, append_req);

    try std.testing.expect(resp_4 != null and resp_4.?.success);
    try std.testing.expect(resp_5 != null and resp_5.?.success);

    node4.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node4.log.lastIndex());
    node4.mutex.unlock();

    node5.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node5.log.lastIndex());
    node5.mutex.unlock();
}

test "Partition: Healing partition syncs diverged logs" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    const node3 = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    _ = try node1.submitCommand("cmd1");
    _ = try node1.submitCommand("cmd2");

    const entries = [_]LogEntry{
        LogEntry.command(1, 1, "cmd1"),
        LogEntry.command(1, 2, "cmd2"),
    };

    const append_req = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 2,
    };

    _ = try node2.handleAppendEntries(append_req);

    try cluster.isolateNode(3);

    node3.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 0), node3.log.lastIndex());
    node3.mutex.unlock();

    cluster.healPartitions();

    _ = try node3.handleAppendEntries(append_req);

    node3.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 2), node3.log.lastIndex());
    try std.testing.expectEqual(@as(LogIndex, 2), node3.volatile_state.commit_index);
    node3.mutex.unlock();
}

test "Partition: Stale leader cannot commit after partition" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    try cluster.isolateNode(1);

    _ = try node1.submitCommand("stale_cmd");

    node1.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node1.log.lastIndex());
    try std.testing.expectEqual(@as(LogIndex, 0), node1.volatile_state.commit_index);
    node1.mutex.unlock();

    const entries = [_]LogEntry{
        LogEntry.command(1, 1, "stale_cmd"),
    };

    const append_req = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const resp2 = cluster.sendAppendEntries(1, 2, append_req);
    const resp3 = cluster.sendAppendEntries(1, 3, append_req);

    try std.testing.expect(resp2 == null);
    try std.testing.expect(resp3 == null);
}

test "Partition: PreVote prevents disruption from partitioned node" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    var config1 = test_utils.testConfig(1);
    config1.enable_prevote = true;
    var config2 = test_utils.testConfig(2);
    config2.enable_prevote = true;
    var config3 = test_utils.testConfig(3);
    config3.enable_prevote = true;

    const node1 = try cluster.addNode(config1);
    const node2 = try cluster.addNode(config2);
    const node3 = try cluster.addNode(config3);

    try node1.startElection();
    try node1.becomeLeader();
    const initial_term = node1.getCurrentTerm();

    const heartbeat = AppendEntriesRequest{
        .term = initial_term,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };
    _ = try node2.handleAppendEntries(heartbeat);
    _ = try node3.handleAppendEntries(heartbeat);

    try cluster.isolateNode(3);

    cluster.tick(350);
    node3.startPreVote();

    const prevote_req = raftz.PreVoteRequest{
        .term = initial_term + 1,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp1 = cluster.sendPreVote(3, 1, prevote_req);
    const resp2 = cluster.sendPreVote(3, 2, prevote_req);

    try std.testing.expect(resp1 == null);
    try std.testing.expect(resp2 == null);

    cluster.healPartitions();

    _ = node2.handlePreVote(prevote_req);

    try std.testing.expectEqual(initial_term, node1.getCurrentTerm());
}

test "Partition: Asymmetric partition handling" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    const partition_1_2 = [_]ServerId{ 1, 2 };
    const partition_2_3 = [_]ServerId{ 2, 3 };
    const partitions = [_][]const ServerId{ &partition_1_2, &partition_2_3 };
    try cluster.partition(&partitions);

    const entries = [_]LogEntry{
        LogEntry.command(1, 1, "cmd1"),
    };

    const append_req = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const resp2 = cluster.sendAppendEntries(1, 2, append_req);
    try std.testing.expect(resp2 != null);
    try std.testing.expect(resp2.?.success);

    const resp3 = cluster.sendAppendEntries(1, 3, append_req);
    try std.testing.expect(resp3 == null);

    node2.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), node2.log.lastIndex());
    node2.mutex.unlock();

    const forward_req = AppendEntriesRequest{
        .term = 1,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const resp_3_from_2 = cluster.sendAppendEntries(2, 3, forward_req);
    try std.testing.expect(resp_3_from_2 != null);
}

test "Partition: Recovery with conflicting entries" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();
    _ = try node1.submitCommand("cmd1");

    const entries1 = [_]LogEntry{
        LogEntry.command(1, 1, "cmd1"),
    };

    _ = try node2.handleAppendEntries(.{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries1,
        .leader_commit = 0,
    });

    try cluster.isolateNode(1);

    _ = try node1.submitCommand("node1_cmd2");
    _ = try node1.submitCommand("node1_cmd3");

    cluster.tick(350);
    try node2.startElection();

    const vote_req = RequestVoteRequest{
        .term = 2,
        .candidate_id = 2,
        .last_log_index = 1,
        .last_log_term = 1,
    };

    const resp3 = cluster.sendRequestVote(2, 3, vote_req);
    try std.testing.expect(resp3 != null and resp3.?.vote_granted);

    try node2.becomeLeader();
    _ = try node2.submitCommand("node2_cmd2");

    cluster.healPartitions();

    const new_leader_heartbeat = AppendEntriesRequest{
        .term = 2,
        .leader_id = 2,
        .prev_log_index = 1,
        .prev_log_term = 1,
        .entries = &.{},
        .leader_commit = 0,
    };

    const resp = try node1.handleAppendEntries(new_leader_heartbeat);

    try std.testing.expectEqual(Role.follower, node1.getRole());
    try std.testing.expectEqual(@as(Term, 2), node1.getCurrentTerm());

    try std.testing.expect(!resp.success or resp.term == 2);
}
