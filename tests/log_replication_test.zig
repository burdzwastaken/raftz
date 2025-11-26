//! Log Replication Tests

const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const LogEntry = raftz.LogEntry;
const AppendEntriesRequest = raftz.AppendEntriesRequest;

test "Log Replication: Leader appends entries to local log" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 1);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    try leader.becomeLeader();

    const idx1 = try leader.submitCommand("command1");
    const idx2 = try leader.submitCommand("command2");
    const idx3 = try leader.submitCommand("command3");

    try std.testing.expectEqual(@as(raftz.LogIndex, 1), idx1);
    try std.testing.expectEqual(@as(raftz.LogIndex, 2), idx2);
    try std.testing.expectEqual(@as(raftz.LogIndex, 3), idx3);
}

test "Log Replication: Follower accepts matching entries" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    _ = try leader.submitCommand("cmd1");

    const entries = [_]LogEntry{
        .{ .term = 1, .index = 1, .command = "cmd1" },
    };

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const response = try follower.handleAppendEntries(request);

    try std.testing.expect(response.success);
    try std.testing.expectEqual(@as(raftz.LogIndex, 1), response.match_index);
}

test "Log Replication: Follower rejects mismatched prev_log" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    follower.mutex.lock();
    _ = try follower.log.append(1, "different_cmd");
    follower.mutex.unlock();

    const entries = [_]LogEntry{
        .{ .term = 1, .index = 2, .command = "cmd2" },
    };

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 1,
        .prev_log_term = 2,
        .entries = &entries,
        .leader_commit = 0,
    };

    const response = try follower.handleAppendEntries(request);

    try std.testing.expect(!response.success);
}

test "Log Replication: Follower deletes conflicting entries" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    follower.mutex.lock();
    _ = try follower.log.append(1, "cmd1");
    _ = try follower.log.append(2, "old_cmd2");
    _ = try follower.log.append(2, "old_cmd3");
    try std.testing.expectEqual(@as(raftz.LogIndex, 3), follower.log.lastIndex());
    follower.mutex.unlock();

    const entries = [_]LogEntry{
        .{ .term = 1, .index = 2, .command = "new_cmd2" },
    };

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 1,
        .prev_log_term = 1,
        .entries = &entries,
        .leader_commit = 0,
    };

    const response = try follower.handleAppendEntries(request);

    try std.testing.expect(response.success);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(raftz.LogIndex, 2), follower.log.lastIndex());
    const entry2 = follower.log.get(2).?;
    try std.testing.expectEqualStrings("new_cmd2", entry2.command);
    follower.mutex.unlock();
}

test "Log Replication: Follower updates commit index" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    const entries = [_]LogEntry{
        .{ .term = 1, .index = 1, .command = "cmd1" },
        .{ .term = 1, .index = 2, .command = "cmd2" },
    };

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 2,
    };

    _ = try follower.handleAppendEntries(request);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(raftz.LogIndex, 2), follower.volatile_state.commit_index);
    follower.mutex.unlock();
}

test "Log Replication: Heartbeat with no entries" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    const response = try follower.handleAppendEntries(request);

    try std.testing.expect(response.success);

    try std.testing.expect(!follower.isElectionTimeout());
}

test "Log Replication: Leader Append-Only property" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 1);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    try leader.becomeLeader();

    _ = try leader.submitCommand("cmd1");
    _ = try leader.submitCommand("cmd2");
    _ = try leader.submitCommand("cmd3");

    leader.mutex.lock();
    const initial_count = leader.log.entries.items.len;
    const entry1_term = leader.log.get(1).?.term;
    const entry1_cmd = try leader.allocator.dupe(u8, leader.log.get(1).?.command);
    defer leader.allocator.free(entry1_cmd);
    leader.mutex.unlock();

    _ = try leader.submitCommand("cmd4");

    leader.mutex.lock();
    try std.testing.expectEqual(initial_count + 1, leader.log.entries.items.len);

    try std.testing.expectEqual(entry1_term, leader.log.get(1).?.term);
    try std.testing.expectEqualStrings(entry1_cmd, leader.log.get(1).?.command);
    leader.mutex.unlock();
}

test "Log Replication: Follower accepts batched entries" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    const entries = [_]LogEntry{
        .{ .term = 1, .index = 1, .command = "cmd1" },
        .{ .term = 1, .index = 2, .command = "cmd2" },
        .{ .term = 1, .index = 3, .command = "cmd3" },
        .{ .term = 1, .index = 4, .command = "cmd4" },
    };

    const request = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const response = try follower.handleAppendEntries(request);

    try std.testing.expect(response.success);
    try std.testing.expectEqual(@as(raftz.LogIndex, 4), response.match_index);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(raftz.LogIndex, 4), follower.log.lastIndex());
    follower.mutex.unlock();
}

test "Log Replication: Invalid AppendEntries are rejected" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    const invalid_term = AppendEntriesRequest{
        .term = 0,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    var resp = try follower.handleAppendEntries(invalid_term);
    try std.testing.expect(!resp.success);

    const invalid_leader = AppendEntriesRequest{
        .term = 1,
        .leader_id = 0,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    resp = try follower.handleAppendEntries(invalid_leader);
    try std.testing.expect(!resp.success);

    const invalid_prev = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 5,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    resp = try follower.handleAppendEntries(invalid_prev);
    try std.testing.expect(!resp.success);

    const bad_entries = [_]LogEntry{
        .{ .term = 0, .index = 1, .command = "cmd" },
    };

    const invalid_entry_term = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &bad_entries,
        .leader_commit = 0,
    };

    resp = try follower.handleAppendEntries(invalid_entry_term);
    try std.testing.expect(!resp.success);

    const bad_command_entries = [_]LogEntry{
        .{ .term = 1, .index = 1, .command = "" },
    };

    const invalid_command = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &bad_command_entries,
        .leader_commit = 0,
    };

    resp = try follower.handleAppendEntries(invalid_command);
    try std.testing.expect(!resp.success);
}

test "Log Replication: Follower converts to follower on higher term" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    const request = AppendEntriesRequest{
        .term = 2,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    const response = try leader.handleAppendEntries(request);

    try std.testing.expect(response.success);
    try std.testing.expectEqual(@as(raftz.Term, 2), leader.getCurrentTerm());
    try std.testing.expectEqual(raftz.Role.follower, leader.getRole());
    try std.testing.expectEqual(@as(?raftz.ServerId, 2), leader.getLeader());
}

test "Log Replication: Commit index never decreases" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 2);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));

    try leader.becomeLeader();

    const request1 = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 5,
    };

    _ = try follower.handleAppendEntries(request1);

    follower.mutex.lock();
    const commit_after_first = follower.volatile_state.commit_index;
    follower.mutex.unlock();

    const request2 = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 3,
    };

    _ = try follower.handleAppendEntries(request2);

    follower.mutex.lock();
    try std.testing.expect(follower.volatile_state.commit_index >= commit_after_first);
    follower.mutex.unlock();
}
