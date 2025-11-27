const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const ServerId = raftz.ServerId;
const Term = raftz.Term;
const TimeoutNowRequest = raftz.TimeoutNowRequest;

test "Leadership Transfer: Basic initiation" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.becomeLeader();

    try leader.transferLeadership(2);

    leader.mutex.lock();
    defer leader.mutex.unlock();

    try std.testing.expect(leader.transferring_leadership);
    try std.testing.expectEqual(@as(?ServerId, 2), leader.transfer_target);
}

test "Leadership Transfer: Cannot transfer to self" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.becomeLeader();

    try std.testing.expectError(error.CannotTransferToSelf, leader.transferLeadership(1));
}

test "Leadership Transfer: Cannot transfer to invalid target" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.becomeLeader();

    try std.testing.expectError(error.InvalidTransferTarget, leader.transferLeadership(69));
}

test "Leadership Transfer: Only leader can transfer" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const follower = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try std.testing.expectError(error.NotLeader, follower.transferLeadership(2));
}

test "Leadership Transfer: Target ready when caught up" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.becomeLeader();
    _ = try leader.submitCommand("cmd1");

    try leader.transferLeadership(2);

    try std.testing.expect(!leader.isTransferTargetReady());

    leader.mutex.lock();
    const leader_state = &(leader.leader orelse unreachable);
    try leader_state.match_index.put(2, 1);
    leader.mutex.unlock();

    try std.testing.expect(leader.isTransferTargetReady());
}

test "Leadership Transfer: Abort clears state" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.becomeLeader();
    try leader.transferLeadership(2);

    try std.testing.expect(leader.transferring_leadership);

    leader.abortLeadershipTransfer();

    leader.mutex.lock();
    defer leader.mutex.unlock();

    try std.testing.expect(!leader.transferring_leadership);
    try std.testing.expectEqual(@as(?ServerId, null), leader.transfer_target);
}

test "Leadership Transfer: TimeoutNow triggers immediate election" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const request = TimeoutNowRequest{
        .term = 1,
        .leader_id = 1,
    };

    const response = try node.handleTimeoutNow(request);

    try std.testing.expectEqual(@as(Term, 1), response.term);

    node.mutex.lock();
    defer node.mutex.unlock();
    try std.testing.expectEqual(@as(i64, 0), node.last_heartbeat);
}

test "Leadership Transfer: TimeoutNow with stale term" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node.startElection();
    try node.startElection();

    const current_term = node.getCurrentTerm();

    const request = TimeoutNowRequest{
        .term = 1,
        .leader_id = 1,
    };

    const response = try node.handleTimeoutNow(request);

    try std.testing.expectEqual(current_term, response.term);
}

test "Leadership Transfer: Timeout detection" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.becomeLeader();
    try leader.transferLeadership(2);

    try std.testing.expect(!leader.isLeadershipTransferTimeout());

    leader.mutex.lock();
    leader.transfer_start_time -= 1000;
    leader.mutex.unlock();

    try std.testing.expect(leader.isLeadershipTransferTimeout());
}
