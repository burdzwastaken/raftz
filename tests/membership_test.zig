//! Cluster Membership Change Tests

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
const ClusterConfig = raftz.ClusterConfig;
const KvStore = raftz.KvStore;
const rpc = raftz.rpc;

test "Membership: AddServer basic success" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    const response = try leader.handleAddServer(add_req);

    try std.testing.expect(response.success);
    try std.testing.expectEqual(@as(?ServerId, 1), response.leader_id);
    try std.testing.expectEqual(@as(Term, 1), response.term);

    leader.mutex.lock();
    try std.testing.expect(leader.log.lastIndex() > 0);
    const last_entry = leader.log.get(leader.log.lastIndex()).?;
    switch (last_entry.data) {
        .configuration => {},
        .command => try std.testing.expect(false),
    }
    leader.mutex.unlock();
}

test "Membership: AddServer fails for non-leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const follower = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    const response = try follower.handleAddServer(add_req);

    try std.testing.expect(!response.success);
}

test "Membership: AddServer fails for existing server" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req = rpc.AddServerRequest{
        .new_server = 2,
    };

    const response = try leader.handleAddServer(add_req);

    try std.testing.expect(!response.success);
}

test "Membership: AddServer fails during ongoing change" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req1 = rpc.AddServerRequest{
        .new_server = 4,
    };
    const resp1 = try leader.handleAddServer(add_req1);
    try std.testing.expect(resp1.success);

    leader.mutex.lock();
    leader.volatile_state.commit_index = leader.log.lastIndex();
    leader.mutex.unlock();
    try leader.applyCommitted();

    const add_req2 = rpc.AddServerRequest{
        .new_server = 5,
    };
    const resp2 = try leader.handleAddServer(add_req2);
    try std.testing.expect(!resp2.success);
}

test "Membership: RemoveServer basic success" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const remove_req = rpc.RemoveServerRequest{
        .old_server = 3,
    };

    const response = try leader.handleRemoveServer(remove_req);

    try std.testing.expect(response.success);
    try std.testing.expectEqual(@as(?ServerId, 1), response.leader_id);

    leader.mutex.lock();
    try std.testing.expect(leader.log.lastIndex() > 0);
    const last_entry = leader.log.get(leader.log.lastIndex()).?;
    switch (last_entry.data) {
        .configuration => {},
        .command => try std.testing.expect(false),
    }
    leader.mutex.unlock();
}

test "Membership: RemoveServer fails for non-leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const follower = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const remove_req = rpc.RemoveServerRequest{
        .old_server = 3,
    };

    const response = try follower.handleRemoveServer(remove_req);

    try std.testing.expect(!response.success);
}

test "Membership: RemoveServer fails for non-existent server" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const remove_req = rpc.RemoveServerRequest{
        .old_server = 99,
    };

    const response = try leader.handleRemoveServer(remove_req);

    try std.testing.expect(!response.success);
}

test "Membership: RemoveServer fails during ongoing change" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 4);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));
    _ = try cluster.addNode(test_utils.testConfig(4));

    try leader.startElection();
    try leader.becomeLeader();

    const remove_req1 = rpc.RemoveServerRequest{
        .old_server = 4,
    };
    const resp1 = try leader.handleRemoveServer(remove_req1);
    try std.testing.expect(resp1.success);

    leader.mutex.lock();
    leader.volatile_state.commit_index = leader.log.lastIndex();
    leader.mutex.unlock();
    try leader.applyCommitted();

    const remove_req2 = rpc.RemoveServerRequest{
        .old_server = 3,
    };
    const resp2 = try leader.handleRemoveServer(remove_req2);
    try std.testing.expect(!resp2.success);
}

test "Membership: Joint consensus configuration created" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    _ = try leader.handleAddServer(add_req);

    leader.mutex.lock();
    const entry = leader.log.get(leader.log.lastIndex()).?;
    switch (entry.data) {
        .configuration => |cfg| {
            try std.testing.expect(cfg.new_servers != null);
            try std.testing.expectEqual(@as(usize, 3), cfg.old_servers.len);
            try std.testing.expectEqual(@as(usize, 4), cfg.new_servers.?.len);
        },
        else => try std.testing.expect(false),
    }
    leader.mutex.unlock();
}

test "Membership: Leader indices updated for new server" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.submitCommand("cmd1");
    _ = try leader.submitCommand("cmd2");

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    _ = try leader.handleAddServer(add_req);

    leader.mutex.lock();
    const leader_state = leader.leader orelse unreachable;
    const next_idx = leader_state.next_index.get(4);
    const match_idx = leader_state.match_index.get(4);
    leader.mutex.unlock();

    try std.testing.expect(next_idx != null);
    try std.testing.expect(match_idx != null);
    try std.testing.expectEqual(@as(LogIndex, 0), match_idx.?);
}

test "Membership: Configuration entry replicated" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    _ = try leader.handleAddServer(add_req);

    leader.mutex.lock();
    const config_entry = leader.log.get(leader.log.lastIndex()).?;
    leader.mutex.unlock();

    const entries = [_]LogEntry{config_entry.*};

    const append_req = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const response = try follower.handleAppendEntries(append_req);
    try std.testing.expect(response.success);

    follower.mutex.lock();
    try std.testing.expect(follower.log.lastIndex() > 0);
    follower.mutex.unlock();
}

test "Membership: New server initialized with correct indices" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.submitCommand("cmd1");
    _ = try leader.submitCommand("cmd2");
    _ = try leader.submitCommand("cmd3");

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    _ = try leader.handleAddServer(add_req);

    leader.mutex.lock();
    const leader_state = leader.leader orelse unreachable;
    const next_idx = leader_state.next_index.get(4).?;
    const last_idx = leader.log.lastIndex();
    leader.mutex.unlock();

    try std.testing.expectEqual(last_idx + 1, next_idx);
}

test "Membership: Response includes current leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const heartbeat = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };
    _ = try follower.handleAppendEntries(heartbeat);

    const add_req = rpc.AddServerRequest{
        .new_server = 4,
    };

    const response = try follower.handleAddServer(add_req);
    try std.testing.expect(!response.success);
    try std.testing.expectEqual(@as(?ServerId, 1), response.leader_id);
}

test "Membership: Follower replicates membership change via AppendEntries" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    const follower = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.handleAddServer(.{ .new_server = 4 });

    leader.mutex.lock();
    const config_entry = leader.log.get(leader.log.lastIndex()).?;
    const entries = [_]LogEntry{config_entry.*};
    leader.mutex.unlock();

    const append_req = AppendEntriesRequest{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &entries,
        .leader_commit = 0,
    };

    const resp = try follower.handleAppendEntries(append_req);
    try std.testing.expect(resp.success);

    follower.mutex.lock();
    try std.testing.expectEqual(@as(LogIndex, 1), follower.log.lastIndex());
    const follower_entry = follower.log.get(1).?;
    switch (follower_entry.data) {
        .configuration => {},
        .command => try std.testing.expect(false),
    }
    follower.mutex.unlock();
}

test "Membership: ClusterConfig.contains works correctly" {
    const servers = [_]ServerId{ 1, 2, 3 };
    const config = ClusterConfig.simple(&servers);

    try std.testing.expect(config.contains(1));
    try std.testing.expect(config.contains(2));
    try std.testing.expect(config.contains(3));
    try std.testing.expect(!config.contains(4));
    try std.testing.expect(!config.contains(0));
}

test "Membership: Joint consensus majority size" {
    const old_servers = [_]ServerId{ 1, 2, 3 };
    const new_servers = [_]ServerId{ 1, 2, 3, 4 };
    const joint_config = ClusterConfig.joint(&old_servers, &new_servers);

    const majority = joint_config.majoritySize();
    try std.testing.expect(majority >= 2);
}

test "Membership: AddLearner basic success" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req = rpc.AddLearnerRequest{
        .learner_id = 4,
    };

    const response = try leader.handleAddLearner(add_req);

    try std.testing.expect(response.success);
    try std.testing.expectEqual(@as(?ServerId, 1), response.leader_id);
    try std.testing.expectEqual(@as(Term, 1), response.term);

    leader.mutex.lock();
    try std.testing.expect(leader.cluster.isLearner(4));
    try std.testing.expect(!leader.cluster.isVoter(4));
    try std.testing.expect(leader.cluster.contains(4));
    leader.mutex.unlock();
}

test "Membership: AddLearner fails for non-leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const follower = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const add_req = rpc.AddLearnerRequest{
        .learner_id = 4,
    };

    const response = try follower.handleAddLearner(add_req);

    try std.testing.expect(!response.success);
}

test "Membership: AddLearner fails for existing server" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const add_req = rpc.AddLearnerRequest{
        .learner_id = 2,
    };

    const response = try leader.handleAddLearner(add_req);

    try std.testing.expect(!response.success);
}

test "Membership: AddLearner initializes replication indices" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.submitCommand("cmd1");
    _ = try leader.submitCommand("cmd2");

    const add_req = rpc.AddLearnerRequest{
        .learner_id = 4,
    };

    _ = try leader.handleAddLearner(add_req);

    leader.mutex.lock();
    const leader_state = leader.leader orelse unreachable;
    const next_idx = leader_state.next_index.get(4);
    const match_idx = leader_state.match_index.get(4);
    leader.mutex.unlock();

    try std.testing.expect(next_idx != null);
    try std.testing.expect(match_idx != null);
    try std.testing.expectEqual(@as(LogIndex, 0), match_idx.?);
}

test "Membership: RemoveLearner basic success" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.handleAddLearner(.{ .learner_id = 4 });

    leader.mutex.lock();
    try std.testing.expect(leader.cluster.isLearner(4));
    leader.mutex.unlock();

    const remove_req = rpc.RemoveLearnerRequest{
        .learner_id = 4,
    };

    const response = try leader.handleRemoveLearner(remove_req);

    try std.testing.expect(response.success);

    leader.mutex.lock();
    try std.testing.expect(!leader.cluster.isLearner(4));
    try std.testing.expect(!leader.cluster.contains(4));
    leader.mutex.unlock();
}

test "Membership: RemoveLearner fails for non-learner" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const remove_req = rpc.RemoveLearnerRequest{
        .learner_id = 2,
    };

    const response = try leader.handleRemoveLearner(remove_req);

    try std.testing.expect(!response.success);
}

test "Membership: RemoveLearner cleans up replication indices" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.handleAddLearner(.{ .learner_id = 4 });

    leader.mutex.lock();
    var leader_state = &(leader.leader orelse unreachable);
    try std.testing.expect(leader_state.next_index.get(4) != null);
    leader.mutex.unlock();

    _ = try leader.handleRemoveLearner(.{ .learner_id = 4 });

    leader.mutex.lock();
    leader_state = &(leader.leader orelse unreachable);
    try std.testing.expect(leader_state.next_index.get(4) == null);
    try std.testing.expect(leader_state.match_index.get(4) == null);
    leader.mutex.unlock();
}

test "Membership: PromoteLearner basic success" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.handleAddLearner(.{ .learner_id = 4 });

    leader.mutex.lock();
    try std.testing.expect(leader.cluster.isLearner(4));
    try std.testing.expect(!leader.cluster.isVoter(4));
    leader.mutex.unlock();

    const promote_req = rpc.PromoteLearnerRequest{
        .learner_id = 4,
    };

    const response = try leader.handlePromoteLearner(promote_req);

    try std.testing.expect(response.success);

    leader.mutex.lock();
    try std.testing.expect(leader.log.lastIndex() > 0);
    const last_entry = leader.log.get(leader.log.lastIndex()).?;
    switch (last_entry.data) {
        .configuration => |cfg| {
            try std.testing.expect(cfg.new_servers != null);
        },
        .command => try std.testing.expect(false),
    }
    leader.mutex.unlock();
}

test "Membership: PromoteLearner fails for non-learner" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    const promote_req = rpc.PromoteLearnerRequest{
        .learner_id = 2,
    };

    const response = try leader.handlePromoteLearner(promote_req);

    try std.testing.expect(!response.success);
}

test "Membership: PromoteLearner fails during ongoing config change" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const leader = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try leader.startElection();
    try leader.becomeLeader();

    _ = try leader.handleAddLearner(.{ .learner_id = 4 });
    _ = try leader.handleAddLearner(.{ .learner_id = 5 });

    _ = try leader.handleAddServer(.{ .new_server = 6 });

    leader.mutex.lock();
    leader.volatile_state.commit_index = leader.log.lastIndex();
    leader.mutex.unlock();
    try leader.applyCommitted();

    const promote_req = rpc.PromoteLearnerRequest{
        .learner_id = 4,
    };

    const response = try leader.handlePromoteLearner(promote_req);

    try std.testing.expect(!response.success);
}

test "Membership: ClusterConfig.isLearner works correctly" {
    const servers = [_]ServerId{ 1, 2, 3 };
    const learners = [_]ServerId{ 4, 5 };
    const config = ClusterConfig.withLearners(&servers, &learners);

    try std.testing.expect(!config.isLearner(1));
    try std.testing.expect(!config.isLearner(2));
    try std.testing.expect(!config.isLearner(3));
    try std.testing.expect(config.isLearner(4));
    try std.testing.expect(config.isLearner(5));
    try std.testing.expect(!config.isLearner(6));
}

test "Membership: ClusterConfig.isVoter works correctly" {
    const servers = [_]ServerId{ 1, 2, 3 };
    const learners = [_]ServerId{ 4, 5 };
    const config = ClusterConfig.withLearners(&servers, &learners);

    try std.testing.expect(config.isVoter(1));
    try std.testing.expect(config.isVoter(2));
    try std.testing.expect(config.isVoter(3));
    try std.testing.expect(!config.isVoter(4));
    try std.testing.expect(!config.isVoter(5));
    try std.testing.expect(!config.isVoter(6));
}

test "Membership: ClusterConfig.contains includes learners" {
    const servers = [_]ServerId{ 1, 2, 3 };
    const learners = [_]ServerId{ 4, 5 };
    const config = ClusterConfig.withLearners(&servers, &learners);

    try std.testing.expect(config.contains(1));
    try std.testing.expect(config.contains(4));
    try std.testing.expect(!config.contains(6));
}
