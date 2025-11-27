//! Leader Election Tests

const std = @import("std");
const raftz = @import("raftz");
const test_utils = @import("test_utils.zig");

const Node = raftz.Node;
const Role = raftz.Role;
const ServerId = raftz.ServerId;
const Term = raftz.Term;
const RequestVoteRequest = raftz.RequestVoteRequest;

test "Leader Election: Basic election succeeds" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    _ = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const node1 = cluster.getNode(1).?;
    try node1.startElection();

    try std.testing.expectEqual(Role.candidate, node1.getRole());
    try std.testing.expectEqual(@as(Term, 1), node1.getCurrentTerm());

    const node2 = cluster.getNode(2).?;
    const node3 = cluster.getNode(3).?;

    const vote_req = RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp2 = node2.handleRequestVote(vote_req);
    const resp3 = node3.handleRequestVote(vote_req);

    try std.testing.expect(resp2.vote_granted);
    try std.testing.expect(resp3.vote_granted);

    try node1.becomeLeader();
    try std.testing.expectEqual(Role.leader, node1.getRole());
}

test "Leader Election: Election Safety - at most one leader per term" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 5);
    defer cluster.deinit();

    for (1..6) |i| {
        _ = try cluster.addNode(test_utils.testConfig(@intCast(i)));
    }

    const node1 = cluster.getNode(1).?;
    const node2 = cluster.getNode(2).?;

    try node1.startElection();
    try node2.startElection();

    try std.testing.expectEqual(@as(Term, 1), node1.getCurrentTerm());
    try std.testing.expectEqual(@as(Term, 1), node2.getCurrentTerm());

    const node3 = cluster.getNode(3).?;

    const vote_req1 = RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const vote_req2 = RequestVoteRequest{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp1 = node3.handleRequestVote(vote_req1);
    try std.testing.expect(resp1.vote_granted);

    const resp2 = node3.handleRequestVote(vote_req2);
    try std.testing.expect(!resp2.vote_granted);
}

test "Leader Election: Candidate rejects stale votes" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node2.startElection();
    try node2.startElection();
    try node2.startElection();
    try node2.startElection();
    try node2.startElection();

    try std.testing.expectEqual(@as(Term, 5), node2.getCurrentTerm());

    const resp = node2.handleRequestVote(.{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    });
    try std.testing.expect(!resp.vote_granted);
    try std.testing.expectEqual(@as(Term, 5), resp.term);

    try node1.stepDown(resp.term);
    try std.testing.expectEqual(@as(Term, 5), node1.getCurrentTerm());
}

test "Leader Election: Node with stale log cannot win" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.becomeLeader();
    _ = try node1.submitCommand("command1");
    _ = try node1.submitCommand("command2");

    try node2.startElection();

    const vote_req = RequestVoteRequest{
        .term = 2,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp = node1.handleRequestVote(vote_req);
    try std.testing.expect(!resp.vote_granted);
}

test "Leader Election: Higher term candidate gets vote" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try std.testing.expectEqual(@as(Term, 1), node1.getCurrentTerm());

    try node2.startElection();
    try node2.startElection();
    try std.testing.expectEqual(@as(Term, 2), node2.getCurrentTerm());

    const vote_req = RequestVoteRequest{
        .term = 2,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp = node1.handleRequestVote(vote_req);

    try std.testing.expect(resp.vote_granted);
    try std.testing.expectEqual(@as(Term, 2), node1.getCurrentTerm());
    try std.testing.expectEqual(Role.follower, node1.getRole());
}

test "Leader Election: Election timeout causes new election" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try std.testing.expect(!node1.isElectionTimeout());

    cluster.tick(350);

    try std.testing.expect(node1.isElectionTimeout());
}

test "Leader Election: Leader doesn't timeout" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    cluster.tick(1000);
    try std.testing.expect(!node1.isElectionTimeout());
    try std.testing.expectEqual(Role.leader, node1.getRole());
}

test "Leader Election: Pre-vote prevents disruption" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    const node2 = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    const initial_term = node1.getCurrentTerm();

    const heartbeat = raftz.AppendEntriesRequest{
        .term = initial_term,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    _ = try node2.handleAppendEntries(heartbeat);

    try std.testing.expect(!node2.isElectionTimeout());
}

test "Leader Election: Vote granted only with up-to-date log" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.becomeLeader();
    node1.mutex.lock();
    node1.persistent.current_term = 2;
    node1.mutex.unlock();

    _ = try node1.submitCommand("cmd1");
    _ = try node1.submitCommand("cmd2");

    const vote_req_old_term = RequestVoteRequest{
        .term = 3,
        .candidate_id = 2,
        .last_log_index = 2,
        .last_log_term = 1,
    };

    const resp1 = node1.handleRequestVote(vote_req_old_term);
    try std.testing.expect(!resp1.vote_granted);

    const vote_req_shorter = RequestVoteRequest{
        .term = 4,
        .candidate_id = 2,
        .last_log_index = 1,
        .last_log_term = 2,
    };

    const resp2 = node1.handleRequestVote(vote_req_shorter);
    try std.testing.expect(!resp2.vote_granted);

    const vote_req_current = RequestVoteRequest{
        .term = 5,
        .candidate_id = 2,
        .last_log_index = 2,
        .last_log_term = 2,
    };

    const resp3 = node1.handleRequestVote(vote_req_current);
    try std.testing.expect(resp3.vote_granted);
}

test "Leader Election: Invalid requests are rejected" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    const invalid_term_req = RequestVoteRequest{
        .term = 0,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp1 = node1.handleRequestVote(invalid_term_req);
    try std.testing.expect(!resp1.vote_granted);

    const invalid_candidate_req = RequestVoteRequest{
        .term = 1,
        .candidate_id = 0,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp2 = node1.handleRequestVote(invalid_candidate_req);
    try std.testing.expect(!resp2.vote_granted);
}

test "Leader Election: PreVote doesn't increment term" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();
    const initial_term = node1.getCurrentTerm();

    const pre_vote_req = raftz.PreVoteRequest{
        .term = initial_term + 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp = node1.handlePreVote(pre_vote_req);

    try std.testing.expectEqual(initial_term, node1.getCurrentTerm());
    try std.testing.expectEqual(Role.leader, node1.getRole());

    try std.testing.expect(!resp.vote_granted);
}

test "Leader Election: PreVote granted when no recent leader" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    cluster.tick(350);

    const pre_vote_req = raftz.PreVoteRequest{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp = node1.handlePreVote(pre_vote_req);

    try std.testing.expect(resp.vote_granted);

    try std.testing.expectEqual(@as(Term, 0), node1.getCurrentTerm());
}

test "Leader Election: PreVote rejects stale candidate" {
    const allocator = std.testing.allocator;

    var cluster = try test_utils.TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(test_utils.testConfig(1));
    _ = try cluster.addNode(test_utils.testConfig(2));
    _ = try cluster.addNode(test_utils.testConfig(3));

    try node1.becomeLeader();
    node1.mutex.lock();
    node1.persistent.current_term = 5;
    node1.mutex.unlock();
    _ = try node1.submitCommand("cmd1");

    const pre_vote_req = raftz.PreVoteRequest{
        .term = 4,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const resp = node1.handlePreVote(pre_vote_req);

    try std.testing.expect(!resp.vote_granted);
    try std.testing.expectEqual(@as(Term, 5), resp.term);
}
