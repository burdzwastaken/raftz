//! Crash Recovery Tests

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
const Storage = raftz.Storage;
const rpc = raftz.rpc;

test "Crash Recovery: Term persists across restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_term";
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

        try node.startElection();
        try node.startElection();
        try node.startElection();

        try std.testing.expectEqual(@as(Term, 3), node.getCurrentTerm());
    }

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

        try std.testing.expectEqual(@as(Term, 3), node.getCurrentTerm());
    }
}

test "Crash Recovery: Voted for persists across restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_voted";
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

        const vote_req = RequestVoteRequest{
            .term = 1,
            .candidate_id = 2,
            .last_log_index = 0,
            .last_log_term = 0,
        };

        const resp = node.handleRequestVote(vote_req);
        try std.testing.expect(resp.vote_granted);
    }

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

        const vote_req = RequestVoteRequest{
            .term = 1,
            .candidate_id = 3,
            .last_log_index = 0,
            .last_log_term = 0,
        };

        const resp = node.handleRequestVote(vote_req);
        try std.testing.expect(!resp.vote_granted);
    }
}

test "Crash Recovery: Log persists across restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_log";
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

        _ = try node.submitCommand("SET x 1");
        _ = try node.submitCommand("SET y 2");
        _ = try node.submitCommand("SET z 3");

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 3), node.log.lastIndex());
        node.mutex.unlock();
    }

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

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 3), node.log.lastIndex());

        const entry1 = node.log.get(1).?;
        try std.testing.expectEqualStrings("SET x 1", entry1.data.command);
        node.mutex.unlock();
    }
}

test "Crash Recovery: Last applied persists" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_applied";
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

        node.mutex.lock();
        _ = try node.log.append(1, "cmd1");
        _ = try node.log.append(1, "cmd2");
        node.volatile_state.commit_index = 2;
        node.mutex.unlock();

        try node.applyCommitted();

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.last_applied);
        node.mutex.unlock();
    }

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

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.last_applied);
        node.mutex.unlock();
    }
}

test "Crash Recovery: Node starts as follower after restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_role";
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

        try node.startElection();
        try node.becomeLeader();

        try std.testing.expectEqual(Role.leader, node.getRole());
    }

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

        try std.testing.expectEqual(Role.follower, node.getRole());
    }
}

test "Crash Recovery: Leader unknown after restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_leader_unknown";
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

        const heartbeat = AppendEntriesRequest{
            .term = 1,
            .leader_id = 2,
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &.{},
            .leader_commit = 0,
        };

        _ = try node.handleAppendEntries(heartbeat);
        try std.testing.expectEqual(@as(?ServerId, 2), node.getLeader());
    }

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

        try std.testing.expectEqual(@as(?ServerId, null), node.getLeader());
    }
}

test "Crash Recovery: Snapshot restored on restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_snapshot";
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

        _ = try node.submitCommand("SET a 1");
        _ = try node.submitCommand("SET b 2");
        _ = try node.submitCommand("SET c 3");
        _ = try node.submitCommand("SET d 4");
        _ = try node.submitCommand("SET e 5");

        node.mutex.lock();
        node.volatile_state.commit_index = 5;
        node.mutex.unlock();

        try node.applyCommitted();
        try node.createSnapshot();
    }

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

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 5), node.volatile_state.last_applied);
        try std.testing.expectEqual(@as(LogIndex, 5), node.volatile_state.commit_index);
        node.mutex.unlock();
    }
}

test "Crash Recovery: Entries after snapshot preserved" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_entries_after_snap";
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

        _ = try node.submitCommand("cmd1");
        _ = try node.submitCommand("cmd2");
        _ = try node.submitCommand("cmd3");

        node.mutex.lock();
        node.volatile_state.commit_index = 3;
        node.mutex.unlock();

        try node.applyCommitted();
        try node.createSnapshot();

        _ = try node.submitCommand("cmd4");
        _ = try node.submitCommand("cmd5");
    }

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

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 3), node.volatile_state.last_applied);
        try std.testing.expect(node.log.lastIndex() >= 2);
        node.mutex.unlock();
    }
}

test "Crash Recovery: Election after cluster restart" {
    const allocator = std.testing.allocator;

    const dir1 = "test_crash_cluster1";
    const dir2 = "test_crash_cluster2";
    const dir3 = "test_crash_cluster3";

    std.fs.cwd().makeDir(dir1) catch {};
    std.fs.cwd().makeDir(dir2) catch {};
    std.fs.cwd().makeDir(dir3) catch {};
    defer {
        std.fs.cwd().deleteTree(dir1) catch {};
        std.fs.cwd().deleteTree(dir2) catch {};
        std.fs.cwd().deleteTree(dir3) catch {};
    }

    var storage1 = try Storage.init(allocator, dir1);
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, dir2);
    defer storage2.deinit();
    var storage3 = try Storage.init(allocator, dir3);
    defer storage3.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig.simple(&servers);

    var final_term: Term = 0;

    {
        var kv1 = KvStore.init(allocator);
        defer kv1.deinit();
        var kv2 = KvStore.init(allocator);
        defer kv2.deinit();
        var kv3 = KvStore.init(allocator);
        defer kv3.deinit();

        var node1 = try Node.init(allocator, test_utils.testConfig(1), cluster, kv1.stateMachine(), &storage1);
        defer node1.deinit();
        var node2 = try Node.init(allocator, test_utils.testConfig(2), cluster, kv2.stateMachine(), &storage2);
        defer node2.deinit();
        var node3 = try Node.init(allocator, test_utils.testConfig(3), cluster, kv3.stateMachine(), &storage3);
        defer node3.deinit();

        try node1.startElection();

        const vote_req = RequestVoteRequest{
            .term = 1,
            .candidate_id = 1,
            .last_log_index = 0,
            .last_log_term = 0,
        };

        _ = node2.handleRequestVote(vote_req);
        _ = node3.handleRequestVote(vote_req);

        try node1.becomeLeader();
        final_term = node1.getCurrentTerm();
    }

    {
        var kv1 = KvStore.init(allocator);
        defer kv1.deinit();
        var kv2 = KvStore.init(allocator);
        defer kv2.deinit();
        var kv3 = KvStore.init(allocator);
        defer kv3.deinit();

        var node1 = try Node.init(allocator, test_utils.testConfig(1), cluster, kv1.stateMachine(), &storage1);
        defer node1.deinit();
        var node2 = try Node.init(allocator, test_utils.testConfig(2), cluster, kv2.stateMachine(), &storage2);
        defer node2.deinit();
        var node3 = try Node.init(allocator, test_utils.testConfig(3), cluster, kv3.stateMachine(), &storage3);
        defer node3.deinit();

        try std.testing.expectEqual(Role.follower, node1.getRole());
        try std.testing.expectEqual(final_term, node1.getCurrentTerm());

        try node2.startElection();
        try std.testing.expect(node2.getCurrentTerm() > final_term);
    }
}

test "Crash Recovery: Uncommitted entries don't affect state machine" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_uncommitted";
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

        _ = try node.submitCommand("cmd1");
        _ = try node.submitCommand("cmd2");

        node.mutex.lock();
        node.volatile_state.commit_index = 2;
        node.mutex.unlock();

        try node.applyCommitted();

        _ = try node.submitCommand("uncommitted1");
        _ = try node.submitCommand("uncommitted2");

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 4), node.log.lastIndex());
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.commit_index);
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.last_applied);
        node.mutex.unlock();
    }

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

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 2), node.volatile_state.last_applied);
        node.mutex.unlock();
    }
}

test "Crash Recovery: Follower catches up after restart" {
    const allocator = std.testing.allocator;

    const dir_path = "test_crash_catchup";
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
            test_utils.testConfig(2),
            cluster,
            kv.stateMachine(),
            &storage,
        );
        defer node.deinit();

        const entries = [_]LogEntry{
            LogEntry.command(1, 1, "cmd1"),
            LogEntry.command(1, 2, "cmd2"),
        };

        const req = AppendEntriesRequest{
            .term = 1,
            .leader_id = 1,
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &entries,
            .leader_commit = 2,
        };

        _ = try node.handleAppendEntries(req);
    }

    {
        var kv = KvStore.init(allocator);
        defer kv.deinit();

        var node = try Node.init(
            allocator,
            test_utils.testConfig(2),
            cluster,
            kv.stateMachine(),
            &storage,
        );
        defer node.deinit();

        node.mutex.lock();
        const initial_log_len = node.log.lastIndex();
        node.mutex.unlock();

        const new_entries = [_]LogEntry{
            LogEntry.command(1, 3, "cmd3"),
            LogEntry.command(1, 4, "cmd4"),
        };

        const req = AppendEntriesRequest{
            .term = 1,
            .leader_id = 1,
            .prev_log_index = initial_log_len,
            .prev_log_term = 1,
            .entries = &new_entries,
            .leader_commit = 4,
        };

        const resp = try node.handleAppendEntries(req);
        try std.testing.expect(resp.success);

        node.mutex.lock();
        try std.testing.expectEqual(@as(LogIndex, 4), node.log.lastIndex());
        try std.testing.expectEqual(@as(LogIndex, 4), node.volatile_state.commit_index);
        node.mutex.unlock();
    }
}
