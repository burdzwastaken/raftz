//! Test utilities and helpers for Raft testing

const std = @import("std");
const raftz = @import("raftz");

const Node = raftz.Node;
const ServerId = raftz.ServerId;
const Term = raftz.Term;
const LogIndex = raftz.LogIndex;
const Role = raftz.Role;
const Config = raftz.Config;
const ClusterConfig = raftz.ClusterConfig;
const Storage = raftz.Storage;
const StateMachine = raftz.StateMachine;
const KvStore = raftz.KvStore;
const Allocator = std.mem.Allocator;
const rpc = raftz.rpc;

/// Mock storage that matches Storage interface for testing
pub const MockStorage = struct {
    allocator: Allocator,
    fail_save_state: bool = false,
    fail_load_state: bool = false,
    fail_save_log: bool = false,
    fail_load_log: bool = false,
    fail_save_snapshot: bool = false,
    fail_load_snapshot: bool = false,
    state: struct {
        term: Term = 0,
        voted_for: ?ServerId = null,
        last_applied: LogIndex = 0,
    } = .{},
    log_entries: std.ArrayListUnmanaged(raftz.LogEntry) = .{},
    snapshot: ?struct {
        data: []const u8,
        last_index: LogIndex,
        last_term: Term,
        config_type: raftz.types.ConfigurationType,
        servers: []ServerId,
        new_servers: ?[]ServerId,
        learners: []ServerId,
    } = null,

    pub fn init(allocator: Allocator) MockStorage {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MockStorage) void {
        for (self.log_entries.items) |*entry| {
            var mutable_entry = entry.*;
            mutable_entry.data.deinit(self.allocator);
        }
        self.log_entries.deinit(self.allocator);
        if (self.snapshot) |snap| {
            self.allocator.free(snap.data);
            self.allocator.free(snap.servers);
            if (snap.new_servers) |ns| {
                self.allocator.free(ns);
            }
            self.allocator.free(snap.learners);
        }
    }

    pub fn saveState(self: *MockStorage, term: Term, voted_for: ?ServerId, last_applied: LogIndex) !void {
        if (self.fail_save_state) return error.MockStorageError;
        self.state.term = term;
        self.state.voted_for = voted_for;
        self.state.last_applied = last_applied;
    }

    pub fn loadState(self: *MockStorage) !struct { term: Term, voted_for: ?ServerId, last_applied: LogIndex } {
        if (self.fail_load_state) return error.MockStorageError;
        return .{
            .term = self.state.term,
            .voted_for = self.state.voted_for,
            .last_applied = self.state.last_applied,
        };
    }

    pub fn saveLog(self: *MockStorage, log: *const raftz.log.Log) !void {
        if (self.fail_save_log) return error.MockStorageError;

        for (self.log_entries.items) |*entry| {
            self.allocator.free(entry.command);
        }
        self.log_entries.clearRetainingCapacity();

        for (log.entries.items) |entry| {
            const cmd_copy = try self.allocator.dupe(u8, entry.command);
            try self.log_entries.append(self.allocator, .{
                .term = entry.term,
                .index = entry.index,
                .command = cmd_copy,
            });
        }
    }

    pub fn loadLog(self: *MockStorage, log: *raftz.log.Log) !void {
        if (self.fail_load_log) return error.MockStorageError;

        for (self.log_entries.items) |entry| {
            _ = try log.append(entry.term, entry.command);
        }
    }

    pub fn saveSnapshot(
        self: *MockStorage,
        data: []const u8,
        last_index: LogIndex,
        last_term: Term,
        cluster_config: ClusterConfig,
    ) !void {
        if (self.fail_save_snapshot) return error.MockStorageError;

        if (self.snapshot) |snap| {
            self.allocator.free(snap.data);
            self.allocator.free(snap.servers);
            if (snap.new_servers) |ns| {
                self.allocator.free(ns);
            }
            self.allocator.free(snap.learners);
        }

        const data_copy = try self.allocator.dupe(u8, data);
        const servers_copy = try self.allocator.dupe(ServerId, cluster_config.servers);
        const new_servers_copy = if (cluster_config.new_servers) |ns|
            try self.allocator.dupe(ServerId, ns)
        else
            null;
        const learners_copy = try self.allocator.dupe(ServerId, cluster_config.learners);

        self.snapshot = .{
            .data = data_copy,
            .last_index = last_index,
            .last_term = last_term,
            .config_type = cluster_config.config_type,
            .servers = servers_copy,
            .new_servers = new_servers_copy,
            .learners = learners_copy,
        };
    }

    pub fn loadSnapshot(self: *MockStorage) !?raftz.persistence.Storage.SnapshotData {
        if (self.fail_load_snapshot) return error.MockStorageError;

        if (self.snapshot) |snap| {
            const data_copy = try self.allocator.dupe(u8, snap.data);
            const servers_copy = try self.allocator.dupe(ServerId, snap.servers);
            const new_servers_copy = if (snap.new_servers) |ns|
                try self.allocator.dupe(ServerId, ns)
            else
                null;
            const learners_copy = try self.allocator.dupe(ServerId, snap.learners);

            return .{
                .data = data_copy,
                .last_index = snap.last_index,
                .last_term = snap.last_term,
                .config_type = snap.config_type,
                .servers = servers_copy,
                .new_servers = new_servers_copy,
                .learners = learners_copy,
            };
        }
        return null;
    }
};

/// Network partition state for simulating network failures
const PartitionState = struct {
    partitions: std.ArrayList(std.AutoHashMap(ServerId, void)),
    allocator: Allocator,

    pub fn init(allocator: Allocator) PartitionState {
        return .{
            .partitions = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PartitionState) void {
        for (self.partitions.items) |*partition| {
            partition.deinit();
        }
        self.partitions.deinit(self.allocator);
    }

    pub fn canCommunicate(self: *PartitionState, from: ServerId, to: ServerId) bool {
        if (self.partitions.items.len == 0) return true;

        for (self.partitions.items) |partition| {
            const has_from = partition.contains(from);
            const has_to = partition.contains(to);
            if (has_from and has_to) return true;
        }
        return false;
    }

    pub fn clear(self: *PartitionState) void {
        for (self.partitions.items) |*partition| {
            partition.deinit();
        }
        self.partitions.clearRetainingCapacity();
    }
};

/// Test cluster for simulating multiple Raft nodes
pub const TestCluster = struct {
    allocator: Allocator,
    nodes: std.ArrayList(*Node),
    storages: std.ArrayList(*MockStorage),
    state_machines: std.ArrayList(*KvStore),
    cluster_config: ClusterConfig,
    server_ids: []ServerId,
    partition_state: PartitionState,
    network_delay_ms: u64,

    pub fn init(allocator: Allocator, num_nodes: usize) !*TestCluster {
        const cluster = try allocator.create(TestCluster);
        errdefer allocator.destroy(cluster);

        const server_ids = try allocator.alloc(ServerId, num_nodes);
        errdefer allocator.free(server_ids);

        for (server_ids, 0..) |*id, i| {
            id.* = @intCast(i + 1);
        }

        cluster.* = .{
            .allocator = allocator,
            .nodes = .{},
            .storages = .{},
            .state_machines = .{},
            .cluster_config = ClusterConfig.simple(server_ids),
            .server_ids = server_ids,
            .partition_state = PartitionState.init(allocator),
            .network_delay_ms = 0,
        };

        return cluster;
    }

    pub fn deinit(self: *TestCluster) void {
        for (self.nodes.items) |node| {
            node.deinit();
            self.allocator.destroy(node);
        }
        self.nodes.deinit(self.allocator);

        for (self.storages.items) |storage| {
            storage.deinit();
            self.allocator.destroy(storage);
        }
        self.storages.deinit(self.allocator);

        for (self.state_machines.items) |sm| {
            sm.deinit();
            self.allocator.destroy(sm);
        }
        self.state_machines.deinit(self.allocator);

        self.partition_state.deinit();
        self.allocator.free(self.server_ids);
        self.allocator.destroy(self);
    }

    pub fn addNode(self: *TestCluster, config: Config) !*Node {
        const kv = try self.allocator.create(KvStore);
        kv.* = KvStore.init(self.allocator);
        try self.state_machines.append(self.allocator, kv);

        const node = try self.allocator.create(Node);
        node.* = try Node.init(
            self.allocator,
            config,
            self.cluster_config,
            kv.stateMachine(),
            null,
        );
        try self.nodes.append(self.allocator, node);

        return node;
    }

    pub fn getNode(self: *TestCluster, id: ServerId) ?*Node {
        for (self.nodes.items) |node| {
            if (node.config.id == id) return node;
        }
        return null;
    }

    pub fn countRoles(self: *TestCluster) struct { leaders: usize, followers: usize, candidates: usize } {
        var leaders: usize = 0;
        var followers: usize = 0;
        var candidates: usize = 0;

        for (self.nodes.items) |node| {
            switch (node.getRole()) {
                .leader => leaders += 1,
                .follower => followers += 1,
                .pre_candidate => {},
                .candidate => candidates += 1,
            }
        }

        return .{ .leaders = leaders, .followers = followers, .candidates = candidates };
    }

    pub fn getLeader(self: *TestCluster) ?*Node {
        for (self.nodes.items) |node| {
            if (node.getRole() == .leader) return node;
        }
        return null;
    }

    pub fn findLeader(self: *TestCluster, term: ?Term) ?*Node {
        for (self.nodes.items) |node| {
            if (node.getRole() == .leader) {
                if (term) |t| {
                    if (node.getCurrentTerm() == t) return node;
                } else {
                    return node;
                }
            }
        }
        return null;
    }

    pub fn assertOneLeader(self: *TestCluster) !*Node {
        const roles = self.countRoles();
        if (roles.leaders != 1) {
            return error.NoLeaderElected;
        }
        return self.getLeader().?;
    }

    pub fn waitForLeader(self: *TestCluster, timeout_ms: u64) !*Node {
        const start = std.time.milliTimestamp();
        while (true) {
            if (self.getLeader()) |leader| {
                return leader;
            }

            const elapsed: u64 = @intCast(std.time.milliTimestamp() - start);
            if (elapsed >= timeout_ms) {
                return error.TimeoutWaitingForLeader;
            }

            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }

    pub fn waitForStableLeader(self: *TestCluster, timeout_ms: u64, stability_ms: u64) !*Node {
        const start = std.time.milliTimestamp();
        var current_leader: ?*Node = null;
        var stable_since: i64 = 0;

        while (true) {
            const leader = self.getLeader();

            if (leader != current_leader) {
                current_leader = leader;
                stable_since = std.time.milliTimestamp();
            }

            if (current_leader) |stable_leader| {
                const stable_duration: u64 = @intCast(std.time.milliTimestamp() - stable_since);
                if (stable_duration >= stability_ms) {
                    return stable_leader;
                }
            }

            const elapsed: u64 = @intCast(std.time.milliTimestamp() - start);
            if (elapsed >= timeout_ms) {
                return error.TimeoutWaitingForStableLeader;
            }

            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }

    pub fn tick(self: *TestCluster, ms: i64) void {
        for (self.nodes.items) |node| {
            node.mutex.lock();
            node.last_heartbeat -= ms;
            node.mutex.unlock();
        }
    }

    /// Partition the cluster into isolated groups
    pub fn partition(self: *TestCluster, partitions: []const []const ServerId) !void {
        self.partition_state.clear();

        for (partitions) |group| {
            var partition_map = std.AutoHashMap(ServerId, void).init(self.allocator);
            for (group) |server_id| {
                try partition_map.put(server_id, {});
            }
            try self.partition_state.partitions.append(self.allocator, partition_map);
        }
    }

    pub fn healPartitions(self: *TestCluster) void {
        self.partition_state.clear();
    }

    /// Isolate a single node from the rest of the cluster
    pub fn isolateNode(self: *TestCluster, id: ServerId) !void {
        var isolated = std.AutoHashMap(ServerId, void).init(self.allocator);
        try isolated.put(id, {});

        var others = std.AutoHashMap(ServerId, void).init(self.allocator);
        for (self.server_ids) |server_id| {
            if (server_id != id) {
                try others.put(server_id, {});
            }
        }

        self.partition_state.clear();
        try self.partition_state.partitions.append(self.allocator, isolated);
        try self.partition_state.partitions.append(self.allocator, others);
    }

    pub fn canCommunicate(self: *TestCluster, from: ServerId, to: ServerId) bool {
        return self.partition_state.canCommunicate(from, to);
    }

    pub fn setNetworkDelay(self: *TestCluster, delay_ms: u64) void {
        self.network_delay_ms = delay_ms;
    }

    pub fn sendRequestVote(
        self: *TestCluster,
        from: ServerId,
        to: ServerId,
        request: raftz.RequestVoteRequest,
    ) ?rpc.RequestVoteResponse {
        if (!self.canCommunicate(from, to)) {
            return null;
        }

        if (self.network_delay_ms > 0) {
            std.Thread.sleep(self.network_delay_ms * std.time.ns_per_ms);
        }

        const target = self.getNode(to) orelse return null;
        return target.handleRequestVote(request);
    }

    pub fn sendPreVote(
        self: *TestCluster,
        from: ServerId,
        to: ServerId,
        request: raftz.PreVoteRequest,
    ) ?rpc.PreVoteResponse {
        if (!self.canCommunicate(from, to)) {
            return null;
        }

        if (self.network_delay_ms > 0) {
            std.Thread.sleep(self.network_delay_ms * std.time.ns_per_ms);
        }

        const target = self.getNode(to) orelse return null;
        return target.handlePreVote(request);
    }

    pub fn sendAppendEntries(
        self: *TestCluster,
        from: ServerId,
        to: ServerId,
        request: raftz.AppendEntriesRequest,
    ) ?rpc.AppendEntriesResponse {
        if (!self.canCommunicate(from, to)) {
            return null;
        }

        if (self.network_delay_ms > 0) {
            std.Thread.sleep(self.network_delay_ms * std.time.ns_per_ms);
        }

        const target = self.getNode(to) orelse return null;
        return target.handleAppendEntries(request) catch return null;
    }

    pub fn sendInstallSnapshot(
        self: *TestCluster,
        from: ServerId,
        to: ServerId,
        request: rpc.InstallSnapshotRequest,
    ) ?rpc.InstallSnapshotResponse {
        if (!self.canCommunicate(from, to)) {
            return null;
        }

        if (self.network_delay_ms > 0) {
            std.Thread.sleep(self.network_delay_ms * std.time.ns_per_ms);
        }

        const target = self.getNode(to) orelse return null;
        return target.handleInstallSnapshot(request) catch return null;
    }

    pub fn sendTimeoutNow(
        self: *TestCluster,
        from: ServerId,
        to: ServerId,
        request: raftz.TimeoutNowRequest,
    ) ?rpc.TimeoutNowResponse {
        if (!self.canCommunicate(from, to)) {
            return null;
        }

        if (self.network_delay_ms > 0) {
            std.Thread.sleep(self.network_delay_ms * std.time.ns_per_ms);
        }

        const target = self.getNode(to) orelse return null;
        return target.handleTimeoutNow(request) catch return null;
    }

    pub fn broadcastRequestVote(
        self: *TestCluster,
        from: ServerId,
        request: raftz.RequestVoteRequest,
    ) std.ArrayList(rpc.RequestVoteResponse) {
        var responses: std.ArrayList(rpc.RequestVoteResponse) = .{};

        for (self.server_ids) |server_id| {
            if (server_id == from) continue;

            if (self.sendRequestVote(from, server_id, request)) |response| {
                responses.append(self.allocator, response) catch {};
            }
        }

        return responses;
    }

    pub fn broadcastPreVote(
        self: *TestCluster,
        from: ServerId,
        request: raftz.PreVoteRequest,
    ) std.ArrayList(rpc.PreVoteResponse) {
        var responses: std.ArrayList(rpc.PreVoteResponse) = .{};

        for (self.server_ids) |server_id| {
            if (server_id == from) continue;

            if (self.sendPreVote(from, server_id, request)) |response| {
                responses.append(self.allocator, response) catch {};
            }
        }

        return responses;
    }

    pub fn broadcastAppendEntries(
        self: *TestCluster,
        from: ServerId,
        request: raftz.AppendEntriesRequest,
    ) std.ArrayList(rpc.AppendEntriesResponse) {
        var responses: std.ArrayList(rpc.AppendEntriesResponse) = .{};

        for (self.server_ids) |server_id| {
            if (server_id == from) continue;

            if (self.sendAppendEntries(from, server_id, request)) |response| {
                responses.append(self.allocator, response) catch {};
            }
        }

        return responses;
    }

    pub fn inspectState(self: *TestCluster) ClusterState {
        var state = ClusterState{
            .nodes = .{},
        };

        for (self.nodes.items) |node| {
            node.mutex.lock();
            defer node.mutex.unlock();

            state.nodes.append(self.allocator, .{
                .id = node.config.id,
                .role = node.role,
                .term = node.persistent.current_term,
                .commit_index = node.volatile_state.commit_index,
                .last_applied = node.volatile_state.last_applied,
                .log_length = node.log.lastIndex(),
                .voted_for = node.persistent.voted_for,
                .current_leader = node.current_leader,
            }) catch {};
        }

        return state;
    }

    /// Check if all nodes have the same committed log
    pub fn checkLogConsistency(self: *TestCluster) !void {
        if (self.nodes.items.len == 0) return;

        var max_commit: LogIndex = 0;
        for (self.nodes.items) |node| {
            node.mutex.lock();
            const commit_idx = node.volatile_state.commit_index;
            node.mutex.unlock();

            if (commit_idx > max_commit) {
                max_commit = commit_idx;
            }
        }

        if (max_commit == 0) return;

        var idx: LogIndex = 1;
        while (idx <= max_commit) : (idx += 1) {
            var first_entry: ?struct { term: Term, data: []const u8 } = null;

            for (self.nodes.items) |node| {
                node.mutex.lock();
                defer node.mutex.unlock();

                if (idx > node.volatile_state.commit_index) continue;

                const entry = node.log.get(idx) orelse continue;
                const entry_data = switch (entry.data) {
                    .command => |cmd| cmd,
                    .configuration => "",
                };

                if (first_entry) |first| {
                    if (first.term != entry.term or !std.mem.eql(u8, first.data, entry_data)) {
                        return error.LogInconsistency;
                    }
                } else {
                    first_entry = .{ .term = entry.term, .data = entry_data };
                }
            }
        }
    }

    /// Wait for all nodes to commit up to a certain index
    pub fn waitForCommit(self: *TestCluster, target_index: LogIndex, timeout_ms: u64) !void {
        const start = std.time.milliTimestamp();

        while (true) {
            var all_committed = true;

            for (self.nodes.items) |node| {
                node.mutex.lock();
                const commit_idx = node.volatile_state.commit_index;
                node.mutex.unlock();

                if (commit_idx < target_index) {
                    all_committed = false;
                    break;
                }
            }

            if (all_committed) return;

            const elapsed: u64 = @intCast(std.time.milliTimestamp() - start);
            if (elapsed >= timeout_ms) {
                return error.TimeoutWaitingForCommit;
            }

            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }
};

/// Nodes state snapshot
pub const NodeState = struct {
    id: ServerId,
    role: Role,
    term: Term,
    commit_index: LogIndex,
    last_applied: LogIndex,
    log_length: LogIndex,
    voted_for: ?ServerId,
    current_leader: ?ServerId,
};

/// Cluster state snapshot
pub const ClusterState = struct {
    nodes: std.ArrayList(NodeState),

    pub fn deinit(self: *ClusterState, allocator: Allocator) void {
        self.nodes.deinit(allocator);
    }

    pub fn print(self: *ClusterState) void {
        std.debug.print("\nCluster State:\n", .{});
        for (self.nodes.items) |node| {
            std.debug.print("Node {d}: {s} term={d} commit={d} applied={d} log_len={d} voted_for={?d} leader={?d}\n", .{
                node.id,
                @tagName(node.role),
                node.term,
                node.commit_index,
                node.last_applied,
                node.log_length,
                node.voted_for,
                node.current_leader,
            });
        }
        std.debug.print("\n", .{});
    }
};

/// Wait for a condition to be true with timeout
pub fn waitFor(
    condition: anytype,
    timeout_ms: u64,
    check_interval_ms: u64,
) !void {
    const start = std.time.milliTimestamp();
    while (true) {
        if (@call(.auto, condition, .{})) {
            return;
        }

        const elapsed: u64 = @intCast(std.time.milliTimestamp() - start);
        if (elapsed >= timeout_ms) {
            return error.Timeout;
        }

        std.time.sleep(check_interval_ms * std.time.ns_per_ms);
    }
}

/// Create a test config with deterministic timeouts for testing
pub fn testConfig(id: ServerId) Config {
    return .{
        .id = id,
        .election_timeout_min = 150,
        .election_timeout_max = 300,
        .heartbeat_interval = 50,
        .max_append_entries = 100,
    };
}

test "TestCluster: Setup" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    _ = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));

    try std.testing.expectEqual(@as(usize, 3), cluster.nodes.items.len);

    const roles = cluster.countRoles();
    try std.testing.expectEqual(@as(usize, 3), roles.followers);
    try std.testing.expectEqual(@as(usize, 0), roles.leaders);
}

test "TestCluster: Wait for leader" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    const leader = try cluster.waitForLeader(1000);
    try std.testing.expectEqual(node1.config.id, leader.config.id);
}

test "TestCluster: Find leader with term" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();
    const term = node1.getCurrentTerm();

    const leader = cluster.findLeader(term);
    try std.testing.expect(leader != null);
    try std.testing.expectEqual(term, leader.?.getCurrentTerm());

    const no_leader = cluster.findLeader(term + 1);
    try std.testing.expect(no_leader == null);
}

test "TestCluster: Network Partitions" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 5);
    defer cluster.deinit();

    _ = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));
    _ = try cluster.addNode(testConfig(4));
    _ = try cluster.addNode(testConfig(5));

    try std.testing.expect(cluster.canCommunicate(1, 2));
    try std.testing.expect(cluster.canCommunicate(1, 5));

    const partition1 = [_]ServerId{ 1, 2 };
    const partition2 = [_]ServerId{ 3, 4, 5 };
    const partitions = [_][]const ServerId{ &partition1, &partition2 };
    try cluster.partition(&partitions);

    try std.testing.expect(cluster.canCommunicate(1, 2));
    try std.testing.expect(cluster.canCommunicate(3, 5));

    try std.testing.expect(!cluster.canCommunicate(1, 3));
    try std.testing.expect(!cluster.canCommunicate(2, 5));

    cluster.healPartitions();
    try std.testing.expect(cluster.canCommunicate(1, 3));
    try std.testing.expect(cluster.canCommunicate(2, 5));
}

test "TestCluster: Isolate node" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    _ = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));

    try cluster.isolateNode(1);

    try std.testing.expect(!cluster.canCommunicate(1, 2));
    try std.testing.expect(!cluster.canCommunicate(1, 3));

    try std.testing.expect(cluster.canCommunicate(2, 3));
}

test "TestCluster: State inspection" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    var state = cluster.inspectState();
    defer state.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), state.nodes.items.len);

    var found_leader = false;
    for (state.nodes.items) |node_state| {
        if (node_state.role == .leader) {
            found_leader = true;
            try std.testing.expectEqual(node1.config.id, node_state.id);
        }
    }
    try std.testing.expect(found_leader);
}

test "TestCluster: Log consistency check" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(testConfig(1));
    const node2 = try cluster.addNode(testConfig(2));
    const node3 = try cluster.addNode(testConfig(3));

    try node1.startElection();
    try node1.becomeLeader();

    _ = try node1.submitCommand("cmd1");
    _ = try node1.submitCommand("cmd2");

    const leader_term = node1.getCurrentTerm();

    node2.mutex.lock();
    _ = try node2.log.append(leader_term, "cmd1");
    _ = try node2.log.append(leader_term, "cmd2");
    node2.volatile_state.commit_index = 2;
    node2.mutex.unlock();

    node3.mutex.lock();
    _ = try node3.log.append(leader_term, "cmd1");
    _ = try node3.log.append(leader_term, "cmd2");
    node3.volatile_state.commit_index = 2;
    node3.mutex.unlock();

    node1.mutex.lock();
    node1.volatile_state.commit_index = 2;
    node1.mutex.unlock();

    try cluster.checkLogConsistency();

    node3.mutex.lock();
    if (node3.log.entries.items.len > 0) {
        const len = node3.log.entries.items.len;
        var popped = node3.log.entries.items[len - 1];
        popped.deinit(allocator);
        _ = node3.log.entries.pop();
    }
    _ = try node3.log.append(leader_term, "different_cmd");
    node3.mutex.unlock();

    const result = cluster.checkLogConsistency();
    try std.testing.expectError(error.LogInconsistency, result);
}

test "TestCluster: Network delay simulation" {
    const allocator = std.testing.allocator;

    var cluster = try TestCluster.init(allocator, 3);
    defer cluster.deinit();

    const node1 = try cluster.addNode(testConfig(1));
    _ = try cluster.addNode(testConfig(2));
    _ = try cluster.addNode(testConfig(3));

    try node1.startElection();

    const vote_req = raftz.RequestVoteRequest{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const start1 = std.time.milliTimestamp();
    _ = cluster.sendRequestVote(1, 2, vote_req);
    const elapsed1: u64 = @intCast(std.time.milliTimestamp() - start1);

    cluster.setNetworkDelay(50);

    const start2 = std.time.milliTimestamp();
    _ = cluster.sendRequestVote(1, 2, vote_req);
    const elapsed2: u64 = @intCast(std.time.milliTimestamp() - start2);

    try std.testing.expect(elapsed2 >= 50);
    try std.testing.expect(elapsed2 > elapsed1);
}

test "MockStorage: Basic operations" {
    const allocator = std.testing.allocator;

    var storage = MockStorage.init(allocator);
    defer storage.deinit();

    try storage.saveState(5, 2, 10);
    const state = try storage.loadState();

    try std.testing.expectEqual(@as(Term, 5), state.term);
    try std.testing.expectEqual(@as(?ServerId, 2), state.voted_for);
    try std.testing.expectEqual(@as(LogIndex, 10), state.last_applied);
}
