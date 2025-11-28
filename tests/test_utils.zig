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

    pub fn saveSnapshot(self: *MockStorage, data: []const u8, last_index: LogIndex, last_term: Term) !void {
        if (self.fail_save_snapshot) return error.MockStorageError;

        if (self.snapshot) |snap| {
            self.allocator.free(snap.data);
        }

        const data_copy = try self.allocator.dupe(u8, data);
        self.snapshot = .{
            .data = data_copy,
            .last_index = last_index,
            .last_term = last_term,
        };
    }

    pub fn loadSnapshot(self: *MockStorage) !?struct { data: []const u8, last_index: LogIndex, last_term: Term } {
        if (self.fail_load_snapshot) return error.MockStorageError;

        if (self.snapshot) |snap| {
            const data_copy = try self.allocator.dupe(u8, snap.data);
            return .{
                .data = data_copy,
                .last_index = snap.last_index,
                .last_term = snap.last_term,
            };
        }
        return null;
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
            .cluster_config = ClusterConfig.single(server_ids),
            .server_ids = server_ids,
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
            null, // no persistent storage for now...
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
                .pre_candidate => {}, // transitional state
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

    pub fn assertOneLeader(self: *TestCluster) !*Node {
        const roles = self.countRoles();
        if (roles.leaders != 1) {
            return error.NoLeaderElected;
        }
        return self.getLeader().?;
    }

    pub fn tick(self: *TestCluster, ms: i64) void {
        for (self.nodes.items) |node| {
            node.mutex.lock();
            node.last_heartbeat -= ms;
            node.mutex.unlock();
        }
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

test "TestCluster basic setup" {
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

test "MockStorage basic operations" {
    const allocator = std.testing.allocator;

    var storage = MockStorage.init(allocator);
    defer storage.deinit();

    try storage.saveState(5, 2, 10);
    const state = try storage.loadState();

    try std.testing.expectEqual(@as(Term, 5), state.term);
    try std.testing.expectEqual(@as(?ServerId, 2), state.voted_for);
    try std.testing.expectEqual(@as(LogIndex, 10), state.last_applied);
}
