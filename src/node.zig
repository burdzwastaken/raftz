//! Raft node implementation handling consensus operations
//!
//! Node is the core component that implements the Raft consensus algorithm,
//! managing leader election, log replication and state machine application

const std = @import("std");
const types = @import("types.zig");
const log_mod = @import("log.zig");
const rpc = @import("rpc.zig");
const state_machine_mod = @import("state_machine.zig");
const persistence_mod = @import("persistence.zig");
const logging = @import("logging.zig");

const Role = types.Role;
const Term = types.Term;
const LogIndex = types.LogIndex;
const ServerId = types.ServerId;
const Config = types.Config;
const ClusterConfig = types.ClusterConfig;
const Log = log_mod.Log;
const StateMachine = state_machine_mod.StateMachine;
const Storage = persistence_mod.Storage;
const Allocator = std.mem.Allocator;

/// Persistent state on all servers
const PersistentState = struct {
    current_term: Term = 0,
    voted_for: ?ServerId = null,
};

/// Volatile state on all servers
const VolatileState = struct {
    commit_index: LogIndex = 0,
    last_applied: LogIndex = 0,
};

/// Pending read index request awaiting leadership confirmation
const ReadIndexState = struct {
    read_id: u64,
    read_index: LogIndex,
    acks: usize,
    timestamp: i64,
};

/// Volatile state on leaders
const LeaderState = struct {
    next_index: std.AutoHashMap(ServerId, LogIndex),
    match_index: std.AutoHashMap(ServerId, LogIndex),
    pending_reads: std.ArrayListUnmanaged(ReadIndexState),
    next_read_id: u64,

    pub fn init(allocator: Allocator) LeaderState {
        return .{
            .next_index = std.AutoHashMap(ServerId, LogIndex).init(allocator),
            .match_index = std.AutoHashMap(ServerId, LogIndex).init(allocator),
            .pending_reads = .{},
            .next_read_id = 1,
        };
    }

    pub fn deinit(self: *LeaderState, allocator: Allocator) void {
        self.next_index.deinit();
        self.match_index.deinit();
        self.pending_reads.deinit(allocator);
    }

    pub fn reset(self: *LeaderState, cluster: ClusterConfig, last_log_index: LogIndex) !void {
        self.next_index.clearRetainingCapacity();
        self.match_index.clearRetainingCapacity();

        for (cluster.servers) |server_id| {
            try self.next_index.put(server_id, last_log_index + 1);
            try self.match_index.put(server_id, 0);
        }
    }
};

/// Node handles:
/// - Leader election with randomized timeouts
/// - Log replication from leader to followers
/// - State machine application of committed entries
/// - Persistent state management via optional Storage
pub const Node = struct {
    allocator: Allocator,
    config: Config,
    cluster: ClusterConfig,
    role: Role,
    persistent: PersistentState,
    volatile_state: VolatileState,
    leader: ?LeaderState,
    log: Log,
    state_machine: StateMachine,
    storage: ?*Storage,
    current_leader: ?ServerId,
    last_heartbeat: i64,
    election_timeout: u64,
    mutex: std.Thread.Mutex,

    /// Initialize a new Raft node
    ///
    /// If storage is provided, loads persistent state, log entries and snapshots
    /// from disk to recover from a previous run
    pub fn init(
        allocator: Allocator,
        config: Config,
        cluster: ClusterConfig,
        state_machine: StateMachine,
        storage: ?*Storage,
    ) !Node {
        try config.validate();

        var node = Node{
            .allocator = allocator,
            .config = config,
            .cluster = cluster,
            .role = .follower,
            .persistent = .{},
            .volatile_state = .{},
            .leader = null,
            .log = Log.init(allocator),
            .state_machine = state_machine,
            .storage = storage,
            .current_leader = null,
            .last_heartbeat = std.time.milliTimestamp(),
            .election_timeout = config.randomElectionTimeout(),
            .mutex = .{},
        };

        if (storage) |s| {
            const state = try s.loadState();
            node.persistent.current_term = state.term;
            node.persistent.voted_for = state.voted_for;
            node.volatile_state.last_applied = state.last_applied;

            try node.loadSnapshot();

            try s.loadLog(&node.log);

            logging.info("Node {d}: Loaded state - term={d}, log_size={d}, last_applied={d}", .{
                config.id,
                state.term,
                node.log.lastIndex(),
                node.volatile_state.last_applied,
            });
        }

        return node;
    }

    pub fn deinit(self: *Node) void {
        self.log.deinit();
        if (self.leader) |*leader| {
            leader.deinit(self.allocator);
        }
    }

    /// Start pre-vote phase before actual election (if enabled)
    pub fn startPreVote(self: *Node) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.config.enable_prevote) {
            return;
        }

        self.role = .pre_candidate;
        self.election_timeout = self.config.randomElectionTimeout();
        self.last_heartbeat = std.time.milliTimestamp();

        logging.info("Node {d}: Starting pre-vote for term {d}", .{
            self.config.id,
            self.persistent.current_term + 1,
        });
    }

    /// Start a new leader election
    pub fn startElection(self: *Node) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const new_term = self.persistent.current_term + 1;
        const new_voted_for = self.config.id;

        if (self.storage) |storage| {
            try storage.saveState(new_term, new_voted_for, self.volatile_state.last_applied);
        }

        self.role = .candidate;
        self.persistent.current_term = new_term;
        self.persistent.voted_for = new_voted_for;
        self.election_timeout = self.config.randomElectionTimeout();
        self.last_heartbeat = std.time.milliTimestamp();

        logging.info("Node {d}: Starting election for term {d}", .{
            self.config.id,
            self.persistent.current_term,
        });
    }

    pub fn becomeLeader(self: *Node) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        logging.info("Node {d}: Becoming leader for term {d}", .{
            self.config.id,
            self.persistent.current_term,
        });

        self.role = .leader;
        self.current_leader = self.config.id;

        var leader_state = LeaderState.init(self.allocator);
        errdefer leader_state.deinit(self.allocator);

        try leader_state.reset(self.cluster, self.log.lastIndex());

        if (self.leader) |*old_leader| {
            old_leader.deinit(self.allocator);
        }
        self.leader = leader_state;
    }

    pub fn stepDownLocked(self: *Node, new_term: Term) !void {
        if (new_term > self.persistent.current_term) {
            self.persistent.current_term = new_term;
            self.persistent.voted_for = null;

            if (self.storage) |storage| {
                try storage.saveState(
                    self.persistent.current_term,
                    self.persistent.voted_for,
                    self.volatile_state.last_applied,
                );
            }
        }

        if (self.role != .follower and self.role != .pre_candidate) {
            logging.info("Node {d}: Stepping down to follower for term {d}", .{
                self.config.id,
                self.persistent.current_term,
            });

            self.role = .follower;
            if (self.leader) |*leader| {
                leader.deinit(self.allocator);
                self.leader = null;
            }
        } else if (self.role == .pre_candidate) {
            self.role = .follower;
        }
    }

    pub fn stepDown(self: *Node, new_term: Term) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.stepDownLocked(new_term);
    }

    /// Handle PreVote RPC - similar to RequestVote but doesn't update term or grant actual vote
    pub fn handlePreVote(
        self: *Node,
        request: rpc.PreVoteRequest,
    ) rpc.PreVoteResponse {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (request.term == 0 or request.candidate_id == 0) {
            logging.warn("Node {d}: Invalid PreVote with term={d}, candidate={d}", .{
                self.config.id,
                request.term,
                request.candidate_id,
            });
            return .{
                .term = self.persistent.current_term,
                .vote_granted = false,
            };
        }

        if (request.term < self.persistent.current_term) {
            return .{
                .term = self.persistent.current_term,
                .vote_granted = false,
            };
        }

        const now = std.time.milliTimestamp();
        const elapsed: u64 = @intCast(now - self.last_heartbeat);
        const heard_from_leader = elapsed < self.election_timeout;

        if (self.role == .leader and heard_from_leader) {
            return .{
                .term = self.persistent.current_term,
                .vote_granted = false,
            };
        }

        if (heard_from_leader) {
            return .{
                .term = self.persistent.current_term,
                .vote_granted = false,
            };
        }

        const our_last_term = self.log.lastTerm();
        const our_last_index = self.log.lastIndex();

        const vote_granted = blk: {
            if (request.last_log_term < our_last_term) {
                break :blk false;
            }

            if (request.last_log_term == our_last_term and
                request.last_log_index < our_last_index)
            {
                break :blk false;
            }

            break :blk true;
        };

        if (vote_granted) {
            logging.debug("Node {d}: Granted pre-vote to {d} for term {d}", .{
                self.config.id,
                request.candidate_id,
                request.term,
            });
        }

        return .{
            .term = self.persistent.current_term,
            .vote_granted = vote_granted,
        };
    }

    pub fn handleRequestVote(
        self: *Node,
        request: rpc.RequestVoteRequest,
    ) rpc.RequestVoteResponse {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (request.term == 0 or request.candidate_id == 0) {
            logging.warn("Node {d}: Invalid RequestVote with term={d}, candidate={d}", .{
                self.config.id,
                request.term,
                request.candidate_id,
            });
            return .{
                .term = self.persistent.current_term,
                .vote_granted = false,
            };
        }

        if (request.term < self.persistent.current_term) {
            return .{
                .term = self.persistent.current_term,
                .vote_granted = false,
            };
        }

        if (request.term > self.persistent.current_term) {
            const old_term = self.persistent.current_term;
            const old_voted_for = self.persistent.voted_for;
            const old_role = self.role;

            self.persistent.current_term = request.term;
            self.persistent.voted_for = null;
            self.role = .follower;

            if (self.storage) |storage| {
                storage.saveState(
                    self.persistent.current_term,
                    self.persistent.voted_for,
                    self.volatile_state.last_applied,
                ) catch |err| {
                    logging.err("Node {d}: Failed to persist term update in RequestVote, reverting: {}", .{ self.config.id, err });
                    self.persistent.current_term = old_term;
                    self.persistent.voted_for = old_voted_for;
                    self.role = old_role;
                    return .{
                        .term = self.persistent.current_term,
                        .vote_granted = false,
                    };
                };
            }
        }

        const vote_granted = blk: {
            if (self.persistent.voted_for) |voted| {
                if (voted != request.candidate_id) {
                    break :blk false;
                }
            }

            const our_last_term = self.log.lastTerm();
            const our_last_index = self.log.lastIndex();

            if (request.last_log_term < our_last_term) {
                break :blk false;
            }

            if (request.last_log_term == our_last_term and
                request.last_log_index < our_last_index)
            {
                break :blk false;
            }

            break :blk true;
        };

        if (vote_granted) {
            const old_voted_for = self.persistent.voted_for;
            self.persistent.voted_for = request.candidate_id;
            self.last_heartbeat = std.time.milliTimestamp();

            if (self.storage) |storage| {
                storage.saveState(
                    self.persistent.current_term,
                    self.persistent.voted_for,
                    self.volatile_state.last_applied,
                ) catch |err| {
                    logging.err("Node {d}: Failed to persist vote for {d}, reverting: {}", .{ self.config.id, request.candidate_id, err });
                    self.persistent.voted_for = old_voted_for;
                    return .{
                        .term = self.persistent.current_term,
                        .vote_granted = false,
                    };
                };
            }
        }

        return .{
            .term = self.persistent.current_term,
            .vote_granted = vote_granted,
        };
    }

    pub fn handleAppendEntries(
        self: *Node,
        request: rpc.AppendEntriesRequest,
    ) !rpc.AppendEntriesResponse {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (request.term == 0 or request.leader_id == 0) {
            logging.warn("Node {d}: Invalid AppendEntries with term={d}, leader={d}", .{
                self.config.id,
                request.term,
                request.leader_id,
            });
            return .{
                .term = self.persistent.current_term,
                .success = false,
                .match_index = 0,
            };
        }

        if (request.prev_log_index > 0 and request.prev_log_term == 0) {
            logging.warn("Node {d}: Invalid AppendEntries with prev_log_index={d} but prev_log_term=0", .{
                self.config.id,
                request.prev_log_index,
            });
            return .{
                .term = self.persistent.current_term,
                .success = false,
                .match_index = 0,
            };
        }

        for (request.entries) |entry| {
            if (entry.term == 0 or entry.index == 0) {
                logging.warn("Node {d}: Invalid log entry with term={d}, index={d}", .{
                    self.config.id,
                    entry.term,
                    entry.index,
                });
                return .{
                    .term = self.persistent.current_term,
                    .success = false,
                    .match_index = 0,
                };
            }
            if (entry.command.len == 0) {
                logging.warn("Node {d}: Empty command in log entry at index={d}", .{
                    self.config.id,
                    entry.index,
                });
                return .{
                    .term = self.persistent.current_term,
                    .success = false,
                    .match_index = 0,
                };
            }
        }

        if (request.term < self.persistent.current_term) {
            return .{
                .term = self.persistent.current_term,
                .success = false,
                .match_index = 0,
            };
        }

        if (request.term > self.persistent.current_term) {
            const old_term = self.persistent.current_term;
            const old_voted_for = self.persistent.voted_for;

            self.persistent.current_term = request.term;
            self.persistent.voted_for = null;

            if (self.storage) |storage| {
                storage.saveState(
                    self.persistent.current_term,
                    self.persistent.voted_for,
                    self.volatile_state.last_applied,
                ) catch |err| {
                    logging.err("Node {d}: Failed to persist term update in AppendEntries, reverting: {}", .{ self.config.id, err });
                    self.persistent.current_term = old_term;
                    self.persistent.voted_for = old_voted_for;
                    return .{
                        .term = self.persistent.current_term,
                        .success = false,
                        .match_index = 0,
                    };
                };
            }
        }

        if (self.role != .follower) {
            self.role = .follower;
        }

        self.current_leader = request.leader_id;
        self.last_heartbeat = std.time.milliTimestamp();

        if (request.prev_log_index > 0) {
            const prev_term = self.log.termAt(request.prev_log_index);
            if (prev_term != request.prev_log_term) {
                return .{
                    .term = self.persistent.current_term,
                    .success = false,
                    .match_index = self.log.lastIndex(),
                };
            }
        }

        var insert_index = request.prev_log_index + 1;
        for (request.entries, 0..) |entry, i| {
            const entry_index = request.prev_log_index + 1 + @as(LogIndex, @intCast(i));
            if (self.log.get(entry_index)) |existing| {
                if (existing.term != entry.term) {
                    self.log.truncate(entry_index);
                    break;
                }
            } else {
                break;
            }
            insert_index = entry_index + 1;
        }

        for (request.entries) |entry| {
            if (entry.index >= insert_index) {
                _ = try self.log.append(entry.term, entry.command);
            }
        }

        if (request.leader_commit > self.volatile_state.commit_index) {
            self.volatile_state.commit_index = @min(
                request.leader_commit,
                self.log.lastIndex(),
            );
        }

        return .{
            .term = self.persistent.current_term,
            .success = true,
            .match_index = self.log.lastIndex(),
        };
    }

    pub fn isElectionTimeout(self: *Node) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.role == .leader) {
            return false;
        }

        const now = std.time.milliTimestamp();
        const elapsed: u64 = @intCast(now - self.last_heartbeat);
        return elapsed >= self.election_timeout;
    }

    pub fn shouldStartPreVote(self: *Node) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.config.enable_prevote) {
            return false;
        }

        if (self.role != .follower) {
            return false;
        }

        const now = std.time.milliTimestamp();
        const elapsed: u64 = @intCast(now - self.last_heartbeat);
        return elapsed >= self.election_timeout;
    }

    pub fn applyCommitted(self: *Node) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var applied_any = false;
        while (self.volatile_state.last_applied < self.volatile_state.commit_index) {
            self.volatile_state.last_applied += 1;
            if (self.log.get(self.volatile_state.last_applied)) |entry| {
                try self.state_machine.apply(entry.index, entry.command);
                applied_any = true;
            }
        }

        if (applied_any) {
            if (self.storage) |storage| {
                try storage.saveState(self.persistent.current_term, self.persistent.voted_for, self.volatile_state.last_applied);
            }
        }
    }

    pub fn submitCommand(self: *Node, command: []const u8) !LogIndex {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.role != .leader) {
            return error.NotLeader;
        }

        const index = try self.log.append(self.persistent.current_term, command);

        if (self.storage) |storage| {
            try storage.saveLog(&self.log);
        }

        logging.debug("Node {d}: Accepted command at index {d}", .{
            self.config.id,
            index,
        });

        return index;
    }

    pub fn getLeader(self: *Node) ?ServerId {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.current_leader;
    }

    pub fn getRole(self: *Node) Role {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.role;
    }

    pub fn getCurrentTerm(self: *Node) Term {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.persistent.current_term;
    }

    pub fn createSnapshot(self: *Node) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.volatile_state.last_applied == 0) {
            return;
        }

        const snapshot_data = try self.state_machine.snapshot();
        defer self.allocator.free(snapshot_data);

        const last_included_index = self.volatile_state.last_applied;
        const last_included_entry = self.log.get(last_included_index) orelse return error.InvalidSnapshot;
        const last_included_term = last_included_entry.term;

        if (self.storage) |storage| {
            try storage.saveSnapshot(snapshot_data, last_included_index, last_included_term);
            logging.info("Node {d}: Created snapshot at index {d}, term {d}", .{
                self.config.id,
                last_included_index,
                last_included_term,
            });

            self.log.trimBefore(last_included_index);

            try storage.saveLog(&self.log);
        }
    }

    pub fn handleInstallSnapshot(
        self: *Node,
        request: rpc.InstallSnapshotRequest,
    ) !rpc.InstallSnapshotResponse {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (request.term == 0 or request.leader_id == 0) {
            logging.warn("Node {d}: Invalid InstallSnapshot with term={d}, leader={d}", .{
                self.config.id,
                request.term,
                request.leader_id,
            });
            return .{
                .term = self.persistent.current_term,
            };
        }

        if (request.last_included_index == 0 or request.last_included_term == 0) {
            logging.warn("Node {d}: Invalid InstallSnapshot with last_included_index={d}, last_included_term={d}", .{
                self.config.id,
                request.last_included_index,
                request.last_included_term,
            });
            return .{
                .term = self.persistent.current_term,
            };
        }

        if (request.data.len == 0) {
            logging.warn("Node {d}: Empty snapshot data in InstallSnapshot", .{self.config.id});
            return .{
                .term = self.persistent.current_term,
            };
        }

        if (request.term < self.persistent.current_term) {
            return .{
                .term = self.persistent.current_term,
            };
        }

        if (request.term > self.persistent.current_term) {
            self.persistent.current_term = request.term;
            self.persistent.voted_for = null;
            self.role = .follower;
        }

        self.current_leader = request.leader_id;
        self.last_heartbeat = std.time.milliTimestamp();

        if (request.done) {
            if (self.storage) |storage| {
                try storage.saveSnapshot(
                    request.data,
                    request.last_included_index,
                    request.last_included_term,
                );
            }

            try self.state_machine.restore(request.data);

            self.volatile_state.last_applied = request.last_included_index;
            if (self.volatile_state.commit_index < request.last_included_index) {
                self.volatile_state.commit_index = request.last_included_index;
            }

            self.log.trimBefore(request.last_included_index);

            logging.info("Node {d}: Installed snapshot at index {d}", .{
                self.config.id,
                request.last_included_index,
            });
        }

        return .{
            .term = self.persistent.current_term,
        };
    }

    pub fn loadSnapshot(self: *Node) !void {
        const storage = self.storage orelse return;

        if (try storage.loadSnapshot()) |snapshot| {
            defer self.allocator.free(snapshot.data);

            try self.state_machine.restore(snapshot.data);

            self.volatile_state.last_applied = snapshot.last_index;
            self.volatile_state.commit_index = snapshot.last_index;

            logging.info("Node {d}: Loaded snapshot at index {d}, term {d}", .{
                self.config.id,
                snapshot.last_index,
                snapshot.last_term,
            });
        }
    }

    /// Request a read index for linearizable reads
    pub fn requestReadIndex(self: *Node) !LogIndex {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.role != .leader) {
            return error.NotLeader;
        }

        const leader_state = &(self.leader orelse return error.NotLeader);
        const read_index = self.volatile_state.commit_index;

        const read_id = leader_state.next_read_id;
        leader_state.next_read_id += 1;

        try leader_state.pending_reads.append(self.allocator, .{
            .read_id = read_id,
            .read_index = read_index,
            .acks = 1, // leader counts as one ack
            .timestamp = std.time.milliTimestamp(),
        });

        logging.debug("Node {d}: ReadIndex request {d} at commit_index={d}", .{
            self.config.id,
            read_id,
            read_index,
        });

        return read_index;
    }

    /// Handle ReadIndex RPC from followers
    pub fn handleReadIndex(
        self: *Node,
        request: rpc.ReadIndexRequest,
    ) rpc.ReadIndexResponse {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.role != .leader) {
            return .{
                .term = self.persistent.current_term,
                .read_index = 0,
                .success = false,
            };
        }

        const read_index = self.volatile_state.commit_index;

        logging.debug("Node {d}: Handling ReadIndex request {d}, returning read_index={d}", .{
            self.config.id,
            request.read_id,
            read_index,
        });

        return .{
            .term = self.persistent.current_term,
            .read_index = read_index,
            .success = true,
        };
    }

    /// Record ack for a pending read index request
    pub fn ackReadIndex(self: *Node, read_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const leader_state = &(self.leader orelse return);

        for (leader_state.pending_reads.items) |*read_state| {
            if (read_state.read_id == read_id) {
                read_state.acks += 1;
                break;
            }
        }
    }

    /// Get confirmed read indexes that have majority ack
    pub fn getConfirmedReadIndex(self: *Node) ?LogIndex {
        self.mutex.lock();
        defer self.mutex.unlock();

        const leader_state = &(self.leader orelse return null);
        const majority = self.cluster.majoritySize();

        var confirmed_index: ?LogIndex = null;

        // remove confirmed reads and track highest confirmed index
        var i: usize = 0;
        while (i < leader_state.pending_reads.items.len) {
            const read_state = leader_state.pending_reads.items[i];
            if (read_state.acks >= majority) {
                if (confirmed_index == null or read_state.read_index > confirmed_index.?) {
                    confirmed_index = read_state.read_index;
                }
                _ = leader_state.pending_reads.swapRemove(i);
            } else {
                i += 1;
            }
        }

        return confirmed_index;
    }
};

test "Node initialization" {
    const allocator = std.testing.allocator;

    var kv = state_machine_mod.KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]ServerId{ 1, 2, 3 };
    const cluster = ClusterConfig{ .servers = &servers };

    var node = try Node.init(
        allocator,
        .{ .id = 1 },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    try std.testing.expectEqual(Role.follower, node.role);
    try std.testing.expectEqual(@as(Term, 0), node.persistent.current_term);
}
