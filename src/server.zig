//! Server orchestration for Raft nodes
//!
//! The Server manages the event loop, network listeners and coordinates
//! between the Node and Transport layers

const std = @import("std");
const types = @import("types.zig");
const node_mod = @import("node.zig");
const network_mod = @import("network.zig");
const rpc = @import("rpc.zig");
const logging = @import("logging.zig");

const Allocator = std.mem.Allocator;
const Node = node_mod.Node;
const Transport = network_mod.Transport;
const ServerId = types.ServerId;

/// Server handles:
/// - Listening for incoming RPC connections
/// - Event loop for elections, heartbeats and timeouts
/// - Handling concurrent RPC connections
pub const Server = struct {
    allocator: Allocator,
    node: *Node,
    transport: *Transport,
    running: std.atomic.Value(bool),
    listener_thread: ?std.Thread,
    event_thread: ?std.Thread,
    connection_threads: std.ArrayListUnmanaged(std.Thread),
    threads_mutex: std.Thread.Mutex,

    /// Initialize a new server
    pub fn init(allocator: Allocator, node: *Node, transport: *Transport) Server {
        return .{
            .allocator = allocator,
            .node = node,
            .transport = transport,
            .running = std.atomic.Value(bool).init(false),
            .listener_thread = null,
            .event_thread = null,
            .connection_threads = .{},
            .threads_mutex = std.Thread.Mutex{},
        };
    }

    /// Start the server threads
    pub fn start(self: *Server) !void {
        self.running.store(true, .release);

        logging.info("Server starting for node {d}", .{self.node.config.id});

        self.listener_thread = try std.Thread.spawn(.{}, listenerLoop, .{self});
        self.event_thread = try std.Thread.spawn(.{}, eventLoop, .{self});
    }

    /// Stop the server threads
    pub fn stop(self: *Server) void {
        self.running.store(false, .release);

        if (self.event_thread) |thread| {
            thread.join();
        }
        if (self.listener_thread) |thread| {
            thread.join();
        }

        self.threads_mutex.lock();
        defer self.threads_mutex.unlock();

        for (self.connection_threads.items) |thread| {
            thread.detach();
        }
        self.connection_threads.clearRetainingCapacity();
    }

    pub fn deinit(self: *Server) void {
        self.connection_threads.deinit(self.allocator);
    }

    fn eventLoop(self: *Server) !void {
        while (self.running.load(.acquire)) {
            const role = self.node.getRole();

            switch (role) {
                .follower => try self.followerTick(),
                .pre_candidate => try self.preCandidateTick(),
                .candidate => try self.candidateTick(),
                .leader => try self.leaderTick(),
            }

            try self.node.applyCommitted();

            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }

    fn followerTick(self: *Server) !void {
        if (self.node.shouldStartPreVote()) {
            logging.debug("Node {d}: Election timeout, starting pre-vote", .{
                self.node.config.id,
            });
            self.node.startPreVote();
            try self.runPreVote();
        } else if (self.node.isElectionTimeout() and !self.node.config.enable_prevote) {
            logging.debug("Node {d}: Election timeout, starting election", .{
                self.node.config.id,
            });
            try self.node.startElection();
            try self.runElection();
        }
    }

    fn preCandidateTick(self: *Server) !void {
        if (self.node.isElectionTimeout()) {
            logging.debug("Node {d}: Pre-vote timeout, retrying pre-vote", .{
                self.node.config.id,
            });
            self.node.startPreVote();
            try self.runPreVote();
        }
    }

    fn candidateTick(self: *Server) !void {
        if (self.node.isElectionTimeout()) {
            logging.debug("Node {d}: Election timeout, retrying election", .{
                self.node.config.id,
            });
            try self.node.startElection();
            try self.runElection();
        }
    }

    fn leaderTick(self: *Server) !void {
        const last_heartbeat = blk: {
            self.node.mutex.lock();
            defer self.node.mutex.unlock();
            break :blk self.node.last_heartbeat;
        };

        const now = std.time.milliTimestamp();
        const elapsed: u64 = @intCast(now - last_heartbeat);

        if (elapsed >= self.node.config.heartbeat_interval) {
            try self.sendHeartbeats();

            self.node.mutex.lock();
            defer self.node.mutex.unlock();
            self.node.last_heartbeat = std.time.milliTimestamp();
        }
    }

    fn runPreVote(self: *Server) !void {
        const request = blk: {
            self.node.mutex.lock();
            defer self.node.mutex.unlock();

            break :blk rpc.PreVoteRequest{
                .term = self.node.persistent.current_term + 1,
                .candidate_id = self.node.config.id,
                .last_log_index = self.node.log.lastIndex(),
                .last_log_term = self.node.log.lastTerm(),
            };
        };

        var votes: usize = 1;
        var votes_needed: usize = 0;

        {
            self.node.mutex.lock();
            defer self.node.mutex.unlock();
            votes_needed = self.node.cluster.majoritySize();
        }

        for (self.node.cluster.servers) |peer_id| {
            if (peer_id == self.node.config.id) continue;

            if (!self.running.load(.acquire)) return;

            const response = self.transport.sendPreVote(peer_id, request) catch |err| {
                logging.debug("Node {d}: Failed to send PreVote to {d}: {}", .{
                    self.node.config.id,
                    peer_id,
                    err,
                });
                continue;
            };

            if (response.vote_granted) {
                votes += 1;
                logging.debug("Node {d}: Got pre-vote from {d} (total: {d}/{d})", .{
                    self.node.config.id,
                    peer_id,
                    votes,
                    votes_needed,
                });
            } else if (response.term > self.node.persistent.current_term) {
                logging.debug("Node {d}: Saw higher term {d} in pre-vote, stepping down", .{
                    self.node.config.id,
                    response.term,
                });
                self.node.stepDown(response.term) catch |err| {
                    logging.err("Node {d}: Failed to step down: {}", .{ self.node.config.id, err });
                };
                return;
            }

            if (votes >= votes_needed) {
                logging.info("Node {d}: Won pre-vote with {d}/{d} votes, starting real election", .{
                    self.node.config.id,
                    votes,
                    votes_needed,
                });
                try self.node.startElection();
                try self.runElection();
                return;
            }
        }

        if (votes < votes_needed) {
            logging.debug("Node {d}: Lost pre-vote with {d}/{d} votes, stepping back to follower", .{
                self.node.config.id,
                votes,
                votes_needed,
            });
            self.node.mutex.lock();
            defer self.node.mutex.unlock();
            self.node.role = .follower;
        }
    }

    fn runElection(self: *Server) !void {
        const request = blk: {
            self.node.mutex.lock();
            defer self.node.mutex.unlock();

            break :blk rpc.RequestVoteRequest{
                .term = self.node.persistent.current_term,
                .candidate_id = self.node.config.id,
                .last_log_index = self.node.log.lastIndex(),
                .last_log_term = self.node.log.lastTerm(),
            };
        };

        var votes: usize = 1;
        var votes_needed: usize = 0;

        {
            self.node.mutex.lock();
            defer self.node.mutex.unlock();
            votes_needed = self.node.cluster.majoritySize();
        }

        for (self.node.cluster.servers) |peer_id| {
            if (peer_id == self.node.config.id) continue;

            // are we running?
            if (!self.running.load(.acquire)) return;

            const response = self.transport.sendRequestVote(peer_id, request) catch |err| {
                logging.debug("Node {d}: Failed to send RequestVote to {d}: {}", .{
                    self.node.config.id,
                    peer_id,
                    err,
                });
                continue;
            };

            if (response.vote_granted) {
                votes += 1;
                logging.debug("Node {d}: Got vote from {d} (total: {d}/{d})", .{
                    self.node.config.id,
                    peer_id,
                    votes,
                    votes_needed,
                });
            } else if (response.term > request.term) {
                logging.debug("Node {d}: Saw higher term {d}, stepping down", .{
                    self.node.config.id,
                    response.term,
                });
                self.node.stepDown(response.term) catch |err| {
                    logging.err("Node {d}: Failed to step down: {}", .{ self.node.config.id, err });
                };
                return;
            }

            if (votes >= votes_needed) {
                logging.info("Node {d}: Won election with {d}/{d} votes!", .{
                    self.node.config.id,
                    votes,
                    votes_needed,
                });
                try self.node.becomeLeader();
                return;
            }
        }

        if (votes < votes_needed) {
            logging.debug("Node {d}: Lost election with {d}/{d} votes", .{
                self.node.config.id,
                votes,
                votes_needed,
            });
        }
    }

    fn sendHeartbeats(self: *Server) !void {
        for (self.node.cluster.servers) |peer_id| {
            if (peer_id == self.node.config.id) continue;

            if (!self.running.load(.acquire)) return;

            try self.replicateToPeer(peer_id);
        }
    }

    fn replicateToPeer(self: *Server, peer_id: ServerId) !void {
        const request = blk: {
            self.node.mutex.lock();
            defer self.node.mutex.unlock();

            const leader_state = self.node.leader orelse return;
            const next_index = leader_state.next_index.get(peer_id) orelse return;

            const prev_log_index = if (next_index > 0) next_index - 1 else 0;
            const prev_log_term = self.node.log.termAt(prev_log_index);

            const entries = self.node.log.entriesFrom(next_index);

            break :blk rpc.AppendEntriesRequest{
                .term = self.node.persistent.current_term,
                .leader_id = self.node.config.id,
                .prev_log_index = prev_log_index,
                .prev_log_term = prev_log_term,
                .entries = entries,
                .leader_commit = self.node.volatile_state.commit_index,
            };
        };

        const response = self.transport.sendAppendEntries(peer_id, request) catch |err| {
            logging.debug("Node {d}: Failed to send AppendEntries to {d}: {any}", .{
                self.node.config.id,
                peer_id,
                err,
            });
            return;
        };

        self.node.mutex.lock();
        defer self.node.mutex.unlock();

        if (response.term > self.node.persistent.current_term) {
            self.node.stepDownLocked(response.term) catch |err| {
                logging.err("Node {d}: Failed to step down from higher term: {}", .{ self.node.config.id, err });
            };
            return;
        }

        var leader_state = &(self.node.leader orelse return);

        if (response.success) {
            const new_match_index = response.match_index;
            try leader_state.match_index.put(peer_id, new_match_index);
            try leader_state.next_index.put(peer_id, new_match_index + 1);

            try self.advanceCommitIndex();
        } else {
            const next_index = leader_state.next_index.get(peer_id) orelse return;
            if (next_index > 1) {
                try leader_state.next_index.put(peer_id, next_index - 1);
            }
        }
    }

    fn advanceCommitIndex(self: *Server) !void {
        const leader_state = self.node.leader orelse return;

        var index = self.node.volatile_state.commit_index + 1;
        while (index <= self.node.log.lastIndex()) : (index += 1) {
            const entry = self.node.log.get(index) orelse break;
            if (entry.term != self.node.persistent.current_term) {
                continue;
            }

            var count: usize = 1;
            for (self.node.cluster.servers) |peer_id| {
                if (peer_id == self.node.config.id) continue;

                const match_index = leader_state.match_index.get(peer_id) orelse 0;
                if (match_index >= index) {
                    count += 1;
                }
            }

            if (count >= self.node.cluster.majoritySize()) {
                self.node.volatile_state.commit_index = index;
                logging.debug("Node {d}: Advanced commit_index to {d}", .{
                    self.node.config.id,
                    index,
                });
            }
        }
    }

    fn listenerLoop(self: *Server) !void {
        const server = &(self.transport.server orelse return error.NoListener);

        while (self.running.load(.acquire)) {
            var conn = server.*.accept() catch |err| {
                if (err == error.WouldBlock) {
                    std.Thread.sleep(100 * std.time.ns_per_ms);
                    continue;
                }
                logging.warn("Failed to accept connection: {any}", .{err});
                continue;
            };

            const thread = std.Thread.spawn(.{}, handleConnectionThread, .{ self, conn }) catch |err| {
                logging.warn("Failed to spawn connection handler: {any}", .{err});
                conn.close(self.allocator);
                continue;
            };

            self.threads_mutex.lock();
            self.connection_threads.append(self.allocator, thread) catch {
                thread.detach();
                self.threads_mutex.unlock();
                continue;
            };
            self.threads_mutex.unlock();
        }
    }

    fn handleConnectionThread(self: *Server, conn: network_mod.Connection) !void {
        var c = conn;
        try self.transport.handleConnection(self.node, &c);
    }
};
