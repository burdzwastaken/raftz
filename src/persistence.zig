//! Persistent storage for Raft node state
//!
//! Saves and loads node state, log entries and snapshots to disk

const std = @import("std");
const types = @import("types.zig");
const log_mod = @import("log.zig");
const session_mod = @import("session.zig");

const Term = types.Term;
const ServerId = types.ServerId;
const LogIndex = types.LogIndex;
const ClientId = types.ClientId;
const SequenceNumber = types.SequenceNumber;
const Log = log_mod.Log;
const LogEntry = log_mod.LogEntry;
const EntryType = log_mod.EntryType;
const ClusterConfig = types.ClusterConfig;
const ConfigurationType = types.ConfigurationType;
const SessionManager = session_mod.SessionManager;
const Allocator = std.mem.Allocator;

/// File-based storage for persistent Raft state
pub const Storage = struct {
    allocator: Allocator,
    dir_path: []const u8,

    pub fn init(allocator: Allocator, dir_path: []const u8) !Storage {
        std.fs.cwd().makeDir(dir_path) catch |err| {
            if (err != error.PathAlreadyExists) {
                return err;
            }
        };

        return .{
            .allocator = allocator,
            .dir_path = dir_path,
        };
    }

    pub fn deinit(self: *Storage) void {
        _ = self;
    }

    /// Save current term vote, and last applied index to disk
    pub fn saveState(self: *Storage, term: Term, voted_for: ?ServerId, last_applied: LogIndex) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "state" });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        var buf: [1024]u8 = undefined;
        const content = try std.fmt.bufPrint(&buf, "{d}\n{?d}\n{d}\n", .{ term, voted_for, last_applied });
        try file.writeAll(content);
        try file.sync();
    }

    /// Load persisted state from disk (returns defaults if file not found)
    pub fn loadState(self: *Storage) !struct { term: Term, voted_for: ?ServerId, last_applied: LogIndex } {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "state" });
        defer self.allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return .{ .term = 0, .voted_for = null, .last_applied = 0 };
            }
            return err;
        };
        defer file.close();

        var buf: [1024]u8 = undefined;
        const n = try file.readAll(&buf);
        const content = buf[0..n];

        var lines = std.mem.splitScalar(u8, content, '\n');
        const term_str = lines.next() orelse return error.InvalidState;
        const voted_for_str = lines.next() orelse return error.InvalidState;
        const last_applied_str = lines.next() orelse return error.InvalidState;

        const term = try std.fmt.parseInt(Term, term_str, 10);
        const voted_for = std.fmt.parseInt(ServerId, voted_for_str, 10) catch null;
        const last_applied = try std.fmt.parseInt(LogIndex, last_applied_str, 10);

        return .{ .term = term, .voted_for = voted_for, .last_applied = last_applied };
    }

    /// Save all log entries to disk
    pub fn saveLog(self: *Storage, log: *Log) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "log" });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        const count: u64 = @intCast(log.entries.items.len);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, count, .little);
        try file.writeAll(&buf);

        for (log.entries.items) |entry| {
            std.mem.writeInt(u64, &buf, entry.term, .little);
            try file.writeAll(&buf);

            std.mem.writeInt(u64, &buf, entry.index, .little);
            try file.writeAll(&buf);

            const entry_type: u8 = @intFromEnum(std.meta.activeTag(entry.data));
            try file.writeAll(&[_]u8{entry_type});

            switch (entry.data) {
                .command => |cmd| {
                    const cmd_len: u64 = @intCast(cmd.len);
                    std.mem.writeInt(u64, &buf, cmd_len, .little);
                    try file.writeAll(&buf);
                    try file.writeAll(cmd);
                },
                .configuration => {
                    std.mem.writeInt(u64, &buf, 0, .little);
                    try file.writeAll(&buf);
                },
                .client_command => |cc| {
                    std.mem.writeInt(u64, &buf, cc.client_id, .little);
                    try file.writeAll(&buf);
                    std.mem.writeInt(u64, &buf, cc.sequence, .little);
                    try file.writeAll(&buf);
                    const cmd_len: u64 = @intCast(cc.command.len);
                    std.mem.writeInt(u64, &buf, cmd_len, .little);
                    try file.writeAll(&buf);
                    try file.writeAll(cc.command);
                },
            }
        }
        try file.sync();
    }

    pub fn loadLog(self: *Storage, log: *Log) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "log" });
        defer self.allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return;
            }
            return err;
        };
        defer file.close();

        var buf: [8]u8 = undefined;
        _ = try file.readAll(&buf);
        const count = std.mem.readInt(u64, &buf, .little);

        var i: usize = 0;
        while (i < count) : (i += 1) {
            _ = try file.readAll(&buf);
            const term = std.mem.readInt(u64, &buf, .little);

            _ = try file.readAll(&buf);
            const index = std.mem.readInt(u64, &buf, .little);

            var type_buf: [1]u8 = undefined;
            _ = try file.readAll(&type_buf);
            const entry_type: EntryType = @enumFromInt(type_buf[0]);

            switch (entry_type) {
                .command => {
                    _ = try file.readAll(&buf);
                    const cmd_len = std.mem.readInt(u64, &buf, .little);

                    const command = try self.allocator.alloc(u8, @intCast(cmd_len));
                    errdefer self.allocator.free(command);
                    _ = try file.readAll(command);

                    try log.entries.append(log.allocator, LogEntry.command(term, index, command));
                },
                .configuration => {
                    _ = try file.readAll(&buf);
                },
                .client_command => {
                    _ = try file.readAll(&buf);
                    const client_id = std.mem.readInt(u64, &buf, .little);
                    _ = try file.readAll(&buf);
                    const sequence = std.mem.readInt(u64, &buf, .little);
                    _ = try file.readAll(&buf);
                    const cmd_len = std.mem.readInt(u64, &buf, .little);

                    const command = try self.allocator.alloc(u8, @intCast(cmd_len));
                    errdefer self.allocator.free(command);
                    _ = try file.readAll(command);

                    try log.entries.append(log.allocator, LogEntry.clientCommand(term, index, client_id, sequence, command));
                },
            }
        }
    }

    /// Save snapshot to disk
    pub fn saveSnapshot(
        self: *Storage,
        data: []const u8,
        last_index: LogIndex,
        last_term: Term,
        cluster_config: ClusterConfig,
    ) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "snapshot" });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        var buf: [16]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], last_index, .little);
        std.mem.writeInt(u64, buf[8..16], last_term, .little);
        try file.writeAll(&buf);

        try self.writeClusterConfig(file, cluster_config);

        const data_len: u64 = @intCast(data.len);
        var len_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &len_buf, data_len, .little);
        try file.writeAll(&len_buf);

        try file.writeAll(data);
        try file.sync();
    }

    fn writeClusterConfig(self: *Storage, file: std.fs.File, config: ClusterConfig) !void {
        _ = self;
        var buf: [8]u8 = undefined;

        const config_type_byte: u8 = if (config.config_type == .joint) 1 else 0;
        try file.writeAll(&[_]u8{config_type_byte});

        std.mem.writeInt(u64, &buf, @intCast(config.servers.len), .little);
        try file.writeAll(&buf);
        for (config.servers) |server_id| {
            std.mem.writeInt(u64, &buf, server_id, .little);
            try file.writeAll(&buf);
        }

        const new_servers_len: u64 = if (config.new_servers) |ns| @intCast(ns.len) else 0;
        std.mem.writeInt(u64, &buf, new_servers_len, .little);
        try file.writeAll(&buf);
        if (config.new_servers) |ns| {
            for (ns) |server_id| {
                std.mem.writeInt(u64, &buf, server_id, .little);
                try file.writeAll(&buf);
            }
        }

        std.mem.writeInt(u64, &buf, @intCast(config.learners.len), .little);
        try file.writeAll(&buf);
        for (config.learners) |server_id| {
            std.mem.writeInt(u64, &buf, server_id, .little);
            try file.writeAll(&buf);
        }
    }

    /// Snapshot data returned from loadSnapshot
    pub const SnapshotData = struct {
        data: []const u8,
        last_index: LogIndex,
        last_term: Term,
        config_type: ConfigurationType,
        servers: []ServerId,
        new_servers: ?[]ServerId,
        learners: []ServerId,

        pub fn deinit(self: *SnapshotData, allocator: Allocator) void {
            allocator.free(self.data);
            allocator.free(self.servers);
            if (self.new_servers) |ns| {
                allocator.free(ns);
            }
            allocator.free(self.learners);
        }

        pub fn toClusterConfig(self: SnapshotData) ClusterConfig {
            return .{
                .config_type = self.config_type,
                .servers = self.servers,
                .new_servers = self.new_servers,
                .learners = self.learners,
            };
        }
    };

    pub fn loadSnapshot(self: *Storage) !?SnapshotData {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "snapshot" });
        defer self.allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return null;
            }
            return err;
        };
        defer file.close();

        var buf: [16]u8 = undefined;
        _ = try file.readAll(&buf);
        const last_index = std.mem.readInt(u64, buf[0..8], .little);
        const last_term = std.mem.readInt(u64, buf[8..16], .little);

        const config = try self.readClusterConfig(file);
        errdefer {
            self.allocator.free(config.servers);
            if (config.new_servers) |ns| self.allocator.free(ns);
            self.allocator.free(config.learners);
        }

        var len_buf: [8]u8 = undefined;
        _ = try file.readAll(&len_buf);
        const data_len = std.mem.readInt(u64, &len_buf, .little);

        const data = try self.allocator.alloc(u8, @intCast(data_len));
        errdefer self.allocator.free(data);
        _ = try file.readAll(data);

        return .{
            .data = data,
            .last_index = last_index,
            .last_term = last_term,
            .config_type = config.config_type,
            .servers = config.servers,
            .new_servers = config.new_servers,
            .learners = config.learners,
        };
    }

    /// Save session state to disk
    pub fn saveSessions(self: *Storage, sessions: *SessionManager) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "sessions" });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        var buf: [8]u8 = undefined;

        const count: u64 = @intCast(sessions.count());
        std.mem.writeInt(u64, &buf, count, .little);
        try file.writeAll(&buf);

        var it = sessions.iterator();
        while (it.next()) |entry| {
            const client_id = entry.key_ptr.*;
            const session = entry.value_ptr.*;

            std.mem.writeInt(u64, &buf, client_id, .little);
            try file.writeAll(&buf);

            std.mem.writeInt(u64, &buf, session.last_sequence, .little);
            try file.writeAll(&buf);

            std.mem.writeInt(u64, &buf, session.last_index, .little);
            try file.writeAll(&buf);

            if (session.last_response) |response| {
                const resp_len: u64 = @intCast(response.len);
                std.mem.writeInt(u64, &buf, resp_len, .little);
                try file.writeAll(&buf);
                try file.writeAll(response);
            } else {
                std.mem.writeInt(u64, &buf, 0, .little);
                try file.writeAll(&buf);
            }
        }

        try file.sync();
    }

    pub fn loadSessions(self: *Storage, sessions: *SessionManager) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "sessions" });
        defer self.allocator.free(path);

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return;
            }
            return err;
        };
        defer file.close();

        var buf: [8]u8 = undefined;

        _ = try file.readAll(&buf);
        const count = std.mem.readInt(u64, &buf, .little);

        var i: u64 = 0;
        while (i < count) : (i += 1) {
            _ = try file.readAll(&buf);
            const client_id = std.mem.readInt(u64, &buf, .little);

            _ = try file.readAll(&buf);
            const last_sequence = std.mem.readInt(u64, &buf, .little);

            _ = try file.readAll(&buf);
            const last_index = std.mem.readInt(u64, &buf, .little);

            _ = try file.readAll(&buf);
            const resp_len = std.mem.readInt(u64, &buf, .little);

            const response: ?[]const u8 = if (resp_len > 0) blk: {
                const resp = try self.allocator.alloc(u8, @intCast(resp_len));
                _ = try file.readAll(resp);
                break :blk resp;
            } else null;

            try sessions.restoreSession(client_id, last_sequence, last_index, response);

            if (response) |resp| {
                self.allocator.free(resp);
            }
        }
    }

    fn readClusterConfig(self: *Storage, file: std.fs.File) !struct {
        config_type: ConfigurationType,
        servers: []ServerId,
        new_servers: ?[]ServerId,
        learners: []ServerId,
    } {
        var buf: [8]u8 = undefined;

        var type_buf: [1]u8 = undefined;
        _ = try file.readAll(&type_buf);
        const config_type: ConfigurationType = if (type_buf[0] == 1) .joint else .simple;

        _ = try file.readAll(&buf);
        const servers_len = std.mem.readInt(u64, &buf, .little);
        const servers = try self.allocator.alloc(ServerId, @intCast(servers_len));
        errdefer self.allocator.free(servers);
        for (servers) |*server_id| {
            _ = try file.readAll(&buf);
            server_id.* = std.mem.readInt(u64, &buf, .little);
        }

        _ = try file.readAll(&buf);
        const new_servers_len = std.mem.readInt(u64, &buf, .little);
        const new_servers: ?[]ServerId = if (new_servers_len > 0) blk: {
            const ns = try self.allocator.alloc(ServerId, @intCast(new_servers_len));
            errdefer self.allocator.free(ns);
            for (ns) |*server_id| {
                _ = try file.readAll(&buf);
                server_id.* = std.mem.readInt(u64, &buf, .little);
            }
            break :blk ns;
        } else null;
        errdefer if (new_servers) |ns| self.allocator.free(ns);

        _ = try file.readAll(&buf);
        const learners_len = std.mem.readInt(u64, &buf, .little);
        const learners = try self.allocator.alloc(ServerId, @intCast(learners_len));
        errdefer self.allocator.free(learners);
        for (learners) |*server_id| {
            _ = try file.readAll(&buf);
            server_id.* = std.mem.readInt(u64, &buf, .little);
        }

        return .{
            .config_type = config_type,
            .servers = servers,
            .new_servers = new_servers,
            .learners = learners,
        };
    }
};

test "Storage save and load state" {
    const allocator = std.testing.allocator;

    const dir_path = "test_storage";
    std.fs.cwd().makeDir(dir_path) catch {};
    defer std.fs.cwd().deleteTree(dir_path) catch {};

    var storage = try Storage.init(allocator, dir_path);
    defer storage.deinit();

    try storage.saveState(5, 2, 10);

    const state = try storage.loadState();
    try std.testing.expectEqual(@as(Term, 5), state.term);
    try std.testing.expectEqual(@as(?ServerId, 2), state.voted_for);
    try std.testing.expectEqual(@as(LogIndex, 10), state.last_applied);
}
