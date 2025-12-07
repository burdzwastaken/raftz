//! Replicated log management
//!
//! Manages the ordered sequence of log entries replicated across the cluster

const std = @import("std");
const types = @import("types.zig");

const Term = types.Term;
const LogIndex = types.LogIndex;
const ServerId = types.ServerId;
const ClientId = types.ClientId;
const SequenceNumber = types.SequenceNumber;
const ClusterConfig = types.ClusterConfig;
const Allocator = std.mem.Allocator;

/// Type of log entry
pub const EntryType = enum(u8) {
    command = 0,
    configuration = 1,
    client_command = 2,
};

/// Command data with client session info for request deduplication
pub const ClientCommandData = struct {
    client_id: ClientId,
    sequence: SequenceNumber,
    command: []const u8,

    pub fn deinit(self: *ClientCommandData, allocator: Allocator) void {
        allocator.free(self.command);
    }

    pub fn clone(self: ClientCommandData, allocator: Allocator) !ClientCommandData {
        return .{
            .client_id = self.client_id,
            .sequence = self.sequence,
            .command = try allocator.dupe(u8, self.command),
        };
    }
};

/// Configuration data stored in a configuration entry
pub const ConfigurationData = struct {
    old_servers: []ServerId,
    new_servers: ?[]ServerId,
    learners: []ServerId,

    pub fn deinit(self: *ConfigurationData, allocator: Allocator) void {
        allocator.free(self.old_servers);
        if (self.new_servers) |new| {
            allocator.free(new);
        }
        allocator.free(self.learners);
    }

    pub fn toClusterConfig(self: ConfigurationData) ClusterConfig {
        if (self.new_servers) |new| {
            return ClusterConfig.jointWithLearners(self.old_servers, new, self.learners);
        }
        return ClusterConfig.withLearners(self.old_servers, self.learners);
    }
};

/// Data in a log entry
pub const EntryData = union(EntryType) {
    command: []const u8,
    configuration: ConfigurationData,
    client_command: ClientCommandData,

    pub fn deinit(self: *EntryData, allocator: Allocator) void {
        switch (self.*) {
            .command => |cmd| allocator.free(cmd),
            .configuration => |*cfg| cfg.deinit(allocator),
            .client_command => |*cc| cc.deinit(allocator),
        }
    }

    pub fn clone(self: EntryData, allocator: Allocator) !EntryData {
        switch (self) {
            .command => |cmd| {
                const cmd_copy = try allocator.dupe(u8, cmd);
                return .{ .command = cmd_copy };
            },
            .configuration => |cfg| {
                const old_servers_copy = try allocator.dupe(ServerId, cfg.old_servers);
                errdefer allocator.free(old_servers_copy);

                const new_servers_copy = if (cfg.new_servers) |new|
                    try allocator.dupe(ServerId, new)
                else
                    null;
                errdefer if (new_servers_copy) |new| allocator.free(new);

                const learners_copy = try allocator.dupe(ServerId, cfg.learners);

                return .{ .configuration = .{
                    .old_servers = old_servers_copy,
                    .new_servers = new_servers_copy,
                    .learners = learners_copy,
                } };
            },
            .client_command => |cc| {
                return .{ .client_command = try cc.clone(allocator) };
            },
        }
    }
};

/// Single entry in the replicated log
pub const LogEntry = struct {
    term: Term,
    index: LogIndex,
    data: EntryData,

    pub fn deinit(self: *LogEntry, allocator: Allocator) void {
        self.data.deinit(allocator);
    }

    pub fn command(term: Term, index: LogIndex, cmd: []const u8) LogEntry {
        return .{
            .term = term,
            .index = index,
            .data = .{ .command = cmd },
        };
    }

    pub fn configuration(term: Term, index: LogIndex, config: ConfigurationData) LogEntry {
        return .{
            .term = term,
            .index = index,
            .data = .{ .configuration = config },
        };
    }

    pub fn clientCommand(term: Term, index: LogIndex, client_id: ClientId, sequence: SequenceNumber, cmd: []const u8) LogEntry {
        return .{
            .term = term,
            .index = index,
            .data = .{ .client_command = .{
                .client_id = client_id,
                .sequence = sequence,
                .command = cmd,
            } },
        };
    }

    pub fn clone(self: LogEntry, allocator: Allocator) !LogEntry {
        return .{
            .term = self.term,
            .index = self.index,
            .data = try self.data.clone(allocator),
        };
    }
};

/// Ordered log of entries to be replicated
pub const Log = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(LogEntry),

    pub fn init(allocator: Allocator) Log {
        return .{
            .allocator = allocator,
            .entries = .{},
        };
    }

    pub fn deinit(self: *Log) void {
        for (self.entries.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.entries.deinit(self.allocator);
    }

    pub fn append(self: *Log, term: Term, command: []const u8) !LogIndex {
        const index = self.lastIndex() + 1;
        const command_copy = try self.allocator.dupe(u8, command);
        errdefer self.allocator.free(command_copy);

        try self.entries.append(self.allocator, LogEntry.command(term, index, command_copy));

        return index;
    }

    /// Append a client command with session tracking info
    pub fn appendClientCommand(
        self: *Log,
        term: Term,
        client_id: ClientId,
        sequence: SequenceNumber,
        command: []const u8,
    ) !LogIndex {
        const index = self.lastIndex() + 1;
        const command_copy = try self.allocator.dupe(u8, command);
        errdefer self.allocator.free(command_copy);

        try self.entries.append(self.allocator, LogEntry.clientCommand(term, index, client_id, sequence, command_copy));

        return index;
    }

    pub fn appendConfig(self: *Log, term: Term, config: ClusterConfig) !LogIndex {
        const index = self.lastIndex() + 1;

        const old_servers_copy = try self.allocator.dupe(ServerId, config.servers);
        errdefer self.allocator.free(old_servers_copy);

        const new_servers_copy = if (config.new_servers) |new|
            try self.allocator.dupe(ServerId, new)
        else
            null;
        errdefer if (new_servers_copy) |new| self.allocator.free(new);

        const learners_copy = try self.allocator.dupe(ServerId, config.learners);
        errdefer self.allocator.free(learners_copy);

        const config_data = ConfigurationData{
            .old_servers = old_servers_copy,
            .new_servers = new_servers_copy,
            .learners = learners_copy,
        };

        try self.entries.append(self.allocator, LogEntry.configuration(term, index, config_data));

        return index;
    }

    /// Get entry at index (returns null if index out of bounds)
    pub fn get(self: *Log, index: LogIndex) ?*LogEntry {
        if (index == 0 or index > self.lastIndex()) {
            return null;
        }
        return &self.entries.items[index - 1];
    }

    /// Get the index of the last entry in the log
    pub fn lastIndex(self: *Log) LogIndex {
        return @intCast(self.entries.items.len);
    }

    /// Get the term of the last entry in the log
    pub fn lastTerm(self: *Log) Term {
        if (self.entries.items.len == 0) {
            return 0;
        }
        return self.entries.items[self.entries.items.len - 1].term;
    }

    pub fn termAt(self: *Log, index: LogIndex) Term {
        if (index == 0) {
            return 0;
        }
        if (self.get(index)) |entry| {
            return entry.term;
        }
        return 0;
    }

    pub fn truncate(self: *Log, from_index: LogIndex) void {
        if (from_index == 0 or from_index > self.lastIndex()) {
            return;
        }

        const start_idx = from_index - 1;
        for (self.entries.items[start_idx..]) |*entry| {
            entry.deinit(self.allocator);
        }

        self.entries.shrinkRetainingCapacity(start_idx);
    }

    pub fn entriesFrom(self: *Log, index: LogIndex) []const LogEntry {
        if (index == 0 or index > self.lastIndex()) {
            return &.{};
        }
        return self.entries.items[index - 1 ..];
    }

    /// Clone entries from a given index (caller owns the returned slice)
    pub fn cloneEntriesFrom(self: *Log, allocator: Allocator, index: LogIndex) ![]LogEntry {
        const entries = self.entriesFrom(index);
        if (entries.len == 0) {
            return &.{};
        }

        const cloned = try allocator.alloc(LogEntry, entries.len);
        errdefer allocator.free(cloned);

        var i: usize = 0;
        errdefer {
            for (cloned[0..i]) |*entry| {
                entry.deinit(allocator);
            }
        }

        for (entries) |entry| {
            cloned[i] = try entry.clone(allocator);
            i += 1;
        }

        return cloned;
    }

    pub fn freeClonedEntries(allocator: Allocator, entries: []LogEntry) void {
        for (entries) |*entry| {
            entry.deinit(allocator);
        }
        allocator.free(entries);
    }

    pub fn trimBefore(self: *Log, before_index: LogIndex) void {
        if (before_index == 0 or before_index > self.lastIndex()) {
            return;
        }

        for (self.entries.items[0..before_index]) |*entry| {
            entry.deinit(self.allocator);
        }

        std.mem.copyForwards(
            LogEntry,
            self.entries.items,
            self.entries.items[before_index..],
        );
        self.entries.shrinkRetainingCapacity(self.entries.items.len - before_index);
    }
};

test "Log basic operations" {
    const allocator = std.testing.allocator;
    var log = Log.init(allocator);
    defer log.deinit();

    try std.testing.expectEqual(@as(LogIndex, 0), log.lastIndex());
    try std.testing.expectEqual(@as(Term, 0), log.lastTerm());

    const index1 = try log.append(1, "command1");
    try std.testing.expectEqual(@as(LogIndex, 1), index1);
    try std.testing.expectEqual(@as(LogIndex, 1), log.lastIndex());
    try std.testing.expectEqual(@as(Term, 1), log.lastTerm());

    const index2 = try log.append(1, "command2");
    try std.testing.expectEqual(@as(LogIndex, 2), index2);
    try std.testing.expectEqual(@as(LogIndex, 2), log.lastIndex());
}

test "Log get and truncate" {
    const allocator = std.testing.allocator;
    var log = Log.init(allocator);
    defer log.deinit();

    _ = try log.append(1, "cmd1");
    _ = try log.append(1, "cmd2");
    _ = try log.append(2, "cmd3");

    const entry = log.get(2);
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(@as(Term, 1), entry.?.term);
    try std.testing.expectEqualStrings("cmd2", entry.?.data.command);

    log.truncate(2);
    try std.testing.expectEqual(@as(LogIndex, 1), log.lastIndex());
    try std.testing.expectEqual(@as(Term, 1), log.lastTerm());
}

test "EntryData.clone command" {
    const allocator = std.testing.allocator;

    const original = EntryData{ .command = "test command" };
    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    try std.testing.expectEqualStrings("test command", cloned.command);
    try std.testing.expect(original.command.ptr != cloned.command.ptr);
}

test "EntryData.clone configuration" {
    const allocator = std.testing.allocator;

    const old_servers = [_]ServerId{ 1, 2, 3 };
    const new_servers = [_]ServerId{ 1, 2, 4 };
    const learners = [_]ServerId{5};

    var original_cfg = ConfigurationData{
        .old_servers = try allocator.dupe(ServerId, &old_servers),
        .new_servers = try allocator.dupe(ServerId, &new_servers),
        .learners = try allocator.dupe(ServerId, &learners),
    };
    defer original_cfg.deinit(allocator);

    var original = EntryData{ .configuration = original_cfg };
    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    try std.testing.expectEqualSlices(ServerId, &old_servers, cloned.configuration.old_servers);
    try std.testing.expectEqualSlices(ServerId, &new_servers, cloned.configuration.new_servers.?);
    try std.testing.expectEqualSlices(ServerId, &learners, cloned.configuration.learners);
    try std.testing.expect(original.configuration.old_servers.ptr != cloned.configuration.old_servers.ptr);
}

test "LogEntry.clone" {
    const allocator = std.testing.allocator;

    const original = LogEntry{
        .term = 5,
        .index = 10,
        .data = .{ .command = "test" },
    };

    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    try std.testing.expectEqual(@as(Term, 5), cloned.term);
    try std.testing.expectEqual(@as(LogIndex, 10), cloned.index);
    try std.testing.expectEqualStrings("test", cloned.data.command);
    try std.testing.expect(original.data.command.ptr != cloned.data.command.ptr);
}

test "Log.cloneEntriesFrom" {
    const allocator = std.testing.allocator;
    var log = Log.init(allocator);
    defer log.deinit();

    _ = try log.append(1, "cmd1");
    _ = try log.append(1, "cmd2");
    _ = try log.append(2, "cmd3");

    const cloned = try log.cloneEntriesFrom(allocator, 2);
    defer Log.freeClonedEntries(allocator, cloned);

    try std.testing.expectEqual(@as(usize, 2), cloned.len);
    try std.testing.expectEqualStrings("cmd2", cloned[0].data.command);
    try std.testing.expectEqualStrings("cmd3", cloned[1].data.command);
    try std.testing.expectEqual(@as(Term, 1), cloned[0].term);
    try std.testing.expectEqual(@as(Term, 2), cloned[1].term);

    log.truncate(2);
    try std.testing.expectEqual(@as(LogIndex, 1), log.lastIndex());
    try std.testing.expectEqualStrings("cmd2", cloned[0].data.command);
    try std.testing.expectEqualStrings("cmd3", cloned[1].data.command);
}

test "Log.cloneEntriesFrom empty" {
    const allocator = std.testing.allocator;
    var log = Log.init(allocator);
    defer log.deinit();

    const cloned = try log.cloneEntriesFrom(allocator, 1);
    try std.testing.expectEqual(@as(usize, 0), cloned.len);

    _ = try log.append(1, "cmd1");
    const cloned2 = try log.cloneEntriesFrom(allocator, 5);
    try std.testing.expectEqual(@as(usize, 0), cloned2.len);
}
