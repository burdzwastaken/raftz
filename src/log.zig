//! Replicated log management
//!
//! Manages the ordered sequence of log entries replicated across the cluster

const std = @import("std");
const types = @import("types.zig");

const Term = types.Term;
const LogIndex = types.LogIndex;
const Allocator = std.mem.Allocator;

/// Single entry in the replicated log
pub const LogEntry = struct {
    term: Term,
    index: LogIndex,
    command: []const u8,

    pub fn deinit(self: *LogEntry, allocator: Allocator) void {
        allocator.free(self.command);
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

    /// Append a new entry to the log
    pub fn append(self: *Log, term: Term, command: []const u8) !LogIndex {
        const index = self.lastIndex() + 1;
        const command_copy = try self.allocator.dupe(u8, command);
        errdefer self.allocator.free(command_copy);

        try self.entries.append(self.allocator, .{
            .term = term,
            .index = index,
            .command = command_copy,
        });

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
    try std.testing.expectEqualStrings("cmd2", entry.?.command);

    log.truncate(2);
    try std.testing.expectEqual(@as(LogIndex, 1), log.lastIndex());
    try std.testing.expectEqual(@as(Term, 1), log.lastTerm());
}
