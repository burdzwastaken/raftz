//! Pluggable state machine interface for Raft
//!
//! State machines apply committed log entries to update application state
//! Includes a built-in key-value store implementation for testing and simple use cases

const std = @import("std");
const types = @import("types.zig");

const LogIndex = types.LogIndex;
const Allocator = std.mem.Allocator;

/// Error set for state machine operations
pub const Error = error{
    InvalidCommand,
    OutOfMemory,
};

/// Interface for pluggable state machines
///
/// Implement this interface to provide custom application logic that
/// executes on committed log entries
pub const StateMachine = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        apply: *const fn (ptr: *anyopaque, index: LogIndex, command: []const u8) Error!void,
        snapshot: *const fn (ptr: *anyopaque) Error![]const u8,
        restore: *const fn (ptr: *anyopaque, snapshot: []const u8) Error!void,
    };

    /// Apply a committed log entry to the state machine
    pub fn apply(self: StateMachine, index: LogIndex, command: []const u8) !void {
        return self.vtable.apply(self.ptr, index, command);
    }

    /// Create a snapshot of current state
    pub fn snapshot(self: StateMachine) ![]const u8 {
        return self.vtable.snapshot(self.ptr);
    }

    /// Restore state from a snapshot
    pub fn restore(self: StateMachine, snapshot_data: []const u8) !void {
        return self.vtable.restore(self.ptr, snapshot_data);
    }
};

/// Built-in key-value store state machine
///
/// Supports SET and DEL commands for testing and simple use cases
pub const KvStore = struct {
    allocator: Allocator,
    data: std.StringHashMap([]const u8),

    pub fn init(allocator: std.mem.Allocator) KvStore {
        return .{
            .allocator = allocator,
            .data = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *KvStore) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.data.deinit();
    }

    pub fn stateMachine(self: *KvStore) StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .apply = applyImpl,
                .snapshot = snapshotImpl,
                .restore = restoreImpl,
            },
        };
    }

    fn applyImpl(ptr: *anyopaque, _: LogIndex, command: []const u8) Error!void {
        const self: *KvStore = @ptrCast(@alignCast(ptr));

        var it = std.mem.splitScalar(u8, command, ' ');
        const cmd = it.next() orelse return Error.InvalidCommand;

        if (std.mem.eql(u8, cmd, "SET")) {
            const key = it.next() orelse return Error.InvalidCommand;
            const value = it.next() orelse return Error.InvalidCommand;

            const value_copy = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(value_copy);

            if (self.data.getPtr(key)) |existing_value| {
                self.allocator.free(existing_value.*);
                existing_value.* = value_copy;
            } else {
                const key_copy = try self.allocator.dupe(u8, key);
                errdefer self.allocator.free(key_copy);
                try self.data.put(key_copy, value_copy);
            }
        } else if (std.mem.eql(u8, cmd, "DEL")) {
            const key = it.next() orelse return Error.InvalidCommand;
            if (self.data.fetchRemove(key)) |removed| {
                self.allocator.free(removed.key);
                self.allocator.free(removed.value);
            }
        }
    }

    fn snapshotImpl(ptr: *anyopaque) Error![]const u8 {
        const self: *KvStore = @ptrCast(@alignCast(ptr));

        var list = std.ArrayListUnmanaged(u8){};
        errdefer list.deinit(self.allocator);

        var it = self.data.iterator();
        while (it.next()) |entry| {
            try list.writer(self.allocator).print("{s}={s}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
        }

        return list.toOwnedSlice(self.allocator);
    }

    fn restoreImpl(ptr: *anyopaque, snapshot_data: []const u8) Error!void {
        const self: *KvStore = @ptrCast(@alignCast(ptr));

        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.data.clearRetainingCapacity();

        var lines = std.mem.splitScalar(u8, snapshot_data, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var parts = std.mem.splitScalar(u8, line, '=');
            const key = parts.next() orelse continue;
            const value = parts.next() orelse continue;

            const key_copy = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(key_copy);
            const value_copy = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(value_copy);

            try self.data.put(key_copy, value_copy);
        }
    }
};

test "KvStore basic operations" {
    const allocator = std.testing.allocator;
    var kv = KvStore.init(allocator);
    defer kv.deinit();

    const sm = kv.stateMachine();

    try sm.apply(1, "SET foo bar");
    try sm.apply(2, "SET baz qux");

    try std.testing.expect(kv.data.contains("foo"));
    try std.testing.expectEqualStrings("bar", kv.data.get("foo").?);
}
