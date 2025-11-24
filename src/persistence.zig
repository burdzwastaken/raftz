const std = @import("std");
const types = @import("types.zig");
const log_mod = @import("log.zig");

const Term = types.Term;
const ServerId = types.ServerId;
const LogIndex = types.LogIndex;
const Log = log_mod.Log;
const LogEntry = log_mod.LogEntry;
const Allocator = std.mem.Allocator;

/// Persistent storage for Raft state
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

    pub fn saveState(self: *Storage, term: Term, voted_for: ?ServerId, last_applied: LogIndex) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "state" });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        var buf: [1024]u8 = undefined;
        const content = try std.fmt.bufPrint(&buf, "{d}\n{?d}\n{d}\n", .{ term, voted_for, last_applied });
        try file.writeAll(content);
    }

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

            const cmd_len: u64 = @intCast(entry.command.len);
            std.mem.writeInt(u64, &buf, cmd_len, .little);
            try file.writeAll(&buf);

            try file.writeAll(entry.command);
        }
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

            _ = try file.readAll(&buf);
            const cmd_len = std.mem.readInt(u64, &buf, .little);

            const command = try self.allocator.alloc(u8, @intCast(cmd_len));
            errdefer self.allocator.free(command);
            _ = try file.readAll(command);

            try log.entries.append(log.allocator, .{
                .term = term,
                .index = index,
                .command = command,
            });
        }
    }

    pub fn saveSnapshot(self: *Storage, data: []const u8, last_index: LogIndex, last_term: Term) !void {
        const path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.dir_path, "snapshot" });
        defer self.allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        var buf: [16]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], last_index, .little);
        std.mem.writeInt(u64, buf[8..16], last_term, .little);
        try file.writeAll(&buf);

        const data_len: u64 = @intCast(data.len);
        var len_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &len_buf, data_len, .little);
        try file.writeAll(&len_buf);

        try file.writeAll(data);
    }

    pub fn loadSnapshot(self: *Storage) !?struct {
        data: []const u8,
        last_index: LogIndex,
        last_term: Term,
    } {
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
