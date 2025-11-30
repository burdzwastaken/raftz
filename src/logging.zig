//! Log helpers

const std = @import("std");

pub const LogLevel = enum(u8) {
    debug = 0,
    info = 1,
    warn = 2,
    err = 3,
    none = 4,

    fn toInt(self: LogLevel) u8 {
        return @intFromEnum(self);
    }
};

var log_level: std.atomic.Value(u8) = std.atomic.Value(u8).init(@intFromEnum(LogLevel.info));

pub fn setLogLevel(level: LogLevel) void {
    log_level.store(@intFromEnum(level), .release);
}

fn getLogLevel() LogLevel {
    return @enumFromInt(log_level.load(.acquire));
}

pub fn debug(comptime fmt: []const u8, args: anytype) void {
    if (getLogLevel().toInt() <= LogLevel.debug.toInt()) {
        std.debug.print("[DEBUG] " ++ fmt ++ "\n", args);
    }
}

pub fn info(comptime fmt: []const u8, args: anytype) void {
    if (getLogLevel().toInt() <= LogLevel.info.toInt()) {
        std.debug.print("[INFO] " ++ fmt ++ "\n", args);
    }
}

pub fn warn(comptime fmt: []const u8, args: anytype) void {
    if (getLogLevel().toInt() <= LogLevel.warn.toInt()) {
        std.debug.print("[WARN] " ++ fmt ++ "\n", args);
    }
}

pub fn err(comptime fmt: []const u8, args: anytype) void {
    if (getLogLevel().toInt() <= LogLevel.err.toInt()) {
        std.debug.print("[ERROR] " ++ fmt ++ "\n", args);
    }
}
