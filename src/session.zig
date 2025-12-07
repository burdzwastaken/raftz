//! Client session tracking for request deduplication

const std = @import("std");
const types = @import("types.zig");

const ClientId = types.ClientId;
const SequenceNumber = types.SequenceNumber;
const LogIndex = types.LogIndex;
const Allocator = std.mem.Allocator;

/// Tracked state for a single client session
pub const Session = struct {
    last_sequence: SequenceNumber,
    last_response: ?[]const u8,
    last_index: LogIndex,

    pub fn deinit(self: *Session, allocator: Allocator) void {
        if (self.last_response) |response| {
            allocator.free(response);
        }
    }
};

/// Manages client sessions for request deduping
pub const SessionManager = struct {
    allocator: Allocator,
    sessions: std.AutoHashMap(ClientId, Session),

    pub fn init(allocator: Allocator) SessionManager {
        return .{
            .allocator = allocator,
            .sessions = std.AutoHashMap(ClientId, Session).init(allocator),
        };
    }

    pub fn deinit(self: *SessionManager) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.sessions.deinit();
    }

    /// Check if request is duplicate, returns cached response if so
    pub fn checkDuplicate(
        self: *SessionManager,
        client_id: ClientId,
        sequence: SequenceNumber,
    ) ?[]const u8 {
        if (self.sessions.get(client_id)) |session| {
            if (sequence == session.last_sequence) {
                return session.last_response;
            }
            if (sequence < session.last_sequence) {
                return session.last_response;
            }
        }
        return null;
    }

    pub fn isStale(
        self: *SessionManager,
        client_id: ClientId,
        sequence: SequenceNumber,
    ) bool {
        if (self.sessions.get(client_id)) |session| {
            return sequence < session.last_sequence;
        }
        return false;
    }

    pub fn registerRequest(
        self: *SessionManager,
        client_id: ClientId,
        sequence: SequenceNumber,
        index: LogIndex,
    ) !void {
        const result = try self.sessions.getOrPut(client_id);
        if (result.found_existing) {
            if (result.value_ptr.last_response) |old_response| {
                self.allocator.free(old_response);
            }
        }
        result.value_ptr.* = .{
            .last_sequence = sequence,
            .last_response = null,
            .last_index = index,
        };
    }

    pub fn recordResponse(
        self: *SessionManager,
        client_id: ClientId,
        sequence: SequenceNumber,
        response: ?[]const u8,
    ) !void {
        if (self.sessions.getPtr(client_id)) |session| {
            if (session.last_sequence == sequence) {
                if (session.last_response) |old| {
                    self.allocator.free(old);
                }
                if (response) |resp| {
                    session.last_response = try self.allocator.dupe(u8, resp);
                } else {
                    session.last_response = null;
                }
            }
        }
    }

    pub fn getSession(self: *SessionManager, client_id: ClientId) ?Session {
        return self.sessions.get(client_id);
    }

    pub fn restoreSession(
        self: *SessionManager,
        client_id: ClientId,
        sequence: SequenceNumber,
        index: LogIndex,
        response: ?[]const u8,
    ) !void {
        const result = try self.sessions.getOrPut(client_id);
        if (result.found_existing) {
            if (result.value_ptr.last_response) |old| {
                self.allocator.free(old);
            }
        }

        const response_copy = if (response) |resp|
            try self.allocator.dupe(u8, resp)
        else
            null;

        result.value_ptr.* = .{
            .last_sequence = sequence,
            .last_response = response_copy,
            .last_index = index,
        };
    }

    pub fn clear(self: *SessionManager) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.sessions.clearRetainingCapacity();
    }

    pub fn count(self: *SessionManager) usize {
        return self.sessions.count();
    }

    pub fn iterator(self: *SessionManager) std.AutoHashMap(ClientId, Session).Iterator {
        return self.sessions.iterator();
    }
};

test "SessionManager basic operations" {
    const allocator = std.testing.allocator;
    var manager = SessionManager.init(allocator);
    defer manager.deinit();

    const client_id: ClientId = 1;

    try std.testing.expect(manager.checkDuplicate(client_id, 1) == null);

    try manager.registerRequest(client_id, 1, 10);

    const dup = manager.checkDuplicate(client_id, 1);
    try std.testing.expect(dup == null);

    try manager.recordResponse(client_id, 1, "result1");

    const cached = manager.checkDuplicate(client_id, 1);
    try std.testing.expect(cached != null);
    try std.testing.expectEqualStrings("result1", cached.?);

    try std.testing.expect(manager.checkDuplicate(client_id, 2) == null);
}

test "SessionManager stale detection" {
    const allocator = std.testing.allocator;
    var manager = SessionManager.init(allocator);
    defer manager.deinit();

    const client_id: ClientId = 1;

    try manager.registerRequest(client_id, 5, 10);

    try std.testing.expect(manager.isStale(client_id, 3));
    try std.testing.expect(!manager.isStale(client_id, 5));
    try std.testing.expect(!manager.isStale(client_id, 6));
}

test "SessionManager restore" {
    const allocator = std.testing.allocator;
    var manager = SessionManager.init(allocator);
    defer manager.deinit();

    const client_id: ClientId = 42;

    try manager.restoreSession(client_id, 10, 100, "cached_result");

    const cached = manager.checkDuplicate(client_id, 10);
    try std.testing.expect(cached != null);
    try std.testing.expectEqualStrings("cached_result", cached.?);

    const session = manager.getSession(client_id);
    try std.testing.expect(session != null);
    try std.testing.expectEqual(@as(SequenceNumber, 10), session.?.last_sequence);
    try std.testing.expectEqual(@as(LogIndex, 100), session.?.last_index);
}

test "SessionManager multiple clients" {
    const allocator = std.testing.allocator;
    var manager = SessionManager.init(allocator);
    defer manager.deinit();

    try manager.registerRequest(1, 1, 10);
    try manager.registerRequest(2, 1, 11);
    try manager.registerRequest(3, 1, 12);

    try manager.recordResponse(1, 1, "client1_result");
    try manager.recordResponse(2, 1, "client2_result");
    try manager.recordResponse(3, 1, "client3_result");

    try std.testing.expectEqualStrings("client1_result", manager.checkDuplicate(1, 1).?);
    try std.testing.expectEqualStrings("client2_result", manager.checkDuplicate(2, 1).?);
    try std.testing.expectEqualStrings("client3_result", manager.checkDuplicate(3, 1).?);

    try std.testing.expectEqual(@as(usize, 3), manager.count());
}
