const std = @import("std");
const rpc = @import("rpc.zig");
const log_mod = @import("log.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// RPC message types for wire protocol
pub const MessageType = enum(u8) {
    request_vote_request = 1,
    request_vote_response = 2,
    append_entries_request = 3,
    append_entries_response = 4,
    install_snapshot_request = 5,
    install_snapshot_response = 6,
    pre_vote_request = 7,
    pre_vote_response = 8,
    read_index_request = 9,
    read_index_response = 10,
    timeout_now_request = 11,
    timeout_now_response = 12,
    add_server_request = 13,
    add_server_response = 14,
    remove_server_request = 15,
    remove_server_response = 16,
};

/// Wire format for messages
pub const Message = union(MessageType) {
    request_vote_request: rpc.RequestVoteRequest,
    request_vote_response: rpc.RequestVoteResponse,
    append_entries_request: rpc.AppendEntriesRequest,
    append_entries_response: rpc.AppendEntriesResponse,
    install_snapshot_request: rpc.InstallSnapshotRequest,
    install_snapshot_response: rpc.InstallSnapshotResponse,
    pre_vote_request: rpc.PreVoteRequest,
    pre_vote_response: rpc.PreVoteResponse,
    read_index_request: rpc.ReadIndexRequest,
    read_index_response: rpc.ReadIndexResponse,
    timeout_now_request: rpc.TimeoutNowRequest,
    timeout_now_response: rpc.TimeoutNowResponse,
    add_server_request: rpc.AddServerRequest,
    add_server_response: rpc.AddServerResponse,
    remove_server_request: rpc.RemoveServerRequest,
    remove_server_response: rpc.RemoveServerResponse,
};

/// Serialize a message to binary format
pub fn serialize(allocator: Allocator, msg: Message) ![]const u8 {
    var list = std.ArrayListUnmanaged(u8){};
    errdefer list.deinit(allocator);

    const writer = list.writer(allocator);

    try writer.writeByte(@intFromEnum(msg));

    switch (msg) {
        .request_vote_request => |req| {
            try writer.writeInt(u64, req.term, .little);
            try writer.writeInt(u64, req.candidate_id, .little);
            try writer.writeInt(u64, req.last_log_index, .little);
            try writer.writeInt(u64, req.last_log_term, .little);
        },
        .request_vote_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
            try writer.writeByte(if (resp.vote_granted) 1 else 0);
        },
        .append_entries_request => |req| {
            try writer.writeInt(u64, req.term, .little);
            try writer.writeInt(u64, req.leader_id, .little);
            try writer.writeInt(u64, req.prev_log_index, .little);
            try writer.writeInt(u64, req.prev_log_term, .little);
            try writer.writeInt(u64, @intCast(req.entries.len), .little);
            for (req.entries) |entry| {
                try writer.writeInt(u64, entry.term, .little);
                try writer.writeInt(u64, entry.index, .little);
                switch (entry.data) {
                    .command => |cmd| {
                        try writer.writeByte(0);
                        try writer.writeInt(u64, @intCast(cmd.len), .little);
                        try writer.writeAll(cmd);
                    },
                    .configuration => |cfg| {
                        try writer.writeByte(1);
                        try writer.writeInt(u64, @intCast(cfg.old_servers.len), .little);
                        for (cfg.old_servers) |server_id| {
                            try writer.writeInt(u64, server_id, .little);
                        }
                        const has_new = cfg.new_servers != null;
                        try writer.writeByte(if (has_new) 1 else 0);
                        if (has_new) {
                            const new = cfg.new_servers.?;
                            try writer.writeInt(u64, @intCast(new.len), .little);
                            for (new) |server_id| {
                                try writer.writeInt(u64, server_id, .little);
                            }
                        }
                    },
                }
            }
            try writer.writeInt(u64, req.leader_commit, .little);
        },
        .append_entries_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
            try writer.writeByte(if (resp.success) 1 else 0);
            try writer.writeInt(u64, resp.match_index, .little);
        },
        .install_snapshot_request => |req| {
            try writer.writeInt(u64, req.term, .little);
            try writer.writeInt(u64, req.leader_id, .little);
            try writer.writeInt(u64, req.last_included_index, .little);
            try writer.writeInt(u64, req.last_included_term, .little);
            try writer.writeInt(u64, req.offset, .little);
            try writer.writeInt(u64, @intCast(req.data.len), .little);
            try writer.writeAll(req.data);
            try writer.writeByte(if (req.done) 1 else 0);
        },
        .install_snapshot_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
        },
        .pre_vote_request => |req| {
            try writer.writeInt(u64, req.term, .little);
            try writer.writeInt(u64, req.candidate_id, .little);
            try writer.writeInt(u64, req.last_log_index, .little);
            try writer.writeInt(u64, req.last_log_term, .little);
        },
        .pre_vote_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
            try writer.writeByte(if (resp.vote_granted) 1 else 0);
        },
        .read_index_request => |req| {
            try writer.writeInt(u64, req.read_id, .little);
        },
        .read_index_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
            try writer.writeInt(u64, resp.read_index, .little);
            try writer.writeByte(if (resp.success) 1 else 0);
        },
        .timeout_now_request => |req| {
            try writer.writeInt(u64, req.term, .little);
            try writer.writeInt(u64, req.leader_id, .little);
        },
        .timeout_now_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
        },
        .add_server_request => |req| {
            try writer.writeInt(u64, req.new_server, .little);
        },
        .add_server_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
            try writer.writeByte(if (resp.success) 1 else 0);
            try writer.writeInt(u64, resp.leader_id orelse 0, .little);
        },
        .remove_server_request => |req| {
            try writer.writeInt(u64, req.old_server, .little);
        },
        .remove_server_response => |resp| {
            try writer.writeInt(u64, resp.term, .little);
            try writer.writeByte(if (resp.success) 1 else 0);
            try writer.writeInt(u64, resp.leader_id orelse 0, .little);
        },
    }

    return list.toOwnedSlice(allocator);
}

/// Deserialize a message from binary format
pub fn deserialize(allocator: Allocator, data: []const u8) !Message {
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    const msg_type_byte = try reader.readByte();
    const msg_type: MessageType = @enumFromInt(msg_type_byte);

    return switch (msg_type) {
        .request_vote_request => .{
            .request_vote_request = .{
                .term = try reader.readInt(u64, .little),
                .candidate_id = try reader.readInt(u64, .little),
                .last_log_index = try reader.readInt(u64, .little),
                .last_log_term = try reader.readInt(u64, .little),
            },
        },
        .request_vote_response => .{
            .request_vote_response = .{
                .term = try reader.readInt(u64, .little),
                .vote_granted = (try reader.readByte()) != 0,
            },
        },
        .append_entries_request => {
            const term = try reader.readInt(u64, .little);
            const leader_id = try reader.readInt(u64, .little);
            const prev_log_index = try reader.readInt(u64, .little);
            const prev_log_term = try reader.readInt(u64, .little);
            const entries_len = try reader.readInt(u64, .little);

            const entries = try allocator.alloc(log_mod.LogEntry, @intCast(entries_len));
            errdefer allocator.free(entries);

            for (entries) |*entry| {
                entry.term = try reader.readInt(u64, .little);
                entry.index = try reader.readInt(u64, .little);
                const entry_type_byte = try reader.readByte();

                if (entry_type_byte == 0) {
                    const cmd_len = try reader.readInt(u64, .little);
                    const command = try allocator.alloc(u8, @intCast(cmd_len));
                    try reader.readNoEof(command);
                    entry.* = log_mod.LogEntry.command(entry.term, entry.index, command);
                } else {
                    const old_servers_len = try reader.readInt(u64, .little);
                    const old_servers = try allocator.alloc(types.ServerId, @intCast(old_servers_len));
                    for (old_servers) |*server_id| {
                        server_id.* = try reader.readInt(u64, .little);
                    }

                    const has_new = (try reader.readByte()) != 0;
                    const new_servers = if (has_new) blk: {
                        const new_servers_len = try reader.readInt(u64, .little);
                        const new = try allocator.alloc(types.ServerId, @intCast(new_servers_len));
                        for (new) |*server_id| {
                            server_id.* = try reader.readInt(u64, .little);
                        }
                        break :blk new;
                    } else null;

                    const config_data = log_mod.ConfigurationData{
                        .old_servers = old_servers,
                        .new_servers = new_servers,
                    };
                    entry.* = log_mod.LogEntry.configuration(entry.term, entry.index, config_data);
                }
            }

            const leader_commit = try reader.readInt(u64, .little);

            return .{
                .append_entries_request = .{
                    .term = term,
                    .leader_id = leader_id,
                    .prev_log_index = prev_log_index,
                    .prev_log_term = prev_log_term,
                    .entries = entries,
                    .leader_commit = leader_commit,
                },
            };
        },
        .append_entries_response => .{
            .append_entries_response = .{
                .term = try reader.readInt(u64, .little),
                .success = (try reader.readByte()) != 0,
                .match_index = try reader.readInt(u64, .little),
            },
        },
        .install_snapshot_request => {
            const term = try reader.readInt(u64, .little);
            const leader_id = try reader.readInt(u64, .little);
            const last_included_index = try reader.readInt(u64, .little);
            const last_included_term = try reader.readInt(u64, .little);
            const offset = try reader.readInt(u64, .little);
            const data_len = try reader.readInt(u64, .little);
            const snapshot_data = try allocator.alloc(u8, @intCast(data_len));
            try reader.readNoEof(snapshot_data);
            const done = (try reader.readByte()) != 0;

            return .{
                .install_snapshot_request = .{
                    .term = term,
                    .leader_id = leader_id,
                    .last_included_index = last_included_index,
                    .last_included_term = last_included_term,
                    .offset = offset,
                    .data = snapshot_data,
                    .done = done,
                },
            };
        },
        .install_snapshot_response => .{
            .install_snapshot_response = .{
                .term = try reader.readInt(u64, .little),
            },
        },
        .pre_vote_request => .{
            .pre_vote_request = .{
                .term = try reader.readInt(u64, .little),
                .candidate_id = try reader.readInt(u64, .little),
                .last_log_index = try reader.readInt(u64, .little),
                .last_log_term = try reader.readInt(u64, .little),
            },
        },
        .pre_vote_response => .{
            .pre_vote_response = .{
                .term = try reader.readInt(u64, .little),
                .vote_granted = (try reader.readByte()) != 0,
            },
        },
        .read_index_request => .{
            .read_index_request = .{
                .read_id = try reader.readInt(u64, .little),
            },
        },
        .read_index_response => .{
            .read_index_response = .{
                .term = try reader.readInt(u64, .little),
                .read_index = try reader.readInt(u64, .little),
                .success = (try reader.readByte()) != 0,
            },
        },
        .timeout_now_request => .{
            .timeout_now_request = .{
                .term = try reader.readInt(u64, .little),
                .leader_id = try reader.readInt(u64, .little),
            },
        },
        .timeout_now_response => .{
            .timeout_now_response = .{
                .term = try reader.readInt(u64, .little),
            },
        },
        .add_server_request => .{
            .add_server_request = .{
                .new_server = try reader.readInt(u64, .little),
            },
        },
        .add_server_response => {
            const term = try reader.readInt(u64, .little);
            const success = (try reader.readByte()) != 0;
            const leader_id_raw = try reader.readInt(u64, .little);
            return .{
                .add_server_response = .{
                    .term = term,
                    .success = success,
                    .leader_id = if (leader_id_raw == 0) null else leader_id_raw,
                },
            };
        },
        .remove_server_request => .{
            .remove_server_request = .{
                .old_server = try reader.readInt(u64, .little),
            },
        },
        .remove_server_response => {
            const term = try reader.readInt(u64, .little);
            const success = (try reader.readByte()) != 0;
            const leader_id_raw = try reader.readInt(u64, .little);
            return .{
                .remove_server_response = .{
                    .term = term,
                    .success = success,
                    .leader_id = if (leader_id_raw == 0) null else leader_id_raw,
                },
            };
        },
    };
}

pub fn freeMessage(allocator: Allocator, msg: Message) void {
    switch (msg) {
        .append_entries_request => |req| {
            for (req.entries) |*entry| {
                var mutable_entry = entry.*;
                mutable_entry.data.deinit(allocator);
            }
            allocator.free(req.entries);
        },
        .install_snapshot_request => |req| {
            allocator.free(req.data);
        },
        else => {},
    }
}

test "serialize and deserialize RequestVote" {
    const allocator = std.testing.allocator;

    const msg = Message{
        .request_vote_request = .{
            .term = 5,
            .candidate_id = 2,
            .last_log_index = 10,
            .last_log_term = 4,
        },
    };

    const data = try serialize(allocator, msg);
    defer allocator.free(data);

    const parsed = try deserialize(allocator, data);
    defer freeMessage(allocator, parsed);

    try std.testing.expect(parsed == .request_vote_request);
    try std.testing.expectEqual(@as(u64, 5), parsed.request_vote_request.term);
    try std.testing.expectEqual(@as(u64, 2), parsed.request_vote_request.candidate_id);
}
