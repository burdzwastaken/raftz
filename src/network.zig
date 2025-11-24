const std = @import("std");
const types = @import("types.zig");
const rpc = @import("rpc.zig");
const protocol = @import("protocol.zig");
const logging = @import("logging.zig");

const ServerId = types.ServerId;
const Allocator = std.mem.Allocator;

/// Address information for a peer
pub const Address = struct {
    host: []const u8,
    port: u16,

    pub fn format(self: Address, writer: anytype) !void {
        try writer.print("{s}:{d}", .{ self.host, self.port });
    }
};

/// Connection to a peer
pub const Connection = struct {
    stream: std.net.Stream,
    address: Address,
    owns_address: bool,

    pub fn init(stream: std.net.Stream, address: Address, owns_address: bool) Connection {
        return .{
            .stream = stream,
            .address = address,
            .owns_address = owns_address,
        };
    }

    pub fn close(self: *Connection, allocator: Allocator) void {
        self.stream.close();
        if (self.owns_address) {
            allocator.free(self.address.host);
        }
    }

    pub fn sendMessage(self: *Connection, allocator: Allocator, msg: protocol.Message) !void {
        const data = try protocol.serialize(allocator, msg);
        defer allocator.free(data);

        const len: u32 = @intCast(data.len);
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, len, .little);

        const written_len = try self.stream.write(&len_bytes);
        if (written_len != 4) return error.PartialWrite;

        var pos: usize = 0;
        while (pos < data.len) {
            const n = try self.stream.write(data[pos..]);
            if (n == 0) return error.EndOfStream;
            pos += n;
        }
    }

    pub fn receiveMessage(self: *Connection, allocator: Allocator, max_message_size: usize) !protocol.Message {
        var len_bytes: [4]u8 = undefined;
        var read_pos: usize = 0;
        while (read_pos < 4) {
            const n = try self.stream.read(len_bytes[read_pos..]);
            if (n == 0) return error.EndOfStream;
            read_pos += n;
        }

        const len = std.mem.readInt(u32, &len_bytes, .little);

        if (len > max_message_size) {
            return error.MessageTooLarge;
        }

        if (len == 0) {
            return error.EmptyMessage;
        }

        const data = try allocator.alloc(u8, len);
        errdefer allocator.free(data);

        var pos: usize = 0;
        while (pos < len) {
            const n = try self.stream.read(data[pos..]);
            if (n == 0) return error.EndOfStream;
            pos += n;
        }

        defer allocator.free(data);
        return protocol.deserialize(allocator, data);
    }

    pub fn sendRequest(
        self: *Connection,
        allocator: Allocator,
        request: protocol.Message,
        timeout_ms: u64,
    ) !protocol.Message {
        if (comptime @hasDecl(std.posix, "timeval")) {
            const timeout_sec: u32 = @intCast(timeout_ms / 1000);
            const timeout_usec: u32 = @intCast((timeout_ms % 1000) * 1000);

            const timeval = std.posix.timeval{
                .sec = @intCast(timeout_sec),
                .usec = @intCast(timeout_usec),
            };

            std.posix.setsockopt(
                self.stream.handle,
                std.posix.SOL.SOCKET,
                std.posix.SO.RCVTIMEO,
                std.mem.asBytes(&timeval),
            ) catch |err| {
                logging.debug("Failed to set receive timeout: {}", .{err});
            };

            std.posix.setsockopt(
                self.stream.handle,
                std.posix.SOL.SOCKET,
                std.posix.SO.SNDTIMEO,
                std.mem.asBytes(&timeval),
            ) catch |err| {
                logging.debug("Failed to set send timeout: {}", .{err});
            };
        }

        try self.sendMessage(allocator, request);
        return self.receiveMessage(allocator, protocol.MAX_MESSAGE_SIZE);
    }
};

/// Server for accepting incoming connections
pub const Server = struct {
    allocator: Allocator,
    listener: std.net.Server,
    address: Address,

    pub fn init(allocator: Allocator, address: Address) !Server {
        const addr = try std.net.Address.parseIp(address.host, address.port);
        const listener = try addr.listen(.{
            .reuse_address = true,
            .force_nonblocking = true,
        });

        logging.info("Server listening on {f}", .{address});

        return .{
            .allocator = allocator,
            .listener = listener,
            .address = address,
        };
    }

    pub fn deinit(self: *Server) void {
        self.listener.deinit();
    }

    pub fn accept(self: *Server) !Connection {
        const conn = try self.listener.accept();
        return Connection.init(conn.stream, .{
            .host = try self.allocator.dupe(u8, "unknown"),
            .port = 0,
        }, true);
    }
};

/// Network transport for sending RPCs between nodes
pub const Transport = struct {
    allocator: Allocator,
    peers: std.AutoHashMap(ServerId, Address),
    server: ?Server,
    timeout_ms: u64,
    max_message_size: usize,

    pub fn init(allocator: Allocator, timeout_ms: u64) Transport {
        return .{
            .allocator = allocator,
            .peers = std.AutoHashMap(ServerId, Address).init(allocator),
            .server = null,
            .timeout_ms = timeout_ms,
            .max_message_size = protocol.MAX_MESSAGE_SIZE,
        };
    }

    pub fn initWithMaxSize(allocator: Allocator, timeout_ms: u64, max_message_size: usize) Transport {
        return .{
            .allocator = allocator,
            .peers = std.AutoHashMap(ServerId, Address).init(allocator),
            .server = null,
            .timeout_ms = timeout_ms,
            .max_message_size = max_message_size,
        };
    }

    pub fn deinit(self: *Transport) void {
        var it = self.peers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.host);
        }
        self.peers.deinit();

        if (self.server) |*server| {
            server.deinit();
        }
    }

    pub fn listen(self: *Transport, address: Address) !void {
        self.server = try Server.init(self.allocator, address);
    }

    pub fn addPeer(self: *Transport, id: ServerId, address: Address) !void {
        const host_copy = try self.allocator.dupe(u8, address.host);
        try self.peers.put(id, .{
            .host = host_copy,
            .port = address.port,
        });
    }

    fn connectToPeer(self: *Transport, target: ServerId) !Connection {
        const address = self.peers.get(target) orelse return error.ServerNotFound;
        const addr = try std.net.Address.parseIp(address.host, address.port);
        const stream = try std.net.tcpConnectToAddress(addr);
        return Connection.init(stream, address, false);
    }

    pub fn sendRequestVote(
        self: *Transport,
        target: ServerId,
        request: rpc.RequestVoteRequest,
    ) !rpc.RequestVoteResponse {
        var conn = try self.connectToPeer(target);
        defer conn.close(self.allocator);

        const request_msg = protocol.Message{ .request_vote_request = request };
        const response_msg = try conn.sendRequest(self.allocator, request_msg, self.timeout_ms);

        return switch (response_msg) {
            .request_vote_response => |resp| resp,
            else => error.InvalidResponse,
        };
    }

    pub fn sendAppendEntries(
        self: *Transport,
        target: ServerId,
        request: rpc.AppendEntriesRequest,
    ) !rpc.AppendEntriesResponse {
        var conn = try self.connectToPeer(target);
        defer conn.close(self.allocator);

        const request_msg = protocol.Message{ .append_entries_request = request };
        const response_msg = try conn.sendRequest(self.allocator, request_msg, self.timeout_ms);

        return switch (response_msg) {
            .append_entries_response => |resp| resp,
            else => error.InvalidResponse,
        };
    }

    pub fn sendInstallSnapshot(
        self: *Transport,
        target: ServerId,
        request: rpc.InstallSnapshotRequest,
    ) !rpc.InstallSnapshotResponse {
        var conn = try self.connectToPeer(target);
        defer conn.close(self.allocator);

        const request_msg = protocol.Message{ .install_snapshot_request = request };
        const response_msg = try conn.sendRequest(self.allocator, request_msg, self.timeout_ms);

        return switch (response_msg) {
            .install_snapshot_response => |resp| resp,
            else => error.InvalidResponse,
        };
    }

    pub fn handleConnection(self: *Transport, node: anytype, conn: *Connection) !void {
        defer conn.close(self.allocator);

        while (true) {
            const msg = conn.receiveMessage(self.allocator, self.max_message_size) catch |err| {
                if (err == error.EndOfStream) return;
                logging.debug("Failed to receive message: {}", .{err});
                return err;
            };
            defer protocol.freeMessage(self.allocator, msg);

            const response: protocol.Message = switch (msg) {
                .request_vote_request => |req| .{
                    .request_vote_response = node.handleRequestVote(req),
                },
                .append_entries_request => |req| .{
                    .append_entries_response = try node.handleAppendEntries(req),
                },
                .install_snapshot_request => |req| .{
                    .install_snapshot_response = try node.handleInstallSnapshot(req),
                },
                else => return error.InvalidRequest,
            };

            conn.sendMessage(self.allocator, response) catch |err| {
                if (err == error.BrokenPipe or err == error.EndOfStream or err == error.ConnectionResetByPeer) {
                    logging.debug("Connection closed while sending response: {}", .{err});
                    return;
                }
                return err;
            };
        }
    }
};

test "Address formatting" {
    const addr = Address{ .host = "127.0.0.1", .port = 5000 };

    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try std.fmt.format(fbs.writer(), "{f}", .{addr});
    const result = fbs.getWritten();

    try std.testing.expectEqualStrings("127.0.0.1:5000", result);
}
