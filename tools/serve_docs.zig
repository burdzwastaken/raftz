const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const address = try std.net.Address.parseIp("127.0.0.1", 8080);
    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    const url = "http://127.0.0.1:8080";
    std.debug.print("serving docs at {s}\n", .{url});
    std.debug.print("press Ctrl+C to stop\n", .{});

    while (true) {
        const connection = try server.accept();
        const thread = try std.Thread.spawn(.{}, handleConnection, .{ allocator, connection });
        thread.detach();
    }
}

fn handleConnection(allocator: std.mem.Allocator, connection: std.net.Server.Connection) void {
    defer connection.stream.close();
    handleRequest(allocator, connection) catch |err| {
        std.debug.print("Error handling request: {}\n", .{err});
    };
}

fn handleRequest(allocator: std.mem.Allocator, connection: std.net.Server.Connection) !void {
    var buffer: [4096]u8 = undefined;
    const bytes_read = try connection.stream.read(&buffer);
    if (bytes_read == 0) return;

    const request = buffer[0..bytes_read];

    var lines = std.mem.splitScalar(u8, request, '\n');
    const request_line = lines.first();

    var parts = std.mem.splitScalar(u8, request_line, ' ');
    _ = parts.next();
    const path = parts.next() orelse "/";

    // file path
    const file_path = if (std.mem.eql(u8, path, "/"))
        "zig-out/docs/index.html"
    else
        try std.fmt.allocPrint(allocator, "zig-out/docs{s}", .{path});
    defer if (!std.mem.eql(u8, path, "/")) allocator.free(file_path);

    const file = std.fs.cwd().openFile(file_path, .{}) catch {
        try send404(connection.stream);
        return;
    };
    defer file.close();

    const file_size = (try file.stat()).size;
    const content_type = getContentType(file_path);

    const header = try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: {s}\r\n" ++
        "Content-Length: {d}\r\n" ++
        "Connection: close\r\n" ++
        "\r\n", .{ content_type, file_size });
    defer allocator.free(header);

    _ = try connection.stream.writeAll(header);

    // chunks to avoid loading large files into memory
    var file_buffer: [8192]u8 = undefined;
    while (true) {
        const n = try file.read(&file_buffer);
        if (n == 0) break;
        _ = try connection.stream.writeAll(file_buffer[0..n]);
    }
}

fn send404(stream: std.net.Stream) !void {
    const response = "HTTP/1.1 404 Not Found\r\n" ++
        "Content-Type: text/plain\r\n" ++
        "Content-Length: 13\r\n" ++
        "Connection: close\r\n" ++
        "\r\n" ++
        "404 Not Found";
    _ = try stream.writeAll(response);
}

fn getContentType(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".js")) return "application/javascript";
    if (std.mem.endsWith(u8, path, ".wasm")) return "application/wasm";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css";
    if (std.mem.endsWith(u8, path, ".tar")) return "application/x-tar";
    return "application/octet-stream";
}
