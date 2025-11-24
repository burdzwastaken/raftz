const std = @import("std");
const raft = @import("raftz");

pub fn cleanupDataDirs(dirs: []const []const u8) void {
    for (dirs) |dir| {
        std.fs.cwd().deleteTree(dir) catch |err| {
            if (err != error.FileNotFound) {
                std.debug.print("Warning: Failed to delete {s}: {}\n", .{ dir, err });
            }
        };
    }
}

pub fn printNodeStatus(node: *raft.Node) void {
    node.mutex.lock();
    const id = node.config.id;
    const role = node.role;
    const term = node.persistent.current_term;
    const leader = node.current_leader;
    const commit = node.volatile_state.commit_index;
    const log_size = node.log.lastIndex();
    node.mutex.unlock();

    std.debug.print("Node {d}: role={s}, term={d}, leader={?d}, commit={d}, log_size={d}\n", .{
        id,
        @tagName(role),
        term,
        leader,
        commit,
        log_size,
    });
}

pub fn printKvStore(kv: *raft.KvStore) void {
    var it = kv.data.iterator();
    var count: usize = 0;
    while (it.next()) |entry| {
        std.debug.print("  {s} = {s}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
        count += 1;
    }
    if (count == 0) {
        std.debug.print("  (empty)\n", .{});
    }
}
