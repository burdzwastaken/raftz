const std = @import("std");
const raft = @import("raftz");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Raft implementation in Zig\n", .{});

    var kv = raft.KvStore.init(allocator);
    defer kv.deinit();

    const servers = [_]raft.ServerId{ 1, 2, 3 };
    const cluster = raft.ClusterConfig.simple(&servers);

    var node = try raft.Node.init(
        allocator,
        .{
            .id = 1,
            .election_timeout_min = 150,
            .election_timeout_max = 300,
            .heartbeat_interval = 50,
        },
        cluster,
        kv.stateMachine(),
        null,
    );
    defer node.deinit();

    std.debug.print("Node initialized:\n", .{});
    std.debug.print("  ID: {d}\n", .{node.config.id});
    std.debug.print("  Role: {s}\n", .{@tagName(node.getRole())});
    std.debug.print("  Term: {d}\n", .{node.getCurrentTerm()});
    std.debug.print("  Current Leader: {?d}\n", .{node.getLeader()});
    std.debug.print("  Cluster size: {d}\n", .{cluster.servers.len});
    std.debug.print("\n", .{});

    std.debug.print("Simulating RequestVote RPC...\n", .{});
    const vote_request = raft.RequestVoteRequest{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    };

    const vote_response = node.handleRequestVote(vote_request);
    std.debug.print("  Vote granted: {}\n", .{vote_response.vote_granted});
    std.debug.print("  Term: {d}\n", .{vote_response.term});
    std.debug.print("\n", .{});

    std.debug.print("Simulating AppendEntries RPC (heartbeat)...\n", .{});
    const append_request = raft.AppendEntriesRequest{
        .term = 1,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    };

    const append_response = try node.handleAppendEntries(append_request);
    std.debug.print("  Success: {}\n", .{append_response.success});
    std.debug.print("  Term: {d}\n", .{append_response.term});
    std.debug.print("\n", .{});

    std.debug.print("Example completed successfully!\n", .{});
}
