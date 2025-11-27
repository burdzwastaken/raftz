//! Common types used throughout the Raft implementation

const std = @import("std");

/// Unique identifier for a server in the Raft cluster
pub const ServerId = u64;

/// Term number used for leader election, log consistency and used to detect stale leaders
pub const Term = u64;

/// Index position in the replicated log (1-based indexing)
pub const LogIndex = u64;

/// Role of a server in the Raft cluster
pub const Role = enum {
    /// Follower: Passive server that replicates log entries from the leader
    follower,
    /// PreCandidate: Server in pre-vote phase before starting real election
    pre_candidate,
    /// Candidate: Server attempting to become the leader through election
    candidate,
    /// Leader: Server that handles client requests and replicates logs to followers
    leader,
};

/// Configuration for a Raft node
pub const Config = struct {
    /// Unique identifier for this server
    id: ServerId,
    /// Minimum election timeout in milliseconds (default: 150ms)
    election_timeout_min: u64 = 150,
    /// Maximum election timeout in milliseconds (default: 300ms)
    election_timeout_max: u64 = 300,
    /// Heartbeat interval in milliseconds (default: 50ms)
    heartbeat_interval: u64 = 50,
    /// Maximum number of entries to send in a single AppendEntries RPC (default: 100)
    max_append_entries: usize = 100,
    /// Enable pre-vote optimization to prevent disruptions from partitioned nodes (default: true)
    enable_prevote: bool = true,
    /// Timeout for ReadIndex requests in milliseconds (default: 5000ms)
    read_timeout: u64 = 5000,
    /// Timeout for leadership transfer in milliseconds (default: election_timeout_max)
    leadership_transfer_timeout: ?u64 = null,

    /// Validates configuration parameters
    pub fn validate(self: Config) !void {
        if (self.election_timeout_max <= self.election_timeout_min) {
            return error.InvalidElectionTimeout;
        }
        if (self.heartbeat_interval >= self.election_timeout_min) {
            return error.HeartbeatTooLarge;
        }
        if (self.max_append_entries == 0) {
            return error.InvalidMaxAppendEntries;
        }
        if (self.max_append_entries > 10000) {
            return error.MaxAppendEntriesTooLarge;
        }
    }

    /// Generates a random election timeout within the configured range
    pub fn randomElectionTimeout(self: Config) u64 {
        var buf: [8]u8 = undefined;
        std.crypto.random.bytes(&buf);
        const seed = std.mem.readInt(u64, &buf, .little) ^ self.id;
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();
        return self.election_timeout_min +
            random.uintLessThan(u64, self.election_timeout_max - self.election_timeout_min);
    }
};

/// Cluster membership configuration
pub const ClusterConfig = struct {
    /// List of server IDs in the cluster
    servers: []const ServerId,

    /// Returns the number of votes needed for a majority
    /// For a cluster of N servers, majority is floor(N/2) + 1
    pub fn majoritySize(self: ClusterConfig) usize {
        return (self.servers.len / 2) + 1;
    }
};

test "Config.randomElectionTimeout" {
    const config = Config{
        .id = 1,
        .election_timeout_min = 150,
        .election_timeout_max = 300,
    };

    const timeout = config.randomElectionTimeout();
    try std.testing.expect(timeout >= 150);
    try std.testing.expect(timeout < 300);
}

test "Config.validate" {
    const valid_config = Config{
        .id = 1,
        .election_timeout_min = 150,
        .election_timeout_max = 300,
        .heartbeat_interval = 50,
    };
    try valid_config.validate();

    const invalid_timeout = Config{
        .id = 1,
        .election_timeout_min = 300,
        .election_timeout_max = 300,
    };
    try std.testing.expectError(error.InvalidElectionTimeout, invalid_timeout.validate());

    const invalid_heartbeat = Config{
        .id = 1,
        .election_timeout_min = 150,
        .election_timeout_max = 300,
        .heartbeat_interval = 150,
    };
    try std.testing.expectError(error.HeartbeatTooLarge, invalid_heartbeat.validate());
}

test "ClusterConfig.majoritySize" {
    const servers_3 = [_]ServerId{ 1, 2, 3 };
    const cluster_3 = ClusterConfig{ .servers = &servers_3 };
    try std.testing.expectEqual(@as(usize, 2), cluster_3.majoritySize());

    const servers_5 = [_]ServerId{ 1, 2, 3, 4, 5 };
    const cluster_5 = ClusterConfig{ .servers = &servers_5 };
    try std.testing.expectEqual(@as(usize, 3), cluster_5.majoritySize());
}
