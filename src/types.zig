const std = @import("std");

pub const ServerId = u64;

/// Term number in protocol
pub const Term = u64;

/// Log index (1-based)
pub const LogIndex = u64;

/// Server role
pub const Role = enum {
    follower,
    candidate,
    leader,
};

/// Configuration for a Raft node
pub const Config = struct {
    id: ServerId,
    election_timeout_min: u64 = 150,
    election_timeout_max: u64 = 300,
    heartbeat_interval: u64 = 50,
    max_append_entries: usize = 100,

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
    servers: []const ServerId,

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
