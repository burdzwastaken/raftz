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

/// Configuration type for cluster membership
pub const ConfigurationType = enum {
    /// Simple configuration with one set of servers
    simple,
    /// Joint consensus configuration with old and new sets
    joint,
};

/// Cluster membership configuration
pub const ClusterConfig = struct {
    /// Type of configuration (simple or joint)
    config_type: ConfigurationType,
    /// List of voting server IDs in the current/old configuration
    servers: []const ServerId,
    /// List of server IDs in the new configuration (for joint consensus)
    new_servers: ?[]const ServerId,
    /// List of non-voting learner server IDs (receive logs but don't vote)
    learners: []const ServerId,

    /// Create a simple configuration with one set of servers
    pub fn simple(servers: []const ServerId) ClusterConfig {
        return .{
            .config_type = .simple,
            .servers = servers,
            .new_servers = null,
            .learners = &.{},
        };
    }

    /// Create a simple configuration with voting servers and learners
    pub fn withLearners(servers: []const ServerId, learners: []const ServerId) ClusterConfig {
        return .{
            .config_type = .simple,
            .servers = servers,
            .new_servers = null,
            .learners = learners,
        };
    }

    /// Create a joint consensus configuration
    pub fn joint(old_servers: []const ServerId, new_servers: []const ServerId) ClusterConfig {
        return .{
            .config_type = .joint,
            .servers = old_servers,
            .new_servers = new_servers,
            .learners = &.{},
        };
    }

    /// Create a joint consensus configuration with learners
    pub fn jointWithLearners(old_servers: []const ServerId, new_servers: []const ServerId, learners: []const ServerId) ClusterConfig {
        return .{
            .config_type = .joint,
            .servers = old_servers,
            .new_servers = new_servers,
            .learners = learners,
        };
    }

    /// Returns the number of votes needed for a majority
    /// For a cluster of N servers, majority is floor(N/2) + 1
    /// For joint consensus, requires majorities from BOTH configurations
    pub fn majoritySize(self: ClusterConfig) usize {
        return (self.servers.len / 2) + 1;
    }

    /// Returns the majority size for the new configuration (joint consensus only)
    pub fn newMajoritySize(self: ClusterConfig) ?usize {
        if (self.new_servers) |new| {
            return (new.len / 2) + 1;
        }
        return null;
    }

    /// Check if a server is part of the configuration (voting or learner)
    pub fn contains(self: ClusterConfig, server_id: ServerId) bool {
        for (self.servers) |id| {
            if (id == server_id) return true;
        }
        if (self.new_servers) |new| {
            for (new) |id| {
                if (id == server_id) return true;
            }
        }
        for (self.learners) |id| {
            if (id == server_id) return true;
        }
        return false;
    }

    /// Check if a server is a voting member
    pub fn isVoter(self: ClusterConfig, server_id: ServerId) bool {
        for (self.servers) |id| {
            if (id == server_id) return true;
        }
        if (self.new_servers) |new| {
            for (new) |id| {
                if (id == server_id) return true;
            }
        }
        return false;
    }

    /// Check if a server is a learner
    pub fn isLearner(self: ClusterConfig, server_id: ServerId) bool {
        for (self.learners) |id| {
            if (id == server_id) return true;
        }
        return false;
    }

    /// Get all servers that should receive log replication (voters + learners)
    pub fn allServers(self: ClusterConfig) []const ServerId {
        // tbh the caller should iterate over servers/new_servers/learners
        // separately...for convenience I thought I would provide this method
        return self.servers;
    }

    /// Check if we have a majority of votes in the configuration
    /// For simple config: need majority from servers
    /// For joint config: need majorities from BOTH old and new servers
    pub fn hasQuorum(self: ClusterConfig, votes: std.AutoHashMap(ServerId, bool)) bool {
        const old_votes = countVotes(self.servers, votes);
        const has_old_majority = old_votes >= self.majoritySize();

        switch (self.config_type) {
            .simple => return has_old_majority,
            .joint => {
                if (!has_old_majority) return false;
                const new_votes = countVotes(self.new_servers.?, votes);
                const new_maj = self.newMajoritySize().?;
                return new_votes >= new_maj;
            },
        }
    }

    fn countVotes(servers: []const ServerId, votes: std.AutoHashMap(ServerId, bool)) usize {
        var count: usize = 0;
        for (servers) |server_id| {
            if (votes.get(server_id)) |granted| {
                if (granted) count += 1;
            }
        }
        return count;
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
    const cluster_3 = ClusterConfig.simple(&servers_3);
    try std.testing.expectEqual(@as(usize, 2), cluster_3.majoritySize());

    const servers_5 = [_]ServerId{ 1, 2, 3, 4, 5 };
    const cluster_5 = ClusterConfig.simple(&servers_5);
    try std.testing.expectEqual(@as(usize, 3), cluster_5.majoritySize());
}

test "ClusterConfig.joint" {
    const old_servers = [_]ServerId{ 1, 2, 3 };
    const new_servers = [_]ServerId{ 1, 2, 4 };
    const joint_config = ClusterConfig.joint(&old_servers, &new_servers);

    try std.testing.expectEqual(ConfigurationType.joint, joint_config.config_type);
    try std.testing.expectEqual(@as(usize, 2), joint_config.majoritySize());
    try std.testing.expectEqual(@as(usize, 2), joint_config.newMajoritySize().?);
}

test "ClusterConfig.contains" {
    const servers = [_]ServerId{ 1, 2, 3 };
    const config = ClusterConfig.simple(&servers);

    try std.testing.expect(config.contains(1));
    try std.testing.expect(config.contains(2));
    try std.testing.expect(!config.contains(4));
}

test "ClusterConfig.hasQuorum" {
    const servers = [_]ServerId{ 1, 2, 3 };
    const config = ClusterConfig.simple(&servers);

    var votes = std.AutoHashMap(ServerId, bool).init(std.testing.allocator);
    defer votes.deinit();

    try votes.put(1, true);
    try std.testing.expect(!config.hasQuorum(votes)); // 1 vote not enough

    try votes.put(2, true);
    try std.testing.expect(config.hasQuorum(votes)); // 2 votes is majority
}
