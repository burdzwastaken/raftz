const std = @import("std");

// core modules (for advanced use)
pub const log = @import("log.zig");
pub const logging = @import("logging.zig");
pub const network = @import("network.zig");
pub const node = @import("node.zig");
pub const persistence = @import("persistence.zig");
pub const protocol = @import("protocol.zig");
pub const rpc = @import("rpc.zig");
pub const server = @import("server.zig");
pub const state_machine = @import("state_machine.zig");
pub const types = @import("types.zig");

// commonly used types
pub const Node = node.Node;
pub const Server = server.Server;
pub const Transport = network.Transport;
pub const LogEntry = log.LogEntry;
pub const ServerId = types.ServerId;
pub const Term = types.Term;
pub const LogIndex = types.LogIndex;
pub const Role = types.Role;
pub const Config = types.Config;
pub const ClusterConfig = types.ClusterConfig;
pub const Storage = persistence.Storage;
pub const StateMachine = state_machine.StateMachine;
pub const KvStore = state_machine.KvStore;
pub const RequestVoteRequest = rpc.RequestVoteRequest;
pub const AppendEntriesRequest = rpc.AppendEntriesRequest;

test {
    std.testing.refAllDecls(@This());
}
