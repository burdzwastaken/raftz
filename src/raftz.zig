//! Raft consensus algorithm implementation in Zig
//!
//! This library provides a complete implementation of the Raft consensus algorithm,
//! including leader election, log replication and state machine replication

const std = @import("std");

/// Log management and replication
pub const log = @import("log.zig");
/// Structured logging utilities
pub const logging = @import("logging.zig");
/// Network transport layer
pub const network = @import("network.zig");
/// Raft node implementation
pub const node = @import("node.zig");
/// Persistent state storage
pub const persistence = @import("persistence.zig");
/// Raft protocol implementation
pub const protocol = @import("protocol.zig");
/// RPC message types
pub const rpc = @import("rpc.zig");
/// Server orchestration
pub const server = @import("server.zig");
/// Pluggable state machine interface
pub const state_machine = @import("state_machine.zig");
/// Client session tracking for request deduplication
pub const session = @import("session.zig");
/// Common type definitions
pub const types = @import("types.zig");

/// A Raft cluster node handling consensus operations
pub const Node = node.Node;
/// Server that orchestrates the Raft node and network transport
pub const Server = server.Server;
/// Network transport for RPC communication between nodes
pub const Transport = network.Transport;
/// A single entry in the replicated log
pub const LogEntry = log.LogEntry;
/// Unique identifier for a server in the cluster
pub const ServerId = types.ServerId;
/// Raft term number for leader election
pub const Term = types.Term;
/// Index position in the replicated log
pub const LogIndex = types.LogIndex;
/// Unique identifier for a client session
pub const ClientId = types.ClientId;
/// Sequence number for client requests
pub const SequenceNumber = types.SequenceNumber;
/// Node role (follower, candidate, or leader)
pub const Role = types.Role;
/// Node configuration parameters
pub const Config = types.Config;
/// Cluster membership configuration
pub const ClusterConfig = types.ClusterConfig;
/// Persistent storage interface for node state
pub const Storage = persistence.Storage;
/// State machine interface for applying committed log entries
pub const StateMachine = state_machine.StateMachine;
/// Built-in key-value store state machine
pub const KvStore = state_machine.KvStore;
/// RPC request for pre-vote phase (prevents election disruptions)
pub const PreVoteRequest = rpc.PreVoteRequest;
/// RPC response for pre-vote phase
pub const PreVoteResponse = rpc.PreVoteResponse;
/// RPC request for vote during leader election
pub const RequestVoteRequest = rpc.RequestVoteRequest;
/// RPC request for log replication and heartbeats
pub const AppendEntriesRequest = rpc.AppendEntriesRequest;
/// RPC request for linearizable reads
pub const ReadIndexRequest = rpc.ReadIndexRequest;
/// RPC response for ReadIndex protocol
pub const ReadIndexResponse = rpc.ReadIndexResponse;
/// RPC request to immediately start election (leadership transfer)
pub const TimeoutNowRequest = rpc.TimeoutNowRequest;
/// RPC response for TimeoutNow
pub const TimeoutNowResponse = rpc.TimeoutNowResponse;

test {
    std.testing.refAllDecls(@This());
}
