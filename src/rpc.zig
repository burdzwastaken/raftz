//! Raft RPC message types
//!
//! Defines request and response structures for Raft RPCs

const std = @import("std");
const types = @import("types.zig");
const log = @import("log.zig");

const Term = types.Term;
const LogIndex = types.LogIndex;
const ServerId = types.ServerId;
const LogEntry = log.LogEntry;

/// Request from candidate seeking votes during election
pub const RequestVoteRequest = struct {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
};

/// Response to RequestVote RPC
pub const RequestVoteResponse = struct {
    term: Term,
    vote_granted: bool,
};

/// Request from leader to replicate log entries or send heartbeat
pub const AppendEntriesRequest = struct {
    term: Term,
    leader_id: ServerId,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: []const LogEntry,
    leader_commit: LogIndex,
};

/// Response to AppendEntries RPC
pub const AppendEntriesResponse = struct {
    term: Term,
    success: bool,
    match_index: LogIndex,
};

/// Request from leader to install snapshot on follower
pub const InstallSnapshotRequest = struct {
    term: Term,
    leader_id: ServerId,
    last_included_index: LogIndex,
    last_included_term: Term,
    offset: u64,
    data: []const u8,
    done: bool,
};

/// Response to InstallSnapshot RPC
pub const InstallSnapshotResponse = struct {
    term: Term,
};

/// Request for pre-vote phase
pub const PreVoteRequest = struct {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
};

/// Response to PreVote RPC
pub const PreVoteResponse = struct {
    term: Term,
    vote_granted: bool,
};

/// Request for read index (linearizable read without log replication)
pub const ReadIndexRequest = struct {
    read_id: u64,
};

/// Response to ReadIndex RPC
pub const ReadIndexResponse = struct {
    term: Term,
    read_index: LogIndex,
    success: bool,
};
