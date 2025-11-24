const std = @import("std");
const types = @import("types.zig");
const log = @import("log.zig");

const Term = types.Term;
const LogIndex = types.LogIndex;
const ServerId = types.ServerId;
const LogEntry = log.LogEntry;

/// RequestVote RPC request
pub const RequestVoteRequest = struct {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
};

/// RequestVote RPC response
pub const RequestVoteResponse = struct {
    term: Term,
    vote_granted: bool,
};

/// AppendEntries RPC request
pub const AppendEntriesRequest = struct {
    term: Term,
    leader_id: ServerId,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: []const LogEntry,
    leader_commit: LogIndex,
};

/// AppendEntries RPC response
pub const AppendEntriesResponse = struct {
    term: Term,
    success: bool,
    match_index: LogIndex,
};

/// InstallSnapshot RPC request
pub const InstallSnapshotRequest = struct {
    term: Term,
    leader_id: ServerId,
    last_included_index: LogIndex,
    last_included_term: Term,
    offset: u64,
    data: []const u8,
    done: bool,
};

/// InstallSnapshot RPC response
pub const InstallSnapshotResponse = struct {
    term: Term,
};
