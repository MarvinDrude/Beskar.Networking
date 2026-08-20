using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Events;

/// <summary>
/// Event context dispatched when node role transitions.
/// </summary>
/// <param name="NodeId">Identifier of the local node.</param>
/// <param name="OldRole">The previous role.</param>
/// <param name="NewRole">The new role.</param>
/// <param name="Term">The election term at the time of transition.</param>
public sealed record RaftRoleChangedContext(string NodeId, RaftRole OldRole, RaftRole NewRole, ulong Term);

/// <summary>
/// Event context dispatched when leader changes.
/// </summary>
/// <param name="NodeId">Identifier of the local node.</param>
/// <param name="LeaderId">The new leader's identifier (or null if unknown).</param>
/// <param name="Term">The election term.</param>
public sealed record RaftLeaderChangedContext(string NodeId, string? LeaderId, ulong Term);

/// <summary>
/// Event context dispatched when a log entry is committed and applied.
/// </summary>
/// <param name="NodeId">Identifier of the local node.</param>
/// <param name="Entry">The committed log entry.</param>
/// <param name="Result">The state machine execution result.</param>
public sealed record RaftEntryCommittedContext(string NodeId, RaftLogEntry Entry, ReadOnlyMemory<byte> Result);
