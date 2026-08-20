using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;

namespace Beskar.Networking.Raft.Transport;

/// <summary>
/// Container representing an incoming Raft RPC request.
/// </summary>
public sealed class RaftRpcRequest
{
   public RaftMessageType MessageType { get; }
   public object Payload { get; }

   public RaftRpcRequest(RaftMessageType messageType, object payload)
   {
      MessageType = messageType;
      Payload = payload;
   }

   public RequestVoteRequest AsRequestVote() => (RequestVoteRequest)Payload;
   public AppendEntriesRequest AsAppendEntries() => (AppendEntriesRequest)Payload;
   public InstallSnapshotRequest AsInstallSnapshot() => (InstallSnapshotRequest)Payload;
}

/// <summary>
/// Container representing an outgoing Raft RPC response.
/// </summary>
public sealed class RaftRpcResponse
{
   public RaftMessageType MessageType { get; }
   public object Payload { get; }

   public RaftRpcResponse(RaftMessageType messageType, object payload)
   {
      MessageType = messageType;
      Payload = payload;
   }

   public static RaftRpcResponse FromRequestVote(RequestVoteResponse response)
      => new(RaftMessageType.RequestVoteResponse, response);

   public static RaftRpcResponse FromAppendEntries(AppendEntriesResponse response)
      => new(RaftMessageType.AppendEntriesResponse, response);

   public static RaftRpcResponse FromInstallSnapshot(InstallSnapshotResponse response)
      => new(RaftMessageType.InstallSnapshotResponse, response);
}
