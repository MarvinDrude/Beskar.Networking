using Beskar.Networking.Raft.Protocol.Messages;

namespace Beskar.Networking.Raft.Transport;

/// <summary>
/// Defines the cluster transport abstraction for Raft peer-to-peer RPC communication.
/// </summary>
public interface IRaftTransport : IAsyncDisposable
{
   /// <summary>
   /// Starts the transport and registers the local RPC handler for incoming requests from peers.
   /// </summary>
   public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler, CancellationToken ct = default);

   /// <summary>
   /// Stops the transport and closes active peer connections.
   /// </summary>
   public ValueTask StopAsync(CancellationToken ct = default);

   /// <summary>
   /// Sends a RequestVote RPC to the specified peer and awaits response.
   /// </summary>
   public ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request, CancellationToken ct = default);

   /// <summary>
   /// Sends an AppendEntries RPC to the specified peer and awaits response.
   /// </summary>
   public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default);

   /// <summary>
   /// Sends an InstallSnapshot RPC to the specified peer and awaits response.
   /// </summary>
   public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken ct = default);
}
