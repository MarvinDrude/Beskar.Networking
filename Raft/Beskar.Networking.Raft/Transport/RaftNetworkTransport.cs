using System.Buffers;
using System.Collections.Concurrent;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Raft.Protocol.Codec;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;

namespace Beskar.Networking.Raft.Transport;

/// <summary>
/// Production network transport implementation for Raft clusters running over any <see cref="INetworkListener"/> and <see cref="INetworkClient"/>.
/// </summary>
public sealed class RaftNetworkTransport : IRaftTransport
{
   private readonly INetworkListener _listener;
   private readonly IReadOnlyDictionary<string, RaftPeerEndpoint> _peers;
   private readonly ConcurrentDictionary<string, PeerConnectionHolder> _activePeerConnections = new();
   private readonly TimeSpan _rpcTimeout;
   private Func<RaftRpcRequest, ValueTask<RaftRpcResponse>>? _rpcHandler;
   private CancellationTokenSource? _cts;
   private Task? _acceptLoopTask;
   private int _disposed;

   public RaftNetworkTransport(
      INetworkListener listener,
      IEnumerable<RaftPeerEndpoint> peers,
      TimeSpan? rpcTimeout = null)
   {
      _listener = listener;
      _peers = peers.ToDictionary(p => p.PeerId, StringComparer.Ordinal);
      _rpcTimeout = rpcTimeout ?? TimeSpan.FromMilliseconds(1000);
   }

   public async ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler, CancellationToken ct = default)
   {
      _rpcHandler = rpcHandler;
      _cts = new CancellationTokenSource();

      var bindResult = await _listener.BindAsync(ct);
      if (bindResult.Failed)
      {
         throw new InvalidOperationException($"Failed to bind Raft transport listener: {bindResult.Error.Message}");
      }

      _acceptLoopTask = Task.Run(() => RunAcceptLoopAsync(_cts.Token), CancellationToken.None);
   }

   public async ValueTask StopAsync(CancellationToken ct = default)
   {
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return;
      }

      if (_cts != null)
      {
         await _cts.CancelAsync();
      }

      await _listener.UnbindAsync(ct);

      foreach (var holder in _activePeerConnections.Values)
      {
         await holder.DisposeAsync();
      }
      _activePeerConnections.Clear();

      if (_acceptLoopTask != null)
      {
         try
         {
            await _acceptLoopTask;
         }
         catch
         {
            // Ignore cancellation on shutdown
         }
      }
   }

   public async ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request, CancellationToken ct = default)
   {
      return await SendRpcAsync(peerId, RaftMessageType.RequestVote, request, ct) as RequestVoteResponse;
   }

   public async ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default)
   {
      return await SendRpcAsync(peerId, RaftMessageType.AppendEntries, request, ct) as AppendEntriesResponse;
   }

   public async ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken ct = default)
   {
      return await SendRpcAsync(peerId, RaftMessageType.InstallSnapshot, request, ct) as InstallSnapshotResponse;
   }

   private async ValueTask<object?> SendRpcAsync(string peerId, RaftMessageType type, object requestPayload, CancellationToken ct)
   {
      if (!_peers.TryGetValue(peerId, out var peerConfig))
      {
         return null;
      }

      using var rpcCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      rpcCts.CancelAfter(_rpcTimeout);

      try
      {
         var holder = _activePeerConnections.GetOrAdd(peerId, _ => new PeerConnectionHolder(peerConfig));
         return await holder.ExecuteRpcAsync(type, requestPayload, rpcCts.Token);
      }
      catch
      {
         return null;
      }
   }

   private async Task RunAcceptLoopAsync(CancellationToken cancellationToken)
   {
      while (!cancellationToken.IsCancellationRequested)
      {
         var sessionResult = await _listener.AcceptSessionAsync(cancellationToken);
         if (sessionResult.Failed || sessionResult.Success == null)
         {
            if (cancellationToken.IsCancellationRequested)
            {
               break;
            }
            continue;
         }

         var session = sessionResult.Success;
         _ = Task.Run(async () =>
         {
            try
            {
               var streamResult = await session.AcceptStreamAsync(cancellationToken);
               if (streamResult.Failed || streamResult.Success == null)
               {
                  return;
               }

               var stream = streamResult.Success;
               await HandleIncomingStreamAsync(stream, cancellationToken);
            }
            catch
            {
               // Session terminated
            }
            finally
            {
               await session.DisposeAsync();
            }
         }, CancellationToken.None);
      }
   }

   private async Task HandleIncomingStreamAsync(INetworkStream stream, CancellationToken cancellationToken)
   {
      var reader = stream.Transport.Input;
      var writer = stream.Transport.Output;
      var incomingRequests = new List<(RaftMessageType Type, object Payload)>(4);

      try
      {
         while (!cancellationToken.IsCancellationRequested)
         {
            var readResult = await reader.ReadAsync(cancellationToken);
            var buffer = readResult.Buffer;

            incomingRequests.Clear();
            var sequenceReader = new SequenceReader<byte>(buffer);
            while (RaftProtocolCodec.TryReadFrame(ref sequenceReader, out var messageType, out var payload))
            {
               if (payload != null)
               {
                  incomingRequests.Add((messageType, payload));
               }
            }

            reader.AdvanceTo(sequenceReader.Position, buffer.End);

            if (incomingRequests.Count > 0 && _rpcHandler != null)
            {
               for (var i = 0; i < incomingRequests.Count; i++)
               {
                  var (messageType, payload) = incomingRequests[i];
                  var rpcRequest = new RaftRpcRequest(messageType, payload);
                  var rpcResponse = await _rpcHandler(rpcRequest);

                  using (await stream.AcquireWriterLock(cancellationToken))
                  {
                     switch (rpcResponse.MessageType)
                     {
                        case RaftMessageType.RequestVoteResponse:
                           RaftProtocolCodec.WriteRequestVoteResponse(writer, (RequestVoteResponse)rpcResponse.Payload);
                           break;
                        case RaftMessageType.AppendEntriesResponse:
                           RaftProtocolCodec.WriteAppendEntriesResponse(writer, (AppendEntriesResponse)rpcResponse.Payload);
                           break;
                        case RaftMessageType.InstallSnapshotResponse:
                           RaftProtocolCodec.WriteInstallSnapshotResponse(writer, (InstallSnapshotResponse)rpcResponse.Payload);
                           break;
                     }
                     await writer.FlushAsync(cancellationToken);
                  }
               }
            }

            if (readResult.IsCompleted || readResult.IsCanceled)
            {
               break;
            }
         }
      }
      catch
      {
         // Stream closed or error
      }
      finally
      {
         await stream.DisposeAsync();
      }
   }

   public async ValueTask DisposeAsync()
   {
      await StopAsync();
   }

   private sealed class PeerConnectionHolder : IAsyncDisposable
   {
      private readonly RaftPeerEndpoint _peer;
      private readonly SemaphoreSlim _semaphore = new(1, 1);
      private INetworkClient? _client;
      private INetworkStream? _stream;

      public PeerConnectionHolder(RaftPeerEndpoint peer)
      {
         _peer = peer;
      }

      public async ValueTask<object?> ExecuteRpcAsync(RaftMessageType type, object payload, CancellationToken ct)
      {
         await _semaphore.WaitAsync(ct);
         try
         {
            var stream = await GetOrCreateStreamAsync(ct);
            if (stream == null)
            {
               return null;
            }

            var writer = stream.Transport.Output;
            var reader = stream.Transport.Input;

            using (await stream.AcquireWriterLock(ct))
            {
               switch (type)
               {
                  case RaftMessageType.RequestVote:
                     RaftProtocolCodec.WriteRequestVote(writer, (RequestVoteRequest)payload);
                     break;
                  case RaftMessageType.AppendEntries:
                     RaftProtocolCodec.WriteAppendEntries(writer, (AppendEntriesRequest)payload);
                     break;
                  case RaftMessageType.InstallSnapshot:
                     RaftProtocolCodec.WriteInstallSnapshot(writer, (InstallSnapshotRequest)payload);
                     break;
               }
               await writer.FlushAsync(ct);
            }

            var expectedResponseType = type switch
            {
               RaftMessageType.RequestVote => RaftMessageType.RequestVoteResponse,
               RaftMessageType.AppendEntries => RaftMessageType.AppendEntriesResponse,
               RaftMessageType.InstallSnapshot => RaftMessageType.InstallSnapshotResponse,
               _ => (RaftMessageType)0
            };

            while (!ct.IsCancellationRequested)
            {
               var readResult = await reader.ReadAsync(ct);
               var buffer = readResult.Buffer;

               object? responsePayload = null;
               var matched = false;
               var seqReader = new SequenceReader<byte>(buffer);
               while (RaftProtocolCodec.TryReadFrame(ref seqReader, out var responseType, out var framePayload))
               {
                  if (responseType == expectedResponseType)
                  {
                     responsePayload = framePayload;
                     matched = true;
                     break;
                  }
               }

               reader.AdvanceTo(seqReader.Position, buffer.End);

               if (matched)
               {
                  return responsePayload;
               }

               if (readResult.IsCompleted || readResult.IsCanceled)
               {
                  await ResetConnectionAsync();
                  return null;
               }
            }

            return null;
         }
         catch
         {
            await ResetConnectionAsync();
            return null;
         }
         finally
         {
            _semaphore.Release();
         }
      }

      private async ValueTask<INetworkStream?> GetOrCreateStreamAsync(CancellationToken ct)
      {
         if (_stream != null && _client != null && _client.IsConnected)
         {
            return _stream;
         }

         await ResetConnectionAsync();

         _client = _peer.ClientFactory();
         var connectResult = await _client.ConnectAsync(_peer.EndPoint, ct);
         if (connectResult.Failed || connectResult.Success == null)
         {
            return null;
         }

         var session = connectResult.Success;
         var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
         if (streamResult.Failed || streamResult.Success == null)
         {
            return null;
         }

         _stream = streamResult.Success;
         return _stream;
      }

      private async ValueTask ResetConnectionAsync()
      {
         if (_stream != null)
         {
            await _stream.DisposeAsync();
            _stream = null;
         }

         if (_client != null)
         {
            await _client.DisposeAsync();
            _client = null;
         }
      }

      public async ValueTask DisposeAsync()
      {
         await ResetConnectionAsync();
         _semaphore.Dispose();
      }
   }
}
