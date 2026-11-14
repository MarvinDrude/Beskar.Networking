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
   private readonly ConcurrentDictionary<long, Task> _activeSessions = new();
   private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
   private readonly TimeSpan _rpcTimeout;
   private Func<RaftRpcRequest, ValueTask<RaftRpcResponse>>? _rpcHandler;
   private CancellationTokenSource? _cts;
   private Task? _acceptLoopTask;
   private long _nextSessionTaskId;
   private int _running;
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
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

      await _lifecycleLock.WaitAsync(ct);
      try
      {
         ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
         if (Volatile.Read(ref _running) != 0)
         {
            return;
         }

         var bindResult = await _listener.BindAsync(ct);
         if (bindResult.Failed)
         {
            throw new InvalidOperationException($"Failed to bind Raft transport listener: {bindResult.Error.Message}");
         }

         _rpcHandler = rpcHandler;
         _cts?.Dispose();
         _cts = new CancellationTokenSource();
         Volatile.Write(ref _running, 1);
         _acceptLoopTask = Task.Run(() => RunAcceptLoopAsync(_cts.Token), CancellationToken.None);
      }
      finally
      {
         _lifecycleLock.Release();
      }
   }

   public async ValueTask StopAsync(CancellationToken ct = default)
   {
      await _lifecycleLock.WaitAsync(ct);
      try
      {
         if (Interlocked.Exchange(ref _running, 0) == 0)
         {
            return;
         }

         if (_cts != null)
         {
            await _cts.CancelAsync();
         }

         try
         {
            await _listener.UnbindAsync(ct);
         }
         catch
         {
            // Continue cleanup even if listener unbind fails
         }

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
            _acceptLoopTask = null;
         }

         if (!_activeSessions.IsEmpty)
         {
            try
            {
               await Task.WhenAll(_activeSessions.Values);
            }
            catch
            {
               // Ignore cancellation on shutdown
            }
         }

         _cts?.Dispose();
         _cts = null;
      }
      finally
      {
         _lifecycleLock.Release();
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
      if (Volatile.Read(ref _disposed) != 0 || !_peers.TryGetValue(peerId, out var peerConfig))
      {
         return null;
      }

      var transportCts = Volatile.Read(ref _cts);
      if (transportCts is { IsCancellationRequested: true })
      {
         return null;
      }

      using var rpcCts = transportCts != null
         ? CancellationTokenSource.CreateLinkedTokenSource(ct, transportCts.Token)
         : CancellationTokenSource.CreateLinkedTokenSource(ct);

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
         INetworkSession? session = null;
         try
         {
            var sessionResult = await _listener.AcceptSessionAsync(cancellationToken);
            if (!sessionResult.Failed)
            {
               session = sessionResult.Success;
            }
         }
         catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
         {
            break;
         }
         catch
         {
         }

         if (session == null)
         {
            if (cancellationToken.IsCancellationRequested)
            {
               break;
            }

            try
            {
               await Task.Delay(5, cancellationToken);
            }
            catch (OperationCanceledException)
            {
               break;
            }
            continue;
         }
         var taskId = Interlocked.Increment(ref _nextSessionTaskId);
         var sessionTask = Task.Run(async () =>
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
               _activeSessions.TryRemove(taskId, out _);
            }
         }, CancellationToken.None);

         if (!sessionTask.IsCompleted)
         {
            _activeSessions[taskId] = sessionTask;
            if (sessionTask.IsCompleted)
            {
               _activeSessions.TryRemove(taskId, out _);
            }
         }
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
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return;
      }

      await StopAsync();
   }

   private sealed class PeerConnectionHolder : IAsyncDisposable
   {
      private readonly RaftPeerEndpoint _peer;
      private readonly SemaphoreSlim _semaphore = new(1, 1);
      private INetworkClient? _client;
      private INetworkSession? _session;
      private INetworkStream? _stream;

      public PeerConnectionHolder(RaftPeerEndpoint peer)
      {
         _peer = peer;
      }

      public async ValueTask<object?> ExecuteRpcAsync(RaftMessageType type, object payload, CancellationToken ct)
      {
         try
         {
            await _semaphore.WaitAsync(ct);
         }
         catch (ObjectDisposedException)
         {
            return null;
         }

         try
         {
            for (var attempt = 0; attempt < 2; attempt++)
            {
               var wasPooled = _stream != null && _client != null && _client.IsConnected;
               var stream = await GetOrCreateStreamAsync(ct);
               if (stream == null)
               {
                  return null;
               }

               try
               {
                  var writer = stream.Transport.Output;
                  var reader = stream.Transport.Input;

                  using (await stream.AcquireWriterLock(ct))
                  {
                     WriteRequest(writer, type, payload);
                     await writer.FlushAsync(ct);
                  }

                  var expectedResponseType = GetExpectedResponseType(type);
                  var streamClosedWithoutMatch = false;

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

                     if (matched)
                     {
                        // Leave remaining buffered bytes unexamined so sequential RPC reads see them immediately.
                        reader.AdvanceTo(seqReader.Position);
                        return responsePayload;
                     }

                     reader.AdvanceTo(seqReader.Position, buffer.End);

                     if (readResult.IsCompleted || readResult.IsCanceled)
                     {
                        streamClosedWithoutMatch = true;
                        break;
                     }
                  }

                  if (streamClosedWithoutMatch)
                  {
                     await ResetConnectionAsync();
                     if (wasPooled && attempt == 0 && !ct.IsCancellationRequested)
                     {
                        continue;
                     }
                  }

                  return null;
               }
               catch (OperationCanceledException) when (ct.IsCancellationRequested)
               {
                  await ResetConnectionAsync();
                  return null;
               }
               catch
               {
                  await ResetConnectionAsync();
                  if (wasPooled && attempt == 0 && !ct.IsCancellationRequested)
                  {
                     continue;
                  }

                  return null;
               }
            }

            return null;
         }
         finally
         {
            try
            {
               _semaphore.Release();
            }
            catch (ObjectDisposedException)
            {
            }
         }
      }

      private static void WriteRequest(System.Buffers.IBufferWriter<byte> writer, RaftMessageType type, object payload)
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
      }

      private static RaftMessageType GetExpectedResponseType(RaftMessageType type) => type switch
      {
         RaftMessageType.RequestVote => RaftMessageType.RequestVoteResponse,
         RaftMessageType.AppendEntries => RaftMessageType.AppendEntriesResponse,
         RaftMessageType.InstallSnapshot => RaftMessageType.InstallSnapshotResponse,
         _ => (RaftMessageType)0
      };

      private async ValueTask<INetworkStream?> GetOrCreateStreamAsync(CancellationToken ct)
      {
         if (_stream != null && _client != null && _client.IsConnected)
         {
            return _stream;
         }

         await ResetConnectionAsync();

         var client = _peer.ClientFactory();
         INetworkSession? session = null;
         INetworkStream? stream = null;
         try
         {
            var connectResult = await client.ConnectAsync(_peer.EndPoint, ct);
            if (connectResult.Failed || connectResult.Success == null)
            {
               await client.DisposeAsync();
               return null;
            }

            session = connectResult.Success;
            var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
            if (streamResult.Failed || streamResult.Success == null)
            {
               await DisposeClientAndUnownedSessionAsync(client, session);
               return null;
            }

            stream = streamResult.Success;
            _client = client;
            _session = session;
            _stream = stream;
            return _stream;
         }
         catch
         {
            if (stream != null)
            {
               await stream.DisposeAsync();
            }

            await DisposeClientAndUnownedSessionAsync(client, session);
            throw;
         }
      }

      private async ValueTask ResetConnectionAsync()
      {
         var stream = _stream;
         _stream = null;
         var session = _session;
         _session = null;
         var client = _client;
         _client = null;

         if (stream != null)
         {
            try
            {
               await stream.DisposeAsync();
            }
            catch
            {
            }
         }

         await DisposeClientAndUnownedSessionAsync(client, session);
      }

      private static async ValueTask DisposeClientAndUnownedSessionAsync(INetworkClient? client, INetworkSession? session)
      {
         var ownsSession = session != null && client != null && ReferenceEquals(session, client.Session);

         if (client != null)
         {
            try
            {
               await client.DisposeAsync();
            }
            catch
            {
            }
         }

         if (session != null && !ownsSession)
         {
            try
            {
               await session.DisposeAsync();
            }
            catch
            {
            }
         }
      }

      public async ValueTask DisposeAsync()
      {
         await ResetConnectionAsync();
         _semaphore.Dispose();
      }
   }
}
