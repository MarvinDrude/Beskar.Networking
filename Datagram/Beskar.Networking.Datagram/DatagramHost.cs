using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Nat;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Reliability;
using Beskar.Networking.Datagram.Security;
using Beskar.Networking.Datagram.Telemetry;
using Beskar.Networking.Datagram.Transport;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Datagram;

/// <summary>
/// Exception thrown when a remote peer or server explicitly rejects an incoming handshake.
/// </summary>
public sealed class DatagramConnectionRejectedException(string message, byte[]? rejectReason = null)
   : Exception(message)
{
   public ReadOnlyMemory<byte> RejectReason { get; } = rejectReason ?? [];
}

/// <summary>
/// High-performance, specialized UDP datagram host supporting multiple simultaneous peers,
/// 5 channel delivery modes, built-in AEAD security, and single-socket NAT hole punching.
/// </summary>
public sealed partial class DatagramHost : IAsyncDisposable
{
   private readonly IPEndPoint _bindEndPoint;
   private readonly DatagramOptions _options;
   private readonly IDatagramTransport _transport;
   private readonly TimeProvider _timeProvider;
   private readonly DatagramChannelConfig?[] _channelTable;
   private readonly DatagramChannelConfig[] _priorityChannels;
   private readonly ulong _channelConfigHash;
   private readonly int _trailerSize;

   private readonly DatagramServerHandshakeCrypto? _serverHandshakeCrypto;
   private readonly ConcurrentDictionary<uint, DatagramPeer> _peersById = new();
   private readonly ConcurrentDictionary<uint, DatagramPeer> _peersBySessionToken = new();
   private readonly ConcurrentQueue<DatagramEvent> _pollQueue = new();
   private readonly Channel<DatagramEvent> _asyncEventChannel;
   private readonly List<(byte[] Buffer, int Length, SocketAddress Address)> _scratchOutgoingBatch = new(64);
   private readonly Lock _tickLock = new();

   private CancellationTokenSource? _cts;
   private Task? _receiveLoopTask;
   private Task? _tickLoopTask;
   private Task? _asyncDispatchTask;

   private uint _nextPeerId;
   private uint _nextLocalTokenSeed = 100;
   private int _started;
   private int _disposed;
   private int _queuedPollCount;

   public DatagramOptions Options => _options;
   public DatagramHostEvents Events { get; } = new();
   public IPEndPoint? LocalEndPoint => _transport.LocalEndPoint;
   public IReadOnlyCollection<DatagramPeer> Peers => (IReadOnlyCollection<DatagramPeer>)_peersById.Values;
   public int PeerCount => _peersById.Count;

   public DatagramHost(
      DatagramOptions? options = null,
      IDatagramTransport? transport = null,
      TimeProvider? timeProvider = null)
      : this(new IPEndPoint(IPAddress.Any, 0), options, transport, timeProvider)
   {
   }

   public DatagramHost(
      IPEndPoint bindEndPoint,
      DatagramOptions? options = null,
      IDatagramTransport? transport = null,
      TimeProvider? timeProvider = null)
   {
      _bindEndPoint = bindEndPoint;
      _options = options ?? new DatagramOptions();
      _transport = transport ?? new UdpSocketDatagramTransport();
      _timeProvider = timeProvider ?? TimeProvider.System;

      _channelTable = _options.BuildValidatedChannelTable(out _priorityChannels, out _channelConfigHash);

      _trailerSize = _options.Security.Mode switch
      {
         DatagramSecurityMode.Aead => DatagramCryptoSession.TagSize,
         DatagramSecurityMode.Crc32C => Crc32C.Size,
         _ => 0
      };

      _serverHandshakeCrypto = new DatagramServerHandshakeCrypto(_options.Security.AeadCipher, _timeProvider);

      _asyncEventChannel = Channel.CreateBounded<DatagramEvent>(new BoundedChannelOptions(_options.MaxQueuedEvents)
      {
         SingleReader = false,
         SingleWriter = false,
         FullMode = BoundedChannelFullMode.DropWrite
      });
   }

   /// <summary>
   /// Binds the underlying UDP transport and starts background I/O loops according to <see cref="DatagramOptions"/>.
   /// </summary>
   public void Start()
   {
      if (Interlocked.CompareExchange(ref _started, 1, 0) != 0)
      {
         return;
      }

      _cts = new CancellationTokenSource();
      _transport.Bind(_bindEndPoint, _options);

      var isDeterministicPollableSim = _options.ManualTick && _transport is SimulatedDatagramTransport;
      if (!isDeterministicPollableSim)
      {
         _receiveLoopTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));
      }

      if (!_options.ManualTick)
      {
         _tickLoopTask = Task.Run(() => BackgroundTickLoopAsync(_cts.Token));
      }

      if (_options.DispatchMode == DatagramEventDispatchMode.Async)
      {
         _asyncDispatchTask = Task.Run(() => AsyncEventDispatchLoopAsync(_cts.Token));
      }

      TraceLogger.LogNeutralInfo("DatagramHost started on {0} (Mode: {1}, Security: {2})",
         _transport.LocalEndPoint, _options.DispatchMode, _options.Security.Mode);
   }

   public bool TryGetPeer(uint peerId, out DatagramPeer? peer) => _peersById.TryGetValue(peerId, out peer);

   /// <summary>
   /// Broadcasts a message on <paramref name="channelId"/> to all connected peers (optionally excluding one).
   /// </summary>
   public void Broadcast(byte channelId, ReadOnlySpan<byte> payload, DatagramPeer? exclude = null)
   {
      foreach (var peer in _peersById.Values)
      {
         if (ReferenceEquals(peer, exclude) || peer.State != DatagramPeerState.Connected)
         {
            continue;
         }

         peer.TrySend(channelId, payload);
      }
   }

   /// <summary>
   /// Sends a message on <paramref name="channelId"/> to a specific set of peer IDs.
   /// </summary>
   public void SendTo(ReadOnlySpan<uint> peerIds, byte channelId, ReadOnlySpan<byte> payload)
   {
      for (var i = 0; i < peerIds.Length; i++)
      {
         if (_peersById.TryGetValue(peerIds[i], out var peer) && peer.State == DatagramPeerState.Connected)
         {
            peer.TrySend(channelId, payload);
         }
      }
   }

   /// <summary>
   /// Attempts to dequeue the next event when <see cref="DatagramOptions.DispatchMode"/> is <see cref="DatagramEventDispatchMode.Poll"/>.
   /// Caller must dispose the returned <see cref="DatagramEvent"/> to return pooled payload buffers.
   /// </summary>
   public bool TryPollEvent(out DatagramEvent evt)
   {
      if (_pollQueue.TryDequeue(out evt))
      {
         Interlocked.Decrement(ref _queuedPollCount);
         return true;
      }
      return false;
   }

   /// <summary>
   /// Drains all queued poll events and automatically disposes each event after invoking <paramref name="handler"/>.
   /// </summary>
   public int PollEvents(Action<DatagramEvent> handler)
   {
      var count = 0;
      while (TryPollEvent(out var evt))
      {
         try
         {
            handler(evt);
         }
         finally
         {
            evt.Dispose();
         }
         count++;
      }
      return count;
   }

   /// <summary>
   /// Consumes incoming events asynchronously when <see cref="DatagramOptions.DispatchMode"/> is <see cref="DatagramEventDispatchMode.Async"/>.
   /// </summary>
   public async IAsyncEnumerable<DatagramEvent> ReadEventsAsync([EnumeratorCancellation] CancellationToken ct = default)
   {
      await foreach (var evt in _asyncEventChannel.Reader.ReadAllAsync(ct))
      {
         yield return evt;
      }
   }

   /// <summary>
   /// Executes one protocol tick: drains any synchronous simulated inbox, checks timeouts,
   /// flushes outgoing/retransmitted datagrams, and pumps handshake/NAT retries.
   /// </summary>
   public void Tick()
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         return;
      }

      if (Volatile.Read(ref _started) == 0)
      {
         Start();
      }

      if (_options.ManualTick && _transport is SimulatedDatagramTransport sim)
      {
         DrainSimulatedTransport(sim);
      }

      var now = _timeProvider.GetTimestamp();

      lock (_tickLock)
      {
         _scratchOutgoingBatch.Clear();

         TickPendingHandshakes(now, _scratchOutgoingBatch);
         TickActivePunchSessions(now, _scratchOutgoingBatch);

         foreach (var peer in _peersById.Values)
         {
            peer.BuildOutgoingBatch(now, _scratchOutgoingBatch, out var disconnectTrigger, out var disconnectPayload);
            if (disconnectTrigger.HasValue)
            {
               RemovePeerAndEmitDisconnect(peer, disconnectTrigger.Value, disconnectPayload);
            }
         }

         CleanupExpiredAckCache(now);

         for (var i = 0; i < _scratchOutgoingBatch.Count; i++)
         {
            var (buffer, length, address) = _scratchOutgoingBatch[i];
            DispatchSendAndReturnBuffer(buffer, length, address);
         }

         _scratchOutgoingBatch.Clear();
      }
   }

   private void DrainSimulatedTransport(SimulatedDatagramTransport sim)
   {
      var maxPacketBuffer = Math.Max(2048, _options.MaxDatagramSize + 64);
      var recvBuf = ArrayPool<byte>.Shared.Rent(maxPacketBuffer);
      var remoteSa = new SocketAddress(_bindEndPoint.AddressFamily);

      try
      {
         var tempTask = DrainLoop();
         if (!tempTask.IsCompleted)
         {
            tempTask.AsTask().GetAwaiter().GetResult();
         }
      }
      finally
      {
         ArrayPool<byte>.Shared.Return(recvBuf);
      }

      ValueTask DrainLoop()
      {
         while (sim.TryDequeueImmediate(recvBuf, remoteSa, out var bytesRead))
         {
            var vt = ProcessRawDatagramAsync(recvBuf.AsMemory(0, bytesRead), remoteSa, CancellationToken.None);
            if (!vt.IsCompletedSuccessfully)
            {
               return AwaitAndContinue(vt);
            }
         }
         return ValueTask.CompletedTask;

         async ValueTask AwaitAndContinue(ValueTask pending)
         {
            await pending;
            while (sim.TryDequeueImmediate(recvBuf, remoteSa, out var bytesRead))
            {
               await ProcessRawDatagramAsync(recvBuf.AsMemory(0, bytesRead), remoteSa, CancellationToken.None);
            }
         }
      }
   }

   private async Task ReceiveLoopAsync(CancellationToken ct)
   {
      var bufSize = Math.Max(2048, _options.MaxDatagramSize + 64);
      var buffer = ArrayPool<byte>.Shared.Rent(bufSize);
      var remoteSa = new SocketAddress(_bindEndPoint.AddressFamily);

      try
      {
         while (!ct.IsCancellationRequested)
         {
            int received;
            try
            {
               received = await _transport.ReceiveFromAsync(buffer, remoteSa, ct);
            }
            catch (SocketException ex) when (ex.SocketErrorCode is SocketError.ConnectionReset or SocketError.MessageSize)
            {
               continue;
            }

            if (received <= 0)
            {
               continue;
            }

            await ProcessRawDatagramAsync(buffer.AsMemory(0, received), remoteSa, ct);
         }
      }
      catch (OperationCanceledException)
      {
      }
      catch (ObjectDisposedException)
      {
      }
      catch (Exception ex)
      {
         TraceLogger.LogNeutralError("DatagramHost receive loop failed: {0}", ex.Message);
      }
      finally
      {
         ArrayPool<byte>.Shared.Return(buffer);
      }
   }

   private async Task BackgroundTickLoopAsync(CancellationToken ct)
   {
      using var timer = new PeriodicTimer(_options.TickInterval);
      try
      {
         while (await timer.WaitForNextTickAsync(ct))
         {
            Tick();
         }
      }
      catch (OperationCanceledException)
      {
      }
   }

   private ValueTask ProcessRawDatagramAsync(ReadOnlyMemory<byte> rawMemory, SocketAddress remoteSa, CancellationToken ct)
   {
      var rawSpan = rawMemory.Span;
      if (!DatagramWireHeader.TryRead(rawSpan, out var header))
      {
         return ValueTask.CompletedTask;
      }

      switch (header.Kind)
      {
         case DatagramPacketKind.ConnectRequest:
            HandleConnectRequest(rawSpan, remoteSa);
            return ValueTask.CompletedTask;

         case DatagramPacketKind.ConnectChallenge:
            HandleConnectChallenge(rawSpan, remoteSa);
            return ValueTask.CompletedTask;

         case DatagramPacketKind.ConnectAccept:
            return HandleConnectAcceptAsync(rawMemory, remoteSa, ct);

         case DatagramPacketKind.ConnectAck:
            HandleConnectAck(header, rawSpan, remoteSa);
            return ValueTask.CompletedTask;

         case DatagramPacketKind.PunchProbe:
            HandlePunchProbe(rawSpan, remoteSa);
            return ValueTask.CompletedTask;

         case DatagramPacketKind.RendezvousMapped:
         case DatagramPacketKind.RendezvousIntroduce:
            HandleRendezvousPacket(header, rawSpan);
            return ValueTask.CompletedTask;
      }

      // Established peer traffic: O(1) lookup by SessionToken
      if (header.SessionToken == 0 || !_peersBySessionToken.TryGetValue(header.SessionToken, out var peer))
      {
         return ValueTask.CompletedTask;
      }

      var bodyBuffer = ArrayPool<byte>.Shared.Rent(rawSpan.Length);
      try
      {
         if (!peer.TryVerifyAndDecrypt(in header, rawSpan, bodyBuffer, out var bodyLen))
         {
            return ValueTask.CompletedTask;
         }

         var endpointMismatch = !peer.RemoteSocketAddressMatches(remoteSa);
         if (endpointMismatch && !_options.Security.AllowEndpointMigration)
         {
            return ValueTask.CompletedTask;
         }

         var now = _timeProvider.GetTimestamp();
         var processed = peer.ProcessVerifiedIncomingPacket(
            in header,
            bodyBuffer.AsSpan(0, bodyLen),
            now,
            EmitEvent,
            out var isNewHighestPacketSeq,
            out var disconnectTrigger,
            out var disconnectPayload,
            out var replyTimeSync,
            out var replyTimeSyncLen,
            out var targetAddress);

         if (!processed)
         {
            return ValueTask.CompletedTask;
         }

         if (endpointMismatch && isNewHighestPacketSeq)
         {
            var newEp = MaterializeEndPoint(remoteSa);
            var migrated = peer.MigrateEndPoint(newEp);
            if (migrated.HasValue)
            {
               targetAddress = newEp.Serialize();
               EmitEvent(DatagramEvent.PeerMigrated(peer, migrated.Value.OldEndPoint, migrated.Value.NewEndPoint));
            }
         }

         if (replyTimeSync is not null)
         {
            DispatchSendAndReturnBuffer(replyTimeSync, replyTimeSyncLen, targetAddress);
         }

         if (disconnectTrigger.HasValue)
         {
            RemovePeerAndEmitDisconnect(peer, disconnectTrigger.Value, disconnectPayload);
         }
      }
      finally
      {
         ArrayPool<byte>.Shared.Return(bodyBuffer);
      }

      return ValueTask.CompletedTask;
   }

   private void RemovePeerAndEmitDisconnect(
      DatagramPeer peer,
      DatagramDisconnectReason reason,
      DatagramPayloadLease reasonPayload)
   {
      var removed = _peersById.TryRemove(peer.PeerId, out _);
      _peersBySessionToken.TryRemove(peer.SessionToken, out _);

      if (removed)
      {
         DatagramMetrics.PeersActive.Add(-1);
         DatagramMetrics.PeersDisconnected.Add(1);
         EmitEvent(DatagramEvent.PeerDisconnected(peer, reason, reasonPayload));
      }
      else
      {
         reasonPayload.Dispose();
      }

      peer.Dispose();
   }

   private void EmitEvent(DatagramEvent evt)
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         evt.Dispose();
         return;
      }

      if (_options.DispatchMode == DatagramEventDispatchMode.Async)
      {
         if (!_asyncEventChannel.Writer.TryWrite(evt))
         {
            evt.Dispose();
         }
         return;
      }

      if (Interlocked.Increment(ref _queuedPollCount) > _options.MaxQueuedEvents)
      {
         Interlocked.Decrement(ref _queuedPollCount);
         evt.Dispose();
         return;
      }

      _pollQueue.Enqueue(evt);
   }

   private async Task AsyncEventDispatchLoopAsync(CancellationToken ct)
   {
      var hasCallbacks = Events.PeerConnected is not null
         || Events.PeerDisconnected is not null
         || Events.MessageReceived is not null
         || Events.PeerMigrated is not null;

      if (!hasCallbacks)
      {
         // User is reading directly via ReadEventsAsync(ct)
         return;
      }

      try
      {
         await foreach (var evt in _asyncEventChannel.Reader.ReadAllAsync(ct))
         {
            try
            {
               switch (evt.Type)
               {
                  case DatagramEventType.PeerConnected when Events.PeerConnected is not null:
                     await Events.PeerConnected(evt.Peer, ct);
                     break;
                  case DatagramEventType.PeerDisconnected when Events.PeerDisconnected is not null:
                     await Events.PeerDisconnected(evt.Peer, evt.DisconnectReason, evt.Payload.Memory, ct);
                     break;
                  case DatagramEventType.MessageReceived when Events.MessageReceived is not null:
                     await Events.MessageReceived(evt.Peer, evt.ChannelId, evt.DeliveryMode, evt.Payload.Memory, ct);
                     break;
                  case DatagramEventType.PeerMigrated when Events.PeerMigrated is not null:
                     await Events.PeerMigrated(evt.Peer, evt.PreviousEndPoint!, evt.NewEndPoint!, ct);
                     break;
               }
            }
            catch (Exception ex)
            {
               TraceLogger.LogNeutralError("DatagramHost event handler threw exception: {0}", ex.Message);
            }
            finally
            {
               evt.Dispose();
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
   }

   private void DispatchSendAndReturnBuffer(byte[] rentedBuffer, int length, SocketAddress destination)
   {
      var ct = _cts?.Token ?? CancellationToken.None;
      var vt = _transport.SendToAsync(rentedBuffer.AsMemory(0, length), destination, ct);
      if (vt.IsCompletedSuccessfully)
      {
         ArrayPool<byte>.Shared.Return(rentedBuffer);
      }
      else
      {
         _ = CompleteSendAsync(vt, rentedBuffer);
      }

      static async Task CompleteSendAsync(ValueTask pending, byte[] buf)
      {
         try
         {
            await pending;
         }
         catch
         {
            // Transient UDP send failure handled by reliability/timeout layer
         }
         finally
         {
            ArrayPool<byte>.Shared.Return(buf);
         }
      }
   }

   private static IPEndPoint MaterializeEndPoint(SocketAddress sa)
   {
      var template = sa.Family == AddressFamily.InterNetworkV6
         ? new IPEndPoint(IPAddress.IPv6Any, 0)
         : new IPEndPoint(IPAddress.Any, 0);
      return (IPEndPoint)template.Create(sa);
   }

   private uint AllocateUniqueSessionToken()
   {
      while (true)
      {
         var token = Interlocked.Increment(ref _nextLocalTokenSeed);
         if (token != 0 && !_peersBySessionToken.ContainsKey(token))
         {
            return token;
         }
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      if (_cts is not null)
      {
         await _cts.CancelAsync();
      }

      CancelAllPendingHandshakes();
      CancelAllPunchSessions();

      _asyncEventChannel.Writer.TryComplete();
      await _transport.DisposeAsync();

      if (_receiveLoopTask is not null)
      {
         try { await _receiveLoopTask; }
         catch
         {
            // ignored
         }
      }
      if (_tickLoopTask is not null)
      {
         try { await _tickLoopTask; }
         catch
         {
            // ignored
         }
      }
      if (_asyncDispatchTask is not null)
      {
         try { await _asyncDispatchTask; }
         catch
         {
            // ignored
         }
      }

      foreach (var peer in _peersById.Values)
      {
         DatagramMetrics.PeersActive.Add(-1);
         peer.Dispose();
      }
      _peersById.Clear();
      _peersBySessionToken.Clear();

      while (_pollQueue.TryDequeue(out var evt))
      {
         evt.Dispose();
      }
      while (_asyncEventChannel.Reader.TryRead(out var evt))
      {
         evt.Dispose();
      }

      _serverHandshakeCrypto?.Dispose();
      _cts?.Dispose();
   }
}
