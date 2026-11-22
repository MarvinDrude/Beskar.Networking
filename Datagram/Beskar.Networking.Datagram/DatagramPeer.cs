using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Reliability;
using Beskar.Networking.Datagram.Security;
using Beskar.Networking.Datagram.Telemetry;

namespace Beskar.Networking.Datagram;

/// <summary>
/// Represents a single authenticated remote peer managed by a <see cref="DatagramHost"/>.
/// </summary>
public sealed class DatagramPeer : IDisposable
{
   private readonly DatagramOptions _options;
   private readonly TimeProvider _timeProvider;
   private readonly DatagramChannelConfig[] _priorityChannels;
   private readonly DatagramChannelConfig?[] _channelTable;
   private readonly ChannelSendState?[] _sendChannels = new ChannelSendState[64];
   private readonly ChannelReceiveState?[] _receiveChannels = new ChannelReceiveState[64];
   private readonly AckTracker _ackTracker = new();
   private readonly SentPacketLedger _ledger = new();
   private readonly List<SentMessageRef> _scratchLedgerMessages = new(32);
   private readonly RttEstimator _rtt;
   private readonly TokenBucketPacer _pacer;
   private readonly Lock _lock = new();
   private readonly DatagramCryptoSession? _crypto;
   private readonly ConcurrentDictionary<string, object?> _properties = new();
   private readonly int _maxBodySize;
   private readonly int _trailerSize;

   private IPEndPoint _remoteEndPoint;
   private SocketAddress _remoteSocketAddress;
   private uint _nextPacketSeq = 1;
   private long _lastReceivedTimestamp;
   private long _lastSentTimestamp;
   private long _lastTimeSyncSentTimestamp;
   private bool _ackDirty;
   private int _unackedDataPacketsReceived;
   private long _ackDirtySinceTimestamp;

   private long _bytesSent;
   private long _bytesReceived;
   private long _datagramsSent;
   private long _datagramsReceived;

   private volatile DatagramPeerState _state = DatagramPeerState.Connected;
   private byte[]? _pendingDisconnectReason;
   private int _disconnectPacketsRemaining;
   private DatagramDisconnectReason _disconnectReason;
   private int _disposed;

   public uint PeerId { get; }
   public uint SessionToken { get; }
   public bool IsOutboundInitiator { get; }
   public ReadOnlyMemory<byte> ConnectMetadata { get; }
   public DatagramPeerState State => _state;
   public IPEndPoint RemoteEndPoint => Volatile.Read(ref _remoteEndPoint);

   /// <summary>
   /// User-defined attachment slot (e.g., player/session state object).
   /// </summary>
   public object? Tag { get; set; }

   internal DatagramPeer(
      uint peerId,
      uint sessionToken,
      bool isOutboundInitiator,
      IPEndPoint remoteEndPoint,
      ReadOnlyMemory<byte> connectMetadata,
      DatagramOptions options,
      DatagramChannelConfig?[] channelTable,
      DatagramChannelConfig[] priorityChannels,
      DatagramCryptoSession? crypto,
      TimeProvider timeProvider,
      object? initialTag = null)
   {
      PeerId = peerId;
      SessionToken = sessionToken;
      IsOutboundInitiator = isOutboundInitiator;
      _remoteEndPoint = remoteEndPoint;
      _remoteSocketAddress = remoteEndPoint.Serialize();
      ConnectMetadata = connectMetadata;
      _options = options;
      _channelTable = channelTable;
      _priorityChannels = priorityChannels;
      _crypto = crypto;
      _timeProvider = timeProvider;
      Tag = initialTag;

      _trailerSize = options.Security.Mode switch
      {
         DatagramSecurityMode.Aead => DatagramCryptoSession.TagSize,
         DatagramSecurityMode.Crc32C => Crc32C.Size,
         _ => 0
      };
      _maxBodySize = options.MaxDatagramSize - DatagramWireHeader.Size - _trailerSize;

      var now = timeProvider.GetTimestamp();
      _lastReceivedTimestamp = now;
      _lastSentTimestamp = now;
      _lastTimeSyncSentTimestamp = now;
      _rtt = new RttEstimator(options.MinRtoMs, options.MaxRtoMs, options.TickInterval, timeProvider, now);
      _pacer = new TokenBucketPacer(options.MaxBytesPerSecondPerPeer, options.PacingBurstBytes, now);

      foreach (var ch in priorityChannels)
      {
         _sendChannels[ch.ChannelId] = new ChannelSendState(ch, _maxBodySize);
         _receiveChannels[ch.ChannelId] = new ChannelReceiveState(ch);
      }
   }

   public void SetProperty<T>(string key, T value) => _properties[key] = value;

   public bool TryGetProperty<T>(string key, [MaybeNullWhen(false)] out T value)
   {
      if (_properties.TryGetValue(key, out var raw) && raw is T typed)
      {
         value = typed;
         return true;
      }
      value = default;
      return false;
   }

   public DatagramPeerStats Stats
   {
      get
      {
         lock (_lock)
         {
            long unacked = 0;
            long retransmitted = 0;
            foreach (var ch in _priorityChannels)
            {
               var s = _sendChannels[ch.ChannelId]!;
               unacked += s.UnackedCount;
               retransmitted += s.TotalRetransmitted;
            }

            return new DatagramPeerStats
            {
               SmoothedRttMs = _rtt.SmoothedRttMs,
               RttVarianceMs = _rtt.RttVarianceMs,
               CurrentRtoMs = _rtt.CurrentRtoMs,
               PacketLossRate = _rtt.PacketLossRate,
               CongestionState = _rtt.CongestionState,
               RecommendedSendScale = _rtt.RecommendedSendScale,
               BytesSent = _bytesSent,
               BytesReceived = _bytesReceived,
               DatagramsSent = _datagramsSent,
               DatagramsReceived = _datagramsReceived,
               MessagesRetransmitted = retransmitted,
               UnackedReliableMessages = unacked,
               EstimatedClockOffsetTicks = _rtt.EstimatedClockOffsetTicks
            };
         }
      }
   }

   public DateTimeOffset EstimatedRemoteUtcNow =>
      _timeProvider.GetUtcNow() + TimeSpan.FromTicks(Stats.EstimatedClockOffsetTicks);

   /// <summary>
   /// Synchronously enqueues a message onto <paramref name="channelId"/> with zero Task allocation.
   /// </summary>
   public DatagramSendResult TrySend(byte channelId, ReadOnlySpan<byte> payload)
   {
      if (_state != DatagramPeerState.Connected || Volatile.Read(ref _disposed) == 1)
      {
         return DatagramSendResult.PeerNotConnected;
      }

      if (channelId >= 64)
      {
         return DatagramSendResult.InvalidChannel;
      }

      lock (_lock)
      {
         if (_state != DatagramPeerState.Connected || Volatile.Read(ref _disposed) == 1)
         {
            return DatagramSendResult.PeerNotConnected;
         }

         var sendState = _sendChannels[channelId];
         if (sendState is null)
         {
            return DatagramSendResult.InvalidChannel;
         }

         return sendState.Enqueue(payload, _timeProvider.GetTimestamp(), _maxBodySize);
      }
   }

   /// <summary>
   /// Initiates a graceful disconnect sequence, sending redundant disconnect notifications.
   /// </summary>
   public void Disconnect(ReadOnlySpan<byte> reason = default)
   {
      lock (_lock)
      {
         if (_state is DatagramPeerState.Disconnecting or DatagramPeerState.Disconnected)
         {
            return;
         }

         _state = DatagramPeerState.Disconnecting;
         _disconnectReason = DatagramDisconnectReason.GracefulLocal;

         if (reason.IsEmpty)
         {
            _pendingDisconnectReason = null;
         }
         else
         {
            var clampedLen = Math.Min(reason.Length, _maxBodySize);
            _pendingDisconnectReason = [.. reason[..clampedLen]];
         }

         _disconnectPacketsRemaining = 3;
      }
   }

   internal void InitializeHandshakeSeqs(uint lastRemotePacketSeq, uint nextLocalPacketSeq)
   {
      lock (_lock)
      {
         if (lastRemotePacketSeq > 0)
         {
            _ackTracker.TryRecordReceivedPacket(lastRemotePacketSeq);
         }
         _nextPacketSeq = nextLocalPacketSeq;
      }
   }

   internal bool RemoteSocketAddressMatches(SocketAddress candidate)
   {
      var current = Volatile.Read(ref _remoteSocketAddress);
      return current.Size == candidate.Size
         && current.Buffer.Span[0..current.Size].SequenceEqual(candidate.Buffer.Span[0..candidate.Size]);
   }

   internal (IPEndPoint OldEndPoint, IPEndPoint NewEndPoint)? MigrateEndPoint(IPEndPoint newEndPoint)
   {
      lock (_lock)
      {
         var oldEp = _remoteEndPoint;
         if (oldEp.Equals(newEndPoint))
         {
            return null;
         }

         _remoteEndPoint = newEndPoint;
         Volatile.Write(ref _remoteSocketAddress, newEndPoint.Serialize());
         return (oldEp, newEndPoint);
      }
   }

   internal bool TryVerifyAndDecrypt(
      in DatagramWireHeader header,
      ReadOnlySpan<byte> rawDatagram,
      Span<byte> plaintextBodyDest,
      out int bodyLength)
   {
      bodyLength = 0;

      if (_state == DatagramPeerState.Disconnected || Volatile.Read(ref _disposed) == 1)
      {
         return false;
      }

      if (_options.Security.Mode == DatagramSecurityMode.Aead)
      {
         if (!header.IsEncrypted || rawDatagram.Length < DatagramWireHeader.Size + DatagramCryptoSession.TagSize)
         {
            DatagramMetrics.SecurityRejections.Add(1);
            return false;
         }

         bodyLength = rawDatagram.Length - DatagramWireHeader.Size - DatagramCryptoSession.TagSize;
         var aad = rawDatagram[0..DatagramWireHeader.Size];
         var cipherText = rawDatagram.Slice(DatagramWireHeader.Size, bodyLength);
         var tag = rawDatagram.Slice(DatagramWireHeader.Size + bodyLength, DatagramCryptoSession.TagSize);

         lock (_lock)
         {
            if (_state == DatagramPeerState.Disconnected || Volatile.Read(ref _disposed) == 1)
            {
               return false;
            }

            if (_crypto is null || !_crypto.TryDecrypt(header.PacketSeq, aad, cipherText, tag, plaintextBodyDest[0..bodyLength]))
            {
               DatagramMetrics.SecurityRejections.Add(1);
               return false;
            }
         }

         return true;
      }

      if (header.IsEncrypted)
      {
         DatagramMetrics.SecurityRejections.Add(1);
         return false;
      }

      if (_options.Security.Mode == DatagramSecurityMode.Crc32C)
      {
         if (rawDatagram.Length < DatagramWireHeader.Size + Crc32C.Size)
         {
            DatagramMetrics.SecurityRejections.Add(1);
            return false;
         }

         bodyLength = rawDatagram.Length - DatagramWireHeader.Size - Crc32C.Size;
         var computed = Crc32C.Compute(rawDatagram[0..(DatagramWireHeader.Size + bodyLength)]);
         var wireCrc = BinaryPrimitives.ReadUInt32LittleEndian(rawDatagram.Slice(DatagramWireHeader.Size + bodyLength, Crc32C.Size));
         if (computed != wireCrc)
         {
            DatagramMetrics.SecurityRejections.Add(1);
            return false;
         }

         rawDatagram.Slice(DatagramWireHeader.Size, bodyLength).CopyTo(plaintextBodyDest);
         return true;
      }

      bodyLength = rawDatagram.Length - DatagramWireHeader.Size;
      rawDatagram.Slice(DatagramWireHeader.Size, bodyLength).CopyTo(plaintextBodyDest);
      return true;
   }

   internal bool ProcessVerifiedIncomingPacket(
      in DatagramWireHeader header,
      ReadOnlySpan<byte> body,
      long nowTimestamp,
      Action<DatagramEvent> emitEvent,
      out bool isNewHighestPacketSeq,
      out DatagramDisconnectReason? disconnectTrigger,
      out DatagramPayloadLease disconnectPayload,
      out byte[]? replyTimeSyncPacket,
      out int replyTimeSyncLength,
      out SocketAddress targetAddress)
   {
      isNewHighestPacketSeq = false;
      disconnectTrigger = null;
      disconnectPayload = DatagramPayloadLease.Empty;
      replyTimeSyncPacket = null;
      replyTimeSyncLength = 0;

      lock (_lock)
      {
         targetAddress = _remoteSocketAddress;
         if (_state == DatagramPeerState.Disconnected || Volatile.Read(ref _disposed) == 1)
         {
            return false;
         }

         var prevHighest = _ackTracker.HighestReceivedSeq;
         if (!_ackTracker.TryRecordReceivedPacket(header.PacketSeq))
         {
            return false; // Duplicate or outside replay window
         }

         isNewHighestPacketSeq = header.PacketSeq > prevHighest;

         _lastReceivedTimestamp = nowTimestamp;
         _bytesReceived += DatagramWireHeader.Size + body.Length + _trailerSize;
         _datagramsReceived++;
         DatagramMetrics.DatagramsReceived.Add(1);
         DatagramMetrics.BytesReceived.Add(DatagramWireHeader.Size + body.Length + _trailerSize);

         if (header.Kind == DatagramPacketKind.Data)
         {
            if (!_ackDirty)
            {
               _ackDirty = true;
               _ackDirtySinceTimestamp = nowTimestamp;
            }
            _unackedDataPacketsReceived++;
         }

         _ledger.ProcessAcks(
            header.AckSeq,
            header.AckBitfield,
            _nextPacketSeq - 1,
            nowTimestamp,
            _timeProvider,
            _rtt,
            OnChannelMessageAcked);

         switch (header.Kind)
         {
            case DatagramPacketKind.Data:
               ProcessDataBody(body, emitEvent);
               break;

            case DatagramPacketKind.AckOnly:
               break;

            case DatagramPacketKind.Disconnect:
               _state = DatagramPeerState.Disconnected;
               disconnectTrigger = DatagramDisconnectReason.GracefulRemote;
               disconnectPayload = DatagramPayloadLease.CopyFrom(body);
               return true;

            case DatagramPacketKind.TimeSyncPing when body.Length >= 8:
            {
               var clientSendTicks = BinaryPrimitives.ReadInt64LittleEndian(body[0..8]);
               var serverTicks = _timeProvider.GetUtcNow().UtcTicks;

               Span<byte> pongBody = stackalloc byte[24];

               BinaryPrimitives.WriteInt64LittleEndian(pongBody[0..8], clientSendTicks);
               BinaryPrimitives.WriteInt64LittleEndian(pongBody[8..16], serverTicks);
               BinaryPrimitives.WriteInt64LittleEndian(pongBody[16..24], _timeProvider.GetUtcNow().UtcTicks);

               replyTimeSyncPacket = BuildProtectedPacketLocked(DatagramPacketKind.TimeSyncPong, pongBody, nowTimestamp, out replyTimeSyncLength);
               break;
            }

            case DatagramPacketKind.TimeSyncPong when body.Length >= 24:
            {
               var clientSendTicks = BinaryPrimitives.ReadInt64LittleEndian(body[0..8]);
               var serverRecvTicks = BinaryPrimitives.ReadInt64LittleEndian(body[8..16]);
               var serverSendTicks = BinaryPrimitives.ReadInt64LittleEndian(body[16..24]);
               var clientRecvTicks = _timeProvider.GetUtcNow().UtcTicks;

               _rtt.RecordTimeSyncSample(clientSendTicks, serverRecvTicks, serverSendTicks, clientRecvTicks);
               break;
            }
         }

         return true;
      }
   }

   internal void BuildOutgoingBatch(
      long nowTimestamp,
      List<(byte[] Buffer, int Length, SocketAddress Address)> outgoingBatch,
      out DatagramDisconnectReason? disconnectTrigger,
      out DatagramPayloadLease disconnectPayload)
   {
      disconnectTrigger = null;
      disconnectPayload = DatagramPayloadLease.Empty;

      lock (_lock)
      {
         if (_state == DatagramPeerState.Disconnected || Volatile.Read(ref _disposed) == 1)
         {
            return;
         }

         if (_state == DatagramPeerState.Disconnecting)
         {
            if (_disconnectPacketsRemaining > 0)
            {
               var reasonSpan = _pendingDisconnectReason is null
                  ? ReadOnlySpan<byte>.Empty
                  : _pendingDisconnectReason.AsSpan();

               for (var i = 0; i < _disconnectPacketsRemaining; i++)
               {
                  var pkt = BuildProtectedPacketLocked(DatagramPacketKind.Disconnect, reasonSpan, nowTimestamp, out var len);
                  outgoingBatch.Add((pkt, len, _remoteSocketAddress));
               }
               _disconnectPacketsRemaining = 0;
            }

            _state = DatagramPeerState.Disconnected;
            disconnectTrigger = _disconnectReason;
            disconnectPayload = _pendingDisconnectReason is null
               ? DatagramPayloadLease.Empty
               : DatagramPayloadLease.CopyFrom(_pendingDisconnectReason);

            return;
         }

         var timeoutTicks = _options.PeerTimeout.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;
         if (nowTimestamp - _lastReceivedTimestamp >= timeoutTicks)
         {
            _state = DatagramPeerState.Disconnected;
            disconnectTrigger = DatagramDisconnectReason.Timeout;
            return;
         }

         if (_nextPacketSeq >= uint.MaxValue - 1024)
         {
            _state = DatagramPeerState.Disconnected;
            disconnectTrigger = DatagramDisconnectReason.GracefulLocal;
            return;
         }

         var rtoTicks = _rtt.GetCurrentRtoTicks(_timeProvider);
         var maxRtoTicks = (long)(_options.MaxRtoMs * _timeProvider.TimestampFrequency / 1000.0);
         var lossCutoffTicks = Math.Max(rtoTicks * 3 / 2, _timeProvider.TimestampFrequency / 4);
         var maxRetryDurationTicks = _options.MaxReliableRetryDuration.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;

         foreach (var ch in _priorityChannels)
         {
            if (_sendChannels[ch.ChannelId]!.HasExpiredReliableMessage(nowTimestamp, maxRetryDurationTicks))
            {
               _state = DatagramPeerState.Disconnected;
               disconnectTrigger = DatagramDisconnectReason.MaxRetriesExceeded;
               return;
            }
         }

         _ledger.DetectLossesAndFastRetransmits(
            _nextPacketSeq - 1,
            nowTimestamp,
            lossCutoffTicks,
            _timeProvider,
            _rtt,
            OnMessageFastRetransmit);

         _rtt.UpdateLossWindow(nowTimestamp, _timeProvider);

         var rentedScratch = _maxBodySize > 2048 ? ArrayPool<byte>.Shared.Rent(_maxBodySize) : null;
         var bodyScratch = rentedScratch is not null
            ? rentedScratch.AsSpan(0, _maxBodySize)
            : stackalloc byte[_maxBodySize];

         try
         {
            var maxDatagramsPerTick = 32;

            for (var d = 0; d < maxDatagramsPerTick; d++)
            {
               if (!HasAnyReadyChannelData(nowTimestamp))
               {
                  break;
               }

               if (!_pacer.CanSend(_options.MaxDatagramSize, nowTimestamp, _timeProvider))
               {
                  break;
               }

               _scratchLedgerMessages.Clear();
               var bodyOffset = 0;
               var rtoLossRecordedForDatagram = false;

               foreach (var ch in _priorityChannels)
               {
                  var sendState = _sendChannels[ch.ChannelId]!;
                  sendState.PackIntoDatagram(
                     bodyScratch,
                     ref bodyOffset,
                     _maxBodySize,
                     nowTimestamp,
                     rtoTicks,
                     maxRtoTicks,
                     maxRetryDurationTicks,
                     _timeProvider,
                     _rtt,
                     _scratchLedgerMessages,
                     ref rtoLossRecordedForDatagram,
                     out var maxRetriesExceeded);

                  if (maxRetriesExceeded)
                  {
                     _state = DatagramPeerState.Disconnected;
                     disconnectTrigger = DatagramDisconnectReason.MaxRetriesExceeded;
                     return;
                  }
               }

               if (bodyOffset == 0)
               {
                  break;
               }

               var packetSeq = _nextPacketSeq++;
               _ledger.BeginRecord(packetSeq, nowTimestamp, out var ledgerMessages);

               for (var m = 0; m < _scratchLedgerMessages.Count; m++)
               {
                  ledgerMessages.Add(_scratchLedgerMessages[m]);
               }

               var pkt = BuildProtectedPacketWithSeqLocked(
                  DatagramPacketKind.Data,
                  packetSeq,
                  bodyScratch[0..bodyOffset],
                  nowTimestamp,
                  out var totalLen);

               _pacer.Consume(totalLen, nowTimestamp, _timeProvider);
               outgoingBatch.Add((pkt, totalLen, _remoteSocketAddress));
            }
         }
         finally
         {
            if (rentedScratch is not null)
            {
               ArrayPool<byte>.Shared.Return(rentedScratch);
            }
         }

         // immediate ACK after 2+ incoming Data packets (RFC 1122), delayed ACK (~15ms), or KeepAlive
         var keepAliveTicks = _options.KeepAliveInterval.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;
         var delayedAckTicks = Math.Max(1, _timeProvider.TimestampFrequency / 65); // ~15ms

         if (_unackedDataPacketsReceived >= 2
             || (_ackDirty && nowTimestamp - _ackDirtySinceTimestamp >= delayedAckTicks)
             || nowTimestamp - _lastSentTimestamp >= keepAliveTicks)
         {
            var pkt = BuildProtectedPacketLocked(DatagramPacketKind.AckOnly, ReadOnlySpan<byte>.Empty, nowTimestamp, out var len);
            outgoingBatch.Add((pkt, len, _remoteSocketAddress));
         }

         // periodic TimeSyncPing from outbound initiator
         if (IsOutboundInitiator && _options.TimeSyncInterval > TimeSpan.Zero)
         {
            var syncTicks = _options.TimeSyncInterval.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;
            if (nowTimestamp - _lastTimeSyncSentTimestamp >= syncTicks)
            {
               _lastTimeSyncSentTimestamp = nowTimestamp;
               Span<byte> pingBody = stackalloc byte[8];

               BinaryPrimitives.WriteInt64LittleEndian(pingBody, _timeProvider.GetUtcNow().UtcTicks);
               var pkt = BuildProtectedPacketLocked(DatagramPacketKind.TimeSyncPing, pingBody, nowTimestamp, out var len);

               outgoingBatch.Add((pkt, len, _remoteSocketAddress));
            }
         }
      }
   }

   internal byte[] BuildRawPacketSnapshot(DatagramPacketKind kind, ReadOnlySpan<byte> body, long nowTimestamp, out int length)
   {
      lock (_lock)
      {
         return BuildProtectedPacketLocked(kind, body, nowTimestamp, out length);
      }
   }

   private void ProcessDataBody(ReadOnlySpan<byte> body, Action<DatagramEvent> emitEvent)
   {
      var offset = 0;
      while (offset < body.Length)
      {
         if (!DatagramMessageCodec.TryRead(body[offset..], _channelTable, out var msg, out var consumed) || consumed == 0)
         {
            break;
         }

         offset += consumed;
         var recvChannel = _receiveChannels[msg.ChannelId];
         if (recvChannel is null)
         {
            continue;
         }

         var mode = _channelTable[msg.ChannelId]!.DeliveryMode;
         recvChannel.HandleMessage(in msg, (chId, lease) =>
         {
            emitEvent(DatagramEvent.MessageReceived(this, chId, mode, lease));
         });
      }
   }

   private bool HasAnyReadyChannelData(long nowTimestamp)
   {
      foreach (var ch in _priorityChannels)
      {
         if (_sendChannels[ch.ChannelId]!.HasPendingOrReadyData(nowTimestamp))
         {
            return true;
         }
      }
      return false;
   }

   private void OnChannelMessageAcked(byte channelId, ushort channelSeq)
   {
      _sendChannels[channelId]?.MarkAcked(channelSeq);
   }

   private void OnMessageFastRetransmit(byte channelId, ushort channelSeq)
   {
      _sendChannels[channelId]?.RequestFastRetransmit(channelSeq);
   }

   private byte[] BuildProtectedPacketLocked(DatagramPacketKind kind, ReadOnlySpan<byte> body, long nowTimestamp, out int totalLength)
   {
      var seq = _nextPacketSeq++;
      return BuildProtectedPacketWithSeqLocked(kind, seq, body, nowTimestamp, out totalLength);
   }

   private byte[] BuildProtectedPacketWithSeqLocked(
      DatagramPacketKind kind,
      uint packetSeq,
      ReadOnlySpan<byte> body,
      long nowTimestamp,
      out int totalLength)
   {
      totalLength = DatagramWireHeader.Size + body.Length + _trailerSize;
      var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
      var span = buffer.AsSpan(0, totalLength);

      var isEncrypted = _options.Security.Mode == DatagramSecurityMode.Aead;
      var header = new DatagramWireHeader(
         kind,
         DatagramWireHeader.CurrentVersion,
         isEncrypted,
         SessionToken,
         packetSeq,
         _ackTracker.HighestReceivedSeq,
         _ackTracker.OutgoingAckBitfield);

      header.Write(span[0..DatagramWireHeader.Size]);

      if (isEncrypted)
      {
         _crypto!.Encrypt(
            packetSeq,
            span[0..DatagramWireHeader.Size],
            body,
            span.Slice(DatagramWireHeader.Size, body.Length),
            span.Slice(DatagramWireHeader.Size + body.Length, DatagramCryptoSession.TagSize));
      }
      else
      {
         if (!body.IsEmpty)
         {
            body.CopyTo(span.Slice(DatagramWireHeader.Size, body.Length));
         }

         if (_options.Security.Mode == DatagramSecurityMode.Crc32C)
         {
            var crc = Crc32C.Compute(span[0..(DatagramWireHeader.Size + body.Length)]);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(DatagramWireHeader.Size + body.Length, Crc32C.Size), crc);
         }
      }

      _ackDirty = false;

      _unackedDataPacketsReceived = 0;
      _lastSentTimestamp = nowTimestamp;
      _bytesSent += totalLength;
      _datagramsSent++;

      DatagramMetrics.DatagramsSent.Add(1);
      DatagramMetrics.BytesSent.Add(totalLength);

      return buffer;
   }

   public void Dispose()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      lock (_lock)
      {
         _state = DatagramPeerState.Disconnected;
         foreach (var ch in _priorityChannels)
         {
            _sendChannels[ch.ChannelId]?.Dispose();
            _receiveChannels[ch.ChannelId]?.Dispose();
         }
         _crypto?.Dispose();
      }
   }
}
