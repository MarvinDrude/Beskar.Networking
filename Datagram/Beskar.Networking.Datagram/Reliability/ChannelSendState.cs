using System.Buffers;
using System.Numerics;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Telemetry;

namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Manages outgoing queuing, fragmentation, sequence assignment, sliding window,
/// and retransmission timers for a single <see cref="DatagramChannelConfig"/>.
/// </summary>
internal sealed class ChannelSendState : IDisposable
{
   private sealed class OutgoingEntry
   {
      public byte[]? RentedBuffer;
      public int Length;
      public ushort ChannelSeq;
      public ushort FragGroupId;
      public ushort FragIndex;
      public ushort FragCount;
      public bool IsFragment;
      public long EnqueuedTimestamp;
      public long NextRetransmitTimestamp;
      public int RetryCount;
      public bool NeedsFastRetransmit;
   }

   private readonly DatagramChannelConfig _config;
   private readonly bool _isReliable;
   private readonly bool _hasChannelSeq;
   private readonly int _chunkPayloadSize;
   private readonly int _slotMask;
   private readonly Queue<OutgoingEntry> _pendingQueue = new();
   private readonly OutgoingEntry?[] _inFlightSlots;
   private readonly List<ushort> _inFlightOrder = new();

   private ushort _nextChannelSeq;
   private ushort _nextToFlySeq;
   private ushort _sendBase;
   private ushort _nextFragGroupId = 1;
   private int _inFlightCount;

   public DatagramChannelConfig Config => _config;
   public long UnackedCount => _isReliable ? _inFlightCount + _pendingQueue.Count : 0;
   public long TotalRetransmitted { get; private set; }

   public bool HasExpiredReliableMessage(long nowTimestamp, long maxRetryDurationTicks)
   {
      if (!_isReliable || _inFlightOrder.Count == 0)
      {
         return false;
      }

      for (var i = 0; i < _inFlightOrder.Count; i++)
      {
         var seq = _inFlightOrder[i];
         var slot = _inFlightSlots[seq & _slotMask];
         if (slot is not null && slot.ChannelSeq == seq && nowTimestamp - slot.EnqueuedTimestamp > maxRetryDurationTicks)
         {
            return true;
         }
      }

      return false;
   }

   public bool HasPendingOrReadyData(long nowTimestamp)
   {
      if (_pendingQueue.Count > 0)
      {
         if (!_isReliable) return true;
         var next = _pendingQueue.Peek();
         if (_inFlightCount < _config.WindowSize && SeqMath.Diff(next.ChannelSeq, _sendBase) < _config.WindowSize)
         {
            return true;
         }
      }

      for (var i = 0; i < _inFlightOrder.Count; i++)
      {
         var seq = _inFlightOrder[i];
         var slot = _inFlightSlots[seq & _slotMask];

         if (slot is not null && slot.ChannelSeq == seq
            && (slot.NeedsFastRetransmit || slot.NextRetransmitTimestamp <= nowTimestamp))
         {
            return true;
         }
      }

      return false;
   }

   public ChannelSendState(DatagramChannelConfig config, int maxBodySize)
   {
      _config = config;
      _isReliable = config.DeliveryMode is DatagramDeliveryMode.ReliableUnordered
         or DatagramDeliveryMode.ReliableOrdered
         or DatagramDeliveryMode.ReliableSequenced;

      _hasChannelSeq = DatagramMessageCodec.HasChannelSeq(config.DeliveryMode);
      _chunkPayloadSize = Math.Max(64, maxBodySize - DatagramMessageCodec.MaxChannelMessageOverhead);

      if (_isReliable)
      {
         var capacity = (int)BitOperations.RoundUpToPowerOf2((uint)config.WindowSize);
         _slotMask = capacity - 1;
         _inFlightSlots = new OutgoingEntry?[capacity];
      }
      else
      {
         _slotMask = 0;
         _inFlightSlots = [];
      }
   }

   public DatagramSendResult Enqueue(ReadOnlySpan<byte> payload, long nowTimestamp, int maxBodySize)
   {
      var singleOverhead = DatagramMessageCodec.GetEncodedSize(_config.DeliveryMode, isFragment: false, payload.Length) - payload.Length;
      var maxSinglePayload = maxBodySize - singleOverhead;

      if (payload.Length <= maxSinglePayload)
      {
         if (_config.DeliveryMode == DatagramDeliveryMode.ReliableSequenced)
         {
            SupersedeExisting();
         }
         else if (_pendingQueue.Count + _inFlightCount >= _config.MaxSendQueueDepth)
         {
            return DatagramSendResult.QueueFull;
         }

         var rented = ArrayPool<byte>.Shared.Rent(Math.Max(1, payload.Length));
         if (!payload.IsEmpty)
         {
            payload.CopyTo(rented);
         }

         var seq = _hasChannelSeq ? _nextChannelSeq++ : (ushort)0;
         _pendingQueue.Enqueue(new OutgoingEntry
         {
            RentedBuffer = rented,
            Length = payload.Length,
            ChannelSeq = seq,
            IsFragment = false,
            EnqueuedTimestamp = nowTimestamp
         });

         return DatagramSendResult.Queued;
      }

      if (!_config.AllowFragmentation)
      {
         return DatagramSendResult.PayloadTooLarge;
      }

      var fragCount = (payload.Length + _chunkPayloadSize - 1) / _chunkPayloadSize;
      if (fragCount > _config.MaxFragmentCount || (_isReliable && fragCount > _config.WindowSize))
      {
         return DatagramSendResult.PayloadTooLarge;
      }

      if (_config.DeliveryMode == DatagramDeliveryMode.ReliableSequenced)
      {
         if (fragCount > _config.MaxSendQueueDepth)
         {
            return DatagramSendResult.PayloadTooLarge;
         }

         SupersedeExisting();
      }
      else if (_pendingQueue.Count + _inFlightCount + fragCount > _config.MaxSendQueueDepth)
      {
         return DatagramSendResult.QueueFull;
      }

      var groupId = _nextFragGroupId++;
      var offset = 0;

      for (var i = 0; i < fragCount; i++)
      {
         var chunkLen = Math.Min(_chunkPayloadSize, payload.Length - offset);
         var rented = ArrayPool<byte>.Shared.Rent(chunkLen);

         payload.Slice(offset, chunkLen).CopyTo(rented);
         offset += chunkLen;

         var seq = _hasChannelSeq ? _nextChannelSeq++ : (ushort)0;
         _pendingQueue.Enqueue(new OutgoingEntry
         {
            RentedBuffer = rented,
            Length = chunkLen,
            ChannelSeq = seq,
            IsFragment = true,
            FragGroupId = groupId,
            FragIndex = (ushort)i,
            FragCount = (ushort)fragCount,
            EnqueuedTimestamp = nowTimestamp
         });
      }

      return DatagramSendResult.Queued;
   }

   public void PackIntoDatagram(
      Span<byte> bodyDest,
      ref int bodyOffset,
      int maxBodyLength,
      long nowTimestamp,
      long baseRtoTicks,
      long maxRtoTicks,
      long maxRetryDurationTicks,
      TimeProvider timeProvider,
      RttEstimator rtt,
      List<SentMessageRef> ledgerMessages,
      ref bool rtoLossRecordedForDatagram,
      out bool maxRetriesExceeded)
   {
      maxRetriesExceeded = false;

      // retransmit expired or fast-retransmit flagged in-flight messages first
      if (_isReliable && _inFlightOrder.Count > 0)
      {
         var writeIdx = 0;
         for (var i = 0; i < _inFlightOrder.Count; i++)
         {
            var seq = _inFlightOrder[i];
            var entry = _inFlightSlots[seq & _slotMask];
            if (entry is null || entry.ChannelSeq != seq)
            {
               continue; // already ACKed or superseded — prune lazily
            }

            _inFlightOrder[writeIdx++] = seq;

            if (nowTimestamp - entry.EnqueuedTimestamp > maxRetryDurationTicks)
            {
               maxRetriesExceeded = true;
               return;
            }

            if (!entry.NeedsFastRetransmit && entry.NextRetransmitTimestamp > nowTimestamp)
            {
               continue;
            }

            var encodedSize = DatagramMessageCodec.GetEncodedSize(_config.DeliveryMode, entry.IsFragment, entry.Length);
            if (bodyOffset + encodedSize > maxBodyLength)
            {
               continue; // wait for next datagram
            }

            WriteEntry(bodyDest[bodyOffset..], entry);
            bodyOffset += encodedSize;

            if (!entry.NeedsFastRetransmit && !rtoLossRecordedForDatagram)
            {
               rtt.RecordPacketLost(nowTimestamp, timeProvider);
               rtoLossRecordedForDatagram = true;
            }

            entry.NeedsFastRetransmit = false;
            entry.RetryCount++;

            var backoffShift = Math.Min(entry.RetryCount, 4);
            var nextDelay = Math.Min(maxRtoTicks, baseRtoTicks << backoffShift);
            entry.NextRetransmitTimestamp = nowTimestamp + nextDelay;

            ledgerMessages.Add(new SentMessageRef(_config.ChannelId, entry.ChannelSeq));
            DatagramMetrics.PacketsRetransmitted.Add(1);
            TotalRetransmitted++;
         }

         if (writeIdx < _inFlightOrder.Count)
         {
            _inFlightOrder.RemoveRange(writeIdx, _inFlightOrder.Count - writeIdx);
         }
      }

      // pack unsent messages from pending queue
      while (_pendingQueue.Count > 0)
      {
         var next = _pendingQueue.Peek();

         if (_isReliable)
         {
            if (_inFlightCount >= _config.WindowSize || SeqMath.Diff(next.ChannelSeq, _sendBase) >= _config.WindowSize)
            {
               break;
            }
         }

         var encodedSize = DatagramMessageCodec.GetEncodedSize(_config.DeliveryMode, next.IsFragment, next.Length);
         if (bodyOffset + encodedSize > maxBodyLength)
         {
            break;
         }

         _pendingQueue.Dequeue();
         WriteEntry(bodyDest[bodyOffset..], next);
         bodyOffset += encodedSize;

         if (_isReliable)
         {
            _nextToFlySeq = (ushort)(next.ChannelSeq + 1);
            next.NextRetransmitTimestamp = nowTimestamp + baseRtoTicks;
            _inFlightSlots[next.ChannelSeq & _slotMask] = next;
            _inFlightOrder.Add(next.ChannelSeq);
            _inFlightCount++;
            ledgerMessages.Add(new SentMessageRef(_config.ChannelId, next.ChannelSeq));
         }
         else
         {
            ReturnEntryBuffer(next);
         }
      }
   }

   public void MarkAcked(ushort channelSeq)
   {
      if (!_isReliable)
      {
         return;
      }

      var slotIdx = channelSeq & _slotMask;
      var entry = _inFlightSlots[slotIdx];
      if (entry is null || entry.ChannelSeq != channelSeq)
      {
         return;
      }

      ReturnEntryBuffer(entry);
      _inFlightSlots[slotIdx] = null;
      _inFlightCount--;

      while (_sendBase != _nextToFlySeq && _inFlightSlots[_sendBase & _slotMask] is null)
      {
         _sendBase++;
      }
   }

   public void RequestFastRetransmit(ushort channelSeq)
   {
      if (!_isReliable)
      {
         return;
      }

      var entry = _inFlightSlots[channelSeq & _slotMask];
      if (entry is not null && entry.ChannelSeq == channelSeq)
      {
         entry.NeedsFastRetransmit = true;
      }
   }

   private void SupersedeExisting()
   {
      while (_pendingQueue.Count > 0)
      {
         ReturnEntryBuffer(_pendingQueue.Dequeue());
      }

      for (var i = 0; i < _inFlightOrder.Count; i++)
      {
         var seq = _inFlightOrder[i];
         var slotIdx = seq & _slotMask;
         var entry = _inFlightSlots[slotIdx];
         if (entry is not null)
         {
            ReturnEntryBuffer(entry);
            _inFlightSlots[slotIdx] = null;
         }
      }

      _inFlightOrder.Clear();
      _inFlightCount = 0;
      _sendBase = _nextChannelSeq;
      _nextToFlySeq = _nextChannelSeq;
   }

   private void WriteEntry(Span<byte> dest, OutgoingEntry entry)
   {
      var payload = entry.Length == 0
         ? ReadOnlySpan<byte>.Empty
         : entry.RentedBuffer.AsSpan(0, entry.Length);

      DatagramMessageCodec.Write(
         dest,
         _config.ChannelId,
         _config.DeliveryMode,
         entry.ChannelSeq,
         entry.IsFragment,
         entry.FragGroupId,
         entry.FragIndex,
         entry.FragCount,
         payload);
   }

   private static void ReturnEntryBuffer(OutgoingEntry entry)
   {
      var buf = entry.RentedBuffer;
      entry.RentedBuffer = null;
      if (buf is not null)
      {
         ArrayPool<byte>.Shared.Return(buf);
      }
   }

   public void Dispose()
   {
      while (_pendingQueue.Count > 0)
      {
         ReturnEntryBuffer(_pendingQueue.Dequeue());
      }

      for (var i = 0; i < _inFlightSlots.Length; i++)
      {
         var entry = _inFlightSlots[i];
         if (entry is not null)
         {
            ReturnEntryBuffer(entry);
            _inFlightSlots[i] = null;
         }
      }

      _inFlightOrder.Clear();
      _inFlightCount = 0;
   }
}
