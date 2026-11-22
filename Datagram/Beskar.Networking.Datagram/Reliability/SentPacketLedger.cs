namespace Beskar.Networking.Datagram.Reliability;

internal readonly record struct SentMessageRef(byte ChannelId, ushort ChannelSeq);

/// <summary>
/// Tracks fragment-level acknowledgment/loss completion for multi-fragment messages
/// that carry a caller-supplied delivery notification token.
/// </summary>
internal sealed class DeliveryFragmentTracker(int fragmentCount)
{
   private int _remaining = fragmentCount;
   private bool _resolved;

   public bool TryMarkFragmentAcked()
   {
      if (_resolved)
      {
         return false;
      }

      if (--_remaining <= 0)
      {
         _resolved = true;
         return true;
      }

      return false;
   }

   public bool TryMarkLost()
   {
      if (_resolved)
      {
         return false;
      }

      _resolved = true;
      return true;
   }
}

internal readonly record struct SentUnreliableDeliveryRef(
   byte ChannelId,
   ulong DeliveryToken,
   DeliveryFragmentTracker? Tracker);

/// <summary>
/// 1024-slot ring buffer mapping physical wire <c>PacketSeq</c> numbers to send timestamps,
/// reliable channel messages, and caller-tracked unreliable delivery tokens inside each datagram.
/// </summary>
internal sealed class SentPacketLedger
{
   public const int Capacity = 1024;

   private sealed class Entry
   {
      public uint PacketSeq;
      public long SentAtTimestamp;
      public bool Acked;
      public bool CountedAsLost;
      public bool FastRetransmitTriggered;
      public bool UnreliableLossNotified;
      public bool Occupied;

      public readonly List<SentMessageRef> Messages = new(16);
      public readonly List<SentUnreliableDeliveryRef> UnreliableDeliveries = new(4);
   }

   private readonly Entry[] _entries = new Entry[Capacity];
   private uint _highestAckedSeq;
   private bool _hasAckedAny;
   private uint _oldestUnackedHint = 1;

   public SentPacketLedger()
   {
      for (var i = 0; i < Capacity; i++)
      {
         _entries[i] = new Entry();
      }
   }

   public void BeginRecord(
      uint packetSeq,
      long sentAtTimestamp,
      out List<SentMessageRef> messageList,
      out List<SentUnreliableDeliveryRef> unreliableDeliveryList,
      Action<SentUnreliableDeliveryRef>? onEvictedUnreliableLost = null)
   {
      var entry = _entries[(int)(packetSeq & (Capacity - 1))];

      if (entry.Occupied && !entry.Acked && !entry.UnreliableLossNotified && entry.UnreliableDeliveries.Count > 0)
      {
         entry.UnreliableLossNotified = true;
         if (onEvictedUnreliableLost is not null)
         {
            for (var i = 0; i < entry.UnreliableDeliveries.Count; i++)
            {
               onEvictedUnreliableLost(entry.UnreliableDeliveries[i]);
            }
         }
      }

      entry.PacketSeq = packetSeq;
      entry.SentAtTimestamp = sentAtTimestamp;

      entry.Acked = false;
      entry.CountedAsLost = false;
      entry.FastRetransmitTriggered = false;
      entry.UnreliableLossNotified = false;
      entry.Occupied = true;

      entry.Messages.Clear();
      entry.UnreliableDeliveries.Clear();

      messageList = entry.Messages;
      unreliableDeliveryList = entry.UnreliableDeliveries;
   }

   public void ProcessAcks(
      uint ackSeq,
      ulong ackBitfield,
      uint latestSentPacketSeq,
      long nowTimestamp,
      TimeProvider timeProvider,
      RttEstimator rtt,
      Action<byte, ushort> onMessageAcked,
      Action<SentUnreliableDeliveryRef>? onUnreliableAcked = null)
   {
      if (ackSeq == 0 || ackSeq > latestSentPacketSeq)
      {
         return;
      }

      AckSingle(ackSeq, nowTimestamp, timeProvider, rtt, onMessageAcked, onUnreliableAcked, recordRtt: true);

      for (var i = 0; i < 64; i++)
      {
         if ((ackBitfield & (1UL << i)) == 0)
         {
            continue;
         }

         if (ackSeq <= (uint)(i + 1))
         {
            break;
         }

         var seq = ackSeq - (uint)(i + 1);
         AckSingle(seq, nowTimestamp, timeProvider, rtt, onMessageAcked, onUnreliableAcked, recordRtt: false);
      }

      if (!_hasAckedAny || ackSeq > _highestAckedSeq)
      {
         _hasAckedAny = true;
         _highestAckedSeq = ackSeq;
      }
   }

   public void DetectLossesAndFastRetransmits(
      uint latestSentPacketSeq,
      long nowTimestamp,
      long lossCutoffTicks,
      TimeProvider timeProvider,
      RttEstimator rtt,
      Action<byte, ushort> onFastRetransmit,
      Action<SentUnreliableDeliveryRef>? onUnreliableLost = null)
   {
      if (latestSentPacketSeq == 0)
      {
         return;
      }

      if (latestSentPacketSeq >= Capacity)
      {
         var minTrackable = latestSentPacketSeq - (uint)Capacity + 1;
         if (_oldestUnackedHint < minTrackable)
         {
            _oldestUnackedHint = minTrackable;
         }
      }

      var canAdvanceHint = true;
      for (var seq = _oldestUnackedHint; seq <= latestSentPacketSeq; seq++)
      {
         var entry = _entries[(int)(seq & (Capacity - 1))];
         if (!entry.Occupied || entry.PacketSeq != seq || entry.Acked)
         {
            if (canAdvanceHint && seq < uint.MaxValue)
            {
               _oldestUnackedHint = seq + 1;
            }
            if (seq == uint.MaxValue) break;
            continue;
         }

         var timedOut = nowTimestamp - entry.SentAtTimestamp >= lossCutoffTicks;
         var fastGap = _hasAckedAny && _highestAckedSeq >= seq && _highestAckedSeq - seq >= 3;

         if (!timedOut && !fastGap)
         {
            break;
         }

         if (timedOut && !entry.CountedAsLost)
         {
            entry.CountedAsLost = true;
            rtt.RecordPacketLost(nowTimestamp, timeProvider);
         }

         // fast retransmit if 3 or more newer physical packets have been ACKed
         if (fastGap && !entry.FastRetransmitTriggered)
         {
            entry.FastRetransmitTriggered = true;
            if (!entry.CountedAsLost)
            {
               entry.CountedAsLost = true;
               rtt.RecordPacketLost(nowTimestamp, timeProvider);
            }

            foreach (var msg in entry.Messages)
            {
               onFastRetransmit(msg.ChannelId, msg.ChannelSeq);
            }
         }

         if (!entry.UnreliableLossNotified && entry.UnreliableDeliveries.Count > 0)
         {
            entry.UnreliableLossNotified = true;
            if (onUnreliableLost is not null)
            {
               for (var i = 0; i < entry.UnreliableDeliveries.Count; i++)
               {
                  onUnreliableLost(entry.UnreliableDeliveries[i]);
               }
            }
         }

         var fullyDone = entry.CountedAsLost && (entry.FastRetransmitTriggered || entry.Messages.Count == 0);
         if (canAdvanceHint && fullyDone)
         {
            if (seq < uint.MaxValue)
            {
               _oldestUnackedHint = seq + 1;
            }
         }
         else
         {
            canAdvanceHint = false;
         }

         if (seq == uint.MaxValue) break;
      }
   }

   private void AckSingle(
      uint seq,
      long nowTimestamp,
      TimeProvider timeProvider,
      RttEstimator rtt,
      Action<byte, ushort> onMessageAcked,
      Action<SentUnreliableDeliveryRef>? onUnreliableAcked,
      bool recordRtt)
   {
      var entry = _entries[(int)(seq & (Capacity - 1))];
      if (!entry.Occupied || entry.PacketSeq != seq || entry.Acked)
      {
         return;
      }

      entry.Acked = true;
      rtt.RecordPacketAcked(nowTimestamp, timeProvider);

      if (recordRtt)
      {
         var elapsed = timeProvider.GetElapsedTime(entry.SentAtTimestamp, nowTimestamp);
         rtt.RecordRttSample(elapsed.TotalMilliseconds);
      }

      foreach (var msg in entry.Messages)
      {
         onMessageAcked(msg.ChannelId, msg.ChannelSeq);
      }

      if (!entry.UnreliableLossNotified && entry.UnreliableDeliveries.Count > 0)
      {
         entry.UnreliableLossNotified = true;
         if (onUnreliableAcked is not null)
         {
            for (var i = 0; i < entry.UnreliableDeliveries.Count; i++)
            {
               onUnreliableAcked(entry.UnreliableDeliveries[i]);
            }
         }
      }
   }
}
