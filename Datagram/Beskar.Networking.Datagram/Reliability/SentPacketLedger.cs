namespace Beskar.Networking.Datagram.Reliability;

internal readonly record struct SentMessageRef(byte ChannelId, ushort ChannelSeq);

/// <summary>
/// 1024-slot ring buffer mapping physical wire <c>PacketSeq</c> numbers to send timestamps
/// and the reliable channel messages contained inside each datagram.
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
      public bool Occupied;
      public readonly List<SentMessageRef> Messages = new(16);
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

   public void BeginRecord(uint packetSeq, long sentAtTimestamp, out List<SentMessageRef> messageList)
   {
      var entry = _entries[(int)(packetSeq & (Capacity - 1))];
      entry.PacketSeq = packetSeq;
      entry.SentAtTimestamp = sentAtTimestamp;
      entry.Acked = false;
      entry.CountedAsLost = false;
      entry.FastRetransmitTriggered = false;
      entry.Occupied = true;
      entry.Messages.Clear();

      messageList = entry.Messages;
   }

   public void ProcessAcks(
      uint ackSeq,
      ulong ackBitfield,
      long nowTimestamp,
      TimeProvider timeProvider,
      RttEstimator rtt,
      Action<byte, ushort> onMessageAcked)
   {
      if (ackSeq == 0)
      {
         return;
      }

      AckSingle(ackSeq, nowTimestamp, timeProvider, rtt, onMessageAcked, recordRtt: true);

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
         AckSingle(seq, nowTimestamp, timeProvider, rtt, onMessageAcked, recordRtt: false);
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
      Action<byte, ushort> onFastRetransmit)
   {
      if (!_hasAckedAny)
      {
         return;
      }

      while (_oldestUnackedHint <= latestSentPacketSeq && _oldestUnackedHint <= _highestAckedSeq)
      {
         var entry = _entries[(int)(_oldestUnackedHint & (Capacity - 1))];
         if (!entry.Occupied || entry.PacketSeq != _oldestUnackedHint || entry.Acked)
         {
            if (_oldestUnackedHint == uint.MaxValue) break;
            _oldestUnackedHint++;
            continue;
         }

         if (!entry.CountedAsLost && nowTimestamp - entry.SentAtTimestamp >= lossCutoffTicks)
         {
            entry.CountedAsLost = true;
            rtt.RecordPacketLost(nowTimestamp, timeProvider);
         }

         // fast retransmit if 3 or more newer physical packets have been ACKed
         if (!entry.FastRetransmitTriggered && _highestAckedSeq - entry.PacketSeq >= 3)
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

         if (entry is { CountedAsLost: true, FastRetransmitTriggered: true })
         {
            if (_oldestUnackedHint == uint.MaxValue) break;
            _oldestUnackedHint++;
         }
         else
         {
            break;
         }
      }
   }

   private void AckSingle(
      uint seq,
      long nowTimestamp,
      TimeProvider timeProvider,
      RttEstimator rtt,
      Action<byte, ushort> onMessageAcked,
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
   }
}
