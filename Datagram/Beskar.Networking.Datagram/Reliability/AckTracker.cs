namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Tracks incoming wire <c>PacketSeq</c> values with a 1024-packet anti-replay/duplicate bitmap
/// and maintains the outgoing 64-bit <c>AckBitfield</c>.
/// </summary>
internal sealed class AckTracker
{
   private const int ReplayWindowBits = 1024;
   private const int ReplayWords = ReplayWindowBits / 64;

   private readonly ulong[] _replayBitmap = new ulong[ReplayWords];
   private bool _hasReceivedAny;

   public uint HighestReceivedSeq { get; private set; }
   public ulong OutgoingAckBitfield { get; private set; }

   public bool TryRecordReceivedPacket(uint packetSeq)
   {
      if (packetSeq == 0)
      {
         return false;
      }

      if (!_hasReceivedAny)
      {
         _hasReceivedAny = true;
         HighestReceivedSeq = packetSeq;
         OutgoingAckBitfield = 0;

         SetReplayBit(packetSeq);
         return true;
      }

      if (packetSeq > HighestReceivedSeq)
      {
         var delta = packetSeq - HighestReceivedSeq;

         if (delta >= ReplayWindowBits)
         {
            Array.Clear(_replayBitmap);
         }
         else
         {
            for (uint i = 1; i <= delta; i++)
            {
               ClearReplayBit(HighestReceivedSeq + i);
            }
         }

         if (delta > 64)
         {
            OutgoingAckBitfield = 0;
         }
         else if (delta == 64)
         {
            OutgoingAckBitfield = 1UL << 63;
         }
         else
         {
            OutgoingAckBitfield = (OutgoingAckBitfield << (int)delta) | (1UL << (int)(delta - 1));
         }

         HighestReceivedSeq = packetSeq;
         SetReplayBit(packetSeq);
         return true;
      }

      if (packetSeq == HighestReceivedSeq)
      {
         return false;
      }

      var age = HighestReceivedSeq - packetSeq;
      if (age >= ReplayWindowBits)
      {
         return false;
      }

      if (GetReplayBit(packetSeq))
      {
         return false;
      }

      SetReplayBit(packetSeq);
      if (age <= 64)
      {
         OutgoingAckBitfield |= 1UL << (int)(age - 1);
      }

      return true;
   }

   private bool GetReplayBit(uint seq)
   {
      var idx = (int)(seq & (ReplayWindowBits - 1));
      return (_replayBitmap[idx >> 6] & (1UL << (idx & 63))) != 0;
   }

   private void SetReplayBit(uint seq)
   {
      var idx = (int)(seq & (ReplayWindowBits - 1));
      _replayBitmap[idx >> 6] |= 1UL << (idx & 63);
   }

   private void ClearReplayBit(uint seq)
   {
      var idx = (int)(seq & (ReplayWindowBits - 1));
      _replayBitmap[idx >> 6] &= ~(1UL << (idx & 63));
   }
}
