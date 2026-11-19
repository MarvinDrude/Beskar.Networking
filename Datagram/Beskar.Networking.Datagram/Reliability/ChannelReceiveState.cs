using System.Buffers;
using System.Numerics;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;

namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Manages incoming deduplication, sequence filtering, ordered buffering, and fragment reassembly
/// for a single <see cref="DatagramChannelConfig"/>.
/// </summary>
internal sealed class ChannelReceiveState : IDisposable
{
   private struct OrderedSlot
   {
      public bool Occupied;
      public bool IsFragment;
      public ushort FragGroupId;
      public ushort FragIndex;
      public ushort FragCount;
      public byte[]? RentedBuffer;
      public int Length;
   }

   private readonly DatagramChannelConfig _config;
   private readonly FragmentReassembler _reassembler;
   private readonly int _slotMask;
   private readonly ulong[] _unorderedBitmap;
   private readonly OrderedSlot[] _orderedSlots;

   private ushort _recvBase;
   private ushort _latestSequenced;
   private bool _hasReceivedSequenced;

   public ChannelReceiveState(DatagramChannelConfig config)
   {
      _config = config;
      _reassembler = new FragmentReassembler(config.MaxInFlightFragmentGroups, config.MaxFragmentCount);

      if (config.DeliveryMode == DatagramDeliveryMode.ReliableUnordered)
      {
         var capacity = Math.Max(64, (int)BitOperations.RoundUpToPowerOf2((uint)config.WindowSize));
         _slotMask = capacity - 1;
         _unorderedBitmap = new ulong[capacity / 64];
         _orderedSlots = [];
      }
      else if (config.DeliveryMode == DatagramDeliveryMode.ReliableOrdered)
      {
         var capacity = (int)BitOperations.RoundUpToPowerOf2((uint)config.WindowSize);
         _slotMask = capacity - 1;
         _unorderedBitmap = [];
         _orderedSlots = new OrderedSlot[capacity];
      }
      else
      {
         _slotMask = 0;
         _unorderedBitmap = [];
         _orderedSlots = [];
      }
   }

   public void HandleMessage(in DecodedChannelMessage msg, Action<byte, DatagramPayloadLease> onDelivered)
   {
      switch (_config.DeliveryMode)
      {
         case DatagramDeliveryMode.UnreliableUnordered:
            DeliverOrReassemble(msg, onDelivered);
            break;

         case DatagramDeliveryMode.UnreliableSequenced:
         case DatagramDeliveryMode.ReliableSequenced:
         {
            var logicalSeq = msg.IsFragment
               ? unchecked((ushort)(msg.ChannelSeq - msg.FragIndex))
               : msg.ChannelSeq;

            if (_hasReceivedSequenced)
            {
               if (msg.IsFragment && logicalSeq == _latestSequenced)
               {
                  // Additional fragment belonging to the currently active sequenced fragment group
                  DeliverOrReassemble(msg, onDelivered);
                  break;
               }

               if (!SeqMath.IsGreater(logicalSeq, _latestSequenced))
               {
                  return;
               }

               _reassembler.Clear();
            }

            _hasReceivedSequenced = true;
            _latestSequenced = logicalSeq;
            DeliverOrReassemble(msg, onDelivered);
            break;
         }

         case DatagramDeliveryMode.ReliableUnordered:
            HandleReliableUnordered(in msg, onDelivered);
            break;

         case DatagramDeliveryMode.ReliableOrdered:
            HandleReliableOrdered(in msg, onDelivered);
            break;
      }
   }

   private void HandleReliableUnordered(in DecodedChannelMessage msg, Action<byte, DatagramPayloadLease> onDelivered)
   {
      var diff = SeqMath.Diff(msg.ChannelSeq, _recvBase);
      if (diff < 0 || diff >= _config.WindowSize)
      {
         return;
      }

      var slot = msg.ChannelSeq & _slotMask;
      var wordIdx = slot >> 6;
      var mask = 1UL << (slot & 63);

      if ((_unorderedBitmap[wordIdx] & mask) != 0)
      {
         return; // Duplicate
      }

      _unorderedBitmap[wordIdx] |= mask;
      DeliverOrReassemble(in msg, onDelivered);

      while (true)
      {
         var baseSlot = _recvBase & _slotMask;
         var bWord = baseSlot >> 6;
         var bMask = 1UL << (baseSlot & 63);
         if ((_unorderedBitmap[bWord] & bMask) == 0)
         {
            break;
         }

         _unorderedBitmap[bWord] &= ~bMask;
         _recvBase++;
      }
   }

   private void HandleReliableOrdered(in DecodedChannelMessage msg, Action<byte, DatagramPayloadLease> onDelivered)
   {
      var diff = SeqMath.Diff(msg.ChannelSeq, _recvBase);
      if (diff < 0 || diff >= _config.WindowSize)
      {
         return;
      }

      var slotIdx = msg.ChannelSeq & _slotMask;
      if (_orderedSlots[slotIdx].Occupied)
      {
         return; // Duplicate
      }

      if (msg.ChannelSeq == _recvBase)
      {
         DeliverOrReassemble(in msg, onDelivered);
         _recvBase++;
         DrainContiguousOrdered(onDelivered);
         return;
      }

      byte[]? rented = null;
      if (!msg.Payload.IsEmpty)
      {
         rented = ArrayPool<byte>.Shared.Rent(msg.Payload.Length);
         msg.Payload.CopyTo(rented);
      }

      _orderedSlots[slotIdx] = new OrderedSlot
      {
         Occupied = true,
         IsFragment = msg.IsFragment,
         FragGroupId = msg.FragGroupId,
         FragIndex = msg.FragIndex,
         FragCount = msg.FragCount,
         RentedBuffer = rented,
         Length = msg.Payload.Length
      };
   }

   private void DrainContiguousOrdered(Action<byte, DatagramPayloadLease> onDelivered)
   {
      while (true)
      {
         var slotIdx = _recvBase & _slotMask;
         ref var slot = ref _orderedSlots[slotIdx];
         if (!slot.Occupied)
         {
            break;
         }

         if (!slot.IsFragment)
         {
            var lease = slot.RentedBuffer is not null
               ? DatagramPayloadLease.WrapRented(slot.RentedBuffer, slot.Length)
               : DatagramPayloadLease.Empty;

            slot = default;
            _recvBase++;
            onDelivered(_config.ChannelId, lease);
         }
         else
         {
            var payload = slot.RentedBuffer is not null
               ? slot.RentedBuffer.AsSpan(0, slot.Length)
               : ReadOnlySpan<byte>.Empty;

            var completed = _reassembler.TryAddFragment(
               slot.FragGroupId,
               slot.FragIndex,
               slot.FragCount,
               payload,
               out var assembled);

            if (slot.RentedBuffer is not null)
            {
               ArrayPool<byte>.Shared.Return(slot.RentedBuffer);
            }

            slot = default;
            _recvBase++;

            if (completed)
            {
               onDelivered(_config.ChannelId, assembled);
            }
         }
      }
   }

   private void DeliverOrReassemble(in DecodedChannelMessage msg, Action<byte, DatagramPayloadLease> onDelivered)
   {
      if (!msg.IsFragment)
      {
         onDelivered(_config.ChannelId, DatagramPayloadLease.CopyFrom(msg.Payload));
         return;
      }

      if (_reassembler.TryAddFragment(
         msg.FragGroupId,
         msg.FragIndex,
         msg.FragCount,
         msg.Payload,
         out var assembled))
      {
         onDelivered(_config.ChannelId, assembled);
      }
   }

   public void Dispose()
   {
      for (var i = 0; i < _orderedSlots.Length; i++)
      {
         if (_orderedSlots[i].Occupied && _orderedSlots[i].RentedBuffer is not null)
         {
            ArrayPool<byte>.Shared.Return(_orderedSlots[i].RentedBuffer!);
         }
         _orderedSlots[i] = default;
      }

      _reassembler.Dispose();
   }
}
