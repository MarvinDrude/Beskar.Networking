using System.Buffers;

namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Reassembles fragmented channel messages across out-of-order arrivals with bounded memory.
/// </summary>
internal sealed class FragmentReassembler(int maxInFlightGroups, int maxFragmentCount) : IDisposable
{
   private sealed class FragmentGroup
   {
      public ushort GroupId;
      public ushort FragCount;
      public int ReceivedCount;
      public byte[]?[] Fragments = [];
      public int[] Lengths = [];
      public bool[] Received = [];
      public int TotalPayloadBytes;
      public long LastUpdatedOrder;
   }

   private readonly int _maxGroups = Math.Max(1, maxInFlightGroups);
   private readonly int _maxFragments = Math.Max(1, maxFragmentCount);
   private readonly Dictionary<ushort, FragmentGroup> _groups = new();
   private long _updateCounter;

   public bool TryAddFragment(
      ushort groupId,
      ushort fragIndex,
      ushort fragCount,
      ReadOnlySpan<byte> chunk,
      out DatagramPayloadLease assembled)
   {
      assembled = DatagramPayloadLease.Empty;

      if (fragCount == 0 || fragCount > _maxFragments || fragIndex >= fragCount)
      {
         return false;
      }

      if (!_groups.TryGetValue(groupId, out var group))
      {
         if (_groups.Count >= _maxGroups)
         {
            EvictOldestGroup();
         }

         group = new FragmentGroup
         {
            GroupId = groupId,
            FragCount = fragCount,
            Fragments = new byte[]?[fragCount],
            Lengths = new int[fragCount],
            Received = new bool[fragCount]
         };
         _groups[groupId] = group;
      }
      else if (group.FragCount != fragCount)
      {
         // conflicting group metadata, reset safely
         ReturnGroupBuffers(group);
         _groups.Remove(groupId);
         return false;
      }

      group.LastUpdatedOrder = ++_updateCounter;

      if (group.Received[fragIndex])
      {
         return false;
      }

      var copy = ArrayPool<byte>.Shared.Rent(chunk.Length);
      chunk.CopyTo(copy);
      group.Fragments[fragIndex] = copy;
      group.Lengths[fragIndex] = chunk.Length;
      group.Received[fragIndex] = true;
      group.ReceivedCount++;
      group.TotalPayloadBytes += chunk.Length;

      if (group.ReceivedCount < group.FragCount)
      {
         return false;
      }

      var combined = ArrayPool<byte>.Shared.Rent(Math.Max(1, group.TotalPayloadBytes));
      var offset = 0;

      for (var i = 0; i < group.FragCount; i++)
      {
         var fragBuf = group.Fragments[i]!;
         var fragLen = group.Lengths[i];

         fragBuf.AsSpan(0, fragLen).CopyTo(combined.AsSpan(offset, fragLen));
         offset += fragLen;
      }

      ReturnGroupBuffers(group);
      _groups.Remove(groupId);

      assembled = DatagramPayloadLease.WrapRented(combined, offset);
      return true;
   }

   private void EvictOldestGroup()
   {
      ushort oldestKey = 0;
      var oldestOrder = long.MaxValue;

      foreach (var kvp in _groups)
      {
         if (kvp.Value.LastUpdatedOrder < oldestOrder)
         {
            oldestOrder = kvp.Value.LastUpdatedOrder;
            oldestKey = kvp.Key;
         }
      }

      if (_groups.Remove(oldestKey, out var evicted))
      {
         ReturnGroupBuffers(evicted);
      }
   }

   private static void ReturnGroupBuffers(FragmentGroup group)
   {
      for (var i = 0; i < group.Fragments.Length; i++)
      {
         var buf = group.Fragments[i];
         if (buf is not null)
         {
            ArrayPool<byte>.Shared.Return(buf);
            group.Fragments[i] = null;
         }
      }
   }

   public void Clear()
   {
      foreach (var group in _groups.Values)
      {
         ReturnGroupBuffers(group);
      }
      _groups.Clear();
   }

   public void Dispose()
   {
      Clear();
   }
}
