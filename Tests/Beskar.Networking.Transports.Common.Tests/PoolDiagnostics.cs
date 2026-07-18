using System.Collections;
using System.Reflection;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Tests;

public static class PoolDiagnostics
{
   public static int GetCachedCount(object? pool)
   {
      if (pool == null) return 0;

      var type = pool.GetType();
      var headField = type.GetField("_head", BindingFlags.NonPublic | BindingFlags.Instance);
      var queueField = type.GetField("_queue", BindingFlags.NonPublic | BindingFlags.Instance);

      var head = headField?.GetValue(pool);
      var queue = queueField?.GetValue(pool) as System.Collections.ICollection;

      var count = 0;

      if (head != null) count++;
      if (queue != null) count += queue.Count;

      return count;
   }

   public static int GetActiveCount(object? pool)
   {
      if (pool == null) return 0;

      var type = pool.GetType();
      var currentSizeField = type.GetField("_currentSize", BindingFlags.NonPublic | BindingFlags.Instance);
      if (currentSizeField == null) return 0;

      var currentSize = (int)currentSizeField.GetValue(pool)!;
      return currentSize - GetCachedCount(pool);
   }

   public static int GetCachedBlocksCount(PinnedBlockMemoryPool pool)
   {
      var type = pool.GetType();
      var blocksField = type.GetField("_blocks", BindingFlags.NonPublic | BindingFlags.Instance);

      if (blocksField == null) return 0;

      var blocksValue = blocksField.GetValue(pool);
      if (blocksValue == null) return 0;

      var channelType = blocksValue.GetType();
      var itemsField = channelType.GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance);

      if (itemsField != null)
      {
         if (itemsField.GetValue(blocksValue) is IEnumerable itemsQueue)
         {
            var count = 0;
            foreach (var item in itemsQueue)
            {
               count++;
            }

            return count;
         }
      }

      var prop = channelType.GetProperty("ItemsCountForDebugger", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
      if (prop != null)
      {
         return (int)prop.GetValue(blocksValue)!;
      }

      return 0;
   }
}
