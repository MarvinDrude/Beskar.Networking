using System.Collections.Concurrent;

namespace Beskar.Networking.Transports.Memory;

/// <summary>
/// An internal, thread-safe registry to coordinate binding and resolving active in-memory listeners.
/// </summary>
internal static class MemoryTransportRegistry
{
   private static readonly ConcurrentDictionary<string, MemoryNetworkListener> _listeners = new(StringComparer.OrdinalIgnoreCase);

   public static bool TryRegister(string address, MemoryNetworkListener listener)
   {
      return _listeners.TryAdd(address, listener);
   }

   public static bool TryUnregister(string address, MemoryNetworkListener listener)
   {
      return _listeners.TryRemove(KeyValuePair.Create(address, listener));
   }

   public static MemoryNetworkListener? GetListener(string address)
   {
      return _listeners.GetValueOrDefault(address);
   }
}
