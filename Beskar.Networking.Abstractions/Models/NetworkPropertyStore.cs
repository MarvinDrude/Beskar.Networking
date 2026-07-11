using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// A thread-safe, lazy-instantiated property store implementation using <see cref="ConcurrentDictionary{TKey, TValue}"/>.
/// </summary>
public sealed class NetworkPropertyStore : INetworkPropertyStore
{
   private static readonly IReadOnlyDictionary<string, object?> EmptyDictionary = new ConcurrentDictionary<string, object?>();

   private ConcurrentDictionary<string, object?>? _properties;
   public IReadOnlyDictionary<string, object?> AllProperties => _properties ?? EmptyDictionary;

   public void Set<T>(string key, T value)
   {
      LazyInitializer.EnsureInitialized(ref _properties);
      _properties[key] = value;
   }

   public bool TryGet<T>(string key, [MaybeNullWhen(false)] out T value)
   {
      if (_properties is not null && _properties.TryGetValue(key, out var rawValue))
      {
         if (rawValue is T typedValue)
         {
            value = typedValue;
            return true;
         }

         if (rawValue is null && default(T) is null)
         {
            value = default!;
            return true;
         }
      }

      value = default;
      return false;
   }

   public bool Remove(string key)
   {
      return _properties is not null && _properties.TryRemove(key, out _);
   }

   public void Clear()
   {
      _properties?.Clear();
   }
}
