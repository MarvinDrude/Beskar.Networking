using System.Diagnostics.CodeAnalysis;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Defines a thread-safe, generic property store for network sessions.
/// </summary>
public interface INetworkPropertyStore
{
   /// <summary>
   /// Sets the value of a property.
   /// </summary>
   public void Set<T>(string key, T value);

   /// <summary>
   /// Tries to get the value of a property.
   /// </summary>
   public bool TryGet<T>(string key, [MaybeNullWhen(false)] out T value);

   /// <summary>
   /// Removes the property with the specified key.
   /// </summary>
   public bool Remove(string key);

   /// <summary>
   /// Clears all properties in the store.
   /// </summary>
   public void Clear();

   /// <summary>
   /// Gets a read-only view of all properties currently in the store.
   /// </summary>
   public IReadOnlyDictionary<string, object?> AllProperties { get; }
}
