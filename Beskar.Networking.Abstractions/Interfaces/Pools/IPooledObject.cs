namespace Beskar.Networking.Abstractions.Interfaces.Pools;

/// <summary>
/// Represents a pooled object.
/// </summary>
public interface IPooledObject
{
   /// <summary>
   /// Tries to reset the state of the pooled object.
   /// </summary>
   public bool TryResetState();
}