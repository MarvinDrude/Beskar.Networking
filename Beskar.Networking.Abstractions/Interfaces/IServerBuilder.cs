namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Defines a builder interface for configuring and constructing server instances.
/// </summary>
/// <typeparam name="TSelf">The type of the server builder implementation that inherits this interface.</typeparam>
public interface IServerBuilder<out TSelf>
{
   /// <summary>
   /// Use a specific network listener to listen for incoming connections.
   /// </summary>
   /// <param name="listener">The network listener to use.</param>
   /// <returns>The builder instance.</returns>
   public TSelf Use(INetworkListener listener);
}
