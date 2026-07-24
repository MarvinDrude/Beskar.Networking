using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Client.Contexts;

/// <summary>
/// Context passed when the client attempts to reconnect after an unexpected disconnect.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientReconnectingContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient client attempting reconnection.
   /// </summary>
   public required ResilientClient<TFrame> Client { get; init; }

   /// <summary>
   /// The 1-based attempt count.
   /// </summary>
   public int Attempt { get; init; }

   /// <summary>
   /// The exception that caused the disconnect or previous reconnect failure, if any.
   /// </summary>
   public Exception? LastException { get; init; }
}
