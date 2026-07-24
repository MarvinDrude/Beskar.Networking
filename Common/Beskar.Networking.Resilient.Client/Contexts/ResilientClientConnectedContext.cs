using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Client.Contexts;

/// <summary>
/// Context passed when the client successfully connects to the server and completes handshake.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientConnectedContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient client that connected.
   /// </summary>
   public required ResilientClient<TFrame> Client { get; init; }
}
