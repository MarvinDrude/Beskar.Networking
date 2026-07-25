using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;

namespace Beskar.Networking.Resilient.Client.Contexts;

/// <summary>
/// Context passed when the client disconnects from the server.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientDisconnectedContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient client that disconnected.
   /// </summary>
   public required ResilientClient<TFrame> Client { get; init; }

   /// <summary>
   /// The disconnect payload received from the server, if any.
   /// </summary>
   public DisconnectPacketPayload? DisconnectPayload { get; init; }
}
