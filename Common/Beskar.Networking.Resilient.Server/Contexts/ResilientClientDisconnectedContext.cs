using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Server.Models;

namespace Beskar.Networking.Resilient.Server.Contexts;

/// <summary>
/// Context passed when a client disconnects from the ResilientServer.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientDisconnectedContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The client that was disconnected.
   /// </summary>
   public required ResilientServerClient<TFrame> Client { get; init; }

   /// <summary>
   /// Reason code of why it disconnected
   /// </summary>
   public byte? ReasonCode => Client.DisconnectPayload?.ReasonCode;
}
