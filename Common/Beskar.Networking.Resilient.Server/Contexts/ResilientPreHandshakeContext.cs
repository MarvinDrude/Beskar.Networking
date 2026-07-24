using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Contexts;

/// <summary>
/// Context passed when a new client session is accepted, prior to handshake or registration.
/// Allows inspecting the network session/listener and denying/disconnecting the connection early.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientPreHandshakeContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The network listener that accepted the connection.
   /// </summary>
   public required INetworkListener Listener { get; init; }

   /// <summary>
   /// The accepted network session.
   /// </summary>
   public required INetworkSession Session { get; init; }

   /// <summary>
   /// The cancellation token for the operation.
   /// </summary>
   public CancellationToken CancellationToken { get; init; }

   /// <summary>
   /// Gets or sets whether the connection should be denied and immediately disconnected.
   /// </summary>
   public bool IsDenied { get; set; }

   /// <summary>
   /// Denies the connection request.
   /// </summary>
   public void Deny()
   {
      IsDenied = true;
   }
}
