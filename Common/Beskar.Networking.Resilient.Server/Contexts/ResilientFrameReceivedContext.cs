using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Server.Models;

namespace Beskar.Networking.Resilient.Server.Contexts;

/// <summary>
/// Context passed when a frame is received from a client on ResilientServer.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientFrameReceivedContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The client that sent the frame.
   /// </summary>
   public required ResilientServerClient<TFrame> Client { get; init; }

   /// <summary>
   /// The specific network stream on which the frame arrived.
   /// </summary>
   public required INetworkStream Stream { get; init; }

   /// <summary>
   /// The received protocol frame.
   /// </summary>
   public required TFrame Frame { get; init; }
}
