using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Client.Contexts;

/// <summary>
/// Context passed when a framing protocol frame is received from the server.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientFrameReceivedContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient client that received the frame.
   /// </summary>
   public required ResilientClient<TFrame> Client { get; init; }

   /// <summary>
   /// The network stream on which the frame was received.
   /// </summary>
   public required INetworkStream Stream { get; init; }

   /// <summary>
   /// The received protocol frame.
   /// </summary>
   public required TFrame Frame { get; init; }
}
