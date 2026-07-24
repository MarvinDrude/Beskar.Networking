using Beskar.Memory.Threading;
using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Server.Contexts;

namespace Beskar.Networking.Resilient.Server;

/// <summary>
/// Container for all ResilientServer events and hook pipelines.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerEvents<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// Pipeline fired when a framing protocol frame is received from a client.
   /// </summary>
   public readonly HandlerPipeline<ResilientFrameReceivedContext<TFrame>> FrameReceived = new();
}
