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
   /// Pipeline fired when the server starts running.
   /// </summary>
   public readonly HandlerPipeline<ResilientServerStartContext<TFrame>> OnStart = new();

   /// <summary>
   /// Pipeline fired when the server stops.
   /// </summary>
   public readonly HandlerPipeline<ResilientServerStopContext<TFrame>> OnStop = new();

   /// <summary>
   /// Pipeline fired when a new client connection is accepted before handshake or registration.
   /// Allows inspecting the session and optionally denying/disconnecting the client.
   /// </summary>
   public readonly HandlerPipeline<ResilientPreHandshakeContext<TFrame>> OnPreHandshake = new();

   /// <summary>
   /// Pipeline fired when a client sends a Connect packet payload to initiate connection handshake.
   /// Allows inspecting connect options, performing authentication challenges, or denying the connection.
   /// </summary>
   public readonly HandlerPipeline<ResilientClientConnectContext<TFrame>> OnConnect = new();

   /// <summary>
   /// Pipeline fired when a framing protocol frame is received from a client.
   /// </summary>
   public readonly HandlerPipeline<ResilientFrameReceivedContext<TFrame>> FrameReceived = new();
}
