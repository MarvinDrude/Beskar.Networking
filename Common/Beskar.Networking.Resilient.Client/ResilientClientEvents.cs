using Beskar.Memory.Threading;
using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Client.Contexts;

namespace Beskar.Networking.Resilient.Client;

/// <summary>
/// Container for all ResilientClient events and hook pipelines.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientEvents<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// Pipeline fired when the client successfully connects to the server and completes handshake.
   /// </summary>
   public readonly HandlerPipeline<ResilientClientConnectedContext<TFrame>> OnConnected = new();

   /// <summary>
   /// Pipeline fired asynchronously via Task.Run when the client disconnects from the server.
   /// </summary>
   public readonly HandlerPipeline<ResilientClientDisconnectedContext<TFrame>> OnDisconnected = new();

   /// <summary>
   /// Pipeline fired when the client initiates a reconnection attempt.
   /// </summary>
   public readonly HandlerPipeline<ResilientClientReconnectingContext<TFrame>> OnReconnecting = new();

   /// <summary>
   /// Pipeline fired when the server issues an authentication challenge packet during connection handshake.
   /// </summary>
   public readonly HandlerPipeline<ResilientClientAuthenticateContext<TFrame>> OnAuthenticate = new();

   /// <summary>
   /// Pipeline fired when a framing protocol frame is received from the server.
   /// </summary>
   public readonly HandlerPipeline<ResilientClientFrameReceivedContext<TFrame>> FrameReceived = new();
}
