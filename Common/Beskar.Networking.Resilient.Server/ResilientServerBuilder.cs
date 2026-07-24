using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.Resilient.Server;

/// <summary>
/// Provides functionality to configure and build instances of server.
/// This builder offers a fluent API for specifying server options and network listeners.
/// </summary>
public class ResilientServerBuilder<TFrame>(ResilientServerOptions options)
   : IServerBuilder<ResilientServerBuilder<TFrame>>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   private readonly ResilientServerOptions _options = options;
   private readonly List<INetworkListener> _listeners = [];

   /// <summary>
   /// Builds and returns the configured instance.
   /// </summary>
   /// <returns>A configured instance.</returns>
   public ResilientServer<TFrame> Build()
   {
      var server = new ResilientServer<TFrame>([.. _listeners], _options);
      return server;
   }

   /// <summary>
   /// Configures the server to use the specified network listener.
   /// </summary>
   /// <param name="listener">The network listener to register with the server.</param>
   /// <returns>The builder instance for chaining calls.</returns>
   public ResilientServerBuilder<TFrame> Use(INetworkListener listener)
   {
      _listeners.Add(listener);
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using TCP.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional TCP transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseTcp(IPEndPoint endPoint, TcpTransportOptions? options = null)
   {
      _listeners.Add(new TcpNetworkListener(endPoint, options ?? new TcpTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified port using TCP with IPAddress.Any.
   /// </summary>
   /// <param name="port">The port to listen on.</param>
   /// <param name="options">Optional TCP transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseTcp(int port, TcpTransportOptions? options = null)
   {
      return UseTcp(new IPEndPoint(IPAddress.Any, port), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using WebSockets.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional WebSocket transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseWs(IPEndPoint endPoint, WsTransportOptions? options = null)
   {
      _listeners.Add(new WsNetworkListener(endPoint, options ?? new WsTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified port using WebSockets with IPAddress.Any.
   /// </summary>
   /// <param name="port">The port to listen on.</param>
   /// <param name="options">Optional WebSocket transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseWs(int port, WsTransportOptions? options = null)
   {
      return UseWs(new IPEndPoint(IPAddress.Any, port), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using QUIC.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional QUIC transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseQuic(IPEndPoint endPoint, QuicTransportOptions? options = null)
   {
      _listeners.Add(new QuicNetworkListener(endPoint, options ?? new QuicTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified port using QUIC with IPAddress.Any.
   /// </summary>
   /// <param name="port">The port to listen on.</param>
   /// <param name="options">Optional QUIC transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseQuic(int port, QuicTransportOptions? options = null)
   {
      return UseQuic(new IPEndPoint(IPAddress.Any, port), options);
   }
}
