using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;
using Beskar.Networking.Transports.Memory;
using Beskar.Networking.Transports.NamedPipes;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Udp;
using Beskar.Networking.Transports.Uds;
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

   /// <summary>
   /// Configures the server to listen on the specified endpoint using Unix Domain Sockets (UDS).
   /// </summary>
   /// <param name="endPoint">The UnixDomainSocketEndPoint to listen on.</param>
   /// <param name="options">Optional UDS transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseUds(UnixDomainSocketEndPoint endPoint, UdsTransportOptions? options = null)
   {
      _listeners.Add(new UdsNetworkListener(endPoint, options ?? new UdsTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified socket path using Unix Domain Sockets (UDS).
   /// </summary>
   /// <param name="socketPath">The Unix Domain Socket path to listen on.</param>
   /// <param name="options">Optional UDS transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseUds(string socketPath, UdsTransportOptions? options = null)
   {
      return UseUds(new UnixDomainSocketEndPoint(socketPath), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using Named Pipes.
   /// </summary>
   /// <param name="endPoint">The NamedPipeEndPoint to listen on.</param>
   /// <param name="options">Optional Named Pipes transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseNamedPipes(NamedPipeEndPoint endPoint, NamedPipeTransportOptions? options = null)
   {
      _listeners.Add(new NamedPipeNetworkListener(endPoint, options ?? new NamedPipeTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified pipe name using Named Pipes.
   /// </summary>
   /// <param name="pipeName">The name of the pipe to listen on.</param>
   /// <param name="options">Optional Named Pipes transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseNamedPipes(string pipeName, NamedPipeTransportOptions? options = null)
   {
      return UseNamedPipes(new NamedPipeEndPoint(pipeName), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using in-memory transport.
   /// </summary>
   /// <param name="endPoint">The MemoryEndPoint to listen on.</param>
   /// <param name="options">Optional Memory transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseMemory(MemoryEndPoint endPoint, MemoryTransportOptions? options = null)
   {
      _listeners.Add(new MemoryNetworkListener(endPoint, options ?? new MemoryTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified address using in-memory transport.
   /// </summary>
   /// <param name="address">The in-memory address to listen on.</param>
   /// <param name="options">Optional Memory transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseMemory(string address, MemoryTransportOptions? options = null)
   {
      return UseMemory(new MemoryEndPoint(address), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using UDP.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional UDP transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseUdp(IPEndPoint endPoint, UdpTransportOptions? options = null)
   {
      _listeners.Add(new UdpNetworkListener(endPoint, options ?? new UdpTransportOptions()));
      return this;
   }

   /// <summary>
   /// Configures the server to listen on the specified port using UDP with IPAddress.Any.
   /// </summary>
   /// <param name="port">The port to listen on.</param>
   /// <param name="options">Optional UDP transport options.</param>
   /// <returns>The builder instance.</returns>
   public ResilientServerBuilder<TFrame> UseUdp(int port, UdpTransportOptions? options = null)
   {
      return UseUdp(new IPEndPoint(IPAddress.Any, port), options);
   }
}
