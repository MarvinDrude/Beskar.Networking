using System.Buffers.Text;
using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Mqtt.Server.Options;

public sealed class MqttServerBuilder(MqttServerOptions? options = null)
{
   private readonly MqttServerOptions _options = options ?? new MqttServerOptions();
   private readonly List<INetworkListener> _listeners = [];

   private Action<MqttServer>? _registerClientIdGenerator;

   public MqttServer Build()
   {
      var server = new MqttServer([.. _listeners], _options);
      _registerClientIdGenerator?.Invoke(server);

      return server;
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using TCP.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional TCP transport options.</param>
   /// <returns>The builder instance.</returns>
   public MqttServerBuilder UseTcp(IPEndPoint endPoint, TcpTransportOptions? options = null)
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
   public MqttServerBuilder UseTcp(int port, TcpTransportOptions? options = null)
   {
      return UseTcp(new IPEndPoint(IPAddress.Any, port), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using WebSockets.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional WebSocket transport options.</param>
   /// <returns>The builder instance.</returns>
   public MqttServerBuilder UseWs(IPEndPoint endPoint, WsTransportOptions? options = null)
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
   public MqttServerBuilder UseWs(int port, WsTransportOptions? options = null)
   {
      return UseWs(new IPEndPoint(IPAddress.Any, port), options);
   }

   /// <summary>
   /// Configures the server to listen on the specified endpoint using QUIC.
   /// </summary>
   /// <param name="endPoint">The network endpoint to listen on.</param>
   /// <param name="options">Optional QUIC transport options.</param>
   /// <returns>The builder instance.</returns>
   public MqttServerBuilder UseQuic(IPEndPoint endPoint, QuicTransportOptions? options = null)
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
   public MqttServerBuilder UseQuic(int port, QuicTransportOptions? options = null)
   {
      return UseQuic(new IPEndPoint(IPAddress.Any, port), options);
   }

   /// <summary>
   /// If client connecting did not provide a client identifier, assigns a default one.
   /// The default is a GUID from the underlying transport session.
   /// <remarks>
   /// If you need your own generation logic. You will need to register your own <see cref="ServerEvents.OnConnectIntercept"/> event handler.
   /// </remarks>
   /// </summary>
   public MqttServerBuilder WithDefaultClientIdGenerator()
   {
      _registerClientIdGenerator = server =>
      {
         server.Events.OnConnectIntercept.Add((ctx, ct) =>
         {
            if (!ctx.ConnectOptions.ClientIdUtf8Bytes.IsEmpty) return;

            Span<byte> utf8Span = stackalloc byte[36];
            var success = Utf8Formatter.TryFormat(ctx.NetworkSession.Id, utf8Span, out var bytesWritten, 'D');

            if (success)
            {
               ctx.AssignedClientIdentifierUtf8Bytes = utf8Span.ToArray();
            }
         });
      };

      return this;
   }
}
