using System.Buffers;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws.Enums;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// Represents options for configuring a WebSocket transport client or server.
/// </summary>
public sealed class WsTransportOptions
{
   /// <summary>
   /// The request path expected for the WebSocket connection (e.g. "/" or "/chat").
   /// Defaults to "/".
   /// </summary>
   public string Path { get; set; } = "/";

   /// <summary>
   /// The requested subprotocol during negotiation.
   /// </summary>
   public string? Subprotocol { get; set; }

   /// <summary>
   /// The WebSocket keep-alive ping interval.
   /// Defaults to 30 seconds.
   /// </summary>
   public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(30);

   /// <summary>
   /// The maximum allowed HTTP header size in bytes.
   /// Defaults to 8,192 (8 KB).
   /// </summary>
   public int MaxHeaderSize { get; set; } = 8192;

   /// <summary>
   /// The maximum allowed WebSocket frame payload size in bytes.
   /// Defaults to 4,194,304 (4 MB).
   /// </summary>
   public int MaxFrameSize { get; set; } = 4 * 1024 * 1024;

   /// <summary>
   /// The underlying TCP options used to establish socket connections, SSL, and connection pooling.
   /// </summary>
   public TcpTransportOptions TcpOptions { get; set; } = new();

   /// <summary>
   /// Restricts the time allowed for the initial HTTP Upgrade request to be received.
   /// Defaults to 10 seconds.
   /// </summary>
   public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);

   /// <summary>
   /// An array of allowed origins to validate in the HTTP header handshake.
   /// Set null or empty to allow all origins.
   /// </summary>
   public string[]? AllowedOrigins { get; set; }

   /// <summary>
   /// The Origin header to send during the client-side handshake.
   /// </summary>
   public string? Origin { get; set; }

   /// <summary>
   /// High-level frame-isolated message callback handler.
   /// Invoked per discrete WebSocket frame received from the client.
   /// </summary>
   public Action<WsNetworkSession, ReadOnlySequence<byte>, WebSocketOpcode>? OnMessage { get; set; }
}
