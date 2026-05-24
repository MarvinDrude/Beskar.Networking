using Beskar.Networking.Transports.Tcp;

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
   /// The underlying TCP options used to establish socket connections, SSL, and connection pooling.
   /// </summary>
   public TcpTransportOptions TcpOptions { get; set; } = new();
}
