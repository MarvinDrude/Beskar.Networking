using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Transports.Common.Options;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// Represents the options for the QUIC transport.
/// </summary>
public class QuicTransportOptions
{
   /// <summary>
   /// The Application-Layer Protocol Negotiation (ALPN) protocol string to negotiate.
   /// Both client and server must specify matching protocols.
   /// </summary>
   public string AlpnProtocol { get; set; } = "beskar-quic";

   /// <summary>
   /// The default stream error code used by the connection when a stream is closed abruptly.
   /// </summary>
   public long DefaultStreamErrorCode { get; set; } = 0;

   /// <summary>
   /// The default connection error code used when the connection is closed.
   /// </summary>
   public long DefaultCloseErrorCode { get; set; } = 0;

   /// <summary>
   /// The maximum number of concurrent bidirectional streams that the remote peer can open on this connection.
   /// </summary>
   public int MaxInboundBidirectionalStreams { get; set; } = 100;

   /// <summary>
   /// The maximum number of concurrent unidirectional streams that the remote peer can open on this connection.
   /// </summary>
   public int MaxInboundUnidirectionalStreams { get; set; } = 100;

   /// <summary>
   /// Optional keep-alive interval for the QUIC connection.
   /// </summary>
   public TimeSpan? KeepAliveInterval { get; set; }

   /// <summary>
   /// Custom SSL server authentication options. If not provided, a default self-signed developer certificate is generated automatically.
   /// </summary>
   public SslServerAuthenticationOptions? SslServerOptions { get; set; }

   /// <summary>
   /// Custom SSL client authentication options.
   /// </summary>
   public SslClientAuthenticationOptions? SslClientOptions { get; set; }

   /// <summary>
   /// Options for the underlying Stream connections wrapping the QUIC streams.
   /// </summary>
   public StreamTransportOptions StreamOptions { get; set; } = new();
}
