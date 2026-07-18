using System.Net.Security;
using Beskar.Networking.Transports.Common.Options;

namespace Beskar.Networking.Transports.Tcp;

/// <summary>
/// Represents the options for a TCP transport.
/// </summary>
public class TcpTransportOptions
{
   /// <summary>
   /// Whether to use SSL for the transport.
   /// </summary>
   public bool UseSsl { get; set; }

   /// <summary>
   /// Whether to force the transport to use stream-based communication.
   /// </summary>
   public bool ForceStreamBased { get; set; }

   /// <summary>
   /// The options for the underlying socket transport.
   /// </summary>
   public SocketTransportOptions SocketOptions { get; set; } = new ();

   /// <summary>
   /// The options for the underlying stream transport.
   /// </summary>
   public StreamTransportOptions StreamOptions { get; set; } = new ();

   /// <summary>
   /// The SSL options for the transport.
   /// </summary>
   public SslServerAuthenticationOptions? SslServerOptions { get; set; }

   /// <summary>
   /// The SSL client options for the transport.
   /// </summary>
   public SslClientAuthenticationOptions? SslClientOptions { get; set; }

   /// <summary>
   /// Whether the transport is using stream-based communication.
   /// </summary>
   public bool IsStreamBased => ForceStreamBased || UseSsl;

   /// <summary>
   /// The number of IO queues for the transport.
   /// </summary>
   public int IoQueueCount => IsStreamBased
      ? StreamOptions.IoQueueCount
      : SocketOptions.IoQueueCount;

   /// <summary>
   /// Whether to disable Nagle's algorithm (set NoDelay to true). Defaults to true for performance.
   /// </summary>
   public bool NoDelay { get; set; } = true;

   /// <summary>
   /// The socket send buffer size in bytes. Set null to use OS default. Defaults to 512 KB.
   /// </summary>
   public int? SendBufferSize { get; set; } = 512 * 1024;

   /// <summary>
   /// The socket receive buffer size in bytes. Set null to use OS default. Defaults to 512 KB.
   /// </summary>
   public int? ReceiveBufferSize { get; set; } = 512 * 1024;

   /// <summary>
   /// The delay in milliseconds to wait before retrying to accept a new connection
   /// after an accept exception occurs to prevent CPU busy spinning. Defaults to 10ms.
   /// </summary>
   public int AcceptExceptionDelay { get; set; } = 10;

   /// <summary>
   /// The maximum number of concurrent client handshakes allowed. Defaults to 512.
   /// </summary>
   public int MaxConcurrentHandshakes { get; set; } = 512;

   /// <summary>
   /// The maximum number of pending connections that can be queued in the listener's session channel. Defaults to 1024.
   /// </summary>
   public int MaxPendingConnections { get; set; } = 1024;
}
