using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
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

   /// <summary>
   /// The maximum length of the pending connections queue for the listener socket.
   /// Defaults to 1024.
   /// </summary>
   public int Backlog { get; set; } = 1024;

   /// <summary>
   /// Controls the socket behavior upon closure if unsent data exists in the socket send buffer.
   /// Set null to use the OS default.
   /// </summary>
   public LingerOption? LingerState { get; set; }

   /// <summary>
   /// The handshake timeout for SSL/TLS connections on both client and server.
   /// Defaults to 10 seconds.
   /// </summary>
   public TimeSpan SslHandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);

   /// <summary>
   /// Whether to enable TCP keep-alive probes. Defaults to false.
   /// </summary>
   public bool KeepAlive { get; set; }

   /// <summary>
   /// The number of seconds a connection will remain idle before the first keep-alive probe is sent.
   /// Set null to use the OS default.
   /// </summary>
   public int? KeepAliveTime { get; set; }

   /// <summary>
   /// The number of seconds between subsequent keep-alive probes if no acknowledgment is received.
   /// Set null to use the OS default.
   /// </summary>
   public int? KeepAliveInterval { get; set; }

   /// <summary>
   /// The number of keep-alive probes to send before the connection is declared dead.
   /// Set null to use the OS default.
   /// </summary>
   public int? KeepAliveRetryCount { get; set; }

   /// <summary>
   /// Whether a client certificate is required for SSL/TLS connections.
   /// If configured, overrides SslServerOptions.ClientCertificateRequired.
   /// </summary>
   public bool? ClientCertificateRequired { get; set; }

   /// <summary>
   /// A custom callback to validate client certificates.
   /// If configured, overrides SslServerOptions.RemoteCertificateValidationCallback.
   /// </summary>
   public RemoteCertificateValidationCallback? ClientCertificateValidationCallback { get; set; }

   /// <summary>
   /// The certificate revocation mode for client certificate validation.
   /// If configured, overrides SslServerOptions.CertificateRevocationMode.
   /// </summary>
   public X509RevocationMode? ClientCertificateRevocationMode { get; set; }

   /// <summary>
   /// Configures standard TCP socket options on the specified socket.
   /// </summary>
   /// <param name="socket">The socket to configure.</param>
   public void ConfigureSocket(Socket socket)
   {
      if (NoDelay)
      {
         socket.NoDelay = true;
      }
      if (SendBufferSize.HasValue)
      {
         socket.SendBufferSize = SendBufferSize.Value;
      }
      if (ReceiveBufferSize.HasValue)
      {
         socket.ReceiveBufferSize = ReceiveBufferSize.Value;
      }
      if (LingerState is not null)
      {
         socket.LingerState = LingerState;
      }

      if (KeepAlive || KeepAliveTime.HasValue || KeepAliveInterval.HasValue || KeepAliveRetryCount.HasValue)
      {
         socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

         if (KeepAliveTime.HasValue)
         {
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, KeepAliveTime.Value);
         }

         if (KeepAliveInterval.HasValue)
         {
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, KeepAliveInterval.Value);
         }

         if (KeepAliveRetryCount.HasValue)
         {
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, KeepAliveRetryCount.Value);
         }
      }
   }
}
