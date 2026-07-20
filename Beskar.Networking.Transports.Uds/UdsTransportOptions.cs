using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Options;

namespace Beskar.Networking.Transports.Uds;

/// <summary>
/// Represents the options for a Unix Domain Sockets (UDS) transport.
/// </summary>
public class UdsTransportOptions
{
   /// <summary>
   /// The options for the underlying socket transport.
   /// </summary>
   public SocketTransportOptions SocketOptions { get; set; } = new();

   /// <summary>
   /// The number of IO queues for the transport.
   /// </summary>
   public int IoQueueCount => SocketOptions.IoQueueCount;

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
   /// The maximum number of concurrent client connections/handshakes allowed. Defaults to 512.
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
}
