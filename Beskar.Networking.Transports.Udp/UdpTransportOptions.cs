using System.Net.Sockets;

namespace Beskar.Networking.Transports.Udp;

/// <summary>
/// Represents the options for a UDP transport.
/// </summary>
public class UdpTransportOptions
{
   /// <summary>
   /// The maximum payload size of a single UDP packet. Defaults to 1400 bytes to avoid fragmentation.
   /// </summary>
   public int MaxPacketSize { get; set; } = 1400;

   /// <summary>
   /// The timeout duration for a server-side UDP session to be considered idle and disconnected.
   /// Defaults to 30 seconds.
   /// </summary>
   public TimeSpan ClientSessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

   /// <summary>
   /// The maximum number of pending connections that can be queued in the listener's session channel.
   /// Defaults to 1024.
   /// </summary>
   public int MaxPendingConnections { get; set; } = 1024;

   /// <summary>
   /// The socket send buffer size in bytes. Set null to use OS default. Defaults to 512 KB.
   /// </summary>
   public int? SendBufferSize { get; set; } = 512 * 1024;

   /// <summary>
   /// The socket receive buffer size in bytes. Set null to use OS default. Defaults to 512 KB.
   /// </summary>
   public int? ReceiveBufferSize { get; set; } = 512 * 1024;
}
