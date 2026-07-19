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
   /// The socket send buffer size in bytes. Set null to use OS default. Defaults to 8 MB.
   /// </summary>
   public int? SendBufferSize { get; set; } = 8 * 1024 * 1024;

   /// <summary>
   /// The socket receive buffer size in bytes. Set null to use OS default. Defaults to 8 MB.
   /// </summary>
   public int? ReceiveBufferSize { get; set; } = 8 * 1024 * 1024;

   /// <summary>
   /// The pause threshold in bytes for the incoming session pipe.
   /// Defaults to 1 MB.
   /// </summary>
   public long IncomingPipePauseThreshold { get; set; } = 1024 * 1024;

   /// <summary>
   /// The resume threshold in bytes for the incoming session pipe.
   /// Defaults to 512 KB.
   /// </summary>
   public long IncomingPipeResumeThreshold { get; set; } = 512 * 1024;

   /// <summary>
   /// The pause threshold in bytes for the outgoing session pipe.
   /// Defaults to 1 MB.
   /// </summary>
   public long OutgoingPipePauseThreshold { get; set; } = 1024 * 1024;

   /// <summary>
   /// The resume threshold in bytes for the outgoing session pipe.
   /// Defaults to 512 KB.
   /// </summary>
   public long OutgoingPipeResumeThreshold { get; set; } = 512 * 1024;
}
