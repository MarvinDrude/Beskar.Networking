namespace Beskar.Networking.Abstractions.Enums;

/// <summary>
/// The transport kind used.
/// </summary>
public enum TransportKind
{
   /// <summary>
   /// Could not be determined.
   /// </summary>
   Unknown = 0,

   /// <summary>
   /// TCP - Transmission Control Protocol
   /// </summary>
   Tcp = 1,

   /// <summary>
   /// WebSocket - Web Socket Protocol
   /// </summary>
   WebSocket = 2,

   /// <summary>
   /// QUIC - Quick UDP Internet Connection
   /// </summary>
   Quic = 3,

   /// <summary>
   /// UDP - User Datagram Protocol
   /// </summary>
   Udp = 4
}
