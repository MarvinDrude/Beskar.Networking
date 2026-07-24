namespace Beskar.Networking.Protocol;

/// <summary>
/// Classification of resilient protocol frame kinds used for connection handshakes,
/// keep-alives, and application messages.
/// </summary>
public enum ResilientFrameKind : byte
{
   /// <summary>
   /// Main application message or data payload frame.
   /// </summary>
   Message = 0,

   /// <summary>
   /// Initiates a connection handshake.
   /// </summary>
   Connect = 1,

   /// <summary>
   /// Authentication challenge or response during handshake.
   /// </summary>
   Authenticate = 2,

   /// <summary>
   /// Server acknowledgment of connection handshake.
   /// </summary>
   ConnectAcknowledged = 3,

   /// <summary>
   /// Graceful disconnect signal.
   /// </summary>
   Disconnect = 4,

   /// <summary>
   /// Keep-alive ping request.
   /// </summary>
   Ping = 5,

   /// <summary>
   /// Keep-alive pong response.
   /// </summary>
   Pong = 6
}
