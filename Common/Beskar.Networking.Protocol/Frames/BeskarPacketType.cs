namespace Beskar.Networking.Protocol.Frames;

/// <summary>
/// All packet types of the default Beskar.Networking.Resilient protocol.
/// </summary>
public enum BeskarPacketType : ushort
{
   /// <summary>
   /// Sent by the client to initiate a connection.
   /// </summary>
   Connect,
   /// <summary>
   /// Can be used by server and client to challenge each other.
   /// </summary>
   Authenticate,
   /// <summary>
   /// Returned by the server to acknowledge a connection after connect and/or authenticate.
   /// </summary>
   ConnectAcknowledged,
   /// <summary>
   /// Sent by server to disconnect the client, or by the client to disconnect from the server gracefully.
   /// </summary>
   Disconnect,

   /// <summary>
   /// A ping request is only sent by the client.
   /// </summary>
   Ping,
   /// <summary>
   /// A pong response is only sent by the server upon receiving a ping request.
   /// </summary>
   Pong,

   /// <summary>
   /// Sending main application messages and data payloads.
   /// </summary>
   Message
}
