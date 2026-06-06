namespace Beskar.Networking.Abstractions.Managed;

/// <summary>
/// Represents the connection state of the managed network client.
/// </summary>
public enum ConnectionState
{
   /// <summary>
   /// The client is not connected and no reconnect is in progress.
   /// </summary>
   Disconnected,

   /// <summary>
   /// The client is attempting to connect to the remote endpoint.
   /// </summary>
   Connecting,

   /// <summary>
   /// The client is successfully connected to the remote endpoint.
   /// </summary>
   Connected,

   /// <summary>
   /// The connection was lost, and the client is in the process of reconnecting.
   /// </summary>
   Reconnecting,

   /// <summary>
   /// The client failed to connect after the configured maximum retry attempts.
   /// </summary>
   Failed
}
