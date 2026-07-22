namespace Beskar.Networking.Resilient.Common.Enums;

/// <summary>
/// Represents the state of a resilient client within the networking system.
/// </summary>
public enum ResilientClientState
{
   /// <summary>
   /// Indicates that the resilient client is in the process of establishing a connection to the target server or endpoint.
   /// </summary>
   Connecting,

   /// <summary>
   /// Indicates that the resilient client has successfully established a connection to the target server or endpoint.
   /// </summary>
   Connected,

   /// <summary>
   /// Indicates that the resilient client is in the process of terminating an existing connection to the target server or endpoint.
   /// </summary>
   Disconnecting,

   /// <summary>
   /// Indicates that the resilient client is not connected to the target server or endpoint.
   /// </summary>
   Disconnected
}
