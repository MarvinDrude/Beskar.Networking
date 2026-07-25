namespace Beskar.Networking.Resilient.Common.Enums;

/// <summary>
/// Defines the various states of a resilient server during its lifecycle.
/// </summary>
public enum ResilientServerState : byte
{
   /// <summary>
   /// Indicates that the resilient server is in the process of starting up.
   /// </summary>
   Starting,

   /// <summary>
   /// Represents the state where the resilient server is actively running and operational.
   /// </summary>
   Running,

   /// <summary>
   /// Represents the state where the resilient server is in the process of shutting down.
   /// </summary>
   Stopping,

   /// <summary>
   /// Represents the state where the resilient server has completely stopped its operations.
   /// </summary>
   Stopped
}
