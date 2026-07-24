namespace Beskar.Networking.Resilient.Server;

/// <summary>
/// Configuration options for ResilientServer.
/// </summary>
public sealed class ResilientServerOptions
{
   /// <summary>
   /// Gets or sets the maximum number of concurrent client connections allowed.
   /// 0 means unlimited connections.
   /// </summary>
   public int MaxConnections { get; set; } = 0;

   /// <summary>
   /// Gets or sets whether the server is open to accepting new connections.
   /// </summary>
   public bool OpenToNewConnections { get; set; } = true;

   /// <summary>
   /// Gets the keep-alive options for managing idle client connections.
   /// </summary>
   public ResilientServerKeepAliveOptions KeepAlive { get; } = new();
}
