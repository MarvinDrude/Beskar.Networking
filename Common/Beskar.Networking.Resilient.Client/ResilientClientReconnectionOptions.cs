namespace Beskar.Networking.Resilient.Client;

/// <summary>
/// Represents the configuration options for client automatic reconnection behavior.
/// </summary>
public sealed class ResilientClientReconnectionOptions
{
   /// <summary>
   /// Gets or sets whether automatic reconnection should be attempted when the connection drops unexpectedly.
   /// Default is true.
   /// </summary>
   public bool AutoReconnect { get; set; } = true;

   /// <summary>
   /// Gets or sets the delay between reconnection attempts.
   /// Default is 3 seconds.
   /// </summary>
   public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(3);

   /// <summary>
   /// Gets or sets the maximum number of reconnection attempts before giving up.
   /// 0 means unlimited retries. Default is 0.
   /// </summary>
   public int MaxRetries { get; set; } = 0;
}
