namespace Beskar.Networking.Resilient.Client;

/// <summary>
/// Represents the configuration options for client keep-alive pings.
/// </summary>
public sealed class ResilientClientKeepAliveOptions
{
   /// <summary>
   /// Gets or sets whether automatic keep-alive ping frames should be sent to the server.
   /// </summary>
   public bool Enabled { get; set; } = true;

   /// <summary>
   /// How often ping frames should be sent if no activity occurs.
   /// Defaults to 30 seconds.
   /// </summary>
   public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(30);

   /// <summary>
   /// How long to wait for a Pong response before considering the connection lost.
   /// Defaults to 10 seconds.
   /// </summary>
   public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);
}
