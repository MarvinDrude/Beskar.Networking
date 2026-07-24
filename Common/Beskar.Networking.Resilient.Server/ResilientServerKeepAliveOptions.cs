using Beskar.Networking.Resilient.Common.Enums;

namespace Beskar.Networking.Resilient.Server;

/// <summary>
/// Represents the configuration options for the server keep-alive functionality in a resilient server implementation.
/// </summary>
public sealed class ResilientServerKeepAliveOptions
{
   /// <summary>
   /// How often the server should check for idle connections in a loop.
   /// </summary>
   public TimeSpan CheckInterval { get; set; } = TimeSpan.FromMilliseconds(1_000);

   /// <summary>
   /// The default time a connection should remain alive before being considered idle.
   /// </summary>
   public TimeSpan DefaultKeepAliveTime { get; set; } = TimeSpan.FromMinutes(1);

   /// <summary>
   /// The mode in which the server should handle keep-alive connections.
   /// </summary>
   public ResilientServerKeepAliveMode Mode { get; set; } = ResilientServerKeepAliveMode.ClientConfigured;
}
