using System.Net;
using Beskar.Jobs.Common.Serialization;

namespace Beskar.Jobs.Client.Options;

/// <summary>
/// Configuration options for <see cref="JobClient"/>.
/// </summary>
public sealed class JobClientOptions
{
   /// <summary>
   /// Unique MQTT client identifier for this job client.
   /// </summary>
   public string ClientId { get; set; } = $"beskar-job-client-{Guid.NewGuid():N}";

   /// <summary>
   /// Target MQTT broker endpoint when connecting an owned <c>IMqttClient</c>.
   /// </summary>
   public IPEndPoint? EndPoint { get; set; }

   /// <summary>
   /// Default timeout for RPC commands (such as <c>EnqueueAsync</c> and <c>ScheduleRecurringAsync</c>). Defaults to 10 seconds.
   /// </summary>
   public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(10);

   /// <summary>
   /// Serializer used for job arguments, results, and control packets.
   /// </summary>
   public IJobPayloadSerializer Serializer { get; set; } = JsonJobPayloadSerializer.Default;
}
