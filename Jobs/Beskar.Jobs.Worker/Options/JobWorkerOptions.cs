using System.Net;
using Beskar.Jobs.Common.Serialization;

namespace Beskar.Jobs.Worker.Options;

/// <summary>
/// Configuration options for <see cref="JobWorker"/>.
/// </summary>
public sealed class JobWorkerOptions
{
   /// <summary>
   /// Unique identifier for this worker node within the cluster.
   /// </summary>
   public string WorkerId { get; set; } = $"worker-{Guid.NewGuid():N}";

   /// <summary>
   /// Target MQTT broker endpoint when connecting an owned <c>IMqttClient</c>.
   /// </summary>
   public IPEndPoint? EndPoint { get; set; }

   /// <summary>
   /// Queues this worker subscribes to for job dispatch. Defaults to <c>["default"]</c>.
   /// </summary>
   public List<string> Queues { get; set; } = ["default"];

   /// <summary>
   /// Maximum number of concurrent jobs this worker will execute in parallel.
   /// </summary>
   public int MaxConcurrency { get; set; } = Math.Max(1, Environment.ProcessorCount);

   /// <summary>
   /// Interval at which the worker publishes online capacity heartbeats to the broker. Defaults to 5 seconds.
   /// </summary>
   public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);

   /// <summary>
   /// MQTT Keep-Alive period in seconds. Defaults to 15 seconds.
   /// </summary>
   public ushort KeepAlivePeriodSeconds { get; set; } = 15;

   /// <summary>
   /// Serializer used for job arguments, results, and control payloads.
   /// </summary>
   public IJobPayloadSerializer Serializer { get; set; } = JsonJobPayloadSerializer.Default;
}
