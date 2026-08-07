using System.Diagnostics.Metrics;

namespace Beskar.Mqtt.Common.Telemetry;

/// <summary>
/// Contains System.Diagnostics.Metrics telemetry meters and instruments for MQTT client and broker/server.
/// </summary>
public static class MqttMetrics
{
   /// <summary>
   /// The meter name for MQTT metrics.
   /// </summary>
   public const string MeterName = "Beskar.Mqtt";

   /// <summary>
   /// Meter instance for Beskar MQTT metrics.
   /// </summary>
   public static readonly Meter Meter = new(MeterName, "1.0.0");

   /// <summary>
   /// Total number of PUBLISH packets processed by broker or client.
   /// </summary>
   public static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
      "beskar.mqtt.messages.published",
      "{message}",
      "Total MQTT PUBLISH packets sent or received.");

   /// <summary>
   /// In-flight QoS 1/2 packets waiting for acknowledgment.
   /// </summary>
   public static readonly UpDownCounter<long> QosInflightCount = Meter.CreateUpDownCounter<long>(
      "beskar.mqtt.qos.inflight.count",
      "{packet}",
      "Number of in-flight QoS 1/2 packets waiting for acknowledgment.");

   /// <summary>
   /// Total QoS 1/2 retransmissions triggered.
   /// </summary>
   public static readonly Counter<long> QosRetries = Meter.CreateCounter<long>(
      "beskar.mqtt.qos.retries",
      "{retry}",
      "Total QoS 1/2 retransmission attempts.");

   /// <summary>
   /// Total PUBLISH packets utilizing topic alias compression.
   /// </summary>
   public static readonly Counter<long> TopicAliasHits = Meter.CreateCounter<long>(
      "beskar.mqtt.topic_alias.hits",
      "{hit}",
      "Total PUBLISH packets using Topic Alias optimization.");

   /// <summary>
   /// Total Last Will and Testament (LWT) messages dispatched.
   /// </summary>
   public static readonly Counter<long> LastWillTriggered = Meter.CreateCounter<long>(
      "beskar.mqtt.last_will.triggered",
      "{will}",
      "Total Last Will messages triggered by ungraceful client disconnects.");

   private static readonly KeyValuePair<string, object?>[][][] PublishTags = [
      // Outbound (isInbound = false)
      [
         [new KeyValuePair<string, object?>("direction", "outbound"), new KeyValuePair<string, object?>("qos", 0), new KeyValuePair<string, object?>("retained", false)],
         [new KeyValuePair<string, object?>("direction", "outbound"), new KeyValuePair<string, object?>("qos", 0), new KeyValuePair<string, object?>("retained", true)]
      ],
      [
         [new KeyValuePair<string, object?>("direction", "outbound"), new KeyValuePair<string, object?>("qos", 1), new KeyValuePair<string, object?>("retained", false)],
         [new KeyValuePair<string, object?>("direction", "outbound"), new KeyValuePair<string, object?>("qos", 1), new KeyValuePair<string, object?>("retained", true)]
      ],
      [
         [new KeyValuePair<string, object?>("direction", "outbound"), new KeyValuePair<string, object?>("qos", 2), new KeyValuePair<string, object?>("retained", false)],
         [new KeyValuePair<string, object?>("direction", "outbound"), new KeyValuePair<string, object?>("qos", 2), new KeyValuePair<string, object?>("retained", true)]
      ],
      // Inbound (isInbound = true)
      [
         [new KeyValuePair<string, object?>("direction", "inbound"), new KeyValuePair<string, object?>("qos", 0), new KeyValuePair<string, object?>("retained", false)],
         [new KeyValuePair<string, object?>("direction", "inbound"), new KeyValuePair<string, object?>("qos", 0), new KeyValuePair<string, object?>("retained", true)]
      ],
      [
         [new KeyValuePair<string, object?>("direction", "inbound"), new KeyValuePair<string, object?>("qos", 1), new KeyValuePair<string, object?>("retained", false)],
         [new KeyValuePair<string, object?>("direction", "inbound"), new KeyValuePair<string, object?>("qos", 1), new KeyValuePair<string, object?>("retained", true)]
      ],
      [
         [new KeyValuePair<string, object?>("direction", "inbound"), new KeyValuePair<string, object?>("qos", 2), new KeyValuePair<string, object?>("retained", false)],
         [new KeyValuePair<string, object?>("direction", "inbound"), new KeyValuePair<string, object?>("qos", 2), new KeyValuePair<string, object?>("retained", true)]
      ]
   ];

   public static void RecordPublished(bool isInbound, int qos, bool isRetained)
   {
      if (MessagesPublished.Enabled)
      {
         var qosIndex = Math.Clamp(qos, 0, 2);
         var groupIndex = isInbound ? 3 + qosIndex : qosIndex;
         var retainedIndex = isRetained ? 1 : 0;
         MessagesPublished.Add(1, PublishTags[groupIndex][retainedIndex]);
      }
   }

   public static void RecordQosInflightChange(int delta, int qos)
   {
      if (QosInflightCount.Enabled)
      {
         QosInflightCount.Add(delta, new KeyValuePair<string, object?>("qos", qos));
      }
   }

   public static void RecordQosRetry(int qos)
   {
      if (QosRetries.Enabled)
      {
         QosRetries.Add(1, new KeyValuePair<string, object?>("qos", qos));
      }
   }

   public static void RecordTopicAliasHit()
   {
      if (TopicAliasHits.Enabled)
      {
         TopicAliasHits.Add(1);
      }
   }

   public static void RecordLastWillTriggered()
   {
      if (LastWillTriggered.Enabled)
      {
         LastWillTriggered.Add(1);
      }
   }
}
