using System.Diagnostics.Metrics;

namespace Beskar.Mqtt.Common.Telemetry;

/// <summary>
/// Contains System.Diagnostics.Metrics telemetry meters and instruments for MQTT client and broker operations.
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
   /// Current count of connected MQTT clients on broker.
   /// </summary>
   public static readonly UpDownCounter<long> ConnectedClients = Meter.CreateUpDownCounter<long>(
      "beskar.mqtt.server.clients.connected",
      "{client}",
      "Current count of connected MQTT clients on server.");

   /// <summary>
   /// Current count of active stored sessions on broker.
   /// </summary>
   public static readonly UpDownCounter<long> ActiveSessions = Meter.CreateUpDownCounter<long>(
      "beskar.mqtt.server.sessions.active",
      "{session}",
      "Current count of stored MQTT sessions on server.");

   /// <summary>
   /// Current count of active topic subscriptions on broker.
   /// </summary>
   public static readonly UpDownCounter<long> SubscriptionsActive = Meter.CreateUpDownCounter<long>(
      "beskar.mqtt.subscriptions.active",
      "{subscription}",
      "Current count of active topic subscriptions on server.");

   /// <summary>
   /// Current count of stored retained messages on broker.
   /// </summary>
   public static readonly UpDownCounter<long> RetainedMessagesActive = Meter.CreateUpDownCounter<long>(
      "beskar.mqtt.retained_messages.active",
      "{message}",
      "Current count of stored retained messages on server.");

   /// <summary>
   /// Total MQTT PUBLISH messages sent or received.
   /// </summary>
   public static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
      "beskar.mqtt.messages.published",
      "{message}",
      "Total MQTT PUBLISH messages sent or received.");

   /// <summary>
   /// Current count of QoS 1/2 messages awaiting ACK/COMPLETION.
   /// </summary>
   public static readonly UpDownCounter<long> QosInflightCount = Meter.CreateUpDownCounter<long>(
      "beskar.mqtt.qos.inflight",
      "{message}",
      "Current count of unacknowledged QoS 1/2 messages in-flight.");

   /// <summary>
   /// Total QoS 1/2 retransmissions.
   /// </summary>
   public static readonly Counter<long> QosRetries = Meter.CreateCounter<long>(
      "beskar.mqtt.qos.retries",
      "{retry}",
      "Total QoS 1/2 retransmission attempts.");

   /// <summary>
   /// Total topic alias cache hits during packet encoding/decoding.
   /// </summary>
   public static readonly Counter<long> TopicAliasHits = Meter.CreateCounter<long>(
      "beskar.mqtt.topic_alias.hits",
      "{hit}",
      "Total topic alias cache hits.");

   /// <summary>
   /// Total Last Will and Testament (LWT) messages triggered on unexpected client disconnects.
   /// </summary>
   public static readonly Counter<long> LastWillTriggered = Meter.CreateCounter<long>(
      "beskar.mqtt.last_will.triggered",
      "{will}",
      "Total Last Will and Testament (LWT) messages published.");

   private static readonly KeyValuePair<string, object?>[] TagInbound = [new KeyValuePair<string, object?>("direction", "inbound")];
   private static readonly KeyValuePair<string, object?>[] TagOutbound = [new KeyValuePair<string, object?>("direction", "outbound")];

   public static void RecordClientConnectedChange(int delta)
   {
      if (ConnectedClients.Enabled && delta != 0)
      {
         ConnectedClients.Add(delta);
      }
   }

   public static void RecordActiveSessionChange(int delta)
   {
      if (ActiveSessions.Enabled && delta != 0)
      {
         ActiveSessions.Add(delta);
      }
   }

   public static void RecordSubscriptionChange(int delta)
   {
      if (SubscriptionsActive.Enabled && delta != 0)
      {
         SubscriptionsActive.Add(delta);
      }
   }

   public static void RecordRetainedMessageChange(int delta)
   {
      if (RetainedMessagesActive.Enabled && delta != 0)
      {
         RetainedMessagesActive.Add(delta);
      }
   }

   public static void RecordPublished(bool isInbound, int qos, bool isRetained)
   {
      if (MessagesPublished.Enabled)
      {
         var tags = isInbound ? TagInbound : TagOutbound;
         MessagesPublished.Add(1, tags);
      }
   }

   public static void RecordQosInflightChange(int delta)
   {
      if (QosInflightCount.Enabled && delta != 0)
      {
         QosInflightCount.Add(delta);
      }
   }

   public static void RecordQosRetry()
   {
      if (QosRetries.Enabled)
      {
         QosRetries.Add(1);
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
