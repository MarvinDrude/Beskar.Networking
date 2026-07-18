using System.Text;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.ChaosSimulator;

public static class ClientBehaviors
{
   public static async Task ExecutePublisherBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      var pubTopics = new[] { "sensors/temp", "sensors/humidity", "qos/level/0", "qos/level/1", "qos/level/2", "iot/device/123/telemetry" };
      var pubCount = Random.Shared.Next(5, 12);

      for (var i = 0; i < pubCount && !ct.IsCancellationRequested; i++)
      {
         var topic = pubTopics[Random.Shared.Next(pubTopics.Length)];
         var qos = (QualityOfServiceType)Random.Shared.Next(3); // QoS 0, 1, or 2
         var payload = $"payload-{qos}-value-{Random.Shared.Next(100)}";

         var publishOptions = PublishOptions.Create()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfService(qos)
            .Build();

         Program.LogChaos("CLIENT", transport, version, "PUBLISH", $"Client '{clientId}' publishing to '{topic}' (QoS {qos}).", ConsoleColor.Yellow);

         var result = await client.PublishAsync(publishOptions, ct);

         if (!result.Failed)
         {
            Interlocked.Increment(ref Program.ClientPublishesSent);
            if (qos == QualityOfServiceType.AtMostOnce)
            {
               Interlocked.Increment(ref Program.ServerPublishesQoS0);
               Interlocked.Increment(ref Program.ServerPublishesTotal);
            }
         }
         else
         {
            Interlocked.Increment(ref Program.ClientPublishesFailed);
            Program.LogChaos("CLIENT", transport, version, "PUB_ERR", $"Client '{clientId}' publish failed: {result.Error.Detail}", ConsoleColor.DarkRed);
         }

         await Task.Delay(Random.Shared.Next(800, 2000), ct);
      }
   }

   public static async Task ExecuteSubscriberBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      // Add receive handler
      using var receiveToken = client.AddMessageReceiveHandler((context, token) =>
      {
         Interlocked.Increment(ref Program.ClientMessagesReceived);
         var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
         Program.LogChaos("CLIENT", transport, version, "RECEIVE", $"Client '{clientId}' received message on '{context.Message.Topic}': '{payload}'", ConsoleColor.Green);
         return ValueTask.CompletedTask;
      });

      // Subscribe to topics
      var topicFilter = Random.Shared.Next(3) switch
      {
         0 => "sensors/#",
         1 => "qos/level/+",
         _ => "iot/device/+/telemetry"
      };

      var subscribeOptions = SubscribeOptions.Create()
         .WithTopicFilter(topicFilter, QualityOfServiceType.ExactlyOnce)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "SUB", $"Client '{clientId}' subscribing to '{topicFilter}'...", ConsoleColor.Cyan);
      var subResult = await client.SubscribeAsync(subscribeOptions, ct);

      if (subResult.Failed)
      {
         Program.LogChaos("CLIENT", transport, version, "SUB_ERR", $"Client '{clientId}' subscription failed: {subResult.Error.Detail}", ConsoleColor.Red);
         return;
      }

      // Remain connected to handle incoming publishes
      var duration = TimeSpan.FromSeconds(Random.Shared.Next(15, 30));
      await Task.Delay(duration, ct);

      // Unsubscribe before exiting
      var unsubscribeOptions = UnsubscribeOptions.Create()
         .WithTopicFilter(topicFilter)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "UNSUB", $"Client '{clientId}' unsubscribing from '{topicFilter}'...", ConsoleColor.Cyan);
      try
      {
         await client.UnsubscribeAsync(unsubscribeOptions, ct);
      }
      catch { /* Ignored */ }
   }

   public static async Task ExecuteKeepAliveBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      Program.LogChaos("CLIENT", transport, version, "IDLE", $"Client '{clientId}' registered with low keep-alive (5s). Monitoring pingreqs...", ConsoleColor.DarkCyan);

      var duration = TimeSpan.FromSeconds(Random.Shared.Next(15, 25));
      var end = DateTimeOffset.UtcNow + duration;

      while (DateTimeOffset.UtcNow < end && !ct.IsCancellationRequested)
      {
         await Task.Delay(1000, ct);
         Interlocked.Increment(ref Program.ClientPingsSent);
      }
   }

   public static async Task ExecuteFlakyBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      var publishOptions = PublishOptions.Create()
         .WithTopic("sensors/temp")
         .WithPayload("FLAKY-BURST-DATA")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "FLAKY_PUB", $"Client '{clientId}' publishing quick payload...", ConsoleColor.Yellow);
      await client.PublishAsync(publishOptions, ct);
      Interlocked.Increment(ref Program.ClientPublishesSent);

      await Task.Delay(Random.Shared.Next(100, 500), ct);
   }
}
