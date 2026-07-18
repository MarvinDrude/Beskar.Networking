using System.Text;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;

namespace Beskar.Mqtt.ChaosSimulator;

public static class ClientBehaviors
{
   public static async Task ExecutePublisherBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      var pubTopics = new[]
      {
         "sensors/temp", "sensors/humidity", "qos/level/0", "qos/level/1", "qos/level/2", "iot/device/123/telemetry"
      };
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

         Program.LogChaos("CLIENT", transport, version, "PUBLISH",
            $"Client '{clientId}' publishing to '{topic}' (QoS {qos}).", ConsoleColor.Yellow);

         var result = await PublishWithTimeoutAsync(client, publishOptions, ct);

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
            Program.LogChaos("CLIENT", transport, version, "PUB_ERR",
               $"Client '{clientId}' publish failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
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
         Program.LogChaos("CLIENT", transport, version, "RECEIVE",
            $"Client '{clientId}' received message on '{context.Message.Topic}': '{payload}'", ConsoleColor.Green);
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

      Program.LogChaos("CLIENT", transport, version, "SUB", $"Client '{clientId}' subscribing to '{topicFilter}'...",
         ConsoleColor.Cyan);
      var subResult = await SubscribeWithTimeoutAsync(client, subscribeOptions, ct);

      if (subResult.Failed)
      {
         Program.LogChaos("CLIENT", transport, version, "SUB_ERR",
            $"Client '{clientId}' subscription failed: {subResult.Error.Detail}", ConsoleColor.Red, true);
         return;
      }

      // Remain connected to handle incoming publishes
      var duration = TimeSpan.FromSeconds(Random.Shared.Next(15, 30));
      await Task.Delay(duration, ct);

      // Unsubscribe before exiting
      var unsubscribeOptions = UnsubscribeOptions.Create()
         .WithTopicFilter(topicFilter)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "UNSUB",
         $"Client '{clientId}' unsubscribing from '{topicFilter}'...", ConsoleColor.Cyan);
      
      await UnsubscribeWithTimeoutAsync(client, unsubscribeOptions, ct);
   }

   public static async Task ExecuteKeepAliveBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      Program.LogChaos("CLIENT", transport, version, "IDLE",
         $"Client '{clientId}' registered with low keep-alive (5s). Monitoring pingreqs...", ConsoleColor.DarkCyan);

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

      Program.LogChaos("CLIENT", transport, version, "FLAKY_PUB", $"Client '{clientId}' publishing quick payload...",
         ConsoleColor.Yellow);
      
      var result = await PublishWithTimeoutAsync(client, publishOptions, ct);
      if (!result.Failed)
      {
         Interlocked.Increment(ref Program.ClientPublishesSent);
      }
      else
      {
         Interlocked.Increment(ref Program.ClientPublishesFailed);
         Program.LogChaos("CLIENT", transport, version, "PUB_ERR",
            $"Client '{clientId}' flaky publish failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
      }

      await Task.Delay(Random.Shared.Next(100, 500), ct);
   }

   public static async Task ExecuteSlowSubscriberBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      using var receiveToken = client.AddMessageReceiveHandler(async (context, token) =>
      {
         Interlocked.Increment(ref Program.ClientMessagesReceived);
         var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
         Program.LogChaos("CLIENT", transport, version, "SLOW_RCV_START",
            $"Client '{clientId}' starting slow processing...", ConsoleColor.DarkGreen);
         try
         {
            await Task.Delay(1500, token); // simulate slow message processing
         }
         catch (OperationCanceledException)
         {
         }

         Program.LogChaos("CLIENT", transport, version, "SLOW_RCV_END",
            $"Client '{clientId}' finished slow processing of message on '{context.Message.Topic}'",
            ConsoleColor.Green);
      });

      var subscribeOptions = SubscribeOptions.Create()
         .WithTopicFilter("sensors/+", QualityOfServiceType.AtLeastOnce)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "SUB_SLOW",
         $"Client '{clientId}' subscribing to 'sensors/+' as a slow subscriber...", ConsoleColor.Cyan);
      var subResult = await SubscribeWithTimeoutAsync(client, subscribeOptions, ct);

      if (subResult.Failed)
      {
         Program.LogChaos("CLIENT", transport, version, "SUB_ERR",
            $"Client '{clientId}' slow subscription failed: {subResult.Error.Detail}", ConsoleColor.Red, true);
         return;
      }

      await Task.Delay(TimeSpan.FromSeconds(20), ct);
   }

   public static async Task ExecuteQos2HeavyPublisherBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      var pubCount = Random.Shared.Next(8, 15);
      var pubOptions = PublishOptions.Create()
         .WithTopic("qos/heavy/2")
         .WithPayload("QOS-2-HEAVY-PAYLOAD-DATA")
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .Build();

      for (var i = 0; i < pubCount && !ct.IsCancellationRequested; i++)
      {
         Program.LogChaos("CLIENT", transport, version, "PUB_Q2", $"Client '{clientId}' publishing QoS 2 message...",
            ConsoleColor.Yellow);
         var result = await PublishWithTimeoutAsync(client, pubOptions, ct);
         if (!result.Failed)
         {
            Interlocked.Increment(ref Program.ClientPublishesSent);
         }
         else
         {
            Interlocked.Increment(ref Program.ClientPublishesFailed);
            Program.LogChaos("CLIENT", transport, version, "PUB_ERR",
               $"Client '{clientId}' QoS 2 publish failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
         }

         await Task.Delay(Random.Shared.Next(400, 1000), ct);
      }
   }

   public static async Task ExecuteWildcardSubscriberBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      using var receiveToken = client.AddMessageReceiveHandler((context, token) =>
      {
         Interlocked.Increment(ref Program.ClientMessagesReceived);
         var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
         Program.LogChaos("CLIENT", transport, version, "WILD_RCV",
            $"Wildcard Client '{clientId}' received message on '{context.Message.Topic}' ({payload.Length} bytes)",
            ConsoleColor.Green);
         return ValueTask.CompletedTask;
      });

      var subscribeOptions = SubscribeOptions.Create()
         .WithTopicFilter("#", QualityOfServiceType.AtMostOnce)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "SUB_WILD", $"Client '{clientId}' subscribing to wildcard '#'...",
         ConsoleColor.Cyan);
      var subResult = await SubscribeWithTimeoutAsync(client, subscribeOptions, ct);

      if (subResult.Failed)
      {
         Program.LogChaos("CLIENT", transport, version, "SUB_ERR",
            $"Client '{clientId}' wildcard subscription failed: {subResult.Error.Detail}", ConsoleColor.Red, true);
         return;
      }

      await Task.Delay(TimeSpan.FromSeconds(25), ct);
   }

   public static async Task ExecuteAuthAlternatorBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      var publishOptions = PublishOptions.Create()
         .WithTopic("sensors/auth")
         .WithPayload("AUTH-ALTERNATE-DATA")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();

      Program.LogChaos("CLIENT", transport, version, "AUTH_ALT",
         $"Client '{clientId}' publishing auth-alternator payload...", ConsoleColor.Yellow);
      
      var result = await PublishWithTimeoutAsync(client, publishOptions, ct);
      if (!result.Failed)
      {
         Interlocked.Increment(ref Program.ClientPublishesSent);
      }
      else
      {
         Interlocked.Increment(ref Program.ClientPublishesFailed);
         Program.LogChaos("CLIENT", transport, version, "PUB_ERR",
            $"Client '{clientId}' auth alternator publish failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
      }
      
      await Task.Delay(Random.Shared.Next(100, 300), ct);
   }

   public static async Task ExecuteChannelCongestorBehaviorAsync(
      IMqttClient client, string clientId, string transport, string version, CancellationToken ct)
   {
      Program.LogChaos("CLIENT", transport, version, "CONGEST",
         $"Client '{clientId}' starting high-speed QoS 0 publish congestor...", ConsoleColor.Yellow);
      var end = DateTimeOffset.UtcNow.AddSeconds(10);
      var pubOptions = PublishOptions.Create()
         .WithTopic("congested/traffic")
         .WithPayload("CONGESTION-FIREHOSE-DATA-PACKET-12345")
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .Build();

      while (DateTimeOffset.UtcNow < end && !ct.IsCancellationRequested)
      {
         var result = await PublishWithTimeoutAsync(client, pubOptions, ct);
         if (!result.Failed)
         {
            Interlocked.Increment(ref Program.ClientPublishesSent);
            Interlocked.Increment(ref Program.ServerPublishesQoS0);
            Interlocked.Increment(ref Program.ServerPublishesTotal);
         }
         else
         {
            Interlocked.Increment(ref Program.ClientPublishesFailed);
            Program.LogChaos("CLIENT", transport, version, "PUB_ERR",
               $"Client '{clientId}' congestor publish failed: {result.Error.Detail}", ConsoleColor.DarkRed, true);
         }

         await Task.Delay(50, ct);
      }
   }

   private static async Task<Result<PublishResult, StringError>> PublishWithTimeoutAsync(
      IMqttClient client, PublishOptions options, CancellationToken ct)
   {
      using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      cts.CancelAfter(TimeSpan.FromSeconds(5));
      try
      {
         return await client.PublishAsync(options, cts.Token);
      }
      catch (Exception ex)
      {
         return new StringError($"Publish failed or timed out: {ex.Message}");
      }
   }

   private static async Task<Result<SubscribeResult, StringError>> SubscribeWithTimeoutAsync(
      IMqttClient client, SubscribeOptions options, CancellationToken ct)
   {
      using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      cts.CancelAfter(TimeSpan.FromSeconds(5));
      try
      {
         return await client.SubscribeAsync(options, cts.Token);
      }
      catch (Exception ex)
      {
         return new StringError($"Subscribe failed or timed out: {ex.Message}");
      }
   }

   private static async Task<Result<UnsubscribeResult, StringError>> UnsubscribeWithTimeoutAsync(
      IMqttClient client, UnsubscribeOptions options, CancellationToken ct)
   {
      using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      cts.CancelAfter(TimeSpan.FromSeconds(5));
      try
      {
         return await client.UnsubscribeAsync(options, cts.Token);
      }
      catch (Exception ex)
      {
         return new StringError($"Unsubscribe failed or timed out: {ex.Message}");
      }
   }
}
