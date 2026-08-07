using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttBugFixesTests
{
   [Test]
   public async Task Client_Disconnect_WithThrowingEventHandler_DoesNotThrowUnhandledException()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;

         var client = MqttClientFactory.CreateTcp();
         client.AddDisconnectedHandler((ctx, ct) =>
         {
            throw new InvalidOperationException("Simulated exception in disconnect handler");
         });

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("test-disconnect-exception-client")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var connectResult = await client.ConnectAsync(connectOptions);
         await Assert.That(connectResult.Failed).IsFalse();

         // Act - Disconnect client.
         await client.DisconnectAsync(new DisconnectOptions
         {
            ReasonCode = DisconnectReasonCode.NormalDisconnection
         });

         await Task.Delay(100);

         await client.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task Server_QoS2_DoesNotLeakInFlightCountWhenSendFailsOrInterrupted()
   {
      var server = MqttServerFactory.CreateBuilder(new MqttServerOptions { ReceiveMaximum = 2 })
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;

         var client = MqttClientFactory.CreateTcp();
         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("test-qos2-inflight-client")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var connectResult = await client.ConnectAsync(connectOptions);
         await Assert.That(connectResult.Failed).IsFalse();

         // Publish multiple QoS 2 messages
         for (var i = 0; i < 5; i++)
         {
            var publishOptions = new PublishOptionsBuilder()
               .WithTopic("test/qos2/topic")
               .WithPayload($"Payload {i}")
               .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
               .Build();

            var pubResult = await client.PublishAsync(publishOptions);
            await Assert.That(pubResult.Failed).IsFalse();
         }

         await client.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task ServerClient_TopicAliases_ConcurrentAccess_IsThreadSafe()
   {
      var serverClient = new MqttServerClient();

      var tasks = new Task[20];
      for (var i = 0; i < tasks.Length; i++)
      {
         var taskIndex = i;
         tasks[i] = Task.Run(() =>
         {
            for (ushort alias = 1; alias <= 100; alias++)
            {
               var topic = Encoding.UTF8.GetBytes($"topic/alias/{taskIndex}/{alias}");
               serverClient.SetTopicAlias(alias, topic);

               serverClient.TryGetTopicAlias(alias, out _);
            }
         });
      }

      await Task.WhenAll(tasks);
   }

   [Test]
   public async Task DeliverNextQueuedMessagesAsync_ConcurrentExecution_EnforcesReceiveMaximum()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var dummyClient = new MqttServerClient();
      var session = new MqttSession(server, dummyClient)
      {
         ClientReceiveMaximum = 1
      };

      // Enqueue 10 offline messages
      for (var i = 0; i < 10; i++)
      {
         var packet = new PublishPacket
         {
            TopicUtf8Bytes = new System.Buffers.ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"test/queue/{i}")),
            Payload = new System.Buffers.ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"payload {i}")),
            QualityOfService = QualityOfServiceType.AtLeastOnce
         };
         session.EnqueueOfflineMessage(new MqttQueuedMessage(
            new MqttPublishMessage(packet),
            QualityOfServiceType.AtLeastOnce,
            false,
            0));
      }

      // Run multiple concurrent DeliverNextQueuedMessagesAsync tasks
      var deliveryTasks = new Task[10];
      for (var i = 0; i < 10; i++)
      {
         deliveryTasks[i] = Task.Run(() => MqttServer.DeliverNextQueuedMessagesAsync(session));
      }

      await Task.WhenAll(deliveryTasks);

      // Verify unacknowledged publish count never exceeded ClientReceiveMaximum (1)
      await Assert.That(session.GetUnacknowledgedPublishCount()).IsLessThanOrEqualTo(1);
   }

   [Test]
   public async Task MqttServerClient_WithMeterListener_TracksMqttMetrics()
   {
      long recordedClientsDelta = 0;
      long recordedSessionsDelta = 0;
      long recordedSubscriptionsDelta = 0;
      long recordedRetainedDelta = 0;
      long recordedMessagesPublished = 0;
      long recordedQosInflightDelta = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == MqttMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };
      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.mqtt.server.clients.connected")
         {
            Interlocked.Add(ref recordedClientsDelta, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.server.sessions.active")
         {
            Interlocked.Add(ref recordedSessionsDelta, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.subscriptions.active")
         {
            Interlocked.Add(ref recordedSubscriptionsDelta, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.retained_messages.active")
         {
            Interlocked.Add(ref recordedRetainedDelta, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.messages.published")
         {
            Interlocked.Add(ref recordedMessagesPublished, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.qos.inflight")
         {
            Interlocked.Add(ref recordedQosInflightDelta, measurement);
         }
      });
      meterListener.Start();

      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         var initialClients = Volatile.Read(ref recordedClientsDelta);
         var initialSessions = Volatile.Read(ref recordedSessionsDelta);
         var initialSubs = Volatile.Read(ref recordedSubscriptionsDelta);
         var initialRetained = Volatile.Read(ref recordedRetainedDelta);

         var client = MqttClientFactory.CreateTcp();
         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("telemetry-test-client")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var connectResult = await client.ConnectAsync(connectOptions);
         await Assert.That(connectResult.Failed).IsFalse();

         var clientsDelta = Volatile.Read(ref recordedClientsDelta) - initialClients;
         await Assert.That(clientsDelta).IsGreaterThanOrEqualTo(1);

         var sessionsDelta = Volatile.Read(ref recordedSessionsDelta) - initialSessions;
         await Assert.That(sessionsDelta).IsGreaterThanOrEqualTo(1);

         // Subscribe to topic
         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("telemetry/topic", QualityOfServiceType.AtLeastOnce)
            .Build();

         var subResult = await client.SubscribeAsync(subOptions);
         await Assert.That(subResult.Failed).IsFalse();

         var subsDelta = Volatile.Read(ref recordedSubscriptionsDelta) - initialSubs;
         await Assert.That(subsDelta).IsGreaterThanOrEqualTo(1);

         // Publish Retained message to trigger retained message active metric
         var retainedPubOptions = new PublishOptionsBuilder()
            .WithTopic("telemetry/retained/topic")
            .WithPayload("Retained Payload")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithRetain(true)
            .Build();

         var retainedResult = await client.PublishAsync(retainedPubOptions);
         await Assert.That(retainedResult.Failed).IsFalse();

         var retainedDelta = Volatile.Read(ref recordedRetainedDelta) - initialRetained;
         await Assert.That(retainedDelta).IsGreaterThanOrEqualTo(1);

         // Publish standard message
         var pubOptions = new PublishOptionsBuilder()
            .WithTopic("telemetry/topic")
            .WithPayload("Telemetry Payload Data")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build();

         var pubResult = await client.PublishAsync(pubOptions);
         await Assert.That(pubResult.Failed).IsFalse();

         await Assert.That(recordedMessagesPublished).IsGreaterThanOrEqualTo(2);

         await client.DisconnectAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.NormalDisconnection });
         await client.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }
}
