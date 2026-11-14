using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttAdversarialAndInterceptTests
{
   [Test]
   public async Task Server_ClientPublishesWildcardTopic_ShouldDisconnectOffendingClientWithoutAffectingOthers()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
      var endpoint = new IPEndPoint(IPAddress.Loopback, port);

      var healthyTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      await using var healthySub = MqttClientFactory.CreateTcp();
      healthySub.AddMessageReceiveHandler((ctx, _) =>
      {
         healthyTcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });
      await healthySub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("healthy-sub").Build());
      await healthySub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("healthy/topic", QualityOfServiceType.AtLeastOnce)
         .Build());

      // Rogue raw socket client sends CONNECT then PUBLISH with wildcard '+' in topic name
      using var tcp = new TcpClient();
      await tcp.ConnectAsync(IPAddress.Loopback, port);
      await using var stream = tcp.GetStream();

      var connectWriter = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(connectWriter).WriteConnect(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("rogue-wildcard-pub")
         .Build());
      await stream.WriteAsync(connectWriter.WrittenMemory);

      // Read CONNACK
      var buf = new byte[256];
      var connAckRead = await stream.ReadAsync(buf);
      await Assert.That(connAckRead).IsGreaterThan(0);

      // Craft raw PUBLISH with invalid topic "bad/+/topic"
      var pubWriter = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(pubWriter).WritePublish(PublishOptions.Create()
         .WithTopic("bad/+/topic")
         .WithPayload("evil")
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .Build(), packetIdentifier: 0);
      await stream.WriteAsync(pubWriter.WrittenMemory);

      // Expect server to disconnect (sending DISCONNECT or closing socket)
      using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var closedOrDisconnected = false;
      while (!readCts.IsCancellationRequested)
      {
         var n = await stream.ReadAsync(buf, readCts.Token);
         if (n == 0 || (n > 0 && (buf[0] >> 4) == (int)MqttPacketType.Disconnect))
         {
            closedOrDisconnected = true;
            break;
         }
      }
      await Assert.That(closedOrDisconnected).IsTrue();

      // Verify healthy clients still communicate normally
      await using var healthyPub = MqttClientFactory.CreateTcp();
      await healthyPub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("healthy-pub").Build());
      await healthyPub.PublishAsync(PublishOptions.Create()
         .WithTopic("healthy/topic")
         .WithPayload("still-ok")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      var received = await healthyTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(received).IsEqualTo("still-ok");

      await healthySub.DisconnectAsync(new DisconnectOptions());
      await healthyPub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Server_MalformedPacketBytes_ShouldDisconnectOffendingClientCleanly()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
      var endpoint = new IPEndPoint(IPAddress.Loopback, port);

      using var tcp = new TcpClient();
      await tcp.ConnectAsync(IPAddress.Loopback, port);
      await using var stream = tcp.GetStream();

      var connectWriter = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(connectWriter).WriteConnect(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("malformed-client")
         .Build());
      await stream.WriteAsync(connectWriter.WrittenMemory);

      var buf = new byte[256];
      var connAckRead = await stream.ReadAsync(buf);
      await Assert.That(connAckRead).IsGreaterThan(0);

      // Send invalid fixed header (packet type 0 / invalid VBI continuation bytes)
      byte[] garbage = [0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
      await stream.WriteAsync(garbage);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var closed = false;
      try
      {
         while (!cts.IsCancellationRequested)
         {
            var n = await stream.ReadAsync(buf, cts.Token);
            if (n == 0)
            {
               closed = true;
               break;
            }
         }
      }
      catch (IOException)
      {
         closed = true;
      }

      await Assert.That(closed).IsTrue();

      // Verify a new legitimate client can still connect
      await using var good = MqttClientFactory.CreateTcp();
      var res = await good.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("good-after-garbage").Build());
      await Assert.That(res.Failed).IsFalse();
      await good.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Server_OnPublishIntercept_BlockedWithReasonCode_ShouldReturnReasonCodeToQos1AndQos2Publishers()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnPublishIntercept.Add((ctx, _) =>
      {
         if (ctx.PublishMessage.Topic.StartsWith("restricted/", StringComparison.Ordinal))
         {
            ctx.IsBlocked = true;
            ctx.ReasonCode = (byte)PubAckReasonCode.NotAuthorized;
         }

         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var deliveredToSub = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      await using var sub = MqttClientFactory.CreateTcp();
      sub.AddMessageReceiveHandler((ctx, _) =>
      {
         deliveredToSub.TrySetResult(ctx.Message.Topic);
         return ValueTask.CompletedTask;
      });
      await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("intercept-sub").Build());
      await sub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("restricted/#", QualityOfServiceType.ExactlyOnce)
         .Build());

      await using var pub = MqttClientFactory.CreateTcp();
      await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("intercept-pub")
         .Build());

      var qos1Res = await pub.PublishAsync(PublishOptions.Create()
         .WithTopic("restricted/secret")
         .WithPayload("nope")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build());

      await Assert.That(qos1Res.Failed).IsFalse();
      await Assert.That(qos1Res.Success.ReasonCode).IsEqualTo(PubAckReasonCode.NotAuthorized);

      var qos2Res = await pub.PublishAsync(PublishOptions.Create()
         .WithTopic("restricted/secret")
         .WithPayload("nope-qos2")
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithRetain(true)
         .Build());

      await Assert.That(qos2Res.Failed).IsFalse();
      await Assert.That((byte)qos2Res.Success.ReasonCode).IsEqualTo((byte)PubRecReasonCode.NotAuthorized);

      // Neither retained store nor subscriber should receive blocked publishes
      await Task.Delay(250);
      await Assert.That(deliveredToSub.Task.IsCompleted).IsFalse();
      await Assert.That(server.RetainedMessages.GetMessages().Count).IsEqualTo(0);

      await sub.DisconnectAsync(new DisconnectOptions());
      await pub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Server_OnConnectIntercept_RejectedWithNotAuthorized_ShouldFailConnect()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnConnectIntercept.Add((ctx, _) =>
      {
         var username = Encoding.UTF8.GetString(ctx.ConnectOptions.UsernameUtf8Bytes.Span);
         if (username == "intruder")
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            ctx.ReasonString = "Access denied for intruder";
         }

         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      await using var rejectedClient = MqttClientFactory.CreateTcp();
      var rejectRes = await rejectedClient.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("intruder-1")
         .WithUsername("intruder")
         .Build());

      await Assert.That(rejectRes.Failed).IsTrue();
      await Assert.That(rejectedClient.IsConnected).IsFalse();

      await using var allowedClient = MqttClientFactory.CreateTcp();
      var allowRes = await allowedClient.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("admin-1")
         .WithUsername("admin")
         .Build());

      await Assert.That(allowRes.Failed).IsFalse();
      await allowedClient.DisconnectAsync(new DisconnectOptions());
   }
}
