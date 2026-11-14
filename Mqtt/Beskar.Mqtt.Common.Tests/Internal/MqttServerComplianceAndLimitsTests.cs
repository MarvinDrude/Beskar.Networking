using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Beskar.Memory.Results;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Internal;

public sealed class MqttServerComplianceAndLimitsTests
{
   [Test]
   public async Task Qos1_LivePublishWhileOfflineQueueNonEmpty_ShouldPreserveOrder()
   {
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions
         {
            SupportPersistentSessions = true
         })
         .UseTcp(0)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      using var rawSub = new TcpClient();
      await rawSub.ConnectAsync(IPAddress.Loopback, port);
      var subStream = rawSub.GetStream();

      await SendRawConnectV5Async(subStream, "order-sub", receiveMax: 1, cleanStart: false, sessionExpiry: 60);
      await ReadNextRawPacketAsync(subStream); // CONNACK

      await SendRawSubscribeV5Async(subStream, 1, "order/topic", QualityOfServiceType.AtLeastOnce);
      await ReadNextRawPacketAsync(subStream); // SUBACK

      await using var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("order-pub")
         .Build());

      await publisher.PublishAsync(PublishOptions.Create()
         .WithTopic("order/topic")
         .WithPayload("msg-1")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      var firstPublish = await ReadNextRawPacketAsync(subStream);
      var firstPacketId = ReadPublishPacketId(firstPublish);
      var receivedPayloads = new List<string> { ReadPublishPayloadUtf8(firstPublish) };

      // Fill offline queue while slot 1 is still unacknowledged
      await publisher.PublishAsync(PublishOptions.Create()
         .WithTopic("order/topic")
         .WithPayload("msg-2")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      await publisher.PublishAsync(PublishOptions.Create()
         .WithTopic("order/topic")
         .WithPayload("msg-3")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      var session = await GetServerSessionAsync(server, "order-sub");
      await Assert.That(session).IsNotNull();
      await Assert.That(session!.OfflineQueueCount).IsEqualTo(2);

      // Hold DeliverySemaphore to simulate active queue drain transition as PUBACK frees the slot
      await session.DeliverySemaphore.WaitAsync();
      try
      {
         await SendRawPubAckV5Async(subStream, firstPacketId);

         // Wait until Ack has reduced unacked count to 0 while msg-2 & msg-3 remain queued
         var deadline = DateTime.UtcNow.AddSeconds(2);
         while (session.GetUnacknowledgedPublishCount() > 0 && DateTime.UtcNow < deadline)
         {
            await Task.Delay(10);
         }

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("order/topic")
            .WithPayload("msg-4")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());
      }
      finally
      {
         session.DeliverySemaphore.Release();
      }

      for (var i = 0; i < 3; i++)
      {
         var next = await ReadNextRawPacketAsync(subStream);
         var pid = ReadPublishPacketId(next);
         receivedPayloads.Add(ReadPublishPayloadUtf8(next));
         await SendRawPubAckV5Async(subStream, pid);
      }

      await Assert.That(string.Join(",", receivedPayloads)).IsEqualTo("msg-1,msg-2,msg-3,msg-4");
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task OutgoingPublishChannel_WhenOverflowDropsQos1Message_ShouldNotLeakUnacknowledgedSlot()
   {
      var listener = new ControlledNetworkListener();
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions
         {
            MaxPendingMessagesPerConnection = 1,
            PendingMessageOverflowBehavior = MessageOverflowBehavior.DropOldest,
            ReceiveMaximum = 10
         })
         .Use(listener)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      var subConnection = listener.AcceptControlledConnection();
      var pubConnection = listener.AcceptControlledConnection();

      await subConnection.SendToServerAsync(EncodeConnectV5("drop-sub", receiveMax: 10));
      await subConnection.ReadPacketTypeAsync(MqttPacketType.ConnAck);
      await subConnection.SendToServerAsync(EncodeSubscribeV5(1, "drop/test", QualityOfServiceType.AtLeastOnce));
      await subConnection.ReadPacketTypeAsync(MqttPacketType.SubAck);

      await pubConnection.SendToServerAsync(EncodeConnectV5("drop-pub"));
      await pubConnection.ReadPacketTypeAsync(MqttPacketType.ConnAck);

      var subSession = await GetServerSessionAsync(server, "drop-sub");
      await Assert.That(subSession).IsNotNull();

      // Stall outgoing stream after ProcessOutgoingPublishesAsync starts sending msg-1
      subConnection.HoldNextServerPublish();

      await pubConnection.SendToServerAsync(EncodePublishV5(10, "drop/test", "msg-1", QualityOfServiceType.AtLeastOnce));
      await subConnection.WaitUntilServerPublishStartedAsync();

      // Enqueue msg-2 into capacity-1 channel, then overflow with msg-3 (DropOldest drops msg-2)
      await pubConnection.SendToServerAsync(EncodePublishV5(11, "drop/test", "msg-2", QualityOfServiceType.AtLeastOnce));
      await pubConnection.SendToServerAsync(EncodePublishV5(12, "drop/test", "msg-3", QualityOfServiceType.AtLeastOnce));

      // Wait until both publishes are processed into subscriber session/channel
      var deadline = DateTime.UtcNow.AddSeconds(3);
      while (subSession!.GetUnacknowledgedPublishCount() < 3 && DateTime.UtcNow < deadline)
      {
         await Task.Delay(10);
      }

      subConnection.ReleaseHeldPublish();

      var delivered = new List<string>();
      for (var i = 0; i < 2; i++)
      {
         var pkt = await subConnection.ReadNextPacketAsync(TimeSpan.FromSeconds(3));
         await Assert.That((MqttPacketType)(pkt[0] >> 4)).IsEqualTo(MqttPacketType.Publish);
         delivered.Add(ReadPublishPayloadUtf8(pkt));
         await subConnection.SendToServerAsync(EncodePubAckV5(ReadPublishPacketId(pkt)));
      }

      // Brief delay for any third packet if somehow not dropped, and for PUBACK processing
      await Task.Delay(150);

      await Assert.That(subSession.GetUnacknowledgedPublishCount()).IsEqualTo(0);
   }

   [Test]
   public async Task OutgoingPublish_LiveBufferedInChannelWhileOfflineDrainTriggered_ShouldPreserveStrictOrder()
   {
      var listener = new ControlledNetworkListener();
      var options = new MqttServerOptions();
      await using var server = new MqttServer([listener], options);
      await server.StartAsync();

      var subConnection = listener.AcceptControlledConnection();
      var pubConnection = listener.AcceptControlledConnection();

      await subConnection.SendToServerAsync(EncodeConnectV5("order-live-sub", receiveMax: 3));
      await subConnection.ReadPacketTypeAsync(MqttPacketType.ConnAck);
      await subConnection.SendToServerAsync(EncodeSubscribeV5(1, "order/live-offline", QualityOfServiceType.AtLeastOnce));
      await subConnection.ReadPacketTypeAsync(MqttPacketType.SubAck);

      await pubConnection.SendToServerAsync(EncodeConnectV5("order-live-pub"));
      await pubConnection.ReadPacketTypeAsync(MqttPacketType.ConnAck);

      var subSession = await GetServerSessionAsync(server, "order-live-sub");
      await Assert.That(subSession).IsNotNull();

      var delivered = new List<string>();

      // msg-0 consumes slot 1 of ReceiveMaximum=3; leave it unacked
      await pubConnection.SendToServerAsync(EncodePublishV5(10, "order/live-offline", "msg-0", QualityOfServiceType.AtLeastOnce));
      await pubConnection.ReadPacketTypeAsync(MqttPacketType.PubAck);
      var pkt0 = await subConnection.ReadNextPacketAsync(TimeSpan.FromSeconds(5));
      await Assert.That((MqttPacketType)(pkt0[0] >> 4)).IsEqualTo(MqttPacketType.Publish);
      delivered.Add(ReadPublishPayloadUtf8(pkt0));

      // msg-1 consumes slot 2 and stalls mid-write inside the outgoing channel consumer
      subConnection.HoldNextServerPublish();
      await pubConnection.SendToServerAsync(EncodePublishV5(11, "order/live-offline", "msg-1", QualityOfServiceType.AtLeastOnce));
      await pubConnection.ReadPacketTypeAsync(MqttPacketType.PubAck);
      await subConnection.WaitUntilServerPublishStartedAsync();

      // msg-2 consumes slot 3 as a live publish buffered inside _outgoingPublishChannel behind msg-1
      await pubConnection.SendToServerAsync(EncodePublishV5(12, "order/live-offline", "msg-2", QualityOfServiceType.AtLeastOnce));
      await pubConnection.ReadPacketTypeAsync(MqttPacketType.PubAck);

      // msg-3 sees DeliverySemaphore == 0 and goes to _offlineQueue
      await pubConnection.SendToServerAsync(EncodePublishV5(13, "order/live-offline", "msg-3", QualityOfServiceType.AtLeastOnce));
      await pubConnection.ReadPacketTypeAsync(MqttPacketType.PubAck);

      var deadline = DateTime.UtcNow.AddSeconds(5);
      while (subSession!.OfflineQueueCount < 1 && DateTime.UtcNow < deadline)
      {
         await Task.Delay(10);
      }
      await Assert.That(subSession.OfflineQueueCount).IsEqualTo(1);

      // Ack msg-0 while msg-1 is still stalled and msg-2 sits in the outgoing channel.
      // Offline drain must enqueue msg-3 behind msg-2 on the ordered outgoing channel rather than bypassing it.
      await subConnection.SendToServerAsync(EncodePubAckV5(ReadPublishPacketId(pkt0)));

      deadline = DateTime.UtcNow.AddSeconds(5);
      while (subSession.OfflineQueueCount > 0 && DateTime.UtcNow < deadline)
      {
         await Task.Delay(10);
      }
      await Assert.That(subSession.OfflineQueueCount).IsEqualTo(0);

      subConnection.ReleaseHeldPublish();

      for (var i = 0; i < 3; i++)
      {
         var pkt = await subConnection.ReadNextPacketAsync(TimeSpan.FromSeconds(5));
         await Assert.That((MqttPacketType)(pkt[0] >> 4)).IsEqualTo(MqttPacketType.Publish);
         delivered.Add(ReadPublishPayloadUtf8(pkt));
         await subConnection.SendToServerAsync(EncodePubAckV5(ReadPublishPacketId(pkt)));
      }

      await Assert.That(delivered.Count).IsEqualTo(4);
      await Assert.That(delivered[0]).IsEqualTo("msg-0");
      await Assert.That(delivered[1]).IsEqualTo("msg-1");
      await Assert.That(delivered[2]).IsEqualTo("msg-2");
      await Assert.That(delivered[3]).IsEqualTo("msg-3");
   }

   [Test]
   [NotInParallel]
   public async Task SessionTakeover_BeforeOldDisconnectFlushes_ShouldFenceOldClientAndPreserveActiveSessionCount()
   {
      long activeSessionDelta = 0;
      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == MqttMetrics.MeterName &&
             instrument.Name == "beskar.mqtt.server.sessions.active")
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };
      meterListener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
      {
         Interlocked.Add(ref activeSessionDelta, measurement);
      });
      meterListener.Start();

      var baseline = Volatile.Read(ref activeSessionDelta);

      var listener = new ControlledNetworkListener();
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions
         {
            SupportPersistentSessions = true
         })
         .Use(listener)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      // Part 1: Stale client traffic during takeover overlap window (CleanSession = false)
      var clientA = listener.AcceptControlledConnection();
      await clientA.SendToServerAsync(EncodeConnectV5("takeover-client", cleanStart: false, sessionExpiry: 60));
      await clientA.ReadPacketTypeAsync(MqttPacketType.ConnAck);

      clientA.HoldNextServerDisconnect();

      var clientB = listener.AcceptControlledConnection();
      await clientB.SendToServerAsync(EncodeConnectV5("takeover-client", cleanStart: false, sessionExpiry: 60));

      // Wait until Client A's DisconnectAsync starts writing DISCONNECT (after _sessions.Update already handed ownership to Client B)
      await clientA.WaitUntilServerDisconnectStartedAsync();

      // Old Client A attempts to mutate the taken-over session before disconnect completes
      await clientA.SendToServerAsync(EncodeSubscribeV5(99, "stale/clientA/topic", QualityOfServiceType.AtLeastOnce));
      await Task.Delay(100);

      clientA.ReleaseHeldDisconnect();
      await clientB.ReadPacketTypeAsync(MqttPacketType.ConnAck);
      await clientA.ReadPacketTypeAsync(MqttPacketType.Disconnect);

      var takenOverSession = await GetServerSessionAsync(server, "takeover-client");
      await Assert.That(takenOverSession).IsNotNull();
      await Assert.That(takenOverSession!.HasSubscription("stale/clientA/topic"u8)).IsFalse();

      // Part 2: CleanSession replacement should not leak beskar.mqtt.server.sessions.active upward
      var cleanA = listener.AcceptControlledConnection();
      await cleanA.SendToServerAsync(EncodeConnectV5("clean-takeover", cleanStart: true, sessionExpiry: 0));
      await cleanA.ReadPacketTypeAsync(MqttPacketType.ConnAck);

      var cleanB = listener.AcceptControlledConnection();
      await cleanB.SendToServerAsync(EncodeConnectV5("clean-takeover", cleanStart: true, sessionExpiry: 0));
      await cleanB.ReadPacketTypeAsync(MqttPacketType.ConnAck);
      await cleanA.ReadPacketTypeAsync(MqttPacketType.Disconnect);

      await Task.Delay(150);

      // Total active sessions live right now = 2 ("takeover-client" and "clean-takeover")
      await Assert.That(Volatile.Read(ref activeSessionDelta) - baseline).IsEqualTo(2);
   }

   [Test]
   public async Task Qos2_DuplicatePublishAndUnknownPubRel_ShouldNotReInvokeInterceptorOrCorruptInFlight()
   {
      var interceptCount = 0;
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .Build();

      server.Events.OnPublishIntercept.Add((ctx, _) =>
      {
         Interlocked.Increment(ref interceptCount);
         if (ctx.PublishMessage.Topic == "blocked/qos2")
         {
            ctx.IsBlocked = true;
            ctx.ReasonCode = 0x87; // NotAuthorized
         }
         return ValueTask.CompletedTask;
      });

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      using var rawClient = new TcpClient();
      await rawClient.ConnectAsync(IPAddress.Loopback, port);
      var stream = rawClient.GetStream();

      await SendRawConnectV5Async(stream, "qos2-dup-client");
      await ReadNextRawPacketAsync(stream); // CONNACK

      // Send QoS 2 PUBLISH twice (initial + DUP=1) before PUBREL
      var pubFirst = EncodePublishV5(42, "allowed/qos2", "payload", QualityOfServiceType.ExactlyOnce, dup: false);
      var pubDup = EncodePublishV5(42, "allowed/qos2", "payload", QualityOfServiceType.ExactlyOnce, dup: true);

      await stream.WriteAsync(pubFirst);
      await ReadNextRawPacketAsync(stream); // PUBREC

      await stream.WriteAsync(pubDup);
      await ReadNextRawPacketAsync(stream); // PUBREC

      await Assert.That(Volatile.Read(ref interceptCount)).IsEqualTo(1);

      await SendRawPubRelV5Async(stream, 42);
      await ReadNextRawPacketAsync(stream); // PUBCOMP

      // Blocked QoS 2 publish followed by PUBREL must yield PUBCOMP(PacketIdentifierNotFound)
      await stream.WriteAsync(EncodePublishV5(43, "blocked/qos2", "payload", QualityOfServiceType.ExactlyOnce, dup: false));
      var blockedPubRec = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(blockedPubRec[0] >> 4)).IsEqualTo(MqttPacketType.PubRec);

      await SendRawPubRelV5Async(stream, 43);
      var compAfterBlocked = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(compAfterBlocked[0] >> 4)).IsEqualTo(MqttPacketType.PubComp);
      await Assert.That((PubCompReasonCode)ReadPubCompReasonCodeV5(compAfterBlocked))
         .IsEqualTo(PubCompReasonCode.PacketIdentifierNotFound);
   }

   [Test]
   public async Task Qos2_InterceptBlockedWithDefaultSuccess_ShouldRetainTrackingAndCompletePubRelWithoutReIntercept()
   {
      var options = new MqttServerOptions();
      var tcpListener = new Beskar.Networking.Transports.Tcp.TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), new Beskar.Networking.Transports.Tcp.TcpTransportOptions());
      await using var server = new MqttServer([tcpListener], options);

      var interceptCount = 0;
      server.Events.OnPublishIntercept.Add((ctx, _) =>
      {
         Interlocked.Increment(ref interceptCount);
         ctx.Block(); // Default ReasonCode == 0 (Success PUBREC)
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var port = ((IPEndPoint)tcpListener.LocalAddress).Port;

      using var rawClient = new TcpClient();
      await rawClient.ConnectAsync(IPAddress.Loopback, port);
      await using var stream = rawClient.GetStream();

      await SendRawConnectV5Async(stream, "qos2-block-success-v5");
      var connAck = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(connAck[0] >> 4)).IsEqualTo(MqttPacketType.ConnAck);

      // First QoS 2 PUBLISH blocked with successful PUBREC must retain packet ID tracking
      await stream.WriteAsync(EncodePublishV5(51, "blocked/qos2-ok", "payload", QualityOfServiceType.ExactlyOnce, dup: false));
      var pubRec1 = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(pubRec1[0] >> 4)).IsEqualTo(MqttPacketType.PubRec);
      await Assert.That(Volatile.Read(ref interceptCount)).IsEqualTo(1);

      // Retransmitted DUP=1 PUBLISH must NOT re-invoke interceptor
      await stream.WriteAsync(EncodePublishV5(51, "blocked/qos2-ok", "payload", QualityOfServiceType.ExactlyOnce, dup: true));
      var pubRec2 = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(pubRec2[0] >> 4)).IsEqualTo(MqttPacketType.PubRec);
      await Assert.That(Volatile.Read(ref interceptCount)).IsEqualTo(1);

      // PUBREL must complete with Success (not PacketIdentifierNotFound)
      await SendRawPubRelV5Async(stream, 51);
      var pubComp = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(pubComp[0] >> 4)).IsEqualTo(MqttPacketType.PubComp);
      await Assert.That((PubCompReasonCode)ReadPubCompReasonCodeV5(pubComp))
         .IsEqualTo(PubCompReasonCode.Success);

      // Second PUBREL for the completed packet ID now reports PacketIdentifierNotFound
      await SendRawPubRelV5Async(stream, 51);
      var duplicatePubComp = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(duplicatePubComp[0] >> 4)).IsEqualTo(MqttPacketType.PubComp);
      await Assert.That((PubCompReasonCode)ReadPubCompReasonCodeV5(duplicatePubComp))
         .IsEqualTo(PubCompReasonCode.PacketIdentifierNotFound);
   }

   [Test]
   public async Task Qos2_V311_InterceptBlockedWithFailureReasonCode_ShouldStillRetainTrackingForPubRel()
   {
      var options = new MqttServerOptions();
      var tcpListener = new Beskar.Networking.Transports.Tcp.TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), new Beskar.Networking.Transports.Tcp.TcpTransportOptions());
      await using var server = new MqttServer([tcpListener], options);

      var interceptCount = 0;
      server.Events.OnPublishIntercept.Add((ctx, _) =>
      {
         Interlocked.Increment(ref interceptCount);
         ctx.Block((byte)PubRecReasonCode.UnspecifiedError);
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var port = ((IPEndPoint)tcpListener.LocalAddress).Port;

      using var rawClient = new TcpClient();
      await rawClient.ConnectAsync(IPAddress.Loopback, port);
      await using var stream = rawClient.GetStream();

      // MQTT 3.1.1 CONNECT ("v311-qos2-blk", CleanSession = true, KeepAlive = 60)
      var clientIdBytes = "v311-qos2-blk"u8.ToArray();
      var connectPacket = new byte[2 + 10 + 2 + clientIdBytes.Length];
      connectPacket[0] = 0x10;
      connectPacket[1] = (byte)(10 + 2 + clientIdBytes.Length);
      connectPacket[2] = 0x00; connectPacket[3] = 0x04;
      connectPacket[4] = (byte)'M'; connectPacket[5] = (byte)'Q'; connectPacket[6] = (byte)'T'; connectPacket[7] = (byte)'T';
      connectPacket[8] = 0x04; // MQTT 3.1.1
      connectPacket[9] = 0x02; // CleanSession
      connectPacket[10] = 0x00; connectPacket[11] = 0x3C;
      connectPacket[12] = (byte)(clientIdBytes.Length >> 8);
      connectPacket[13] = (byte)(clientIdBytes.Length & 0xFF);
      clientIdBytes.CopyTo(connectPacket.AsSpan(14));
      await stream.WriteAsync(connectPacket);
      await stream.FlushAsync();

      var connAck = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(connAck[0] >> 4)).IsEqualTo(MqttPacketType.ConnAck);

      static byte[] EncodePublishV3(ushort packetId, bool dup)
      {
         var topic = "blk/v3"u8;
         var payload = "hi"u8;
         var remLen = 2 + topic.Length + 2 + payload.Length;
         var pkt = new byte[2 + remLen];
         pkt[0] = (byte)(0x34 | (dup ? 0x08 : 0x00));
         pkt[1] = (byte)remLen;
         pkt[2] = 0x00; pkt[3] = (byte)topic.Length;
         topic.CopyTo(pkt.AsSpan(4));
         var pos = 4 + topic.Length;
         pkt[pos] = (byte)(packetId >> 8);
         pkt[pos + 1] = (byte)(packetId & 0xFF);
         payload.CopyTo(pkt.AsSpan(pos + 2));
         return pkt;
      }

      // First QoS 2 PUBLISH in V3.1.1: PUBREC carries no reason code so tracking must remain until PUBREL
      await stream.WriteAsync(EncodePublishV3(61, dup: false));
      var pubRec1 = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(pubRec1[0] >> 4)).IsEqualTo(MqttPacketType.PubRec);
      await Assert.That(Volatile.Read(ref interceptCount)).IsEqualTo(1);

      // Retransmitted DUP=1 PUBLISH must not re-invoke interceptor
      await stream.WriteAsync(EncodePublishV3(61, dup: true));
      var pubRec2 = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(pubRec2[0] >> 4)).IsEqualTo(MqttPacketType.PubRec);
      await Assert.That(Volatile.Read(ref interceptCount)).IsEqualTo(1);

      // PUBREL must receive PUBCOMP with packet ID 61
      await stream.WriteAsync(new byte[] { 0x62, 0x02, 0x00, 61 });
      var pubComp = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(pubComp[0] >> 4)).IsEqualTo(MqttPacketType.PubComp);
      await Assert.That(pubComp[2]).IsEqualTo((byte)0x00);
      await Assert.That(pubComp[3]).IsEqualTo((byte)61);
   }

   [Test]
   public async Task Server_NonConnectFirstPacket_ShouldCloseConnectionInsteadOfResponding()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      using var rawClient = new TcpClient();
      await rawClient.ConnectAsync(IPAddress.Loopback, port);
      var stream = rawClient.GetStream();

      // Crafted 9-byte PINGRESP packet (needs no body parsing in v3) whose 7 body bytes satisfy ParseProtocolVersion(V311)
      byte[] craftedPingRespFirst = [0xD0, 0x07, 0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T', 0x04];
      await stream.WriteAsync(craftedPingRespFirst);

      // Give the server time to inspect the first packet and close if non-CONNECT packets are rejected
      await Task.Delay(150);

      var probeBuffer = new byte[1];
      using var probeCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
      bool serverClosedConnection;
      try
      {
         serverClosedConnection = await stream.ReadAsync(probeBuffer, probeCts.Token) == 0;
      }
      catch (OperationCanceledException)
      {
         serverClosedConnection = false;
      }
      catch (IOException)
      {
         serverClosedConnection = true;
      }

      await Assert.That(serverClosedConnection).IsTrue();
   }

   [Test]
   public async Task Server_DuplicateConnectPacket_ShouldDisconnectWithProtocolError()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      using var rawClient = new TcpClient();
      await rawClient.ConnectAsync(IPAddress.Loopback, port);
      var stream = rawClient.GetStream();

      await SendRawConnectV5Async(stream, "dup-connect-client");
      var connAck = await ReadNextRawPacketAsync(stream);
      await Assert.That((MqttPacketType)(connAck[0] >> 4)).IsEqualTo(MqttPacketType.ConnAck);

      // Second CONNECT on already connected session violates MQTT §3.1.0
      await SendRawConnectV5Async(stream, "dup-connect-client");
      await stream.WriteAsync(new byte[] { 0xC0, 0x00 }); // PINGREQ

      var reply = await TryReadNextRawPacketAsync(stream, TimeSpan.FromMilliseconds(500));
      var respondedWithPingResp = reply is not null && (MqttPacketType)(reply[0] >> 4) == MqttPacketType.PingResp;
      await Assert.That(respondedWithPingResp).IsFalse();
   }

   [Test]
   public async Task DisconnectV5_WithNonZeroSessionExpiryAfterNonZeroConnect_WhenPersistenceDisabled_ShouldNotFailWithProtocolError()
   {
      var options = new MqttServerOptions(); // SupportPersistentSessions defaults to false
      var tcpListener = new Beskar.Networking.Transports.Tcp.TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), new Beskar.Networking.Transports.Tcp.TcpTransportOptions());
      await using var server = new MqttServer([tcpListener], options);

      var disconnectedReasons = new ConcurrentDictionary<string, Beskar.Mqtt.Server.Contexts.MqttDisconnectContext>();
      var firstDisconnectTcs = new TaskCompletionSource<Beskar.Mqtt.Server.Contexts.MqttDisconnectContext>(TaskCreationOptions.RunContinuationsAsynchronously);
      var secondDisconnectTcs = new TaskCompletionSource<Beskar.Mqtt.Server.Contexts.MqttDisconnectContext>(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.OnDisconnect.Add((ctx, _) =>
      {
         var id = System.Text.Encoding.UTF8.GetString(ctx.ServerClient.ClientIdUtf8Bytes.Span);
         if (id is not null)
         {
            disconnectedReasons[id] = ctx;
            if (id == "disc-expiry-ok")
            {
               firstDisconnectTcs.TrySetResult(ctx);
            }
            else if (id == "disc-expiry-err")
            {
               secondDisconnectTcs.TrySetResult(ctx);
            }
         }
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var port = ((IPEndPoint)tcpListener.LocalAddress).Port;

      // Case A: Non-zero SessionExpiryInterval in CONNECT (60) followed by non-zero (30) in DISCONNECT
      // must be allowed even when SupportPersistentSessions is disabled.
      using (var rawClient = new TcpClient())
      {
         await rawClient.ConnectAsync(IPAddress.Loopback, port);
         await using var stream = rawClient.GetStream();

         await SendRawConnectV5Async(stream, "disc-expiry-ok", sessionExpiry: 60);
         var connAck = await ReadNextRawPacketAsync(stream);
         await Assert.That((MqttPacketType)(connAck[0] >> 4)).IsEqualTo(MqttPacketType.ConnAck);

         var disconnectWriter = new System.Buffers.ArrayBufferWriter<byte>();
         new Beskar.Mqtt.Common.Encoders.Version5.PacketVersion5Encoder(disconnectWriter).WriteDisconnect(
            new Beskar.Mqtt.Common.Builders.Disconnecting.DisconnectOptions
            {
               ReasonCode = DisconnectReasonCode.NormalDisconnection,
               SessionExpiryInterval = 30
            });
         await stream.WriteAsync(disconnectWriter.WrittenMemory);
         await stream.FlushAsync();

         var ctx = await firstDisconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
         await Assert.That(ctx.Reason).IsEqualTo(DisconnectReasonCode.NormalDisconnection);

         var trailingPacket = await TryReadNextRawPacketAsync(stream, TimeSpan.FromMilliseconds(300));
         await Assert.That(trailingPacket).IsNull();
      }

      // Case B: Zero SessionExpiryInterval in CONNECT followed by non-zero in DISCONNECT
      // must still fail with MQTT 5.0 ProtocolError.
      using (var rawClient = new TcpClient())
      {
         await rawClient.ConnectAsync(IPAddress.Loopback, port);
         await using var stream = rawClient.GetStream();

         await SendRawConnectV5Async(stream, "disc-expiry-err", sessionExpiry: 0);
         var connAck = await ReadNextRawPacketAsync(stream);
         await Assert.That((MqttPacketType)(connAck[0] >> 4)).IsEqualTo(MqttPacketType.ConnAck);

         var disconnectWriter = new System.Buffers.ArrayBufferWriter<byte>();
         new Beskar.Mqtt.Common.Encoders.Version5.PacketVersion5Encoder(disconnectWriter).WriteDisconnect(
            new Beskar.Mqtt.Common.Builders.Disconnecting.DisconnectOptions
            {
               ReasonCode = DisconnectReasonCode.NormalDisconnection,
               SessionExpiryInterval = 30
            });
         await stream.WriteAsync(disconnectWriter.WrittenMemory);
         await stream.FlushAsync();

         var reply = await ReadNextRawPacketAsync(stream);
         await Assert.That((MqttPacketType)(reply[0] >> 4)).IsEqualTo(MqttPacketType.Disconnect);
         var idx = 1;
         while ((reply[idx++] & 0x80) != 0) { }
         await Assert.That((DisconnectReasonCode)reply[idx]).IsEqualTo(DisconnectReasonCode.ProtocolError);

         var ctx = await secondDisconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
         await Assert.That(ctx.Reason).IsEqualTo(DisconnectReasonCode.ProtocolError);
      }
   }

   [Test]
   public async Task StartAsync_WhenSubsequentListenerFailsToBind_ShouldUnbindEarlierListenersAndResetStateToStopped()
   {
      var listener1 = new FailingMockListener(failBind: false);
      var listener2 = new FailingMockListener(failBind: true);

      await using var server = MqttServerFactory.CreateBuilder()
         .Use(listener1)
         .Use(listener2)
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsTrue();
      await Assert.That(listener1.UnbindCalls).IsEqualTo(1);
      await Assert.That(server.State).IsEqualTo(MqttServerState.Stopped);
   }

   [Test]
   public async Task Server_WhenClientAdvertisesMaximumPacketSize_ShouldNotSendOversizedPublish()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();
      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      await using var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var receivedOversized = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, _) =>
      {
         receivedOversized.TrySetResult(ctx.Message.Payload.Length);
         return ValueTask.CompletedTask;
      });

      await subscriber.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("small-max-pkt-sub")
         .WithMaximumPacketSize(64)
         .Build());

      await subscriber.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("limits/maxpkt", QualityOfServiceType.AtMostOnce)
         .Build());

      await using var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("small-max-pkt-pub")
         .Build());

      await publisher.PublishAsync(PublishOptions.Create()
         .WithTopic("limits/maxpkt")
         .WithPayload(new byte[256])
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .Build());

      var delivered = await Task.WhenAny(receivedOversized.Task, Task.Delay(500)) == receivedOversized.Task;
      await Assert.That(delivered).IsFalse();

      await publisher.DisconnectAsync(new DisconnectOptions());
      await subscriber.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Server_WhenAcceptStreamStalls_ShouldTimeoutAndDisposeSession()
   {
      var hangingSession = new HangingAcceptStreamSession();
      var listener = new SingleSessionMockListener(hangingSession);

      await using var server = MqttServerFactory.CreateBuilder()
         .Use(listener)
         .Build();
      server.Options.DefaultTimeout = TimeSpan.FromMilliseconds(150);

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      await hangingSession.AcceptCalled.Task.WaitAsync(TimeSpan.FromSeconds(3));
      var timedOutDisposed = await Task.WhenAny(hangingSession.Disposed.Task, Task.Delay(700)) == hangingSession.Disposed.Task;

      await Assert.That(timedOutDisposed).IsTrue();
   }

   [Test]
   public async Task MqttServerOptions_ShouldExposeMaxConnectionsAndMaximumPacketSizeConfiguration()
   {
      var maxConnectionsProp = typeof(MqttServerOptions).GetProperty("MaxConnections");
      var maxPacketSizeProp = typeof(MqttServerOptions).GetProperty("MaximumPacketSize");

      await Assert.That(maxConnectionsProp).IsNotNull();
      await Assert.That(maxPacketSizeProp).IsNotNull();
   }

   private static async Task<MqttSession?> GetServerSessionAsync(MqttServer server, string clientId)
   {
      var clientIdBytes = Encoding.UTF8.GetBytes(clientId);
      using var clients = await server.ClientSessions.GetClients();
      foreach (var client in clients.WrittenSpan)
      {
         if (client.ClientIdUtf8Bytes.Span.SequenceEqual(clientIdBytes))
         {
            return client.MqttSession;
         }
      }

      return null;
   }

   private static byte[] EncodeConnectV5(string clientId, ushort receiveMax = 65535, bool cleanStart = true, uint sessionExpiry = 0)
   {
      var writer = new ArrayBufferWriter<byte>();
      var options = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId(clientId)
         .WithReceiveMaximum(receiveMax)
         .WithCleanSession(cleanStart)
         .WithSessionExpiryInterval(sessionExpiry)
         .Build();
      new PacketVersion5Encoder(writer).WriteConnect(options);
      return writer.WrittenSpan.ToArray();
   }

   private static async Task SendRawConnectV5Async(NetworkStream stream, string clientId, ushort receiveMax = 65535, bool cleanStart = true, uint sessionExpiry = 0)
   {
      await stream.WriteAsync(EncodeConnectV5(clientId, receiveMax, cleanStart, sessionExpiry));
   }

   private static byte[] EncodeSubscribeV5(ushort packetId, string filter, QualityOfServiceType qos)
   {
      var writer = new ArrayBufferWriter<byte>();
      var options = SubscribeOptions.Create()
         .WithTopicFilter(filter, qos)
         .Build();
      new PacketVersion5Encoder(writer).WriteSubscribe(options, packetId);
      return writer.WrittenSpan.ToArray();
   }

   private static async Task SendRawSubscribeV5Async(NetworkStream stream, ushort packetId, string filter, QualityOfServiceType qos)
   {
      await stream.WriteAsync(EncodeSubscribeV5(packetId, filter, qos));
   }

   private static byte[] EncodePublishV5(ushort packetId, string topic, string payload, QualityOfServiceType qos, bool dup = false)
   {
      var writer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(writer).WritePublish(new PublishPacket
      {
         PacketIdentifier = packetId,
         TopicUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic)),
         Payload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(payload)),
         QualityOfService = qos,
         Dup = dup
      });
      return writer.WrittenSpan.ToArray();
   }

   private static byte[] EncodePubAckV5(ushort packetId)
   {
      var writer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(writer).WritePubAck(new PubAckPacket
      {
         PacketIdentifier = packetId,
         ReasonCode = PubAckReasonCode.Success
      });
      return writer.WrittenSpan.ToArray();
   }

   private static async Task SendRawPubAckV5Async(NetworkStream stream, ushort packetId)
   {
      await stream.WriteAsync(EncodePubAckV5(packetId));
   }

   private static async Task SendRawPubRelV5Async(NetworkStream stream, ushort packetId)
   {
      var writer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(writer).WritePubRel(new PubRelPacket
      {
         PacketIdentifier = packetId,
         ReasonCode = PubRelReasonCode.Success
      });
      await stream.WriteAsync(writer.WrittenMemory);
   }

   private static async Task<byte[]> ReadNextRawPacketAsync(NetworkStream stream)
   {
      var pkt = await TryReadNextRawPacketAsync(stream, TimeSpan.FromSeconds(5));
      return pkt ?? throw new EndOfStreamException("Connection closed before next MQTT packet arrived.");
   }

   private static async Task<byte[]?> TryReadNextRawPacketAsync(NetworkStream stream, TimeSpan timeout)
   {
      using var cts = new CancellationTokenSource(timeout);
      try
      {
         var header = new byte[1];
         var read = await stream.ReadAsync(header, cts.Token);
         if (read == 0) return null;

         var lenBytes = new List<byte>(4);
         var remaining = 0;
         var multiplier = 1;
         byte b;
         do
         {
            var buf = new byte[1];
            if (await stream.ReadAsync(buf, cts.Token) == 0) return null;
            b = buf[0];
            lenBytes.Add(b);
            remaining += (b & 0x7F) * multiplier;
            multiplier *= 128;
         }
         while ((b & 0x80) != 0);

         var result = new byte[1 + lenBytes.Count + remaining];
         result[0] = header[0];
         lenBytes.CopyTo(result, 1);

         var offset = 1 + lenBytes.Count;
         while (offset < result.Length)
         {
            var chunk = await stream.ReadAsync(result.AsMemory(offset), cts.Token);
            if (chunk == 0) return null;
            offset += chunk;
         }

         return result;
      }
      catch (OperationCanceledException)
      {
         return null;
      }
      catch (IOException)
      {
         return null;
      }
   }

   private static ushort ReadPublishPacketId(ReadOnlySpan<byte> packet)
   {
      var offset = 1;
      byte b;
      do
      {
         b = packet[offset++];
      }
      while ((b & 0x80) != 0);

      var topicLen = (packet[offset] << 8) | packet[offset + 1];
      offset += 2 + topicLen;
      return (ushort)((packet[offset] << 8) | packet[offset + 1]);
   }

   private static string ReadPublishPayloadUtf8(ReadOnlySpan<byte> packet)
   {
      var offset = 1;
      byte b;
      do
      {
         b = packet[offset++];
      }
      while ((b & 0x80) != 0);

      var qos = (QualityOfServiceType)((packet[0] >> 1) & 0x03);
      var topicLen = (packet[offset] << 8) | packet[offset + 1];
      offset += 2 + topicLen;
      if (qos > QualityOfServiceType.AtMostOnce)
      {
         offset += 2; // packet identifier
      }

      // Read v5 property length VBI
      var propLen = 0;
      var mult = 1;
      byte pb;
      do
      {
         pb = packet[offset++];
         propLen += (pb & 0x7F) * mult;
         mult *= 128;
      }
      while ((pb & 0x80) != 0);
      offset += propLen;

      return Encoding.UTF8.GetString(packet.Slice(offset));
   }

   private static byte ReadPubCompReasonCodeV5(ReadOnlySpan<byte> packet)
   {
      var offset = 1;
      var rem = 0;
      var mult = 1;
      byte b;
      do
      {
         b = packet[offset++];
         rem += (b & 0x7F) * mult;
         mult *= 128;
      }
      while ((b & 0x80) != 0);

      if (rem < 3) return 0; // omitted reason code implies Success (0)
      return packet[offset + 2];
   }

   private sealed class FailingMockListener(bool failBind) : INetworkListener
   {
      private readonly TaskCompletionSource _never = new(TaskCreationOptions.RunContinuationsAsynchronously);
      public int UnbindCalls { get; private set; }
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsBound { get; private set; }
      public NetworkListenerStats Stats => default;

      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
      {
         if (failBind)
         {
            return ValueTask.FromResult<VoidResult<NetworkCodeError>>(new NetworkCodeError(1, "Simulated bind failure"));
         }
         IsBound = true;
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
      {
         UnbindCalls++;
         IsBound = false;
         _never.TrySetCanceled();
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
      {
         using var reg = ct.Register(() => _never.TrySetCanceled(ct));
         await _never.Task;
         return new NetworkCodeError(2, "Cancelled");
      }

      public ValueTask DisposeAsync()
      {
         _never.TrySetCanceled();
         return ValueTask.CompletedTask;
      }
   }

   private sealed class SingleSessionMockListener(HangingAcceptStreamSession session) : INetworkListener
   {
      private int _yielded;
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsBound { get; private set; }
      public NetworkListenerStats Stats => default;

      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
      {
         IsBound = true;
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
      {
         IsBound = false;
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
      {
         if (Interlocked.Exchange(ref _yielded, 1) == 0)
         {
            return session;
         }

         await Task.Delay(Timeout.Infinite, ct);
         return new NetworkCodeError(2, "Cancelled");
      }

      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private sealed class HangingAcceptStreamSession : INetworkSession
   {
      public TaskCompletionSource AcceptCalled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
      public TaskCompletionSource Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

      public Guid Id { get; } = Guid.NewGuid();
      public EndPoint RemoteAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public bool IsSupportingMultiplexing => false;
      public bool IsSupportingUnidirectional => false;
      public CancellationToken SessionClosedToken => CancellationToken.None;
      public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
      public NetworkStats Stats => default;
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public TransportKind Transport => TransportKind.Unknown;
      public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);
      public NetworkSessionStats SessionStats => default;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => [];

      public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
      {
         AcceptCalled.TrySetResult();
         await Task.Delay(Timeout.Infinite, ct);
         return new NetworkCodeError(3, "Timed out");
      }

      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
         => throw new NotSupportedException();

      public ValueTask DisposeAsync()
      {
         Disposed.TrySetResult();
         return ValueTask.CompletedTask;
      }
   }

   private sealed class ControlledNetworkListener : INetworkListener
   {
      private readonly Channel<ControlledConnection.ControlledSession> _acceptChannel = Channel.CreateUnbounded<ControlledConnection.ControlledSession>();
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsBound { get; private set; }
      public NetworkListenerStats Stats => default;

      public ControlledConnection AcceptControlledConnection()
      {
         var conn = new ControlledConnection();
         _acceptChannel.Writer.TryWrite(conn.TypedSession);
         return conn;
      }

      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
      {
         IsBound = true;
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
      {
         IsBound = false;
         _acceptChannel.Writer.TryComplete();
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
      {
         var session = await _acceptChannel.Reader.ReadAsync(ct);
         return session;
      }

      public ValueTask DisposeAsync()
      {
         _acceptChannel.Writer.TryComplete();
         return ValueTask.CompletedTask;
      }
   }

   private sealed class ControlledConnection
   {
      private readonly Pipe _clientToServer = new();
      private readonly Pipe _serverToClient = new();
      private readonly ControlledSession _session;
      private readonly ControlledStream _stream;

      public ControlledConnection()
      {
         _session = new ControlledSession(this);
         _stream = new ControlledStream(this);
      }

      public INetworkSession Session => _session;
      public ControlledSession TypedSession => _session;

      private TaskCompletionSource? _holdPublishStarted;
      private TaskCompletionSource? _releasePublish;
      private int _holdPublishActive;

      private TaskCompletionSource? _holdDisconnectStarted;
      private TaskCompletionSource? _releaseDisconnect;
      private int _holdDisconnectActive;

      public void HoldNextServerPublish()
      {
         _holdPublishStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         _releasePublish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         Volatile.Write(ref _holdPublishActive, 1);
      }

      public Task WaitUntilServerPublishStartedAsync() => _holdPublishStarted!.Task.WaitAsync(TimeSpan.FromSeconds(5));

      public void ReleaseHeldPublish() => _releasePublish?.TrySetResult();

      public void HoldNextServerDisconnect()
      {
         _holdDisconnectStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         _releaseDisconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         Volatile.Write(ref _holdDisconnectActive, 1);
      }

      public Task WaitUntilServerDisconnectStartedAsync() => _holdDisconnectStarted!.Task.WaitAsync(TimeSpan.FromSeconds(5));

      public void ReleaseHeldDisconnect() => _releaseDisconnect?.TrySetResult();

      public async Task SendToServerAsync(byte[] bytes)
      {
         await _clientToServer.Writer.WriteAsync(bytes);
         await _clientToServer.Writer.FlushAsync();
      }

      public async Task<byte[]> ReadNextPacketAsync(TimeSpan timeout)
      {
         using var cts = new CancellationTokenSource(timeout);
         var reader = _serverToClient.Reader;
         while (true)
         {
            var res = await reader.ReadAsync(cts.Token);
            var buf = res.Buffer;
            if (TryReadFrame(ref buf, out var frame))
            {
               reader.AdvanceTo(buf.Start);
               return frame;
            }
            reader.AdvanceTo(buf.Start, buf.End);
            if (res.IsCompleted) throw new EndOfStreamException();
         }
      }

      public async Task<byte[]> ReadPacketTypeAsync(MqttPacketType expected)
      {
         var pkt = await ReadNextPacketAsync(TimeSpan.FromSeconds(5));
         var actual = (MqttPacketType)(pkt[0] >> 4);
         if (actual != expected)
         {
            throw new InvalidOperationException($"Expected {expected}, got {actual}.");
         }
         return pkt;
      }

      private static bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out byte[] frame)
      {
         frame = [];
         if (buffer.Length < 2) return false;
         var sr = new SequenceReader<byte>(buffer);
         if (!sr.TryRead(out _)) return false;

         var multiplier = 1;
         var remaining = 0;
         byte encoded;
         do
         {
            if (!sr.TryRead(out encoded)) return false;
            remaining += (encoded & 0x7F) * multiplier;
            multiplier *= 128;
         }
         while ((encoded & 0x80) != 0);

         var headerLen = (int)sr.Consumed;
         if (buffer.Length < headerLen + remaining) return false;
         frame = buffer.Slice(0, headerLen + remaining).ToArray();
         buffer = buffer.Slice(headerLen + remaining);
         return true;
      }

      public sealed class ControlledSession(ControlledConnection conn) : INetworkSession
      {
         private int _streamReturned;
         public Guid Id { get; } = Guid.NewGuid();
         public EndPoint RemoteAddress => new IPEndPoint(IPAddress.Loopback, 12345);
         public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 1883);
         public bool IsSupportingMultiplexing => false;
         public bool IsSupportingUnidirectional => false;
         public CancellationToken SessionClosedToken => CancellationToken.None;
         public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
         public NetworkStats Stats => default;
         public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
         public TransportKind Transport => TransportKind.Unknown;
         public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);
         public NetworkSessionStats SessionStats => default;
         public IReadOnlyCollection<INetworkStream> ActiveStreams => [];

         public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
         {
            if (Interlocked.Exchange(ref _streamReturned, 1) == 0)
            {
               return conn._stream;
            }
            await Task.Delay(Timeout.Infinite, ct);
            return new NetworkCodeError(2, "Cancelled");
         }

         public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
            NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
            => throw new NotSupportedException();

         public async ValueTask DisposeAsync()
         {
            await conn._clientToServer.Writer.CompleteAsync();
            await conn._serverToClient.Writer.CompleteAsync();
         }
      }

      private sealed class ControlledStream : INetworkStream
      {
         private readonly AsyncLock _writerLock = new();
         private readonly InterceptingPipeWriter _interceptingWriter;
         private readonly DuplexPipe _transport;

         public ControlledStream(ControlledConnection conn)
         {
            Session = conn._session;
            _interceptingWriter = new InterceptingPipeWriter(conn);
            _transport = new DuplexPipe(conn._clientToServer.Reader, _interceptingWriter);
         }

         public long StreamId => 1;
         public INetworkSession Session { get; }
         public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
         public IDuplexPipe Transport => _transport;
         public NetworkStats Stats { get; set; }
         public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

         public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
            => _writerLock.LockAsync(cancellationToken);

         public ValueTask DisposeAsync() => ValueTask.CompletedTask;
      }

      private sealed class InterceptingPipeWriter(ControlledConnection conn) : PipeWriter
      {
         private readonly PipeWriter _inner = conn._serverToClient.Writer;
         public override void Advance(int bytes) => _inner.Advance(bytes);
         public override Memory<byte> GetMemory(int sizeHint = 0) => _inner.GetMemory(sizeHint);
         public override Span<byte> GetSpan(int sizeHint = 0) => _inner.GetSpan(sizeHint);
         public override void CancelPendingFlush() => _inner.CancelPendingFlush();
         public override void Complete(Exception? exception = null) => _inner.Complete(exception);

         public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
         {
            if (Volatile.Read(ref conn._holdPublishActive) == 1 && Interlocked.CompareExchange(ref conn._holdPublishActive, 0, 1) == 1)
            {
               conn._holdPublishStarted?.TrySetResult();
               await conn._releasePublish!.Task.WaitAsync(cancellationToken);
            }

            if (Volatile.Read(ref conn._holdDisconnectActive) == 1 && Interlocked.CompareExchange(ref conn._holdDisconnectActive, 0, 1) == 1)
            {
               conn._holdDisconnectStarted?.TrySetResult();
               await conn._releaseDisconnect!.Task.WaitAsync(cancellationToken);
            }

            return await _inner.FlushAsync(cancellationToken);
         }

         public override async ValueTask<FlushResult> WriteAsync(ReadOnlyMemory<byte> source, CancellationToken cancellationToken = default)
         {
            var packetType = source.Length > 0 ? (MqttPacketType)(source.Span[0] >> 4) : 0;
            if (packetType == MqttPacketType.Publish &&
                Volatile.Read(ref conn._holdPublishActive) == 1 &&
                Interlocked.CompareExchange(ref conn._holdPublishActive, 0, 1) == 1)
            {
               conn._holdPublishStarted?.TrySetResult();
               await conn._releasePublish!.Task.WaitAsync(cancellationToken);
            }

            if (packetType == MqttPacketType.Disconnect &&
                Volatile.Read(ref conn._holdDisconnectActive) == 1 &&
                Interlocked.CompareExchange(ref conn._holdDisconnectActive, 0, 1) == 1)
            {
               conn._holdDisconnectStarted?.TrySetResult();
               await conn._releaseDisconnect!.Task.WaitAsync(cancellationToken);
            }

            return await _inner.WriteAsync(source, cancellationToken);
         }
      }

      private sealed class DuplexPipe(PipeReader input, PipeWriter output) : IDuplexPipe
      {
         public PipeReader Input { get; } = input;
         public PipeWriter Output { get; } = output;
      }
   }
}
