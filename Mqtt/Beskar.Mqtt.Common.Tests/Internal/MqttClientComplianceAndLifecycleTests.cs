using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Internal;

public sealed class MqttClientComplianceAndLifecycleTests
{
   [Test]
   [NotInParallel]
   public async Task PublishAsync_BlockedOnReceiveMaximum_DisconnectShouldFailCleanlyWithoutObjectDisposedException()
   {
      var holdFirstPubAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var firstPublishSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      var harness = new MockBrokerHarness(async (h, packetType, packetBytes, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success,
                  ReceiveMaximum = 1,
                  MaximumQualityOfService = QualityOfServiceType.AtLeastOnce
               }));
               break;

            case MqttPacketType.Publish:
               firstPublishSeen.TrySetResult();
               var pktId = ReadPublishPacketId(packetBytes);
               _ = Task.Run(async () =>
               {
                  await holdFirstPubAck.Task;
                  await sendPacket(EncodePubAck(pktId));
               });
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("inflight-sem-client")
         .Build());
      await Assert.That(connectResult.Failed).IsFalse();

      var pub1Task = client.PublishAsync(PublishOptions.Create()
         .WithTopic("flow/one")
         .WithPayload("first")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      await firstPublishSeen.Task.WaitAsync(TimeSpan.FromSeconds(5));

      var queuedTasks = Enumerable.Range(2, 8).Select(i => client.PublishAsync(PublishOptions.Create()
         .WithTopic($"flow/{i}")
         .WithPayload($"msg-{i}")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build())).ToArray();

      // Give queued publishers time to enter _inFlightSemaphore.WaitAsync
      await Task.Delay(50);

      await client.DisconnectAsync(new DisconnectOptions());

      Exception? uncaughtException = null;
      Result<global::Beskar.Mqtt.Protocol.Results.PublishResult, StringError>[]? results = null;
      try
      {
         results = await Task.WhenAll(queuedTasks).WaitAsync(TimeSpan.FromMilliseconds(400));
      }
      catch (Exception ex)
      {
         uncaughtException = ex;
      }
      finally
      {
         holdFirstPubAck.TrySetResult();
      }

      await Assert.That(uncaughtException).IsNull();
      await Assert.That(results).IsNotNull();
      foreach (var res in results!)
      {
         await Assert.That(res.Failed).IsTrue();
      }
   }

   [Test]
   public async Task ConnectAsync_ConcurrentDisconnectAndTopicAliasUpdates_ShouldNotThrowObjectDisposedOrCollectionExceptions()
   {
      var harness = new MockBrokerHarness(async (_, packetType, _, sendPacket) =>
      {
         if (packetType == MqttPacketType.Connect)
         {
            await sendPacket(EncodeConnAck(new ConnAckPacket
            {
               ReasonCode = ConnectReasonCode.Success,
               TopicAliasMaximum = 16
            }));
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

      client.AddConnectingHandler(async (_, ct) =>
      {
         await Task.Yield();
      });

      var aliasWriter = Task.Run(() =>
      {
         ushort alias = 1;
         while (!stopCts.IsCancellationRequested)
         {
            client.SetTopicAlias(alias, "sensors/temp"u8.ToArray());
            client.TryGetTopicAlias(alias, out _);
            alias = (ushort)((alias % 16) + 1);
         }
      });

      var connector = Task.Run(async () =>
      {
         while (!stopCts.IsCancellationRequested)
         {
            await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
               .WithProtocolVersion(MqttProtocolVersion.V50)
               .WithClientId("race-client")
               .WithTopicAliasMaximum(16)
               .Build(), stopCts.Token);
         }
      });

      var disconnector = Task.Run(async () =>
      {
         while (!stopCts.IsCancellationRequested)
         {
            await client.DisconnectAsync(new DisconnectOptions(), stopCts.Token);
         }
      });

      Exception? captured = null;
      try
      {
         await Task.WhenAll(aliasWriter, connector, disconnector);
      }
      catch (Exception ex) when (ex is not OperationCanceledException)
      {
         captured = ex;
      }

      await Assert.That(captured).IsNull();
   }

   [Test]
   public async Task ConnectAsync_WhenServerKeepAliveIsZero_ShouldDisableKeepAlivePings()
   {
      var pingCount = 0;
      var harness = new MockBrokerHarness(async (_, packetType, _, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success,
                  PropertiesBytes = new ReadOnlySequence<byte>(new byte[] { (byte)PropertyIdentifier.ServerKeepAlive, 0x00, 0x00 })
               }));
               break;

            case MqttPacketType.PingReq:
               Interlocked.Increment(ref pingCount);
               await sendPacket(EncodePingResp());
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("zero-keepalive-client")
         .WithKeepAlivePeriod(1)
         .Build());

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(connectResult.Success!.ServerKeepAlive).IsEqualTo((ushort)0);

      await Task.Delay(1600);

      await Assert.That(Volatile.Read(ref pingCount)).IsEqualTo(0);
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ConnectAsync_WithServerAssignedClientIdentifier_ShouldUpdateCurrentConnectOptions()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      await using var client = (MqttClient)MqttClientFactory.CreateTcp();
      var connectResult = await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = ReadOnlyMemory<byte>.Empty,
         CleanSession = true
      });

      await Assert.That(connectResult.Failed).IsFalse();
      var assigned = connectResult.Success!.AssignedClientIdentifier;
      await Assert.That(assigned).IsNotNull().And.IsNotEmpty();

      await Assert.That(Encoding.UTF8.GetString(client.CurrentConnectOptions.ClientIdUtf8Bytes.Span))
         .IsEqualTo(assigned!);

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ConnectAsync_WhenServerReturnsServerReference_ShouldPreserveServerReferenceInError()
   {
      const string expectedServerReference = "alt-broker.internal:1883";
      var harness = new MockBrokerHarness(async (_, packetType, _, sendPacket) =>
      {
         if (packetType == MqttPacketType.Connect)
         {
            await sendPacket(EncodeConnAck(new ConnAckPacket
            {
               ReasonCode = ConnectReasonCode.ServerMoved,
               ServerReferenceUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(expectedServerReference))
            }));
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("redirect-client")
         .Build());

      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(connectResult.Error.Detail).Contains(expectedServerReference);
   }

   [Test]
   public async Task PublishAsync_WhenServerDisablesRetain_ShouldRejectRetainedPublish()
   {
      var sentPublishCount = 0;
      var harness = new MockBrokerHarness(async (_, packetType, _, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success,
                  IsRetainAvailable = false
               }));
               break;

            case MqttPacketType.Publish:
               Interlocked.Increment(ref sentPublishCount);
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("no-retain-client")
         .Build());
      await Assert.That(connectResult.Failed).IsFalse();

      var pubResult = await client.PublishAsync(PublishOptions.Create()
         .WithTopic("cap/retain")
         .WithPayload("hello")
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .WithRetain(true)
         .Build());

      await Assert.That(pubResult.Failed).IsTrue();
      await Assert.That(Volatile.Read(ref sentPublishCount)).IsEqualTo(0);
   }

   [Test]
   public async Task PublishAsync_WhenQosExceedsServerMaximumQos_ShouldRejectPublish()
   {
      var sentPublishCount = 0;
      var harness = new MockBrokerHarness(async (h, packetType, packetBytes, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success,
                  MaximumQualityOfService = QualityOfServiceType.AtMostOnce
               }));
               break;

            case MqttPacketType.Publish:
               Interlocked.Increment(ref sentPublishCount);
               var pktId = ReadPublishPacketId(packetBytes);
               await sendPacket(EncodePubAck(pktId));
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("max-qos-client")
         .Build());
      await Assert.That(connectResult.Failed).IsFalse();

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
      var pubResult = await client.PublishAsync(PublishOptions.Create()
         .WithTopic("cap/qos")
         .WithPayload("hello")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build(), cts.Token);

      await Assert.That(pubResult.Failed).IsTrue();
      await Assert.That(Volatile.Read(ref sentPublishCount)).IsEqualTo(0);
   }

   [Test]
   public async Task SubscribeAsync_WhenWildcardSubscriptionsUnavailable_ShouldRejectWildcardFilter()
   {
      var sentSubscribeCount = 0;
      var harness = new MockBrokerHarness(async (h, packetType, packetBytes, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success,
                  IsWildcardSubscriptionAvailable = false
               }));
               break;

            case MqttPacketType.Subscribe:
               Interlocked.Increment(ref sentSubscribeCount);
               var subId = ReadPacketIdAtVariableHeader(packetBytes);
               await sendPacket(EncodeSubAck(subId, SubscribeReasonCode.GrantedQos1));
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("no-wildcard-client")
         .Build());
      await Assert.That(connectResult.Failed).IsFalse();

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
      var subResult = await client.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("sensors/#", QualityOfServiceType.AtLeastOnce)
         .Build(), cts.Token);

      await Assert.That(subResult.Failed).IsTrue();
      await Assert.That(Volatile.Read(ref sentSubscribeCount)).IsEqualTo(0);
   }

   [Test]
   public async Task PublishAsync_WhenPacketExceedsServerMaximumPacketSize_ShouldFailWithoutTransmitting()
   {
      var sentPublishCount = 0;
      var harness = new MockBrokerHarness(async (_, packetType, _, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success,
                  MaximumPacketSize = 32
               }));
               break;

            case MqttPacketType.Publish:
               Interlocked.Increment(ref sentPublishCount);
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var connectResult = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("max-packet-client")
         .Build());
      await Assert.That(connectResult.Failed).IsFalse();

      var pubResult = await client.PublishAsync(PublishOptions.Create()
         .WithTopic("cap/size")
         .WithPayload(new byte[128])
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .Build());

      await Assert.That(pubResult.Failed).IsTrue();
      await Assert.That(Volatile.Read(ref sentPublishCount)).IsEqualTo(0);
   }

   [Test]
   public async Task SubscribeTopicAsync_WhenBrokerRejectsWithNotAuthorized_ShouldNotRetainOrResubscribeOrphanedSink()
   {
      var subscribeAttempts = 0;
      var harness = new MockBrokerHarness(async (h, packetType, packetBytes, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success
               }));
               break;

            case MqttPacketType.Subscribe:
               Interlocked.Increment(ref subscribeAttempts);
               var subId = ReadPacketIdAtVariableHeader(packetBytes);
               await sendPacket(EncodeSubAck(subId, SubscribeReasonCode.NotAuthorized));
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      var options = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("sub-rejected-client")
         .Build();

      var res1 = await client.ConnectAsync(options);
      await Assert.That(res1.Failed).IsFalse();

      Exception? subscribeException = null;
      try
      {
         await client.SubscribeTopicAsync(
            "restricted/topic",
            (_, _) => ValueTask.CompletedTask,
            QualityOfServiceType.AtLeastOnce);
      }
      catch (Exception ex)
      {
         subscribeException = ex;
      }

      await client.DisconnectAsync(new DisconnectOptions());

      var res2 = await client.ConnectAsync(options);
      await Assert.That(res2.Failed).IsFalse();

      // Wait briefly to allow OnClientConnectedAsync auto-resubscribe to run if orphaned sink remained
      await Task.Delay(150);

      await Assert.That(Volatile.Read(ref subscribeAttempts)).IsEqualTo(1);
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task SubscribeStream_WhenBrokerRejectsSubscription_ShouldFailInsteadOfHanging()
   {
      var harness = new MockBrokerHarness(async (h, packetType, packetBytes, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success
               }));
               break;

            case MqttPacketType.Subscribe:
               var subId = ReadPacketIdAtVariableHeader(packetBytes);
               await sendPacket(EncodeSubAck(subId, SubscribeReasonCode.NotAuthorized));
               break;

            case MqttPacketType.Unsubscribe:
               var unsubId = ReadPacketIdAtVariableHeader(packetBytes);
               await sendPacket(EncodeUnsubAck(unsubId, UnsubscribeReasonCode.Success));
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("stream-denied-client")
         .Build());

      Exception? streamError = null;
      var completedOrFailed = false;

      var stream = client.SubscribeStream("denied/stream");
      await using var enumerator = stream.GetAsyncEnumerator();
      using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
      try
      {
         completedOrFailed = !await enumerator.MoveNextAsync().AsTask().WaitAsync(timeoutCts.Token);
      }
      catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
      {
         // Timed out waiting because SubscribeStream swallowed NotAuthorized
      }
      catch (Exception ex)
      {
         streamError = ex;
         completedOrFailed = true;
      }

      await Assert.That(completedOrFailed || streamError is not null).IsTrue();
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task SubscribeTopicAsync_ConcurrentSubscribeWhileInitialSubAckPending_ShouldNotCompleteBeforeBrokerAck()
   {
      var subSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseSubAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      var harness = new MockBrokerHarness(async (h, packetType, packetBytes, sendPacket) =>
      {
         switch (packetType)
         {
            case MqttPacketType.Connect:
               await sendPacket(EncodeConnAck(new ConnAckPacket
               {
                  ReasonCode = ConnectReasonCode.Success
               }));
               break;

            case MqttPacketType.Subscribe:
               var subId = ReadPacketIdAtVariableHeader(packetBytes);
               subSent.TrySetResult();
               _ = Task.Run(async () =>
               {
                  await releaseSubAck.Task;
                  await sendPacket(EncodeSubAck(subId, SubscribeReasonCode.NotAuthorized));
               });
               break;
         }
      });

      await using var client = new MqttClient(harness.CreateClient());
      await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("sub-race-client")
         .Build());

      var sub1Task = client.SubscribeTopicAsync("race/topic", (_, _) => ValueTask.CompletedTask);
      await subSent.Task.WaitAsync(TimeSpan.FromSeconds(5));

      var sub2Task = client.SubscribeTopicAsync("race/topic", (_, _) => ValueTask.CompletedTask);
      await Task.Delay(50);

      // Concurrent caller must wait for broker SUBACK instead of returning immediately while SUBACK is pending/rejected
      var sub2CompletedBeforeSubAck = sub2Task.IsCompleted;

      releaseSubAck.TrySetResult();
      try { await sub1Task.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
      try { await sub2Task.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }

      await Assert.That(sub2CompletedBeforeSubAck).IsFalse();
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task MidSessionAuth_ClientSendingReAuthenticateToServer_ShouldRespondOrDisconnectWithProtocolError()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      await using var client = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectedTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.AddDisconnectedHandler((ctx, _) =>
      {
         disconnectedTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      var connectRes = await client.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("mid-auth-client")
         .Build());
      await Assert.That(connectRes.Failed).IsFalse();

      await client.SendAsync(new AuthPacket
      {
         ReasonCode = AuthenticateReasonCode.ReAuthenticate,
         AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>("SCRAM-SHA-256"u8.ToArray())
      });

      var disconnected = await Task.WhenAny(disconnectedTcs.Task, Task.Delay(800)) == disconnectedTcs.Task;
      await Assert.That(disconnected).IsTrue();
   }

   [Test]
   public async Task MidSessionAuth_UnsolicitedAuthFromServer_ShouldInvokeAuthenticationHandlerOrDisconnect()
   {
      var authHandlerCalls = 0;
      MockBrokerHarness? capturedHarness = null;

      capturedHarness = new MockBrokerHarness(async (_, packetType, _, sendPacket) =>
      {
         if (packetType == MqttPacketType.Connect)
         {
            await sendPacket(EncodeConnAck(new ConnAckPacket
            {
               ReasonCode = ConnectReasonCode.Success,
               AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>("SCRAM-SHA-256"u8.ToArray())
            }));
         }
      });

      await using var client = new MqttClient(capturedHarness.CreateClient());
      var handler = new CountingAuthHandler(() => Interlocked.Increment(ref authHandlerCalls));
      var disconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      client.AddDisconnectedHandler((_, _) =>
      {
         disconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var options = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("mid-auth-handler-client")
         .WithAuthenticationMethod("SCRAM-SHA-256")
         .Build();
      options.AuthenticationHandler = handler;

      var connectRes = await client.ConnectAsync(options);
      await Assert.That(connectRes.Failed).IsFalse();

      await capturedHarness.SendToClientAsync(EncodeAuth(new AuthPacket
      {
         ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
         AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>("SCRAM-SHA-256"u8.ToArray()),
         AuthenticationDataBytes = new ReadOnlySequence<byte>("challenge-2"u8.ToArray())
      }));

      await Task.Delay(250);

      var wasHandledOrDisconnected = Volatile.Read(ref authHandlerCalls) > 0 || disconnectedTcs.Task.IsCompleted;
      await Assert.That(wasHandledOrDisconnected).IsTrue();
   }

   private sealed class CountingAuthHandler(Action onCalled) : IMqttAuthenticationHandler
   {
      public Task<VoidResult<StringError>> ExecuteAsync(MqttAuthContext context, CancellationToken ct = default)
      {
         onCalled();
         return Task.FromResult<VoidResult<StringError>>(true);
      }
   }

   private static byte[] EncodeConnAck(ConnAckPacket packet)
   {
      var buffer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(buffer).WriteConnAck(in packet);
      return buffer.WrittenSpan.ToArray();
   }

   private static byte[] EncodePubAck(ushort packetId)
   {
      var buffer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(buffer).WritePubAck(new PubAckPacket
      {
         PacketIdentifier = packetId,
         ReasonCode = PubAckReasonCode.Success
      });
      return buffer.WrittenSpan.ToArray();
   }

   private static byte[] EncodeSubAck(ushort packetId, SubscribeReasonCode reasonCode)
   {
      var buffer = new ArrayBufferWriter<byte>();
      var rc = new byte[] { (byte)reasonCode };
      new PacketVersion5Encoder(buffer).WriteSubAck(new SubAckPacket
      {
         PacketIdentifier = packetId,
         ReturnCodesBytes = rc
      });
      return buffer.WrittenSpan.ToArray();
   }

   private static byte[] EncodeUnsubAck(ushort packetId, UnsubscribeReasonCode reasonCode)
   {
      var buffer = new ArrayBufferWriter<byte>();
      var rc = new byte[] { (byte)reasonCode };
      new PacketVersion5Encoder(buffer).WriteUnsubAck(new UnsubAckPacket
      {
         PacketIdentifier = packetId,
         ReasonCodesBytes = rc
      });
      return buffer.WrittenSpan.ToArray();
   }

   private static byte[] EncodePingResp()
   {
      var buffer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(buffer).WritePingResp(new PingRespPacket());
      return buffer.WrittenSpan.ToArray();
   }

   private static byte[] EncodeAuth(AuthPacket packet)
   {
      var buffer = new ArrayBufferWriter<byte>();
      new PacketVersion5Encoder(buffer).WriteAuth(in packet);
      return buffer.WrittenSpan.ToArray();
   }

   private static ushort ReadPacketIdAtVariableHeader(ReadOnlySpan<byte> packet)
   {
      var offset = 1;
      byte b;
      do
      {
         b = packet[offset++];
      }
      while ((b & 0x80) != 0);

      return (ushort)((packet[offset] << 8) | packet[offset + 1]);
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

   private sealed class MockBrokerHarness(Func<MockBrokerHarness, MqttPacketType, byte[], Func<byte[], Task>, Task> onClientPacket)
   {
      private MockStream? _activeStream;
      private readonly AsyncLock _writeLock = new();

      public INetworkClient CreateClient() => new MockNetworkClient(this);

      public async Task SendToClientAsync(byte[] bytes)
      {
         if (_activeStream is { } stream)
         {
            using (await _writeLock.LockAsync())
            {
               await stream.ServerToClientWriter.WriteAsync(bytes);
               await stream.ServerToClientWriter.FlushAsync();
            }
         }
      }

      private async Task PumpClientPacketsAsync(MockStream stream)
      {
         var reader = stream.ClientToServerReader;
         try
         {
            while (true)
            {
               var result = await reader.ReadAsync();
               var buffer = result.Buffer;
               if (result.IsCanceled || (buffer.IsEmpty && result.IsCompleted)) break;

               var consumed = buffer.Start;
               var examined = buffer.End;

               while (TryReadFrame(ref buffer, out var frame))
               {
                  var packetType = (MqttPacketType)(frame[0] >> 4);
                  await onClientPacket(this, packetType, frame, SendToClientAsync);
                  consumed = buffer.Start;
               }

               reader.AdvanceTo(consumed, examined);
               if (result.IsCompleted) break;
            }
         }
         catch
         {
            // ignored on teardown
         }
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

      private sealed class MockNetworkClient(MockBrokerHarness harness) : INetworkClient
      {
         public TransportKind Transport => TransportKind.Unknown;
         public bool IsConnected => Session is not null;
         public NetworkClientStats Stats => default;
         public INetworkSession? Session { get; private set; }
         public EndPoint? LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
         public EndPoint? RemoteAddress => new IPEndPoint(IPAddress.Loopback, 1883);

         public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
         {
            var session = new MockSession(harness);
            Session = session;
            return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(session);
         }

         public async ValueTask DisconnectAsync(CancellationToken ct = default)
         {
            if (Session is { } session)
            {
               Session = null;
               await session.DisposeAsync();
            }
         }

         public ValueTask DisposeAsync() => DisconnectAsync();
      }

      private sealed class MockSession(MockBrokerHarness harness) : INetworkSession
      {
         public Guid Id { get; } = Guid.NewGuid();
         public EndPoint RemoteAddress => new IPEndPoint(IPAddress.Loopback, 1883);
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

         public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
            => throw new NotSupportedException();

         public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
            NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
         {
            var stream = new MockStream(this);
            harness._activeStream = stream;
            _ = Task.Run(() => harness.PumpClientPacketsAsync(stream));
            return ValueTask.FromResult<Result<INetworkStream, NetworkCodeError>>(stream);
         }

         public async ValueTask DisposeAsync()
         {
            if (harness._activeStream is { } stream)
            {
               await stream.DisposeAsync();
            }
         }
      }

      private sealed class MockStream : INetworkStream
      {
         private readonly AsyncLock _clientWriterLock = new();
         private readonly Pipe _clientToServer = new();
         private readonly Pipe _serverToClient = new();
         private readonly DuplexPipePair _transport;

         public MockStream(INetworkSession session)
         {
            Session = session;
            _transport = new DuplexPipePair(_serverToClient.Reader, _clientToServer.Writer);
         }

         public long StreamId => 1;
         public INetworkSession Session { get; }
         public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
         public IDuplexPipe Transport => _transport;
         public NetworkStats Stats { get; set; }
         public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

         public PipeReader ClientToServerReader => _clientToServer.Reader;
         public PipeWriter ServerToClientWriter => _serverToClient.Writer;

         public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
            => _clientWriterLock.LockAsync(cancellationToken);

         public async ValueTask DisposeAsync()
         {
            await _clientToServer.Writer.CompleteAsync();
            await _serverToClient.Writer.CompleteAsync();
         }
      }

      private sealed class DuplexPipePair(PipeReader input, PipeWriter output) : IDuplexPipe
      {
         public PipeReader Input { get; } = input;
         public PipeWriter Output { get; } = output;
      }
   }
}
