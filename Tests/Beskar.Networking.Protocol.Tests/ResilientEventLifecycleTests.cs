using System.Buffers;
using System.Net;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Server;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientEventLifecycleTests
{
   private static async Task<bool> SpinWaitUntilAsync(Func<bool> condition, TimeSpan timeout)
   {
      using var cts = new CancellationTokenSource(timeout);
      while (!cts.IsCancellationRequested)
      {
         if (condition()) return true;
         await Task.Delay(10);
      }

      return condition();
   }

   [Test]
   public async Task Server_OnStart_And_OnStop_Events_ShouldFire()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      var startFired = false;
      var stopFired = false;

      server.Events.OnStart.Add((_, _) =>
      {
         startFired = true;
         return ValueTask.CompletedTask;
      });

      server.Events.OnStop.Add((_, _) =>
      {
         stopFired = true;
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      await Assert.That(startFired).IsTrue();

      await server.StopAsync();
      await Assert.That(stopFired).IsTrue();

      await server.DisposeAsync();
   }

   [Test]
   public async Task Server_FrameReceivedAllPackets_True_ShouldReceiveControlAndMessageFrames()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serverOptions = new ResilientServerOptions
      {
         FrameReceivedAllPackets = true
      };

      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>(serverOptions)
         .UseTcp(listenerEndPoint)
         .Build();

      var receivedKinds = new List<ResilientFrameKind>();
      var lockObj = new object();

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         lock (lockObj)
         {
            receivedKinds.Add(ctx.Frame.GetFrameKind());
         }

         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      });

      await client.ConnectAsync(boundEndPoint);

      var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
         new ReadOnlySequence<byte>("TestMessage"u8.ToArray()));
      await client.SendAsync(msgFrame);

      var pingFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Ping);
      await client.SendAsync(pingFrame);

      var conditionMet = await SpinWaitUntilAsync(() =>
      {
         lock (lockObj)
         {
            return receivedKinds.Contains(ResilientFrameKind.Ping);
         }
      }, TimeSpan.FromSeconds(3));

      await Assert.That(conditionMet).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Client_Disconnected_Event_ShouldReceiveDisconnectPayload()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      await server.StartAsync();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      });

      var disconnectTcs = new TaskCompletionSource<DisconnectPacketPayload>();

      client.Events.OnDisconnected.Add((ctx, _) =>
      {
         if (ctx.DisconnectPayload != null) disconnectTcs.TrySetResult(ctx.DisconnectPayload);
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(boundEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      var serverClient = server.Clients.GetAll().First();
      var disconnectPayload = new DisconnectPacketPayload
      {
         ReasonCode = 0x42,
         ReasonString = "Server-initiated disconnect test"
      };

      await serverClient.DisconnectAsync(disconnectPayload);

      var payload = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(payload.ReasonCode).IsEqualTo((byte)0x42);
      await Assert.That(payload.ReasonString).IsEqualTo("Server-initiated disconnect test");

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Client_AutoReconnect_ShouldTriggerReconnectingEventAndReconnect()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var reconnectingFired = false;
      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromMilliseconds(50),
            MaxRetries = 3
         }
      });

      client.Events.OnReconnecting.Add((_, _) =>
      {
         reconnectingFired = true;
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(boundEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Abruptly close the server side session to trigger client disconnect & auto-reconnect
      var serverClient = server.Clients.GetAll().First();
      await serverClient.ControlStream.Transport.Output.CompleteAsync();
      await serverClient.Session.DisposeAsync();

      var conditionMet = await SpinWaitUntilAsync(() => reconnectingFired, TimeSpan.FromSeconds(3));

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await Assert.That(conditionMet).IsTrue();
   }

   [Test]
   public async Task Client_ConnectEvent_CalledAfterReconnect()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var connectedCount = 0;
      var reconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromMilliseconds(50),
            MaxRetries = 5
         }
      });

      client.Events.OnConnected.Add((_, _) =>
      {
         var count = Interlocked.Increment(ref connectedCount);
         if (count == 2)
         {
            reconnectedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(boundEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(connectedCount).IsEqualTo(1);
      await Assert.That(client.IsConnected).IsTrue();

      // Abruptly close server side session to trigger reconnect
      var serverClient = server.Clients.GetAll().First();
      await serverClient.ControlStream.Transport.Output.CompleteAsync();
      await serverClient.Session.DisposeAsync();

      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(connectedCount).IsEqualTo(2);
      await Assert.That(client.IsConnected).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Client_InitialConnectFails_RetriesInBackgroundWhenAutoReconnectEnabled()
   {
      // Pick an unused endpoint
      var portFinderListener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
      portFinderListener.Start();
      var unusedEndPoint = (IPEndPoint)portFinderListener.LocalEndpoint;
      portFinderListener.Stop();

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromMilliseconds(50),
            MaxRetries = 10
         }
      });

      var connectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnConnected.Add((_, _) =>
      {
         connectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(unusedEndPoint);
      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();

      // Now start a server on that endpoint
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(unusedEndPoint)
         .Build();

      await server.StartAsync();

      // Background auto-reconnect loop should retry and connect successfully
      await connectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(client.IsConnected).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
