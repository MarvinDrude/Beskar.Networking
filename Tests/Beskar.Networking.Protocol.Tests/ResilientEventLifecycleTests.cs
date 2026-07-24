using System.Buffers;
using System.Net;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
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
}
