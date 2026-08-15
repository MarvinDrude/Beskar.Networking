using System.Net;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Server;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientKeepAliveTests
{
   [Test]
   public async Task Client_KeepAlive_ShouldSendPings_And_PreventIdleTimeout()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serverOptions = new ResilientServerOptions();
      serverOptions.KeepAlive.CheckInterval = TimeSpan.FromMilliseconds(50);
      serverOptions.KeepAlive.Mode = ResilientServerKeepAliveMode.ClientConfigured;

      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>(serverOptions)
         .UseTcp(listenerEndPoint)
         .Build();

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var clientOptions = new ResilientClientOptions();
      clientOptions.KeepAlive.Enabled = true;
      clientOptions.KeepAlive.KeepAliveInterval = TimeSpan.FromMilliseconds(400);
      clientOptions.ConnectPayload.KeepAliveSeconds = 1; // 1s keep alive -> server timeout 1.5s
      clientOptions.Reconnecting.AutoReconnect = false;

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);
      var connectResult = await client.ConnectAsync(boundEndPoint);

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Wait 2.5 seconds (longer than the 1.5s server timeout) without sending application messages
      await Task.Delay(2500);

      // Verify client is still connected because automatic pings kept the session alive
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Server_KeepAliveTimeout_ShouldSendDisconnectPayload_And_TriggerClientDisconnectedEvent()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serverOptions = new ResilientServerOptions();
      serverOptions.KeepAlive.CheckInterval = TimeSpan.FromMilliseconds(50);
      serverOptions.KeepAlive.DefaultKeepAliveTime = TimeSpan.FromMilliseconds(400); // 400ms * 1.5 = 600ms timeout
      serverOptions.KeepAlive.Mode = ResilientServerKeepAliveMode.Alawys;

      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>(serverOptions)
         .UseTcp(listenerEndPoint)
         .Build();

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var clientOptions = new ResilientClientOptions();
      clientOptions.KeepAlive.Enabled = false; // Disable client pings so connection goes idle
      clientOptions.Reconnecting.AutoReconnect = false;

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);

      var disconnectTcs = new TaskCompletionSource<DisconnectPacketPayload>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnDisconnected.Add((ctx, _) =>
      {
         if (ctx.DisconnectPayload != null)
         {
            disconnectTcs.TrySetResult(ctx.DisconnectPayload);
         }
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(boundEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Wait for the server keep-alive timeout to fire and send DisconnectPacketPayload
      var payload = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(payload).IsNotNull();
      await Assert.That(payload.ReasonCode).IsEqualTo((byte)0x8D);
      await Assert.That(payload.ReasonString).IsEqualTo("KeepAlive timeout");
      await Assert.That(client.IsConnected).IsFalse();

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
