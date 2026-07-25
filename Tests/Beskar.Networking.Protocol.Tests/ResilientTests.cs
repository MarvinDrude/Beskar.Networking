using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Text;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientTests
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
   public async Task AutoReconnect_ShouldSuccessfullyReconnect_WhenServerAvailable()
   {
      var endpoint = new MemoryEndPoint($"bug1_reconnect_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromMilliseconds(50),
            MaxRetries = 5
         }
      };

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);

      var connectedTcs = new TaskCompletionSource();
      var reconnectedTcs = new TaskCompletionSource();
      var connectCount = 0;

      client.Events.OnConnected.Add((_, _) =>
      {
         var count = Interlocked.Increment(ref connectCount);
         if (count == 1) connectedTcs.TrySetResult();
         else if (count == 2) reconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint);
      await connectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(client.IsConnected).IsTrue();

      // Drop server client session abruptly to trigger auto-reconnect
      var serverClient = server.Clients.GetAll().First();
      await serverClient.ControlStream.Transport.Output.CompleteAsync();
      await serverClient.Session.DisposeAsync();

      // Wait for auto-reconnect to successfully complete
      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.State).IsEqualTo(ResilientClientState.Connected);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ServerReaderLoop_ShouldNotDeadlock_WhenMessageSentBeforeHandshakeComplete()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      var messageReceivedTcs = new TaskCompletionSource<string>();

      server.Events.OnConnect.Add(async (ctx, ct) =>
      {
         // Simulate slow authentication check
         await Task.Delay(100, ct);
      });

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         messageReceivedTcs.TrySetResult(text);
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      // Send Connect frame AND Message frame simultaneously in one write
      using var connectWriter = new Beskar.Networking.Protocol.Utilities.PooledBufferWriter();
      var connectPayload = new ConnectPacketPayload();
      var len = connectPayload.GetEncodedLength();
      if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bytesWritten))
      {
         connectWriter.Advance(bytesWritten);
      }

      var connectFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Connect, new ReadOnlySequence<byte>(connectWriter.WrittenMemory));
      var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>("HelloEarlyMessage"u8.ToArray()));

      using var streamWriter = new Beskar.Networking.Protocol.Utilities.PooledBufferWriter();
      connectFrame.WriteTo(streamWriter);
      msgFrame.WriteTo(streamWriter);

      await socket.SendAsync(streamWriter.WrittenMemory);

      var receivedText = await messageReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedText).IsEqualTo("HelloEarlyMessage");

      socket.Dispose();
      await server.StopAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task DisconnectAsync_ShouldCancelReconnectDelayImmediately()
   {
      var endpoint = new MemoryEndPoint($"bug3_reconnect_cancel_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromSeconds(30), // Long retry interval
            MaxRetries = 5
         }
      };

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      var reconnectingTcs = new TaskCompletionSource();

      client.Events.OnReconnecting.Add((_, _) =>
      {
         reconnectingTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint);

      // Stop server to trigger reconnection loop
      await server.StopAsync();
      await reconnectingTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

      await Assert.That(client.State).IsEqualTo(ResilientClientState.Reconnecting);

      var sw = Stopwatch.StartNew();
      await client.DisconnectAsync();
      sw.Stop();

      // Disconnect should finish in milliseconds, NOT wait 30 seconds!
      await Assert.That(sw.ElapsedMilliseconds).IsLessThan(2000);
      await Assert.That(client.State).IsEqualTo(ResilientClientState.Disconnected);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task KeepAliveService_ShouldOnlyRemoveAndDisconnectClientOnce()
   {
      var endpoint = new MemoryEndPoint($"bug4_keepalive_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverOptions = new ResilientServerOptions
      {
         KeepAlive = new ResilientServerKeepAliveOptions
         {
            Mode = ResilientServerKeepAliveMode.Alawys,
            CheckInterval = TimeSpan.FromMilliseconds(50),
            DefaultKeepAliveTime = TimeSpan.FromMilliseconds(100)
         }
      };

      var server = new ResilientServer<BeskarPacket>([listener], serverOptions);
      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: new ResilientClientOptions
      {
         KeepAlive = new ResilientClientKeepAliveOptions { Enabled = false }
      });

      await client.ConnectAsync(endpoint);
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      // Client stops activity -> keep alive service will disconnect it after timeout
      var serverCleanedUp = await SpinWaitUntilAsync(() => server.Clients.Count == 0, TimeSpan.FromSeconds(3));
      await Assert.That(serverCleanedUp).IsTrue();

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task OnConnected_ShouldNotBeCancelled_ByTransientHandshakeTimeout()
   {
      var endpoint = new MemoryEndPoint($"bug5_onconnected_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         HandshakeTimeout = TimeSpan.FromMilliseconds(200), // Short handshake timeout
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      var onConnectedCompleted = false;

      client.Events.OnConnected.Add(async (ctx, ct) =>
      {
         // Event handler takes 400ms (longer than HandshakeTimeout 200ms)
         await Task.Delay(400, ct);
         onConnectedCompleted = true;
      });

      var connectResult = await client.ConnectAsync(endpoint);

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(onConnectedCompleted).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ClientState_ShouldTransitionToDisconnected_WhenServerDisconnectsClient()
   {
      var endpoint = new MemoryEndPoint($"state_disc_test_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      await client.ConnectAsync(endpoint);
      await Assert.That(client.IsConnected).IsTrue();

      var serverClient = server.Clients.GetAll().First();
      await serverClient.DisconnectAsync(new DisconnectPacketPayload { ReasonString = "Disconnect test" });

      await SpinWaitUntilAsync(() => client.State == ResilientClientState.Disconnected, TimeSpan.FromSeconds(3));

      await Assert.That(client.State).IsEqualTo(ResilientClientState.Disconnected);

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ConnectAsync_ConcurrentCalls_ShouldFailSafelyWithSingleActiveConnection()
   {
      var endpoint = new MemoryEndPoint($"concurrent_connect_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      var connectTask1 = Task.Run(() => client.ConnectAsync(endpoint));
      var connectTask2 = Task.Run(() => client.ConnectAsync(endpoint));

      var results = await Task.WhenAll(connectTask1, connectTask2);

      var successCount = results.Count(r => !r.Failed);
      var failureCount = results.Count(r => r.Failed);

      await Assert.That(successCount).IsEqualTo(1);
      await Assert.That(failureCount).IsEqualTo(1);
      await Assert.That(client.IsConnected).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task PreHandshakeFrames_ShouldProcessInStrictFIFOOrder()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      var receivedSequence = new List<int>();
      var lockObj = new object();

      server.Events.OnConnect.Add(async (ctx, ct) =>
      {
         await Task.Delay(150, ct); // Delay handshake completion
      });

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         if (int.TryParse(text, out var num))
         {
            lock (lockObj) receivedSequence.Add(num);
         }
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      using var streamWriter = new Beskar.Networking.Protocol.Utilities.PooledBufferWriter();

      var connectPayload = new ConnectPacketPayload();
      var len = connectPayload.GetEncodedLength();
      using var connectWriter = new Beskar.Networking.Protocol.Utilities.PooledBufferWriter(len);
      if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bytesWritten))
      {
         connectWriter.Advance(bytesWritten);
      }

      BeskarPacket.CreateFrame(ResilientFrameKind.Connect, new ReadOnlySequence<byte>(connectWriter.WrittenMemory)).WriteTo(streamWriter);

      for (var i = 1; i <= 5; i++)
      {
         var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(i.ToString())));
         msgFrame.WriteTo(streamWriter);
      }

      await socket.SendAsync(streamWriter.WrittenMemory);

      var allReceived = await SpinWaitUntilAsync(() =>
      {
         lock (lockObj) return receivedSequence.Count == 5;
      }, TimeSpan.FromSeconds(5));

      await Assert.That(allReceived).IsTrue();
      int[] snapshot;
      lock (lockObj) snapshot = receivedSequence.ToArray();
      await Assert.That(snapshot).IsEquivalentTo(new[] { 1, 2, 3, 4, 5 });

      socket.Dispose();
      await server.StopAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ServerClient_ManualDispose_ShouldRemoveFromServerClients()
   {
      var endpoint = new MemoryEndPoint($"server_client_dispose_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      await client.ConnectAsync(endpoint);

      await SpinWaitUntilAsync(() => server.Clients.Count == 1, TimeSpan.FromSeconds(3));
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      var serverClient = server.Clients.GetAll().First();
      await serverClient.DisposeAsync();

      await SpinWaitUntilAsync(() => server.Clients.Count == 0, TimeSpan.FromSeconds(3));
      await Assert.That(server.Clients.Count).IsEqualTo(0);

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task AutoReconnect_DisconnectDuringRetry_ShouldFireOnDisconnectedOnlyOnce()
   {
      var endpoint = new MemoryEndPoint($"reconnect_disconnect_once_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromMilliseconds(50),
            MaxRetries = 10
         }
      };

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      var disconnectedCount = 0;
      client.Events.OnDisconnected.Add((_, _) =>
      {
         Interlocked.Increment(ref disconnectedCount);
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint);
      await Assert.That(client.IsConnected).IsTrue();

      await server.StopAsync(); // Triggers auto-reconnect loop

      await SpinWaitUntilAsync(() => client.State == ResilientClientState.Reconnecting, TimeSpan.FromSeconds(3));

      await client.DisconnectAsync();
      await Task.Delay(200);

      await Assert.That(disconnectedCount).IsEqualTo(1);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
