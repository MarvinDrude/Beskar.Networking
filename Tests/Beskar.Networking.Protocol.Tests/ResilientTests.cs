using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;
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

      var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      // Send Connect frame AND Message frame simultaneously in one write
      using var connectWriter = new PooledBufferWriter();
      var connectPayload = new ConnectPacketPayload();
      var len = connectPayload.GetEncodedLength();
      if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bytesWritten))
         connectWriter.Advance(bytesWritten);

      var connectFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Connect,
         new ReadOnlySequence<byte>(connectWriter.WrittenMemory));
      var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
         new ReadOnlySequence<byte>("HelloEarlyMessage"u8.ToArray()));

      using var streamWriter = new PooledBufferWriter();
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
            lock (lockObj)
            {
               receivedSequence.Add(num);
            }

         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      using var streamWriter = new PooledBufferWriter();

      var connectPayload = new ConnectPacketPayload();
      var len = connectPayload.GetEncodedLength();
      using var connectWriter = new PooledBufferWriter(len);
      if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bytesWritten))
         connectWriter.Advance(bytesWritten);

      BeskarPacket.CreateFrame(ResilientFrameKind.Connect, new ReadOnlySequence<byte>(connectWriter.WrittenMemory))
         .WriteTo(streamWriter);

      for (var i = 1; i <= 5; i++)
      {
         var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(i.ToString())));
         msgFrame.WriteTo(streamWriter);
      }

      await socket.SendAsync(streamWriter.WrittenMemory);

      var allReceived = await SpinWaitUntilAsync(() =>
      {
         lock (lockObj)
         {
            return receivedSequence.Count == 5;
         }
      }, TimeSpan.FromSeconds(5));

      await Assert.That(allReceived).IsTrue();
      int[] snapshot;
      lock (lockObj)
      {
         snapshot = receivedSequence.ToArray();
      }

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

   [Test]
   public async Task AutoReconnect_ShouldNotThrowObjectDisposedException_OnSuccessfulReconnect()
   {
      var endpoint = new MemoryEndPoint($"reconnect_ode_{Guid.NewGuid():N}");
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

      await client.ConnectAsync(endpoint);
      await Assert.That(client.IsConnected).IsTrue();

      var reconnectedTcs = new TaskCompletionSource();
      client.Events.OnConnected.Add((ctx, _) =>
      {
         if (ctx.Client.State == ResilientClientState.Connected) reconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Kill the connection to force a reconnect
      var serverClient = server.Clients.GetAll().First();
      await serverClient.Session.DisposeAsync();

      // Wait for reconnect to succeed
      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(client.IsConnected).IsTrue();

      // Ensure sending still works and doesn't throw ODE
      var payload = "TestAfterReconnect"u8.ToArray();
      var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payload));

      var sendSuccessful = true;
      try
      {
         await client.SendAsync(frame);
      }
      catch (Exception)
      {
         sendSuccessful = false;
      }

      await Assert.That(sendSuccessful).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task DisposeAsync_ShouldStopBackgroundReconnectLoopCleanly()
   {
      var endpoint = new MemoryEndPoint($"dispose_reconnect_{Guid.NewGuid():N}");
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
      var reconnectingTcs = new TaskCompletionSource();
      client.Events.OnReconnecting.Add((_, _) =>
      {
         reconnectingTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint);

      // Stop server to trigger reconnect
      await server.StopAsync();
      await reconnectingTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

      await Assert.That(client.State).IsEqualTo(ResilientClientState.Reconnecting);

      // Dispose client
      var sw = Stopwatch.StartNew();
      await client.DisposeAsync();
      sw.Stop();

      // Verify it exits quickly and state is Disconnected
      await Assert.That(sw.ElapsedMilliseconds).IsLessThan(2000);
      await Assert.That(client.State).IsEqualTo(ResilientClientState.Disconnected);

      await server.DisposeAsync();
   }

   [Test]
   public async Task Client_ShouldNotFireFrameReceived_BeforeConnected()
   {
      var endpoint = new MemoryEndPoint($"early_client_frame_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      var isConnectedDuringFrame = false;
      var frameReceived = false;

      client.Events.FrameReceived.Add((ctx, _) =>
      {
         frameReceived = true;
         if (ctx.Client.State == ResilientClientState.Connected) isConnectedDuringFrame = true;
         return ValueTask.CompletedTask;
      });

      var connectionTcs = new TaskCompletionSource();
      client.Events.OnConnected.Add((_, _) =>
      {
         connectionTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint);

      // Send an early message from the server client immediately
      var serverClient = server.Clients.GetAll().First();
      var payload = "EarlyMessage"u8.ToArray();
      var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payload));
      await serverClient.SendAsync(frame);

      await connectionTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

      // Wait a moment for messages to be processed
      await Task.Delay(100);

      if (frameReceived) await Assert.That(isConnectedDuringFrame).IsTrue();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task KeepAliveService_ShouldNotDisconnect_AuthenticatingClient()
   {
      var endpoint = new MemoryEndPoint($"keepalive_auth_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());

      var serverOptions = new ResilientServerOptions
      {
         KeepAlive = new ResilientServerKeepAliveOptions
         {
            Mode = ResilientServerKeepAliveMode.Alawys,
            CheckInterval = TimeSpan.FromMilliseconds(50),
            DefaultKeepAliveTime = TimeSpan.FromMilliseconds(100)
         },
         HandshakeTimeout = TimeSpan.FromSeconds(5)
      };

      var server = new ResilientServer<BeskarPacket>([listener], serverOptions);
      var authStartedTcs = new TaskCompletionSource();
      var finishAuthTcs = new TaskCompletionSource();

      server.Events.OnConnect.Add(async (ctx, ct) =>
      {
         authStartedTcs.TrySetResult();
         await finishAuthTcs.Task.WaitAsync(ct);
      });

      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      var connectTask = Task.Run(() => client.ConnectAsync(endpoint));

      // Wait for authentication on the server to start
      await authStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

      // Wait a bit to let keep-alive check run multiple times
      await Task.Delay(500);

      // Server client should still be in Clients list (not disconnected by keep-alive)
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      // Finish authentication
      finishAuthTcs.TrySetResult();
      var connectResult = await connectTask.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(connectResult.Failed).IsFalse();

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Server_ShouldProcessBufferedAndPostHandshakeFrames_InStrictOrder()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      var receivedSequence = new List<int>();
      var lockObj = new object();
      var connectCompletedTcs = new TaskCompletionSource();

      server.Events.OnConnect.Add(async (ctx, ct) =>
      {
         await Task.Delay(150, ct); // Delay handshake completion
         connectCompletedTcs.TrySetResult();
      });

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         if (int.TryParse(text, out var num))
            lock (lockObj)
            {
               receivedSequence.Add(num);
            }

         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      using var streamWriter = new PooledBufferWriter();

      var connectPayload = new ConnectPacketPayload();
      var len = connectPayload.GetEncodedLength();
      using var connectWriter = new PooledBufferWriter(len);
      if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bytesWritten))
         connectWriter.Advance(bytesWritten);

      BeskarPacket.CreateFrame(ResilientFrameKind.Connect, new ReadOnlySequence<byte>(connectWriter.WrittenMemory))
         .WriteTo(streamWriter);

      for (var i = 1; i <= 5; i++)
      {
         var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(i.ToString())));
         msgFrame.WriteTo(streamWriter);
      }

      await socket.SendAsync(streamWriter.WrittenMemory);

      // Wait for handshake to complete
      await connectCompletedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Send frame 6 immediately after handshake
      using var postStreamWriter = new PooledBufferWriter();
      var postMsgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
         new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("6")));
      postMsgFrame.WriteTo(postStreamWriter);
      await socket.SendAsync(postStreamWriter.WrittenMemory);

      var allReceived = await SpinWaitUntilAsync(() =>
      {
         lock (lockObj)
         {
            return receivedSequence.Count == 6;
         }
      }, TimeSpan.FromSeconds(5));

      await Assert.That(allReceived).IsTrue();
      int[] snapshot;
      lock (lockObj)
      {
         snapshot = receivedSequence.ToArray();
      }

      // Verify strict order 1 to 6
      await Assert.That(snapshot).IsEquivalentTo(new[] { 1, 2, 3, 4, 5, 6 });

      socket.Dispose();
      await server.StopAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ServerStart_WithZeroKeepAliveCheckInterval_ShouldNotThrow()
   {
      var endpoint = new MemoryEndPoint($"keepalive_zero_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverOptions = new ResilientServerOptions
      {
         KeepAlive = new ResilientServerKeepAliveOptions
         {
            Mode = ResilientServerKeepAliveMode.Alawys,
            CheckInterval = TimeSpan.Zero
         }
      };

      var server = new ResilientServer<BeskarPacket>([listener], serverOptions);
      await server.StartAsync();

      await Task.Delay(100);

      await server.StopAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ServerAccept_ConcurrentConnections_ShouldNotExceedMaxConnections()
   {
      var endpoint = new MemoryEndPoint($"max_conn_race_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverOptions = new ResilientServerOptions
      {
         MaxConnections = 1,
         OpenToNewConnections = true
      };

      var server = new ResilientServer<BeskarPacket>([listener], serverOptions);
      await server.StartAsync();

      var clients = new List<ResilientClient<BeskarPacket>>();
      var connectTasks = new List<Task>();

      for (var i = 0; i < 5; i++)
      {
         var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
         clients.Add(client);
         connectTasks.Add(Task.Run(() => client.ConnectAsync(endpoint)));
      }

      await Task.WhenAll(connectTasks);

      await Task.Delay(200);

      var activeClientsCount = server.Clients.Count;
      
      foreach (var client in clients)
      {
         await client.DisposeAsync();
      }
      await server.DisposeAsync();

      await Assert.That(activeClientsCount).IsLessThanOrEqualTo(1);
   }

   [Test]
   public async Task ClientConnect_WithNegativeRetryInterval_ShouldStillReconnect()
   {
      var endpoint = new MemoryEndPoint($"reconnect_neg_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromSeconds(-5),
            MaxRetries = 5
         }
      };

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      var reconnectingTcs = new TaskCompletionSource();
      var reconnectedTcs = new TaskCompletionSource();

      client.Events.OnReconnecting.Add((_, _) =>
      {
         reconnectingTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint);

      client.Events.OnConnected.Add((_, _) =>
      {
         reconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await server.StopAsync();
      await reconnectingTcs.Task.WaitAsync(TimeSpan.FromSeconds(8));

      await Task.Delay(200);

      // Start the server again to allow client to reconnect
      await server.StartAsync();

      // If the loop crashed, this will timeout and throw
      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(8));

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task AutoReconnect_ShouldContinueRetrying_WhenHandshakeFails()
   {
      var endpoint = new MemoryEndPoint($"reconnect_handshake_fail_{Guid.NewGuid():N}");
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
      var reconnectedTcs = new TaskCompletionSource();

      await client.ConnectAsync(endpoint);
      await Assert.That(client.IsConnected).IsTrue();

      // Stop first server to trigger reconnect
      await server.StopAsync();
      await server.DisposeAsync();

      // Setup a second server that will deny the first reconnect attempt but accept the second
      var listenerSec = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverSec = new ResilientServer<BeskarPacket>([listenerSec], new ResilientServerOptions());

      var connectCount = 0;
      serverSec.Events.OnConnect.Add((ctx, ct) =>
      {
         var count = Interlocked.Increment(ref connectCount);
         if (count == 1)
         {
            ctx.Deny();
         }
         return ValueTask.CompletedTask;
      });

      client.Events.OnConnected.Add((_, _) =>
      {
         if (client.State == ResilientClientState.Connected)
         {
            reconnectedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      await serverSec.StartAsync();

      // If the loop crashed on the denied handshake, this will timeout
      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(6));

      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(connectCount).IsEqualTo(2);

      await client.DisposeAsync();
      await serverSec.StopAsync();
      await serverSec.DisposeAsync();
   }

   [Test]
   public async Task AutoReconnect_ShouldContinueRetrying_WhenConnectionExceptionThrown()
   {
      var endpoint = new MemoryEndPoint($"reconnect_exception_{Guid.NewGuid():N}");
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

      var rawClient = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      var wrapperClient = new ExceptionThrowingNetworkClient(rawClient.NetworkClient);
      var client = new ResilientClient<BeskarPacket>(wrapperClient, clientOptions);

      var reconnectedTcs = new TaskCompletionSource();

      await client.ConnectAsync(endpoint);
      await Assert.That(client.IsConnected).IsTrue();

      // Stop server to trigger reconnect
      await server.StopAsync();

      client.Events.OnConnected.Add((_, _) =>
      {
         if (client.State == ResilientClientState.Connected)
         {
            reconnectedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      // Start server again so the second attempt succeeds
      await server.StartAsync();

      // If the loop crashed on the exception (the first attempt throws), this will timeout
      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(6));

      await Assert.That(client.IsConnected).IsTrue();

      await client.DisposeAsync();
      await server.StopAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ClientReaderLoop_ShouldNotLoopInfinitely_WhenStreamCompletedWithPartialData()
   {
      var endpoint = new MemoryEndPoint($"partial_data_bug_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = false
         }
      };

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      await client.ConnectAsync(endpoint);
      await Assert.That(client.IsConnected).IsTrue();

      // Get the server client session
      var serverClient = server.Clients.GetAll().First();
      var output = serverClient.ControlStream.Transport.Output;

      // Write incomplete packet (only 2 magic bytes, header requires more)
      var memory = output.GetMemory(2);
      memory.Span[0] = 0xBE;
      memory.Span[1] = 0x5C;
      output.Advance(2);
      await output.FlushAsync();

      // Complete output (closes connection abruptly with partial bytes in pipe buffer)
      await output.CompleteAsync();

      // If the infinite loop bug is present, the client will never exit the listen task,
      // and therefore client.State will never become Disconnected.
      var disconnected = await SpinWaitUntilAsync(() => client.State == ResilientClientState.Disconnected, TimeSpan.FromSeconds(3));
      
      await Assert.That(disconnected).IsTrue();

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ConnectAsync_ShouldSynchronizeKeepAliveIntervalWithConnectPayload()
   {
      var endpoint = new MemoryEndPoint($"keepalive_sync_test_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      
      var serverOptions = new ResilientServerOptions
      {
         KeepAlive = new ResilientServerKeepAliveOptions
         {
            Mode = ResilientServerKeepAliveMode.ClientConfigured
         }
      };
      var server = new ResilientServer<BeskarPacket>([listener], serverOptions);
      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         KeepAlive = new ResilientClientKeepAliveOptions
         {
            Enabled = true,
            KeepAliveInterval = TimeSpan.FromSeconds(45)
         }
      };
      
      var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      
      await client.ConnectAsync(endpoint);
      
      await Assert.That(client.IsConnected).IsTrue();

      // Verify that KeepAliveSeconds was automatically populated on the client side
      await Assert.That(client.Options.ConnectPayload.KeepAliveSeconds).IsEqualTo((ushort)45);

      // Verify that the server successfully received the keep alive seconds
      var serverClient = server.Clients.GetAll().First();
      await Assert.That(serverClient.ConnectPayload).IsNotNull();
      await Assert.That(serverClient.ConnectPayload!.KeepAliveSeconds).IsEqualTo((ushort)45);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   private class ExceptionThrowingNetworkClient(INetworkClient inner) : INetworkClient
   {
      public TransportKind Transport => inner.Transport;
      public bool IsConnected => inner.IsConnected;
      public INetworkSession? Session => inner.Session;
      public EndPoint? LocalAddress => inner.LocalAddress;
      public EndPoint? RemoteAddress => inner.RemoteAddress;
      public NetworkClientStats Stats => inner.Stats;
      private int _connectCount;

      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
      {
         var count = Interlocked.Increment(ref _connectCount);
         if (count == 2)
         {
            throw new SocketException((int)SocketError.ConnectionRefused);
         }
         return inner.ConnectAsync(endPoint, ct);
      }

      public ValueTask DisconnectAsync(CancellationToken ct = default) => inner.DisconnectAsync(ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }
}

