using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Protocol.Tests;

[NotInParallel]
public class ProtocolAndResilientBugReproductionTests
{
   [Test]
   [Timeout(5000)]
   public async Task ResilientServer_PreHandshakeBufferedFrames_ShouldNotSufferUseAfterReturnBufferCorruption(CancellationToken ct)
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      var releaseHandshakeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var receivedMessages = new List<string>();
      var allMessagesReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.OnConnect.Add(async (_, token) =>
      {
         await releaseHandshakeTcs.Task.WaitAsync(token);
      });

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         lock (receivedMessages)
         {
            receivedMessages.Add(text);
            if (receivedMessages.Count >= 3)
            {
               allMessagesReceivedTcs.TrySetResult();
            }
         }
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      try
      {
         var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
         using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
         await socket.ConnectAsync(boundEndPoint, ct);

         using var connectWriter = new PooledBufferWriter();
         var connectPayload = new ConnectPacketPayload();
         var len = connectPayload.GetEncodedLength();
         if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bytesWritten))
         {
            connectWriter.Advance(bytesWritten);
         }

         var connectFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Connect,
            new ReadOnlySequence<byte>(connectWriter.WrittenMemory));

         // 1st write: Connect + 3000-byte 'A' message
         var firstText = new string('A', 3000);
         var msgFrame1 = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(firstText)));

         using (var writer1 = new PooledBufferWriter())
         {
            connectFrame.WriteTo(writer1);
            msgFrame1.WriteTo(writer1);
            await socket.SendAsync(writer1.WrittenMemory, ct);
         }

         // Wait for server to consume msgFrame1, enqueue it in PreHandshakeFrameChannel, and call AdvanceTo
         await Task.Delay(50, ct);

         // 2nd write: 3000-byte 'B' message (fills remainder of Segment 0 and allocates Segment 1;
         // once consumed, Segment 0 is returned to Pipe's internal segment pool!)
         var secondText = new string('B', 3000);
         var msgFrame2 = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(secondText)));

         using (var writer2 = new PooledBufferWriter())
         {
            msgFrame2.WriteTo(writer2);
            await socket.SendAsync(writer2.WrittenMemory, ct);
         }

         await Task.Delay(50, ct);

         // 3rd write: 3000-byte 'C' message (re-rents Segment 0 from Pipe's segment pool and overwrites msgFrame1's payload!)
         var thirdText = new string('C', 3000);
         var msgFrame3 = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(thirdText)));

         using (var writer3 = new PooledBufferWriter())
         {
            msgFrame3.WriteTo(writer3);
            await socket.SendAsync(writer3.WrittenMemory, ct);
         }

         await Task.Delay(50, ct);

         // Complete handshake so ProcessBufferedPreHandshakeFramesAsync drains PreHandshakeFrameChannel
         releaseHandshakeTcs.TrySetResult();

         await allMessagesReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);

         await Assert.That(receivedMessages[0]).IsEqualTo(firstText);
         await Assert.That(receivedMessages[1]).IsEqualTo(secondText);
         await Assert.That(receivedMessages[2]).IsEqualTo(thirdText);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task BeskarPacket_TryRead_WithNegativePayloadLength_ShouldNotThrowArgumentOutOfRangeException()
   {
      // Magic (0xBE, 0x5C), Version (1), PacketType (Message = 0x0001), Flags (0x0000),
      // followed by negative VarNumber<int> (-1 = 0xFF, 0xFF, 0xFF, 0xFF, 0x0F)
      byte[] rawBytes = [0xBE, 0x5C, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F];

      var seq = new ReadOnlySequence<byte>(rawBytes);

      await Assert.That(() =>
      {
         var reader = new SequenceReader<byte>(seq);
         _ = BeskarPacket.TryRead(ref reader, out _);
      }).ThrowsNothing();
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientServer_ClientStallingDuringAuthenticateChallenge_ShouldTimeOutWithinHandshakeTimeout(CancellationToken ct)
   {
      var endpoint = new MemoryEndPoint($"auth_stall_bug_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverOptions = new ResilientServerOptions
      {
         HandshakeTimeout = TimeSpan.FromMilliseconds(200),
         MaxConnections = 1
      };

      await using var server = new ResilientServer<BeskarPacket>([listener], serverOptions);

      server.Events.OnConnect.Add(async (ctx, _) =>
      {
         await ctx.SendAuthenticateAsync(new AuthenticatePacketPayload
         {
            AuthMethod = "challenge",
            AuthData = "challenge"u8.ToArray()
         });

         var authResp = await ctx.ReceiveAuthenticateAsync();

         if (authResp is null)
         {
            ctx.Deny();
         }
      });

      await server.StartAsync();

      try
      {
         // Raw client connects and sends Connect frame, then never replies to Authenticate challenge
         var rawClient = new MemoryNetworkClient(new MemoryTransportOptions());
         var sessionResult = await rawClient.ConnectAsync(endpoint, ct);
         await using var rawSession = sessionResult.Success!;
         var streamResult = await rawSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
         var rawStream = streamResult.Success!;

         using var connectWriter = new PooledBufferWriter();
         var connectPayload = new ConnectPacketPayload();
         var len = connectPayload.GetEncodedLength();
         if (connectPayload.TryWrite(connectWriter.GetSpan(len), out var bw))
         {
            connectWriter.Advance(bw);
         }

         var connectFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Connect,
            new ReadOnlySequence<byte>(connectWriter.WrittenMemory));
         using var frameWriter = new PooledBufferWriter();
         connectFrame.WriteTo(frameWriter);

         await rawStream.Transport.Output.WriteAsync(frameWriter.WrittenMemory, ct);
         await rawStream.Transport.Output.FlushAsync(ct);

         // First wait until server registers the client in server.Clients
         var registeredDeadline = Environment.TickCount64 + 500;
         while (Environment.TickCount64 < registeredDeadline && server.Clients.Count == 0 && !ct.IsCancellationRequested)
         {
            await Task.Delay(10, ct);
         }

         // Now wait longer than HandshakeTimeout (200ms) for stalled client to be evicted from server.Clients
         await Task.Delay(350, ct);

         await Assert.That(server.Clients.Count).IsEqualTo(0);
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientClient_KeepAlive_ShouldDetectUnresponsiveServerAndLeaveConnectedState_WhenPongNeverArrives(CancellationToken ct)
   {
      var endpoint = new MemoryEndPoint($"client_keepalive_timeout_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      await listener.BindAsync(ct);

      try
      {
         // Background fake server: accepts connection, responds with ConnectAcknowledged, then ignores all Ping frames
         var serverTask = Task.Run(async () =>
         {
            var sessionRes = await listener.AcceptSessionAsync(ct);
            if (sessionRes.Failed) return;
            await using var session = sessionRes.Success!;
            var streamRes = await session.AcceptStreamAsync(ct);
            if (streamRes.Failed) return;
            var stream = streamRes.Success!;

            // Read Connect frame
            var readRes = await stream.Transport.Input.ReadAsync(ct);
            stream.Transport.Input.AdvanceTo(readRes.Buffer.End);

            // Send ConnectAcknowledged
            var ackFrame = BeskarPacket.CreateFrame(ResilientFrameKind.ConnectAcknowledged);
            using var frameWriter = new PooledBufferWriter();
            ackFrame.WriteTo(frameWriter);
            await stream.Transport.Output.WriteAsync(frameWriter.WrittenMemory, ct);
            await stream.Transport.Output.FlushAsync(ct);

            // Now sit silently and never reply with Pong
            try
            {
               await Task.Delay(3000, ct);
            }
            catch (OperationCanceledException)
            {
            }
         }, ct);

         var clientOptions = new ResilientClientOptions
         {
            KeepAlive = new ResilientClientKeepAliveOptions
            {
               Enabled = true,
               KeepAliveInterval = TimeSpan.FromMilliseconds(100),
               Timeout = TimeSpan.FromMilliseconds(150)
            },
            Reconnecting = new ResilientClientReconnectionOptions
            {
               AutoReconnect = false
            }
         };

         await using var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
         var connectRes = await client.ConnectAsync(endpoint, ct);
         await Assert.That(connectRes.IsSuccess).IsTrue();
         await Assert.That(client.State).IsEqualTo(ResilientClientState.Connected);

         // Wait up to 1s for KeepAliveInterval (100ms) + Timeout (150ms) to detect missing Pong and disconnect
         var detectedTimeout = false;
         var deadline = Environment.TickCount64 + 1000;
         while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested)
         {
            if (client.State != ResilientClientState.Connected)
            {
               detectedTimeout = true;
               break;
            }
            await Task.Delay(25, ct);
         }

         await Assert.That(detectedTimeout).IsTrue();
      }
      finally
      {
         await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientClient_AutoReconnect_ShouldTimeOutStalledHandshake_UsingHandshakeTimeout(CancellationToken ct)
   {
      var endpoint = new MemoryEndPoint($"reconnect_handshake_timeout_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var clientOptions = new ResilientClientOptions
      {
         HandshakeTimeout = TimeSpan.FromMilliseconds(150),
         Reconnecting = new ResilientClientReconnectionOptions
         {
            AutoReconnect = true,
            RetryInterval = TimeSpan.FromMilliseconds(50),
            MaxRetries = 3
         }
      };

      await using var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
      var reconnectExhaustedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnDisconnected.Add((_, _) =>
      {
         reconnectExhaustedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(endpoint, ct);
      await Assert.That(client.IsConnected).IsTrue();

      // Stop real server and bind a raw listener that accepts transport connections but never sends ConnectAcknowledged
      await server.StopAsync();
      await server.DisposeAsync();

      var stalledAcceptedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var stalledListener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      await stalledListener.BindAsync(ct);

      try
      {
         _ = Task.Run(async () =>
         {
            while (!ct.IsCancellationRequested)
            {
               var res = await stalledListener.AcceptSessionAsync(ct);
               if (res.Failed) break;
               var sess = res.Success!;
               _ = await sess.AcceptStreamAsync(ct);
               stalledAcceptedTcs.TrySetResult();
               // Never send ConnectAcknowledged!
            }
         }, ct);

         var accepted = await Task.WhenAny(stalledAcceptedTcs.Task, Task.Delay(1500, ct));
         await Assert.That(accepted == stalledAcceptedTcs.Task).IsTrue();

         // Wait for auto-reconnect to time out its attempts using HandshakeTimeout (150ms) and fire OnDisconnected
         var completed = await Task.WhenAny(reconnectExhaustedTcs.Task, Task.Delay(2500, ct));
         await Assert.That(completed == reconnectExhaustedTcs.Task).IsTrue();
         await Assert.That(client.State).IsEqualTo(ResilientClientState.Disconnected);
      }
      finally
      {
         await stalledListener.UnbindAsync(CancellationToken.None);
      }
   }
}

