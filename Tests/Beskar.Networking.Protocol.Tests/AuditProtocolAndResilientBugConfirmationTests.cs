using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Protocol.Attributes;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Protocol.Tests;

[GenerateFramingProtocol]
public partial struct ByteProtocolFieldTestFrame
{
   [ProtocolField(Order = 0)]
   public byte Code { get; set; }

   public readonly ResilientFrameKind GetFrameKind() => ResilientFrameKind.Message;
   public static ByteProtocolFieldTestFrame CreateFrame(ResilientFrameKind kind) => new();
}

[GenerateFramingProtocol]
public partial struct UIntProtocolFieldTestFrame
{
   [ProtocolField(Order = 0)]
   public uint Code { get; set; }

   public readonly ResilientFrameKind GetFrameKind() => ResilientFrameKind.Message;
   public static UIntProtocolFieldTestFrame CreateFrame(ResilientFrameKind kind) => new();
}

[NotInParallel]
public class AuditProtocolAndResilientBugConfirmationTests
{
   [Test]
   [Timeout(5000)]
   public async Task FramingProtocolGenerator_ByteProtocolField_TryWriteIntoEncodedLengthSpan_ShouldNotThrowBufferOverrun(CancellationToken ct)
   {
      var frame = new ByteProtocolFieldTestFrame { Code = 0x42 };
      var encodedLength = frame.GetEncodedLength();
      var destination = new byte[encodedLength];

      var ok = frame.TryWrite(destination, out var bytesWritten);

      await Assert.That(ok).IsTrue();
      await Assert.That(bytesWritten).IsEqualTo(encodedLength);
   }

   [Test]
   [Timeout(5000)]
   public async Task BeskarPacket_TryWrite_ShouldBeZeroAllocationAndNotBoxEnumOnHeap(CancellationToken ct)
   {
      var packet = BeskarPacket.CreateFrame(ResilientFrameKind.Ping);
      var buffer = new byte[32];

      // Warm up JIT
      for (var i = 0; i < 10; i++)
      {
         packet.TryWrite(buffer, out _);
      }

      var beforeBytes = GC.GetAllocatedBytesForCurrentThread();
      for (var i = 0; i < 1000; i++)
      {
         packet.TryWrite(buffer, out _);
      }
      var allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - beforeBytes;

      // Expected: Writing a value-type struct into a preallocated Span<byte> must allocate 0 bytes on the heap.
      await Assert.That(allocatedBytes).IsEqualTo(0L);
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientServer_InvalidMagicBytes_ShouldDisconnectOrRecoverInsteadOfWedgingConnectionForever(CancellationToken ct)
   {
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .Build();

      var disconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var messageReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.FrameReceived.Add((_, _) =>
      {
         messageReceivedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      server.Events.ClientDisconnected.Add((_, _) =>
      {
         disconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      try
      {
         var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
         using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
         await socket.ConnectAsync(boundEndPoint, ct);

         // Complete valid handshake first
         using (var connectWriter = new PooledBufferWriter())
         {
            var connectPayload = new ConnectPacketPayload();
            var len = connectPayload.GetEncodedLength();
            connectPayload.TryWrite(connectWriter.GetSpan(len), out var written);
            connectWriter.Advance(written);

            var connectFrame = BeskarPacket.CreateFrame(
               ResilientFrameKind.Connect,
               new ReadOnlySequence<byte>(connectWriter.WrittenMemory));

            using var frameWriter = new PooledBufferWriter();
            connectFrame.WriteTo(frameWriter);
            await socket.SendAsync(frameWriter.WrittenMemory, ct);
         }

         // Wait for ConnectAck from server
         var ackBuf = new byte[64];
         var read = await socket.ReceiveAsync(ackBuf, ct);
         await Assert.That(read).IsGreaterThan(0);

         // Send 1 corrupt magic byte (0x00) followed by a valid Message frame
         var validMessageFrame = BeskarPacket.CreateFrame(
            ResilientFrameKind.Message,
            "hello"u8.ToArray());

         using (var corruptWriter = new PooledBufferWriter())
         {
            var span = corruptWriter.GetSpan(1);
            span[0] = 0x00; // Invalid magic byte (expected 0xBE, 0x5C)
            corruptWriter.Advance(1);

            validMessageFrame.WriteTo(corruptWriter);
            await socket.SendAsync(corruptWriter.WrittenMemory, ct);
         }

         // Expected: The server must either immediately disconnect the client on protocol violation
         // OR skip the corrupt byte and parse the frame — it must NOT wedge silently forever.
         var completedTask = await Task.WhenAny(
            disconnectedTcs.Task,
            messageReceivedTcs.Task,
            Task.Delay(500, ct));

         var handled = completedTask == disconnectedTcs.Task || completedTask == messageReceivedTcs.Task;
         await Assert.That(handled).IsTrue();
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientServer_MaxConnectionsAndClientDisconnected_ShouldNotFireForRejectedClientAndShouldPreserveHandshakeFlag(CancellationToken ct)
   {
      var endpoint = new MemoryEndPoint($"resilient_max_conn_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions
      {
         MaxConnections = 1
      });

      var disconnectedClients = new List<(Guid Id, bool WasHandshakeCompleted)>();
      var firstClientDisconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.ClientDisconnected.Add((ctx, _) =>
      {
         lock (disconnectedClients)
         {
            disconnectedClients.Add((ctx.Client.Id, ctx.Client.IsHandshakeCompleted));
         }
         firstClientDisconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      try
      {
         var clientOptions = new ResilientClientOptions
         {
            Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
         };

         await using var client1 = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);
         var connect1 = await client1.ConnectAsync(endpoint, ct);
         await Assert.That(connect1.Failed).IsFalse();

         // Connect a 2nd raw MemoryNetworkClient while MaxConnections (1) is full
         await using (var rawClient2 = new MemoryNetworkClient(new MemoryTransportOptions()))
         {
            var rawSessRes = await rawClient2.ConnectAsync(endpoint, ct);
            if (rawSessRes.IsSuccess)
            {
               _ = await rawSessRes.Success!.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
            }
            await Task.Delay(150, ct);
         }

         int rejectedDisconnectCount;
         lock (disconnectedClients)
         {
            rejectedDisconnectCount = disconnectedClients.Count;
         }

         // Expected: Rejected connection #2 (which was rejected by MaxConnections before handshake)
         // must NOT fire Events.ClientDisconnected, and when client1 disconnects, IsHandshakeCompleted must be true.
         await client1.DisconnectAsync();
         await firstClientDisconnectedTcs.Task.WaitAsync(ct);

         bool client1HandshakeCompletedFlag;
         lock (disconnectedClients)
         {
            client1HandshakeCompletedFlag = disconnectedClients.Last().WasHandshakeCompleted;
         }

         await Assert.That(rejectedDisconnectCount).IsEqualTo(0);
         await Assert.That(client1HandshakeCompletedFlag).IsTrue();
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientClient_DisconnectDuringInFlightConnect_ShouldNotTriggerAutoReconnect(CancellationToken ct)
   {
      var serverHandshakeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseServerHandshake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var connectAttempts = 0;

      var endpoint = new MemoryEndPoint($"resilient_cancel_connect_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      server.Events.OnConnect.Add(async (_, token) =>
      {
         var attempt = Interlocked.Increment(ref connectAttempts);
         if (attempt == 1)
         {
            serverHandshakeEntered.TrySetResult();
            await releaseServerHandshake.Task.WaitAsync(token);
         }
      });

      await server.StartAsync();

      try
      {
         var clientOptions = new ResilientClientOptions
         {
            Reconnecting = new ResilientClientReconnectionOptions
            {
               AutoReconnect = true,
               RetryInterval = TimeSpan.FromMilliseconds(30)
            }
         };

         await using var client = ResilientClientFactory.CreateMemory<BeskarPacket>(clientOptions: clientOptions);

         var connectTask = client.ConnectAsync(endpoint, ct);
         await serverHandshakeEntered.Task.WaitAsync(ct);

         // Explicitly disconnect while ConnectAsync is waiting on handshake
         await client.DisconnectAsync();
         releaseServerHandshake.TrySetResult();
         await connectTask;

         await Task.Delay(150, ct);

         // Expected: Client must remain Disconnected and must NOT auto-reconnect after explicit DisconnectAsync()
         await Assert.That(client.State).IsEqualTo(ResilientClientState.Disconnected);
         await Assert.That(Volatile.Read(ref connectAttempts)).IsEqualTo(1);
      }
      finally
      {
         releaseServerHandshake.TrySetResult();
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task ResilientServer_DisposeAsyncWithoutStopAsync_ShouldStopServerAndFireOnStop(CancellationToken ct)
   {
      var endpoint = new MemoryEndPoint($"resilient_dispose_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      var onStopFired = false;
      server.Events.OnStop.Add((_, _) =>
      {
         onStopFired = true;
         return ValueTask.CompletedTask;
      });

      var startRes = await server.StartAsync();
      await Assert.That(startRes.IsSuccess).IsTrue();
      await Assert.That(server.State).IsEqualTo(ResilientServerState.Running);

      // Dispose directly without calling StopAsync() first (e.g. `await using var server = ...`)
      await server.DisposeAsync();

      await Assert.That(server.State).IsEqualTo(ResilientServerState.Stopped);
      await Assert.That(onStopFired).IsTrue();
   }
}

