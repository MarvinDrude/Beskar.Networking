using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Options;
using Beskar.Networking.Transports.Common.Sockets;

namespace Beskar.Networking.Transports.Common.Tests;

[NotInParallel]
public class SocketSenderTests
{
   [Test]
   public async Task GetSpan_WithSizeGreaterThan4Kb_ReturnsSpanWithSufficientCapacity()
   {
      await using var sender = new SocketSender(SharedTransportMemoryPool.GetNext());

      var span8K = sender.GetSpan(8192);
      await Assert.That(span8K.Length).IsGreaterThanOrEqualTo(8192);

      sender.Advance(8192);
      await Assert.That(sender.UnflushedBytes).IsEqualTo(8192);

      var span64K = sender.GetSpan(65536);
      await Assert.That(span64K.Length).IsGreaterThanOrEqualTo(65536);

      sender.Advance(65536);
      await Assert.That(sender.UnflushedBytes).IsEqualTo(8192 + 65536);
   }

   [Test]
   public async Task GetMemory_WithSizeGreaterThan4Kb_ReturnsMemoryWithSufficientCapacity()
   {
      await using var sender = new SocketSender(SharedTransportMemoryPool.GetNext());

      var mem8K = sender.GetMemory(8192);
      await Assert.That(mem8K.Length).IsGreaterThanOrEqualTo(8192);

      var mem70K = sender.GetMemory(70000);
      await Assert.That(mem70K.Length).IsGreaterThanOrEqualTo(70000);
   }

   [Test]
   public async Task SocketSender_SendGreaterThan4Kb_TransmitsAllBytesAccurately()
   {
      using var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      server.Listen(1);

      using var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      await client.ConnectAsync(server.LocalEndPoint!);
      using var accepted = await server.AcceptAsync();

      await using var sender = new SocketSender(SharedTransportMemoryPool.GetNext());
      sender.Initialize(null!, client);

      const int payloadSize = 8192;
      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      var span = sender.GetSpan(payloadSize);
      await Assert.That(span.Length).IsGreaterThanOrEqualTo(payloadSize);
      payload.CopyTo(span);
      sender.Advance(payloadSize);

      var flushResult = await sender.FlushAsync();
      await Assert.That(flushResult.IsCompleted).IsFalse();
      await Assert.That(sender.UnflushedBytes).IsEqualTo(0);

      var received = new byte[payloadSize];
      var totalRead = 0;
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      while (totalRead < payloadSize)
      {
         var read = await accepted.ReceiveAsync(received.AsMemory(totalRead, payloadSize - totalRead), SocketFlags.None, cts.Token);
         if (read == 0) break;
         totalRead += read;
      }

      await Assert.That(totalRead).IsEqualTo(payloadSize);
      await Assert.That(received).IsEquivalentTo(payload);
   }

   [Test]
   public async Task SocketSender_MultipleGetSpansGreaterThan4KbBeforeFlush_TransmitsAllSegments()
   {
      using var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      server.Listen(1);

      using var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      await client.ConnectAsync(server.LocalEndPoint!);
      using var accepted = await server.AcceptAsync();

      await using var sender = new SocketSender(SharedTransportMemoryPool.GetNext());
      sender.Initialize(null!, client);

      const int chunk1Size = 2048;
      const int chunk2Size = 10000;
      const int chunk3Size = 5000;
      const int totalSize = chunk1Size + chunk2Size + chunk3Size;

      var chunk1 = new byte[chunk1Size];
      var chunk2 = new byte[chunk2Size];
      var chunk3 = new byte[chunk3Size];
      Random.Shared.NextBytes(chunk1);
      Random.Shared.NextBytes(chunk2);
      Random.Shared.NextBytes(chunk3);

      var span1 = sender.GetSpan(chunk1Size);
      chunk1.CopyTo(span1);
      sender.Advance(chunk1Size);

      var span2 = sender.GetSpan(chunk2Size);
      await Assert.That(span2.Length).IsGreaterThanOrEqualTo(chunk2Size);
      chunk2.CopyTo(span2);
      sender.Advance(chunk2Size);

      var span3 = sender.GetSpan(chunk3Size);
      await Assert.That(span3.Length).IsGreaterThanOrEqualTo(chunk3Size);
      chunk3.CopyTo(span3);
      sender.Advance(chunk3Size);

      await Assert.That(sender.UnflushedBytes).IsEqualTo(totalSize);

      var flushResult = await sender.FlushAsync();
      await Assert.That(flushResult.IsCompleted).IsFalse();
      await Assert.That(sender.UnflushedBytes).IsEqualTo(0);

      var expected = new byte[totalSize];
      chunk1.CopyTo(expected.AsSpan(0));
      chunk2.CopyTo(expected.AsSpan(chunk1Size));
      chunk3.CopyTo(expected.AsSpan(chunk1Size + chunk2Size));

      var received = new byte[totalSize];
      var totalRead = 0;
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      while (totalRead < totalSize)
      {
         var read = await accepted.ReceiveAsync(received.AsMemory(totalRead, totalSize - totalRead), SocketFlags.None, cts.Token);
         if (read == 0) break;
         totalRead += read;
      }

      await Assert.That(totalRead).IsEqualTo(totalSize);
      await Assert.That(received).IsEquivalentTo(expected);
   }

   [Test]
   public async Task SocketSender_Send64KbPayload_TransmitsAndResetsCleanly()
   {
      using var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      server.Listen(1);

      using var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      await client.ConnectAsync(server.LocalEndPoint!);
      using var accepted = await server.AcceptAsync();

      await using var sender = new SocketSender(SharedTransportMemoryPool.GetNext());
      sender.Initialize(null!, client);

      const int payloadSize = 70000; // > 65536 bytes to test oversized primary block release on reset
      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      var span = sender.GetSpan(payloadSize);
      await Assert.That(span.Length).IsGreaterThanOrEqualTo(payloadSize);
      payload.CopyTo(span);
      sender.Advance(payloadSize);

      await sender.FlushAsync();

      var received = new byte[payloadSize];
      var totalRead = 0;
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      while (totalRead < payloadSize)
      {
         var read = await accepted.ReceiveAsync(received.AsMemory(totalRead, payloadSize - totalRead), SocketFlags.None, cts.Token);
         if (read == 0) break;
         totalRead += read;
      }

      await Assert.That(totalRead).IsEqualTo(payloadSize);
      await Assert.That(received).IsEquivalentTo(payload);

      // Verify reset state
      var resetSuccess = sender.TryResetState();
      await Assert.That(resetSuccess).IsTrue();
      await Assert.That(sender.UnflushedBytes).IsEqualTo(0);

      // Verify subsequent writes work after reset
      var subsequentSpan = sender.GetSpan(8192);
      await Assert.That(subsequentSpan.Length).IsGreaterThanOrEqualTo(8192);
   }
}
