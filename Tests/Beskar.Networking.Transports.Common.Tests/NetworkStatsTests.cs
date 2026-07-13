using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.Transports.Common.Tests;

public class NetworkStatsTests
{
   [Test]
   public async Task NetworkStats_StructProperties_AreModifiable()
   {
      // Arrange
      var stats = new NetworkStats();

      // Act
      stats.BytesReceived = 1234;
      stats.BytesSent = 5678;

      // Assert
      await Assert.That(stats.BytesReceived).IsEqualTo(1234);
      await Assert.That(stats.BytesSent).IsEqualTo(5678);
   }

   [Test]
   public async Task TcpNetworkSessionAndStream_StatsTracking_WorksCorrectly()
   {
      // Arrange
      var duplexPipe = new TestDuplexPipe();
      var session = new TcpNetworkSession(
         new IPEndPoint(IPAddress.Loopback, 0),
         new IPEndPoint(IPAddress.Loopback, 0),
         duplexPipe);

      // Act & Assert - starts at 0
      await Assert.That(session.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(session.Stats.BytesSent).IsEqualTo(0);

      var streamResult = await session.AcceptStreamAsync();
      await Assert.That(streamResult.IsSuccess).IsTrue();
      var stream = streamResult.Success!;

      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(0);

      // Modify stream stats manually
      stream.Stats = new NetworkStats { BytesReceived = 100, BytesSent = 200 };

      // Assert session and stream stats match
      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(100);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(200);
      await Assert.That(session.Stats.BytesReceived).IsEqualTo(100);
      await Assert.That(session.Stats.BytesSent).IsEqualTo(200);
   }

   [Test]
   public async Task WsNetworkSessionAndStream_StatsTracking_WorksCorrectly()
   {
      // Arrange
      var duplexPipe = new TestDuplexPipe();
      var tcpSession = new TcpNetworkSession(
         new IPEndPoint(IPAddress.Loopback, 0),
         new IPEndPoint(IPAddress.Loopback, 0),
         duplexPipe);
      var wsSession = new WsNetworkSession(tcpSession, duplexPipe);

      // Act & Assert
      await Assert.That(wsSession.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(wsSession.Stats.BytesSent).IsEqualTo(0);

      var streamResult = await wsSession.AcceptStreamAsync();
      await Assert.That(streamResult.IsSuccess).IsTrue();
      var stream = streamResult.Success!;

      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(0);

      // Modify stream stats manually
      stream.Stats = new NetworkStats { BytesReceived = 500, BytesSent = 1000 };

      // Assert session and stream stats match
      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(500);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(1000);
      await Assert.That(wsSession.Stats.BytesReceived).IsEqualTo(500);
      await Assert.That(wsSession.Stats.BytesSent).IsEqualTo(1000);
   }

   [Test]
   public async Task Stream_ReadWriteOperations_AutomaticallyIncrementStats()
   {
      // Arrange
      var duplexPipe = new TestDuplexPipe();
      var session = new TcpNetworkSession(
         new IPEndPoint(IPAddress.Loopback, 0),
         new IPEndPoint(IPAddress.Loopback, 0),
         duplexPipe);

      var streamResult = await session.AcceptStreamAsync();
      var stream = streamResult.Success!;

      // Initial stats
      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(0);

      // 1. Verify automatic BytesSent tracking
      var writeMemory = stream.Transport.Output.GetMemory(10);
      "1234567890"u8.CopyTo(writeMemory.Span);
      stream.Transport.Output.Advance(10);
      await stream.Transport.Output.FlushAsync();

      await Assert.That(stream.Stats.BytesSent).IsEqualTo(10);
      await Assert.That(session.Stats.BytesSent).IsEqualTo(10);

      // 2. Verify automatic BytesReceived tracking
      // Push some bytes to the input side of the pipe
      await duplexPipe.ReadPipe.Writer.WriteAsync("abcde"u8.ToArray());
      await duplexPipe.ReadPipe.Writer.FlushAsync();

      // Read from stream's Transport Input
      var readResult = await stream.Transport.Input.ReadAsync();
      await Assert.That(readResult.Buffer.Length).IsEqualTo(5);

      // Advance/consume the bytes
      stream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(5);
      await Assert.That(session.Stats.BytesReceived).IsEqualTo(5);
   }

   private sealed class TestDuplexPipe : IDuplexPipe
   {
      public Pipe ReadPipe { get; } = new();
      public Pipe WritePipe { get; } = new();

      public PipeReader Input => ReadPipe.Reader;
      public PipeWriter Output => WritePipe.Writer;
   }
}
