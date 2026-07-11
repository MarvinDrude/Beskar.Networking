using System.Net;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using TUnit.Assertions;

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
      var session = new TcpNetworkSession(
         new IPEndPoint(IPAddress.Loopback, 0),
         new IPEndPoint(IPAddress.Loopback, 0),
         null!);

      // Act & Assert - starts at 0
      await Assert.That(session.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(session.Stats.BytesSent).IsEqualTo(0);

      var streamResult = await session.AcceptStreamAsync();
      await Assert.That(streamResult.IsSuccess).IsTrue();
      var stream = streamResult.Success!;

      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(0);

      // Modify stream stats
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
      var tcpSession = new TcpNetworkSession(
         new IPEndPoint(IPAddress.Loopback, 0),
         new IPEndPoint(IPAddress.Loopback, 0),
         null!);
      var wsSession = new WsNetworkSession(tcpSession, null!);

      // Act & Assert
      await Assert.That(wsSession.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(wsSession.Stats.BytesSent).IsEqualTo(0);

      var streamResult = await wsSession.AcceptStreamAsync();
      await Assert.That(streamResult.IsSuccess).IsTrue();
      var stream = streamResult.Success!;

      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(0);

      // Modify stream stats
      stream.Stats = new NetworkStats { BytesReceived = 500, BytesSent = 1000 };

      // Assert session and stream stats match
      await Assert.That(stream.Stats.BytesReceived).IsEqualTo(500);
      await Assert.That(stream.Stats.BytesSent).IsEqualTo(1000);
      await Assert.That(wsSession.Stats.BytesReceived).IsEqualTo(500);
      await Assert.That(wsSession.Stats.BytesSent).IsEqualTo(1000);
   }
}
