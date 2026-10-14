using System.Buffers;
using System.Diagnostics.Metrics;
using System.Net;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.Udp.Tests;

[NotInParallel]
public class UdpCriticalBugReproductionTests
{
   [Test]
   [Timeout(5000)]
   public async Task UdpNetworkListener_BoundToIPv6Loopback_ShouldReceiveAndSendDatagramsWithoutError(CancellationToken ct)
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.IPv6Loopback, 0), options);

      var bindResult = await listener.BindAsync(ct);
      await Assert.That(bindResult.Failed).IsFalse();

      try
      {
         var client = new UdpNetworkClient(options);
         var connectResult = await client.ConnectAsync(listener.LocalAddress, ct);
         await Assert.That(connectResult.Failed).IsFalse();

         await using var clientSession = connectResult.Success!;
         var clientStreamResult = await clientSession.OpenStreamAsync(Beskar.Networking.Abstractions.Enums.NetworkStreamDirection.Bidirectional, ct);
         var clientStream = clientStreamResult.Success!;

         var payload = "Hello IPv6 UDP!"u8.ToArray();
         await clientStream.Transport.Output.WriteAsync(payload, ct);
         await clientStream.Transport.Output.FlushAsync(ct);

         using var acceptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         acceptCts.CancelAfter(TimeSpan.FromSeconds(2));

         var acceptResult = await listener.AcceptSessionAsync(acceptCts.Token);
         await Assert.That(acceptResult.Failed).IsFalse();

         await using var serverSession = acceptResult.Success!;
         var serverStreamResult = await serverSession.AcceptStreamAsync(acceptCts.Token);
         var serverStream = serverStreamResult.Success!;

         var readResult = await serverStream.Transport.Input.ReadAsync(acceptCts.Token);
         var readBytes = readResult.Buffer.ToArray();
         serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

         await Assert.That(readBytes).IsEquivalentTo(payload);
      }
      finally
      {
         await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task UdpNetworkSession_DisposeAsync_ShouldDisposeUnderlyingStreamAndDecrementActiveStreamsMetric(CancellationToken ct)
   {
      long activeStreamsDelta = 0;
      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == TransportMetrics.MeterName &&
             instrument.Name == "beskar.transport.streams.active")
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };
      meterListener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
      {
         Interlocked.Add(ref activeStreamsDelta, measurement);
      });
      meterListener.Start();

      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync(ct);

      try
      {
         var client = new UdpNetworkClient(options);
         var connectResult = await client.ConnectAsync(listener.LocalAddress, ct);
         var clientSession = connectResult.Success!;

         // Disposing the session without manually disposing the internal stream should still clean up its stream metric
         await clientSession.DisposeAsync();

         await Assert.That(Interlocked.Read(ref activeStreamsDelta)).IsEqualTo(0);
      }
      finally
      {
         await listener.UnbindAsync(CancellationToken.None);
      }
   }
}
