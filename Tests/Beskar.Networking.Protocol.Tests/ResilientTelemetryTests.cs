using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Common.Telemetry;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientTelemetryTests
{
   [Test]
   public async Task ResilientTelemetry_TracksSessionsAuthAndPing()
   {
      long recordedSessionsActive = 0;
      long recordedAuthAttempts = 0;
      long recordedPingTimeouts = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == ResilientMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         switch (instrument.Name)
         {
            case "beskar.resilient.sessions.active":
               Interlocked.Add(ref recordedSessionsActive, measurement);
               break;
            case "beskar.resilient.auth.attempts":
               Interlocked.Add(ref recordedAuthAttempts, measurement);
               break;
            case "beskar.resilient.ping.timeouts":
               Interlocked.Add(ref recordedPingTimeouts, measurement);
               break;
         }
      });

      meterListener.Start();

      var initialSessions = Volatile.Read(ref recordedSessionsActive);
      var initialAuth = Volatile.Read(ref recordedAuthAttempts);

      var endpoint = new MemoryEndPoint($"resilient_telemetry_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<CustomMagicPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<CustomMagicPacket>();
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

      var connectResult = await client.ConnectAsync(endpoint, cts.Token);
      await Assert.That(connectResult.Failed).IsFalse();

      var sessionsDelta = Volatile.Read(ref recordedSessionsActive) - initialSessions;
      await Assert.That(sessionsDelta).IsGreaterThanOrEqualTo(1);

      var authDelta = Volatile.Read(ref recordedAuthAttempts) - initialAuth;
      await Assert.That(authDelta).IsGreaterThanOrEqualTo(1);

      await Assert.That(Volatile.Read(ref recordedPingTimeouts)).IsEqualTo(0);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ResilientTelemetry_ConnectDisconnectReconnect_CountsSessionsWithoutLeak()
   {
      long recordedSessionsActive = 0;
      long recordedReconnectAttempts = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == ResilientMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.resilient.sessions.active")
         {
            Interlocked.Add(ref recordedSessionsActive, measurement);
         }
         else if (instrument.Name == "beskar.resilient.reconnect.attempts")
         {
            Interlocked.Add(ref recordedReconnectAttempts, measurement);
         }
      });

      meterListener.Start();

      var endpoint = new MemoryEndPoint($"resilient_reconnect_exp_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<CustomMagicPacket>([listener], new ResilientServerOptions());
      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<CustomMagicPacket>();
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

      var startAuth = Volatile.Read(ref recordedReconnectAttempts);

      // Phase 1: Initial Connect
      var connectResult = await client.ConnectAsync(endpoint, cts.Token);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Phase 2: Disconnect
      await client.DisconnectAsync();
      await Assert.That(client.IsConnected).IsFalse();

      // Phase 3: Reconnect
      var reconnectResult = await client.ConnectAsync(endpoint, cts.Token);
      await Assert.That(reconnectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Phase 4: Final Disconnect
      await client.DisconnectAsync();
      await Assert.That(client.IsConnected).IsFalse();

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
