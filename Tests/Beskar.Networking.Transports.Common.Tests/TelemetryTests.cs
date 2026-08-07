using System.Diagnostics.Metrics;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Resilient.Common.Telemetry;

namespace Beskar.Networking.Transports.Common.Tests;

public class TelemetryTests
{
   [Test]
   public async Task TransportMetrics_RecordsValuesCorrectly()
   {
      long recordedBytesSent = 0;
      long recordedBytesReceived = 0;
      long recordedConnectionsOpened = 0;
      long recordedConnectionsClosed = 0;
      long recordedConnectionsActiveDelta = 0;
      long recordedStreamsActiveDelta = 0;

      using var listener = new MeterListener();
      listener.InstrumentPublished = (instrument, meterListener) =>
      {
         if (instrument.Meter.Name == TransportMetrics.MeterName)
         {
            meterListener.EnableMeasurementEvents(instrument);
         }
      };
      listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.transport.bytes.sent")
         {
            Interlocked.Add(ref recordedBytesSent, measurement);
         }
         else if (instrument.Name == "beskar.transport.bytes.received")
         {
            Interlocked.Add(ref recordedBytesReceived, measurement);
         }
         else if (instrument.Name == "beskar.transport.connections.opened")
         {
            Interlocked.Add(ref recordedConnectionsOpened, measurement);
         }
         else if (instrument.Name == "beskar.transport.connections.closed")
         {
            Interlocked.Add(ref recordedConnectionsClosed, measurement);
         }
         else if (instrument.Name == "beskar.transport.connections.active")
         {
            Interlocked.Add(ref recordedConnectionsActiveDelta, measurement);
         }
         else if (instrument.Name == "beskar.transport.streams.active")
         {
            Interlocked.Add(ref recordedStreamsActiveDelta, measurement);
         }
      });
      listener.Start();

      var initialBytesSent = Volatile.Read(ref recordedBytesSent);
      var initialBytesReceived = Volatile.Read(ref recordedBytesReceived);
      var initialOpened = Volatile.Read(ref recordedConnectionsOpened);
      var initialClosed = Volatile.Read(ref recordedConnectionsClosed);

      // Act
      TransportMetrics.RecordBytesSent(1024, TransportKind.Tcp);
      TransportMetrics.RecordBytesReceived(2048, TransportKind.Tcp);
      TransportMetrics.RecordConnectionOpened(TransportKind.Tcp);
      TransportMetrics.RecordStreamOpened(TransportKind.Tcp);
      TransportMetrics.RecordStreamClosed(TransportKind.Tcp);
      TransportMetrics.RecordConnectionClosed(TransportKind.Tcp);

      listener.RecordObservableInstruments();

      // Assert
      await Assert.That(Volatile.Read(ref recordedBytesSent) - initialBytesSent).IsGreaterThanOrEqualTo(1024);
      await Assert.That(Volatile.Read(ref recordedBytesReceived) - initialBytesReceived).IsGreaterThanOrEqualTo(2048);
      await Assert.That(Volatile.Read(ref recordedConnectionsOpened) - initialOpened).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref recordedConnectionsClosed) - initialClosed).IsGreaterThanOrEqualTo(1);
      await Assert.That(recordedConnectionsActiveDelta).IsEqualTo(0); // opened (+1) then closed (-1) = net 0
      await Assert.That(recordedStreamsActiveDelta).IsEqualTo(0); // opened (+1) then closed (-1) = net 0
   }

   [Test]
   public async Task ResilientMetrics_RecordsValuesCorrectly()
   {
      long recordedReconnectAttempts = 0;
      long recordedActiveSessionsDelta = 0;
      long recordedOfflineQueueDelta = 0;
      double recordedPingRtt = 0;

      using var listener = new MeterListener();
      listener.InstrumentPublished = (instrument, meterListener) =>
      {
         if (instrument.Meter.Name == ResilientMetrics.MeterName)
         {
            meterListener.EnableMeasurementEvents(instrument);
         }
      };
      listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.resilient.reconnect.attempts")
         {
            Interlocked.Add(ref recordedReconnectAttempts, measurement);
         }
         else if (instrument.Name == "beskar.resilient.sessions.active")
         {
            Interlocked.Add(ref recordedActiveSessionsDelta, measurement);
         }
         else if (instrument.Name == "beskar.resilient.offline_queue.size")
         {
            Interlocked.Add(ref recordedOfflineQueueDelta, measurement);
         }
      });
      listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.resilient.ping.rtt")
         {
            recordedPingRtt = measurement;
         }
      });
      listener.Start();

      var initialReconnects = Volatile.Read(ref recordedReconnectAttempts);

      // Act
      ResilientMetrics.RecordReconnectAttempt(success: true, durationMs: 45.5);
      ResilientMetrics.RecordPingRtt(rttMs: 12.3, isClient: true);
      ResilientMetrics.RecordSessionStateChange(1, isClient: true);
      ResilientMetrics.RecordOfflineQueueSizeChange(5);
      ResilientMetrics.RecordOfflineQueueSizeChange(-2);

      listener.RecordObservableInstruments();

      // Assert
      await Assert.That(Volatile.Read(ref recordedReconnectAttempts) - initialReconnects).IsGreaterThanOrEqualTo(1);
      await Assert.That(recordedPingRtt).IsEqualTo(12.3);
      await Assert.That(recordedActiveSessionsDelta).IsEqualTo(1);
      await Assert.That(recordedOfflineQueueDelta).IsEqualTo(3);
   }

   [Test]
   public async Task MqttMetrics_RecordsValuesCorrectly()
   {
      long recordedPublishes = 0;
      long recordedTopicAliasHits = 0;
      long recordedConnectedClients = 0;
      long recordedSubscriptions = 0;

      using var listener = new MeterListener();
      listener.InstrumentPublished = (instrument, meterListener) =>
      {
         if (instrument.Meter.Name == MqttMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };
      listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.mqtt.messages.published")
         {
            Interlocked.Add(ref recordedPublishes, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.topic_alias.hits")
         {
            Interlocked.Add(ref recordedTopicAliasHits, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.server.clients.connected")
         {
            Interlocked.Add(ref recordedConnectedClients, measurement);
         }
         else if (instrument.Name == "beskar.mqtt.subscriptions.active")
         {
            Interlocked.Add(ref recordedSubscriptions, measurement);
         }
      });
      listener.Start();

      var initialPublishes = Volatile.Read(ref recordedPublishes);
      var initialAliasHits = Volatile.Read(ref recordedTopicAliasHits);

      // Act
      MqttMetrics.RecordPublished(isInbound: false, qos: 1, isRetained: false);
      MqttMetrics.RecordTopicAliasHit();
      MqttMetrics.RecordClientConnectedChange(1);
      MqttMetrics.RecordSubscriptionChange(2);

      listener.RecordObservableInstruments();

      // Assert
      await Assert.That(Volatile.Read(ref recordedPublishes) - initialPublishes).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref recordedTopicAliasHits) - initialAliasHits).IsGreaterThanOrEqualTo(1);
      await Assert.That(recordedConnectedClients).IsEqualTo(1);
      await Assert.That(recordedSubscriptions).IsEqualTo(2);
   }
}
