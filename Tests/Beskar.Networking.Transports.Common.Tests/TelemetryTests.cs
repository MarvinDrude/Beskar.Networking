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
      });
      listener.Start();

      // Act
      TransportMetrics.RecordBytesSent(1024, TransportKind.Tcp);
      TransportMetrics.RecordBytesReceived(2048, TransportKind.Tcp);

      listener.RecordObservableInstruments();

      // Assert
      await Assert.That(recordedBytesSent).IsEqualTo(1024);
      await Assert.That(recordedBytesReceived).IsEqualTo(2048);
   }

   [Test]
   public async Task ResilientMetrics_RecordsValuesCorrectly()
   {
      long recordedReconnectAttempts = 0;
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
      });
      listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.resilient.ping.rtt")
         {
            recordedPingRtt = measurement;
         }
      });
      listener.Start();

      // Act
      ResilientMetrics.RecordReconnectAttempt(success: true, durationMs: 45.5);
      ResilientMetrics.RecordPingRtt(rttMs: 12.3, isClient: true);

      listener.RecordObservableInstruments();

      // Assert
      await Assert.That(recordedReconnectAttempts).IsEqualTo(1);
      await Assert.That(recordedPingRtt).IsEqualTo(12.3);
   }

   [Test]
   public async Task MqttMetrics_RecordsValuesCorrectly()
   {
      long recordedPublishes = 0;
      long recordedTopicAliasHits = 0;

      using var listener = new MeterListener();
      listener.InstrumentPublished = (instrument, meterListener) =>
      {
         if (instrument.Meter.Name == MqttMetrics.MeterName)
         {
            meterListener.EnableMeasurementEvents(instrument);
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
      });
      listener.Start();

      // Act
      MqttMetrics.RecordPublished(isInbound: false, qos: 1, isRetained: false);
      MqttMetrics.RecordTopicAliasHit();

      listener.RecordObservableInstruments();

      // Assert
      await Assert.That(recordedPublishes).IsEqualTo(1);
      await Assert.That(recordedTopicAliasHits).IsEqualTo(1);
   }
}
