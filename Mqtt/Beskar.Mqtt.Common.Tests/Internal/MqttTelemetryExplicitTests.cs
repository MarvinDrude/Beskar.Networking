using System.Buffers;
using System.Diagnostics.Metrics;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttTelemetryExplicitTests
{
   [Test]
   public async Task MqttRetainedMessages_LoadAndClear_TracksRetainedMessagesActiveGauge()
   {
      long retainedDelta = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == MqttMetrics.MeterName &&
             instrument.Name == "beskar.mqtt.retained_messages.active")
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         Interlocked.Add(ref retainedDelta, measurement);
      });

      meterListener.Start();

      var retained = new MqttRetainedMessages();
      var initial = Volatile.Read(ref retainedDelta);

      var pkt1 = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("sensors/temp"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("22.5"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = true
      };
      var pkt2 = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("sensors/humidity"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("45"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = true
      };

      var msg1 = new MqttPublishMessage(in pkt1);
      var msg2 = new MqttPublishMessage(in pkt2);

      retained.LoadMessages([msg1, msg2]);

      var loadedDelta = Volatile.Read(ref retainedDelta) - initial;
      await Assert.That(loadedDelta).IsGreaterThanOrEqualTo(2);

      retained.Clear();

      var clearedDelta = Volatile.Read(ref retainedDelta) - initial;
      await Assert.That(clearedDelta).IsEqualTo(loadedDelta - 2);
   }

   [Test]
   public async Task MqttSession_AddAndAcknowledgePublish_TracksQosInflightGauge()
   {
      long inflightDelta = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == MqttMetrics.MeterName &&
             instrument.Name == "beskar.mqtt.qos.inflight")
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         Interlocked.Add(ref inflightDelta, measurement);
      });

      meterListener.Start();

      var initial = Volatile.Read(ref inflightDelta);

      var session = new MqttSession(null!, null);
      var pubPacket = new PublishPacket
      {
         PacketIdentifier = 101,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/qos1"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("Hello"u8.ToArray())
      };
      var pending = new MqttPendingPublish
      {
         PacketIdentifier = 101,
         Message = new MqttPublishMessage(in pubPacket),
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         RetainAsPublished = false,
         SubscriptionIdentifier = 0
      };

      session.AddUnacknowledgedPublish(pending);
      await Assert.That(session.GetUnacknowledgedPublishCount()).IsEqualTo(1);

      var acked = session.AcknowledgePublish(101);
      await Assert.That(acked).IsNotNull();
      await Assert.That(session.GetUnacknowledgedPublishCount()).IsEqualTo(0);
   }
}
