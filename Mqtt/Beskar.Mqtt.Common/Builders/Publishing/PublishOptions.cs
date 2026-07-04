using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Publishing;

/// <summary>
/// All options that are available for sending a PUBLISH message in MQTT.
/// </summary>
/// <param name="builderCapacity">What should the property builders start with as their byte capacity.</param>
public sealed class PublishOptions(int builderCapacity = -1) : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;

   /// <summary>
   /// Gets or sets whether this is a duplicate delivery of an earlier PUBLISH packet.
   /// </summary>
   public bool Dup { get; set; }

   /// <summary>
   /// Gets or sets the Quality of Service level for the message.
   /// </summary>
   public QualityOfServiceType QualityOfService { get; set; } = QualityOfServiceType.AtMostOnce;

   /// <summary>
   /// Gets or sets whether the message should be retained by the broker.
   /// </summary>
   public bool Retain { get; set; }

   /// <summary>
   /// Gets or sets the topic name as a UTF-8 encoded byte array.
   /// </summary>
   public ReadOnlyMemory<byte> TopicUtf8Bytes { get; set; }

   /// <summary>
   /// Gets or sets the payload containing the actual data being published.
   /// </summary>
   public ReadOnlySequence<byte> Payload { get; set; }

   /// <summary>
   /// The Payload Format Indicator.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PayloadFormat PayloadFormat { get; set; } = PayloadFormat.Unspecified;

   /// <summary>
   /// The Message Expiry Interval.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint? MessageExpiryInterval { get; set; }

   /// <summary>
   /// The Topic Alias.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ushort? TopicAlias { get; set; }

   /// <summary>
   /// The Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> ResponseTopicUtf8Bytes { get; set; }

   /// <summary>
   /// The Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> CorrelationData { get; set; }

   /// <summary>
   /// The Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> ContentTypeUtf8Bytes { get; set; }

   public override void Clear()
   {
      base.Clear();

      Dup = false;
      QualityOfService = QualityOfServiceType.AtMostOnce;
      Retain = false;

      TopicUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      Payload = ReadOnlySequence<byte>.Empty;
      PayloadFormat = PayloadFormat.Unspecified;

      MessageExpiryInterval = null;
      TopicAlias = null;

      ResponseTopicUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      CorrelationData = ReadOnlyMemory<byte>.Empty;
      ContentTypeUtf8Bytes = ReadOnlyMemory<byte>.Empty;
   }

   /// <summary>
   /// Serializes all set MQTT 5.0 properties into a ReadOnlySequence of bytes.
   /// </summary>
   public ReadOnlySequence<byte> BuildProperties()
   {
      var estimate = 128
         + UserProperties.ByteCount
         + ResponseTopicUtf8Bytes.Length
         + ContentTypeUtf8Bytes.Length
         + CorrelationData.Length;

      var propBuffer = new byte[estimate];
      var propWriter = new ByteWriter(propBuffer);
      var propEncoder = propWriter.AsPublishPropertyEncoder();

      try
      {
         if (PayloadFormat is not PayloadFormat.Unspecified)
         {
            propEncoder.WritePayloadFormatIndicator(PayloadFormat);
         }

         if (MessageExpiryInterval.HasValue)
         {
            propEncoder.WriteMessageExpiryInterval(MessageExpiryInterval.Value);
         }

         if (!ContentTypeUtf8Bytes.IsEmpty)
         {
            propEncoder.WriteContentType(ContentTypeUtf8Bytes.Span);
         }

         if (!ResponseTopicUtf8Bytes.IsEmpty)
         {
            propEncoder.WriteResponseTopic(ResponseTopicUtf8Bytes.Span);
         }

         if (!CorrelationData.IsEmpty)
         {
            propEncoder.WriteCorrelationData(CorrelationData.Span);
         }

         if (TopicAlias.HasValue)
         {
            propEncoder.WriteTopicAlias(TopicAlias.Value);
         }

         if (UserProperties.Count > 0)
         {
            var enumerator = UserProperties.GetEnumerator();
            while (enumerator.MoveNext())
            {
               var prop = enumerator.Current;
               propEncoder.WriteUserProperty(prop.KeyUtf8Bytes, prop.ValueBytes);
            }
         }

         var bytesWritten = propEncoder.Encoder.Writer.Position;
         return bytesWritten == 0
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(propBuffer.AsMemory(0, bytesWritten));
      }
      finally
      {
         propEncoder.Encoder.Writer.Dispose();
      }
   }

   /// <summary>
   /// Creates a new PublishOptionsBuilder.
   /// </summary>
   public static PublishOptionsBuilder Create() => new();
}
