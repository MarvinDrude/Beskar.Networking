using System.Buffers;
using System.Text;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Tests.Builders;

public class DisconnectOptionsTests
{
   [Test]
   public async Task CorrectOptionsBuildingAndProperties()
   {
      // Arrange
      var builder = new DisconnectOptionsBuilder()
         .WithReasonCode(DisconnectReasonCode.KeepAliveTimeout)
         .WithReasonString("Keep alive timeout exceeded")
         .WithSessionExpiryInterval(3600)
         .WithUserProperty("custom-key", "custom-value");

      // Act
      var options = builder.Build();

      // Assert properties directly
      await Assert.That(options.ReasonCode).IsEqualTo(DisconnectReasonCode.KeepAliveTimeout);
      await Assert.That(options.ReasonString).IsEqualTo("Keep alive timeout exceeded");
      await Assert.That(options.SessionExpiryInterval).IsEqualTo(3600U);

      var hasUserProp = false;
      var userPropKey = "";
      var userPropVal = "";
      var hasMoreUserProps = true;

      {
         var userPropEnum = options.UserProperties.GetEnumerator();
         if (userPropEnum.MoveNext())
         {
            hasUserProp = true;
            var userProp = userPropEnum.Current;
            userPropKey = Encoding.UTF8.GetString(userProp.KeyUtf8Bytes);
            userPropVal = Encoding.UTF8.GetString(userProp.ValueBytes);
            hasMoreUserProps = userPropEnum.MoveNext();
         }
      }

      await Assert.That(hasUserProp).IsTrue();
      await Assert.That(userPropKey).IsEqualTo("custom-key");
      await Assert.That(userPropVal).IsEqualTo("custom-value");
      await Assert.That(hasMoreUserProps).IsFalse();
   }

   [Test]
   public async Task ClearingOptionsResetsState()
   {
      // Arrange
      var builder = new DisconnectOptionsBuilder()
         .WithReasonCode(DisconnectReasonCode.KeepAliveTimeout)
         .WithReasonString("test")
         .WithSessionExpiryInterval(123)
         .WithUserProperty("k", "v");

      var options = builder.Build();

      // Act
      options.Clear();

      // Assert
      await Assert.That(options.ReasonCode).IsEqualTo(DisconnectReasonCode.NormalDisconnection);
      await Assert.That(options.ReasonString).IsNull();
      await Assert.That(options.SessionExpiryInterval).IsNull();
      await Assert.That(options.UserProperties.Count).IsEqualTo(0);
   }

   [Test]
   public async Task EncodingDisconnectOptionsCorrectly()
   {
      // Arrange
      var options = new DisconnectOptionsBuilder()
         .WithReasonCode(DisconnectReasonCode.DisconnectWithWillMessage)
         .WithReasonString("will")
         .WithSessionExpiryInterval(60)
         .WithUserProperty("k", "v")
         .Build();

      var bufferWriter = new ArrayBufferWriter<byte>();
      var encoder = new PacketVersion5Encoder(bufferWriter);

      // Act
      encoder.Write(options);

      // Assert
      await Assert.That(bufferWriter.WrittenCount).IsGreaterThan(0);
   }

   [Test]
   public async Task EncodingDisconnectOptionsVersion3Correctly()
   {
      // Arrange
      var options = new DisconnectOptionsBuilder()
         .WithReasonCode(DisconnectReasonCode.DisconnectWithWillMessage)
         .WithReasonString("will")
         .WithSessionExpiryInterval(60)
         .WithUserProperty("k", "v")
         .Build();

      var bufferWriter = new ArrayBufferWriter<byte>();
      var encoder = new PacketVersion3Encoder(bufferWriter, MqttProtocolVersion.V311);

      // Act
      encoder.Write(options);

      // Assert
      await Assert.That(bufferWriter.WrittenCount).IsEqualTo(2);
      await Assert.That(bufferWriter.WrittenSpan[0]).IsEqualTo((byte)0xE0);
      await Assert.That(bufferWriter.WrittenSpan[1]).IsEqualTo((byte)0x00);
   }
}
