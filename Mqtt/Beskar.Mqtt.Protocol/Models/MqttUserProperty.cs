using System.Buffers;
using Beskar.Mqtt.Protocol.Extensions;

namespace Beskar.Mqtt.Protocol.Models;

public sealed class MqttUserProperty
{
   public string Name { get; }
   public ReadOnlyMemory<byte> Value { get; }

   public MqttUserProperty(in MqttProperty property)
   {
      var userProperty = property.AsUserProperty();

      Name = userProperty.KeyBytes.GetUtf8String() ?? string.Empty;
      Value = userProperty.ValueBytes.ToArray();
   }
}
