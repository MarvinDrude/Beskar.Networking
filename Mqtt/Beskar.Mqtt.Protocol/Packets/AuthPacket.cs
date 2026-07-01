using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
public struct AuthPacket : IRawMqttPacket
{
   public AuthenticateReasonCode ReasonCode;
   public ReadOnlySequence<byte> ReasonUtf8Bytes;

   public ReadOnlySequence<byte> AuthenticationMethodUtf8Bytes;
   public ReadOnlySequence<byte> AuthenticationDataBytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "AUTH";
   }
}
