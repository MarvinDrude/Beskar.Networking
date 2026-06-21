using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
public ref struct AuthPacket
{
   public AuthenticateReasonCode ReasonCode;
   public ReadOnlySequence<byte> ReasonUtf8Bytes;

   public ReadOnlySequence<byte> AuthenticationMethodUtf8Bytes;
   public ReadOnlySequence<byte> AuthenticationDataBytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "AUTH";
   }
}
