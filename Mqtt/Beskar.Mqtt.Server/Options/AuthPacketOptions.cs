using System.Buffers;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Server.Options;

public sealed class AuthPacketOptions(in AuthPacket packet)
   : IHeapMqttOptions
{
   public AuthenticateReasonCode ReasonCode { get; } = packet.ReasonCode;

   public ReadOnlyMemory<byte> ReasonUtf8Bytes { get; } = packet.ReasonUtf8Bytes.ToArray();

   public ReadOnlyMemory<byte> AuthenticationMethodUtf8Bytes { get; } = packet.AuthenticationMethodUtf8Bytes.ToArray();
   public ReadOnlyMemory<byte> AuthenticationDataBytes { get; } = packet.AuthenticationDataBytes.ToArray();

   public ReadOnlyMemory<byte> PropertiesBytes { get; } = packet.PropertiesBytes.ToArray();
   public MqttPropertyEnumerator GetProperties() => new(new ReadOnlySequence<byte>(PropertiesBytes));
}
