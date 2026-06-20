using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version3;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion3Encoder(
   IBufferWriter<byte> writer,
   MqttProtocolVersion protocolVersion)
{
   private readonly IBufferWriter<byte> _writer = writer;
   private readonly MqttProtocolVersion _protocolVersion = protocolVersion;
}

