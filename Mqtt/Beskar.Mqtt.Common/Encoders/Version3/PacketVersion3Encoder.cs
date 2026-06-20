using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Common.Encoders.Version3;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion3Encoder(IBufferWriter<byte> writer)
{
   private readonly IBufferWriter<byte> _writer = writer;
}

