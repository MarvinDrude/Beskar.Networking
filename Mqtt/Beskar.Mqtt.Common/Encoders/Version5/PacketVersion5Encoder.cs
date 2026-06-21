using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Common.Encoders.Version5;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion5Encoder(IBufferWriter<byte> writer)
{
   private readonly IBufferWriter<byte> _writer = writer;
}
