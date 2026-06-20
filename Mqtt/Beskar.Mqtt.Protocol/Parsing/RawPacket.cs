using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Parsing;

[StructLayout(LayoutKind.Auto)]
public ref struct RawPacket(byte fixedHeader, int totalLength, int bodyLength)
{
   public readonly byte FixedHeader = fixedHeader;

   public readonly int TotalLength = totalLength;
   public readonly int BodyLength = bodyLength;

   public SequenceReader<byte> Reader;
}
