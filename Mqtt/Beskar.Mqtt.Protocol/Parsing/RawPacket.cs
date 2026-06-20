using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Parsing;

[StructLayout(LayoutKind.Auto)]
public ref struct RawPacket(byte fixedHeader, int totalLength)
{
   public readonly byte FixedHeader = fixedHeader;
   public readonly int TotalLength = totalLength;

   public SequenceReader<byte> Reader;
}
