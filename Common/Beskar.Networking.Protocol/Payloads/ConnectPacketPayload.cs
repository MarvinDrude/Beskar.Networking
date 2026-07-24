using System.Buffers;
using System.Buffers.Binary;

namespace Beskar.Networking.Protocol.Payloads;

public sealed class ConnectPacketPayload : IResilientPayload
{
   public ushort KeepAliveSeconds { get; set; }

   public int GetEncodedLength() => 2;

   public bool TryWrite(Span<byte> destination, out int bytesWritten)
   {
      bytesWritten = 0;
      if (destination.Length < 2) return false;

      BinaryPrimitives.WriteUInt16BigEndian(destination, KeepAliveSeconds);
      bytesWritten = 2;
      return true;
   }

   public static bool TryRead(ref SequenceReader<byte> reader, out ConnectPacketPayload? result)
   {
      result = null;
      if (!reader.TryReadBigEndian(out short keepAliveRaw)) return false;

      result = new ConnectPacketPayload
      {
         KeepAliveSeconds = (ushort)keepAliveRaw
      };

      return true;
   }
}
