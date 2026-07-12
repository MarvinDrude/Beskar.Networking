using System.Buffers;
using System.Buffers.Binary;

namespace Beskar.Adv.Chat.Common;

public static class FrameParser
{
   public static bool TryParseFrame(ref ReadOnlySequence<byte> buffer, out ChatPacket? packet, out SequencePosition consumedPosition)
   {
      packet = null;
      consumedPosition = default;

      if (buffer.Length < 4)
      {
         return false;
      }

      // Read totalLength (4 bytes)
      Span<byte> lengthBytes = stackalloc byte[4];
      buffer.Slice(0, 4).CopyTo(lengthBytes);
      var totalLength = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);

      if (buffer.Length < 4 + totalLength)
      {
         return false;
      }

      if (totalLength < 1)
      {
         // Corrupt length, skip to avoid infinite loops or exception
         consumedPosition = buffer.GetPosition(4);
         return true; 
      }

      // Read Type byte
      var typeByte = buffer.Slice(4, 1).FirstSpan[0];

      // Read Payload
      var payloadLength = totalLength - 1;
      var payloadSequence = buffer.Slice(5, payloadLength);
      var payload = payloadSequence.ToArray();

      packet = new ChatPacket((PacketType)typeByte, payload);
      consumedPosition = buffer.GetPosition(4 + totalLength);
      return true;
   }
}
