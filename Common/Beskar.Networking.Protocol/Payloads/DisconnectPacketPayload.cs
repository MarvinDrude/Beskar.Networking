using System.Buffers;
using System.Text;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Protocol.Payloads;

public sealed class DisconnectPacketPayload : IResilientPayload
{
   public byte ReasonCode { get; set; }

   public string? ReasonString { get; set; }

   public int GetEncodedLength()
   {
      var reasonStrBytesCount = string.IsNullOrEmpty(ReasonString) ? 0 : Encoding.UTF8.GetByteCount(ReasonString);
      return 1 + VarNumber.GetEncodedLength(reasonStrBytesCount) + reasonStrBytesCount;
   }

   public bool TryWrite(Span<byte> destination, out int bytesWritten)
   {
      bytesWritten = 0;
      var required = GetEncodedLength();
      if (destination.Length < required) return false;

      destination[0] = ReasonCode;
      bytesWritten = 1;

      var reasonStrBytesCount = string.IsNullOrEmpty(ReasonString) ? 0 : Encoding.UTF8.GetByteCount(ReasonString);
      bytesWritten += VarNumber.Write(destination[bytesWritten..], reasonStrBytesCount);

      if (reasonStrBytesCount > 0)
      {
         Encoding.UTF8.GetBytes(ReasonString!, destination[bytesWritten..]);
         bytesWritten += reasonStrBytesCount;
      }

      return true;
   }

   public static bool TryRead(ref SequenceReader<byte> reader, out DisconnectPacketPayload? result)
   {
      result = null;
      if (!reader.TryRead(out var reasonCode)) return false;
      if (!VarNumber.TryRead(ref reader, out int reasonStrLen)) return false;

      string? reasonString = null;
      if (reasonStrLen > 0)
      {
         if (reader.UnreadSequence.Length < reasonStrLen) return false;
         var strSeq = reader.UnreadSequence.Slice(0, reasonStrLen);

         reasonString = Encoding.UTF8.GetString(strSeq.ToArray());
         reader.Advance(reasonStrLen);
      }

      result = new DisconnectPacketPayload
      {
         ReasonCode = reasonCode,
         ReasonString = reasonString
      };

      return true;
   }
}
