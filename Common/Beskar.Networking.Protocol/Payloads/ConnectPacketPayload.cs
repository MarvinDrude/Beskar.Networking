using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Protocol.Payloads;

public sealed class ConnectPacketPayload : IResilientPayload
{
   public string? ClientId { get; set; }
   public ushort KeepAliveSeconds { get; set; }
   public bool CleanSession { get; set; } = true;

   public int GetEncodedLength()
   {
      var clientIdBytesCount = string.IsNullOrEmpty(ClientId) ? 0 : Encoding.UTF8.GetByteCount(ClientId);
      return 2 + 1 + VarNumber.GetEncodedLength(clientIdBytesCount) + clientIdBytesCount;
   }

   public bool TryWrite(Span<byte> destination, out int bytesWritten)
   {
      bytesWritten = 0;
      var required = GetEncodedLength();
      if (destination.Length < required) return false;

      BinaryPrimitives.WriteUInt16BigEndian(destination, KeepAliveSeconds);
      destination[2] = (byte)(CleanSession ? 1 : 0);
      bytesWritten = 3;

      var clientIdBytesCount = string.IsNullOrEmpty(ClientId) ? 0 : Encoding.UTF8.GetByteCount(ClientId);
      bytesWritten += VarNumber.Write(destination.Slice(bytesWritten), clientIdBytesCount);

      if (clientIdBytesCount > 0)
      {
         Encoding.UTF8.GetBytes(ClientId!, destination.Slice(bytesWritten));
         bytesWritten += clientIdBytesCount;
      }

      return true;
   }

   public static bool TryRead(ref SequenceReader<byte> reader, out ConnectPacketPayload? result)
   {
      result = null;
      if (!reader.TryReadBigEndian(out short keepAliveRaw)) return false;
      if (!reader.TryRead(out byte cleanSessionByte)) return false;
      if (!VarNumber.TryRead(ref reader, out int clientIdLen)) return false;

      string? clientId = null;
      if (clientIdLen > 0)
      {
         if (reader.UnreadSequence.Length < clientIdLen) return false;
         var strSeq = reader.UnreadSequence.Slice(0, clientIdLen);
         clientId = Encoding.UTF8.GetString(strSeq.ToArray());
         reader.Advance(clientIdLen);
      }

      result = new ConnectPacketPayload
      {
         KeepAliveSeconds = (ushort)keepAliveRaw,
         CleanSession = cleanSessionByte != 0,
         ClientId = clientId
      };

      return true;
   }
}
