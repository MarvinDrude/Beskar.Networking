using System.Buffers;
using System.Text;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Protocol.Payloads;

public sealed class AuthenticatePacketPayload : IResilientPayload
{
   public string? AuthMethod { get; set; }
   public byte[]? AuthData { get; set; }

   public int GetEncodedLength()
   {
      var authMethodBytesCount = string.IsNullOrEmpty(AuthMethod) ? 0 : Encoding.UTF8.GetByteCount(AuthMethod);
      var authDataLength = AuthData?.Length ?? 0;
      return VarNumber.GetEncodedLength(authMethodBytesCount) + authMethodBytesCount + VarNumber.GetEncodedLength(authDataLength) + authDataLength;
   }

   public bool TryWrite(Span<byte> destination, out int bytesWritten)
   {
      bytesWritten = 0;
      var required = GetEncodedLength();
      if (destination.Length < required) return false;

      var authMethodBytesCount = string.IsNullOrEmpty(AuthMethod) ? 0 : Encoding.UTF8.GetByteCount(AuthMethod);
      bytesWritten += VarNumber.Write(destination.Slice(bytesWritten), authMethodBytesCount);

      if (authMethodBytesCount > 0)
      {
         Encoding.UTF8.GetBytes(AuthMethod!, destination.Slice(bytesWritten));
         bytesWritten += authMethodBytesCount;
      }

      var authDataLength = AuthData?.Length ?? 0;
      bytesWritten += VarNumber.Write(destination.Slice(bytesWritten), authDataLength);

      if (authDataLength > 0)
      {
         AuthData!.CopyTo(destination.Slice(bytesWritten));
         bytesWritten += authDataLength;
      }

      return true;
   }

   public static bool TryRead(ref SequenceReader<byte> reader, out AuthenticatePacketPayload? result)
   {
      result = null;
      if (!VarNumber.TryRead(ref reader, out int authMethodLen)) return false;

      string? authMethod = null;
      if (authMethodLen > 0)
      {
         if (reader.UnreadSequence.Length < authMethodLen) return false;
         var strSeq = reader.UnreadSequence.Slice(0, authMethodLen);
         authMethod = Encoding.UTF8.GetString(strSeq.ToArray());
         reader.Advance(authMethodLen);
      }

      if (!VarNumber.TryRead(ref reader, out int authDataLen)) return false;

      byte[]? authData = null;
      if (authDataLen > 0)
      {
         if (reader.UnreadSequence.Length < authDataLen) return false;
         authData = reader.UnreadSequence.Slice(0, authDataLen).ToArray();
         reader.Advance(authDataLen);
      }

      result = new AuthenticatePacketPayload
      {
         AuthMethod = authMethod,
         AuthData = authData
      };

      return true;
   }
}
