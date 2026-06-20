using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using Beskar.Memory.Owners;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Extensions;

public static class SequenceReaderExtensions
{
   extension(ref SequenceReader<byte> reader)
   {
      public bool TryReadUInt16BigEndian(out ushort value)
      {
         Unsafe.SkipInit(out value);
         return reader.TryReadBigEndian(out Unsafe.As<ushort, short>(ref value));
      }

      public bool TryReadUInt32BigEndian(out uint value)
      {
         Unsafe.SkipInit(out value);
         return reader.TryReadBigEndian(out Unsafe.As<uint, int>(ref value));
      }

      public bool TryReadUInt64BigEndian(out ulong value)
      {
         Unsafe.SkipInit(out value);
         return reader.TryReadBigEndian(out Unsafe.As<ulong, long>(ref value));
      }

      public bool TryReadUInt16LittleEndian(out ushort value)
      {
         Unsafe.SkipInit(out value);
         return reader.TryReadLittleEndian(out Unsafe.As<ushort, short>(ref value));
      }

      public bool TryReadUInt32LittleEndian(out uint value)
      {
         Unsafe.SkipInit(out value);
         return reader.TryReadLittleEndian(out Unsafe.As<uint, int>(ref value));
      }

      public bool TryReadUInt64LittleEndian(out ulong value)
      {
         Unsafe.SkipInit(out value);
         return reader.TryReadLittleEndian(out Unsafe.As<ulong, long>(ref value));
      }

      public VariableByteIntegerResult TryReadVariableByteInteger(out uint value)
      {
         var multiplier = 1;
         value = 0U;
         byte encodedByte;

         var copyReader = reader;

         do
         {
            if (!copyReader.TryRead(out encodedByte))
            {
               value = 0U;
               return VariableByteIntegerResult.NotEnoughData;
            }

            value += (uint)((encodedByte & 127) * multiplier);

            if (multiplier > 2097152)
            {
               return VariableByteIntegerResult.ExceedMaxValue;
            }

            multiplier *= 128;
         }
         while ((encodedByte & 128) != 0);

         reader = copyReader;
         return VariableByteIntegerResult.Success;
      }

      public bool TryReadString([MaybeNullWhen(false)] out string value)
      {
         if (!reader.TryReadUInt16BigEndian(out var length))
         {
            value = null;
            return false;
         }

         if (length == 0)
         {
            value = string.Empty;
            return true;
         }

         if (reader.Remaining < length)
         {
            value = null;
            return false;
         }

         if (reader.UnreadSpan.Length >= length)
         {
            value = Encoding.UTF8.GetString(reader.UnreadSpan[..length]);
            reader.Advance(length);

            return true; // fast path
         }

         using var owner = length <= 256
            ? new SpanOwner<byte>(stackalloc byte[length])
            : new SpanOwner<byte>(length);
         var span = owner.Span;

         var slice = reader.Sequence.Slice(reader.Position, length);
         slice.CopyTo(span);

         value = Encoding.UTF8.GetString(span);
         reader.Advance(length);

         return true;
      }
   }
}
