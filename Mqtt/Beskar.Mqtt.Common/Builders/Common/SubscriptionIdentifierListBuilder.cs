using System.Buffers;
using Beskar.Mqtt.Common.Encoders;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Common;

/// <summary>
/// A list builder for subscription identifiers that serializes directly to a byte buffer.
/// </summary>
public sealed class SubscriptionIdentifierListBuilder(IBufferWriter<byte> writer)
{
   /// <summary>
   /// The count of subscription identifiers added.
   /// </summary>
   public int Count { get; private set; }

   /// <summary>
   /// The total byte count of the serialized properties.
   /// </summary>
   public int ByteCount { get; private set; }

   /// <summary>
   /// The written span containing the serialized properties.
   /// </summary>
   public ReadOnlySpan<byte> WrittenSpan => _writer is ArrayBufferWriter<byte> buffer
      ? buffer.WrittenSpan
      : throw new InvalidOperationException("Backing writer does not support written span.");

   private readonly IBufferWriter<byte> _writer = writer;

   /// <summary>
   /// Initializes a new instance of the <see cref="SubscriptionIdentifierListBuilder"/> class with the specified capacity.
   /// </summary>
   public SubscriptionIdentifierListBuilder(int capacity)
      : this(new ArrayBufferWriter<byte>(capacity))
   {
   }

   /// <summary>
   /// Initializes a new instance of the <see cref="SubscriptionIdentifierListBuilder"/> class.
   /// </summary>
   public SubscriptionIdentifierListBuilder()
      : this(new ArrayBufferWriter<byte>())
   {
   }

   /// <summary>
   /// Adds a subscription identifier to the builder.
   /// </summary>
   public SubscriptionIdentifierListBuilder Add(uint subscriptionIdentifier)
   {
      var valLen = PacketEncoder.GetVariableByteIntegerLength((int)subscriptionIdentifier);
      var totalSize = 1 + valLen;
      var span = _writer.GetSpan(totalSize);

      span[0] = (byte)PropertyIdentifier.SubscriptionIdentifier;

      var value = subscriptionIdentifier;
      var index = 1;
      do
      {
         var encodedByte = (byte)(value & 0x7F);
         value >>= 7;

         if (value > 0)
         {
            encodedByte |= 0x80;
         }

         span[index++] = encodedByte;
      }
      while (value > 0);

      _writer.Advance(totalSize);
      Count++;
      ByteCount += totalSize;

      return this;
   }

   /// <summary>
   /// Clears the builder.
   /// </summary>
   public void Clear()
   {
      if (_writer is ArrayBufferWriter<byte> buffer)
      {
         buffer.Clear();
         Count = 0;
         ByteCount = 0;
      }
      else
      {
         throw new InvalidOperationException("Backing writer does not support clearing.");
      }
   }

   /// <summary>
   /// Gets the enumerator for the subscription identifiers.
   /// </summary>
   public SubscriptionIdentifierListEnumerator GetEnumerator() => _writer is ArrayBufferWriter<byte> buffer
      ? new SubscriptionIdentifierListEnumerator(buffer.WrittenSpan)
      : throw new InvalidOperationException("Backing writer does not support automatic enumerator.");

   /// <summary>
   /// An enumerator for subscription identifiers.
   /// </summary>
   public ref struct SubscriptionIdentifierListEnumerator(ReadOnlySpan<byte> buffer)
   {
      private ReadOnlySpan<byte> _buffer = buffer;

      /// <summary>
      /// Gets the current subscription identifier.
      /// </summary>
      public uint Current { get; private set; } = 0;

      /// <summary>
      /// Moves to the next subscription identifier.
      /// </summary>
      public bool MoveNext()
      {
         if (_buffer.Length == 0)
         {
            return false;
         }

         var identifier = (PropertyIdentifier)_buffer[0];
         _buffer = _buffer[1..];

         if (identifier != PropertyIdentifier.SubscriptionIdentifier)
         {
            throw new InvalidOperationException($"Expected SubscriptionIdentifier (0x0B), but found 0x{(byte)identifier:X2}.");
         }

         // Read Variable Byte Integer
         uint value = 0;
         var shift = 0;
         var bytesRead = 0;

         while (true)
         {
            if (_buffer.Length <= bytesRead)
            {
               throw new FormatException("Malformed payload: incomplete variable byte integer.");
            }

            var b = _buffer[bytesRead++];
            value |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
               break;
            }
            shift += 7;
         }

         _buffer = _buffer[bytesRead..];
         Current = value;
         return true;
      }
   }
}
