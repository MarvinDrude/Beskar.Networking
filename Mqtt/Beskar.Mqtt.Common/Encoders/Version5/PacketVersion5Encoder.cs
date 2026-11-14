using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion5Encoder(IBufferWriter<byte> writer)
{
   private readonly IBufferWriter<byte> _writer = writer;

   private static ulong CollectPresentProperties(ReadOnlySequence<byte> propertiesBytes)
   {
      if (propertiesBytes.IsEmpty)
      {
         return 0UL;
      }

      ulong mask = 0;
      var enumerator = new MqttPropertyEnumerator(propertiesBytes);
      while (enumerator.MoveNext())
      {
         var bit = (int)enumerator.Current.Identifier;
         if (bit < 64)
         {
            mask |= 1UL << bit;
         }
      }

      return mask;
   }

   private static ulong CollectPresentProperties(ReadOnlyMemory<byte> propertiesBytes)
   {
      return CollectPresentProperties(new ReadOnlySequence<byte>(propertiesBytes));
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private static bool HasProperty(ulong mask, PropertyIdentifier identifier)
   {
      return (mask & (1UL << (int)identifier)) != 0;
   }

   private static int CalculateReasonPropertiesDataLength(
      ReadOnlyMemory<byte> reasonStringUtf8Bytes,
      ReadOnlyMemory<byte> propertiesBytes)
   {
      var length = propertiesBytes.Length;
      if (!reasonStringUtf8Bytes.IsEmpty &&
          !HasProperty(CollectPresentProperties(propertiesBytes), PropertyIdentifier.ReasonString))
      {
         length += 1 + 2 + reasonStringUtf8Bytes.Length;
      }

      return length;
   }

   private static void WriteReasonProperties(
      ref ByteWriter writer,
      ReadOnlyMemory<byte> reasonStringUtf8Bytes,
      ReadOnlyMemory<byte> propertiesBytes,
      int propsDataLen)
   {
      PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propsDataLen);

      if (propsDataLen == 0)
      {
         return;
      }

      if (!reasonStringUtf8Bytes.IsEmpty &&
          !HasProperty(CollectPresentProperties(propertiesBytes), PropertyIdentifier.ReasonString))
      {
         var propEncoder = new PacketPropertyEncoder(writer);
         try
         {
            propEncoder.Write(PropertyIdentifier.ReasonString, reasonStringUtf8Bytes.Span);
         }
         finally
         {
            writer = propEncoder.Writer;
         }
      }

      if (!propertiesBytes.IsEmpty)
      {
         writer.WriteBytes(propertiesBytes.Span);
      }
   }

   public void Write<TPacket>(in TPacket packet)
      where TPacket : IRawMqttPacket
   {
      // JIT will strip this anyway
      if (typeof(TPacket) == typeof(ConnectPacket))
      {
         WriteConnect(in Unsafe.As<TPacket, ConnectPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PublishPacket))
      {
         WritePublish(in Unsafe.As<TPacket, PublishPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubAckPacket))
      {
         WritePubAck(in Unsafe.As<TPacket, PubAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubRecPacket))
      {
         WritePubRec(in Unsafe.As<TPacket, PubRecPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubRelPacket))
      {
         WritePubRel(in Unsafe.As<TPacket, PubRelPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubCompPacket))
      {
         WritePubComp(in Unsafe.As<TPacket, PubCompPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(SubscribePacket))
      {
         WriteSubscribe(in Unsafe.As<TPacket, SubscribePacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(SubAckPacket))
      {
         WriteSubAck(in Unsafe.As<TPacket, SubAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(UnsubscribePacket))
      {
         WriteUnsubscribe(in Unsafe.As<TPacket, UnsubscribePacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(UnsubAckPacket))
      {
         WriteUnsubAck(in Unsafe.As<TPacket, UnsubAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PingReqPacket))
      {
         WritePingReq(in Unsafe.As<TPacket, PingReqPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PingRespPacket))
      {
         WritePingResp(in Unsafe.As<TPacket, PingRespPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(DisconnectPacket))
      {
         WriteDisconnect(in Unsafe.As<TPacket, DisconnectPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(AuthPacket))
      {
         WriteAuth(in Unsafe.As<TPacket, AuthPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(ConnAckPacket))
      {
         WriteConnAck(in Unsafe.As<TPacket, ConnAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else
      {
         throw new NotSupportedException($"Packet type {typeof(TPacket).Name} is not supported by PacketVersion5Encoder.");
      }
   }

   public void Write<TOptions>(TOptions options, ushort packetIdentifier = 0)
      where TOptions : class, IHeapMqttOptions
   {
      if (typeof(TOptions) == typeof(ConnectOptions))
      {
         WriteConnect(Unsafe.As<TOptions, ConnectOptions>(ref options));
      }
      else if (typeof(TOptions) == typeof(PublishOptions))
      {
         WritePublish(Unsafe.As<TOptions, PublishOptions>(ref options), packetIdentifier);
      }
      else if (typeof(TOptions) == typeof(SubscribeOptions))
      {
         WriteSubscribe(Unsafe.As<TOptions, SubscribeOptions>(ref options), packetIdentifier);
      }
      else if (typeof(TOptions) == typeof(UnsubscribeOptions))
      {
         WriteUnsubscribe(Unsafe.As<TOptions, UnsubscribeOptions>(ref options), packetIdentifier);
      }
      else if (typeof(TOptions) == typeof(DisconnectOptions))
      {
         WriteDisconnect(Unsafe.As<TOptions, DisconnectOptions>(ref options));
      }
      else if (typeof(TOptions) == typeof(AuthPacketOptions))
      {
         WriteAuth(Unsafe.As<TOptions, AuthPacketOptions>(ref options));
      }
      else
      {
         throw new NotSupportedException($"Options type {typeof(TOptions).Name} is not supported by PacketVersion5Encoder.");
      }
   }

   internal static int CalculatePacketLength<TOptions>(TOptions options)
      where TOptions : class, IHeapMqttOptions
   {
      if (typeof(TOptions) == typeof(PublishOptions))
      {
         return CalculatePublishPacketLength(Unsafe.As<TOptions, PublishOptions>(ref options));
      }

      if (typeof(TOptions) == typeof(SubscribeOptions))
      {
         return CalculateSubscribePacketLength(Unsafe.As<TOptions, SubscribeOptions>(ref options));
      }

      if (typeof(TOptions) == typeof(UnsubscribeOptions))
      {
         return CalculateUnsubscribePacketLength(Unsafe.As<TOptions, UnsubscribeOptions>(ref options));
      }

      if (typeof(TOptions) == typeof(DisconnectOptions))
      {
         return CalculateDisconnectPacketLength(Unsafe.As<TOptions, DisconnectOptions>(ref options));
      }

      if (typeof(TOptions) == typeof(AuthPacketOptions))
      {
         return CalculateAuthPacketLength(Unsafe.As<TOptions, AuthPacketOptions>(ref options));
      }

      if (typeof(TOptions) == typeof(ConnectOptions))
      {
         return 0;
      }

      throw new NotSupportedException($"Options type {typeof(TOptions).Name} is not supported by PacketVersion5Encoder.");
   }

   internal static int MeasurePacketLength<TPacket>(in TPacket packet)
      where TPacket : IRawMqttPacket
   {
      if (typeof(TPacket) == typeof(PublishPacket))
      {
         return CalculatePublishPacketLength(in Unsafe.As<TPacket, PublishPacket>(ref Unsafe.AsRef(in packet)));
      }

      var countingWriter = new PooledCountingBufferWriter();
      try
      {
         new PacketVersion5Encoder(countingWriter).Write(in packet);
         return countingWriter.BytesWritten;
      }
      finally
      {
         countingWriter.ReturnBuffer();
      }
   }

   private sealed class PooledCountingBufferWriter : IBufferWriter<byte>
   {
      private byte[]? _buffer;

      public int BytesWritten { get; private set; }

      public void Advance(int count) => BytesWritten += count;

      public Memory<byte> GetMemory(int sizeHint = 0) => EnsureBuffer(sizeHint);

      public Span<byte> GetSpan(int sizeHint = 0) => EnsureBuffer(sizeHint);

      private byte[] EnsureBuffer(int sizeHint)
      {
         var needed = Math.Max(sizeHint, 256);
         if (_buffer is not null && _buffer.Length >= needed)
         {
            return _buffer;
         }

         ReturnBuffer();
         _buffer = ArrayPool<byte>.Shared.Rent(needed);
         return _buffer;
      }

      public void ReturnBuffer()
      {
         if (_buffer is { } buf)
         {
            _buffer = null;
            ArrayPool<byte>.Shared.Return(buf);
         }
      }
   }
}
