using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders;

[StructLayout(LayoutKind.Auto)]
public ref partial struct PacketPropertyEncoder(ByteWriter writer)
{
   private ByteWriter _writer = writer;

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, bool value)
   {
      _writer.WriteByte((byte)identifier);
      _writer.WriteByte(value ? (byte)0x1 : (byte)0x0);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, byte value)
   {
      _writer.WriteByte((byte)identifier);
      _writer.WriteByte(value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, ushort value)
   {
      _writer.WriteByte((byte)identifier);
      _writer.WriteBigEndian(value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, uint value)
   {
      _writer.WriteByte((byte)identifier);
      _writer.WriteBigEndian(value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteVariable(PropertyIdentifier identifier, uint value)
   {
      _writer.WriteByte((byte)identifier);
      PacketEncoder.WriteVariableByteInteger(ref _writer, value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, ReadOnlySequence<byte> sequence)
   {
      _writer.WriteByte((byte)identifier);
      PacketEncoder.WriteSequence(ref _writer, sequence);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, ReadOnlySpan<byte> span)
   {
      _writer.WriteByte((byte)identifier);

      var length = (ushort)span.Length;
      _writer.WriteBigEndian(length);
      _writer.WriteBytes(span);
   }
}
