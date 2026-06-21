using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders;

[StructLayout(LayoutKind.Auto)]
public ref partial struct PacketPropertyEncoder(ByteWriter writer)
{
   public ByteWriter Writer = writer;

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, bool value)
   {
      Writer.WriteByte((byte)identifier);
      Writer.WriteByte(value ? (byte)0x1 : (byte)0x0);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, byte value)
   {
      Writer.WriteByte((byte)identifier);
      Writer.WriteByte(value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, ushort value)
   {
      Writer.WriteByte((byte)identifier);
      Writer.WriteBigEndian(value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, uint value)
   {
      Writer.WriteByte((byte)identifier);
      Writer.WriteBigEndian(value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteVariable(PropertyIdentifier identifier, uint value)
   {
      Writer.WriteByte((byte)identifier);
      PacketEncoder.WriteVariableByteInteger(ref Writer, value);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, ReadOnlySequence<byte> sequence)
   {
      Writer.WriteByte((byte)identifier);
      PacketEncoder.WriteSequence(ref Writer, sequence);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(PropertyIdentifier identifier, ReadOnlySpan<byte> span)
   {
      Writer.WriteByte((byte)identifier);

      var length = (ushort)span.Length;
      Writer.WriteBigEndian(length);

      if (length > 0)
      {
         Writer.WriteBytes(span);
      }
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(ReadOnlySpan<byte> span)
   {
      var length = (ushort)span.Length;
      Writer.WriteBigEndian(length);

      if (length > 0)
      {
         Writer.WriteBytes(span);
      }
   }
}
