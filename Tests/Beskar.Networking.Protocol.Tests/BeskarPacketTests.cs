using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Beskar.Memory.Flags;
using Beskar.Networking.Protocol.Attributes;
using Beskar.Networking.Protocol.Frames;

namespace Beskar.Networking.Protocol.Tests;

[GenerateFramingProtocol]
public partial struct PacketWithFlags8
{
   [MagicBytes(0x12, Order = 0)]
   public partial bool Magic { get; }

   [FlagsField(Order = 1)]
   public PackedBools8 Flags { get; set; }
}

[GenerateFramingProtocol]
public partial struct PacketWithFlags32
{
   [MagicBytes(0x34, Order = 0)]
   public partial bool Magic { get; }

   [FlagsField(Order = 1)]
   public PackedBools32 Flags { get; set; }
}

[GenerateFramingProtocol]
public partial struct PacketWithFlags64
{
   [MagicBytes(0x56, Order = 0)]
   public partial bool Magic { get; }

   [FlagsField(Order = 1)]
   public PackedBools64 Flags { get; set; }
}

public class BeskarPacketTests
{
   [Test]
   public async Task BeskarPacket_GetEncodedLength_ShouldCalculateCorrectSize()
   {
      var packet = new BeskarPacket
      {
         Version = 1,
         PacketType = BeskarPacketType.Connect,
         Flags = default,
         PayloadLength = 5,
         Payload = new ReadOnlySequence<byte>("Hello"u8.ToArray())
      };

      var len = packet.GetEncodedLength();
      // 2 (Magic) + 1 (Ver) + 2 (PType) + 2 (Flags) + 1 (PayloadLen 5) + 5 (Payload) = 13 bytes
      await Assert.That(len).IsEqualTo(13);
   }

   [Test]
   public async Task BeskarPacket_TryWrite_And_TryRead_ShouldRoundtrip()
   {
      var payloadData = "BeskarFramingProtocol"u8.ToArray();
      var original = new BeskarPacket
      {
         Version = 2,
         PacketType = BeskarPacketType.Message,
         Flags = default,
         PayloadLength = payloadData.Length,
         Payload = new ReadOnlySequence<byte>(payloadData)
      };

      var buffer = new byte[original.GetEncodedLength()];
      var writeSuccess = original.TryWrite(buffer, out var bytesWritten);
      await Assert.That(writeSuccess).IsTrue();
      await Assert.That(bytesWritten).IsEqualTo(buffer.Length);

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      var readSuccess = BeskarPacket.TryRead(ref reader, out var readPacket);

      await Assert.That(readSuccess).IsTrue();
      await Assert.That(readPacket.HasValidMagicBytes).IsTrue();
      await Assert.That(readPacket.Version).IsEqualTo((byte)2);
      await Assert.That(readPacket.PacketType).IsEqualTo(BeskarPacketType.Message);
      await Assert.That(readPacket.PayloadLength).IsEqualTo(payloadData.Length);
      await Assert.That(readPacket.Payload.ToArray()).IsEquivalentTo(payloadData);
   }

   [Test]
   public async Task FlagsField_AllSizes_ShouldRoundtrip()
   {
      // Flags8
      byte raw8 = 0xAB;
      var pkt8 = new PacketWithFlags8 { Flags = Unsafe.As<byte, PackedBools8>(ref raw8) };

      var buf8 = new byte[pkt8.GetEncodedLength()];
      await Assert.That(buf8.Length).IsEqualTo(2); // 1 magic + 1 flags8
      await Assert.That(pkt8.TryWrite(buf8, out _)).IsTrue();
      var r8 = new SequenceReader<byte>(new ReadOnlySequence<byte>(buf8));
      await Assert.That(PacketWithFlags8.TryRead(ref r8, out var res8)).IsTrue();
      var flags8Copy = res8.Flags;
      await Assert.That(Unsafe.As<PackedBools8, byte>(ref flags8Copy)).IsEqualTo((byte)0xAB);

      // Flags32
      uint raw32 = 0x12345678;
      var pkt32 = new PacketWithFlags32 { Flags = Unsafe.As<uint, PackedBools32>(ref raw32) };

      var buf32 = new byte[pkt32.GetEncodedLength()];
      await Assert.That(buf32.Length).IsEqualTo(5); // 1 magic + 4 flags32
      await Assert.That(pkt32.TryWrite(buf32, out _)).IsTrue();
      var r32 = new SequenceReader<byte>(new ReadOnlySequence<byte>(buf32));
      await Assert.That(PacketWithFlags32.TryRead(ref r32, out var res32)).IsTrue();
      var flags32Copy = res32.Flags;
      await Assert.That(Unsafe.As<PackedBools32, uint>(ref flags32Copy)).IsEqualTo(0x12345678U);

      // Flags64
      var raw64 = 0x123456789ABCDEF0UL;
      var pkt64 = new PacketWithFlags64 { Flags = Unsafe.As<ulong, PackedBools64>(ref raw64) };

      var buf64 = new byte[pkt64.GetEncodedLength()];
      await Assert.That(buf64.Length).IsEqualTo(9); // 1 magic + 8 flags64
      await Assert.That(pkt64.TryWrite(buf64, out _)).IsTrue();
      var r64 = new SequenceReader<byte>(new ReadOnlySequence<byte>(buf64));
      await Assert.That(PacketWithFlags64.TryRead(ref r64, out var res64)).IsTrue();
      var flags64Copy = res64.Flags;
      await Assert.That(Unsafe.As<PackedBools64, ulong>(ref flags64Copy)).IsEqualTo(0x123456789ABCDEF0UL);
   }
}
