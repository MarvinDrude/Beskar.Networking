using System.Buffers;
using System.Runtime.CompilerServices;
using Beskar.Memory.Flags;
using Beskar.Networking.Protocol.Attributes;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Protocol.Tests;

[GenerateFramingProtocol]
public partial struct PacketWithFlags8
{
   [MagicBytes(0x12, Order = 0)] public partial bool Magic { get; }

   [FlagsField(Order = 1)] public PackedBools8 Flags { get; set; }
}

[GenerateFramingProtocol]
public partial struct PacketWithFlags32
{
   [MagicBytes(0x34, Order = 0)] public partial bool Magic { get; }

   [FlagsField(Order = 1)] public PackedBools32 Flags { get; set; }
}

[GenerateFramingProtocol]
public partial struct PacketWithFlags64
{
   [MagicBytes(0x56, Order = 0)] public partial bool Magic { get; }

   [FlagsField(Order = 1)] public PackedBools64 Flags { get; set; }
}

public partial class OuterClass
{
   [GenerateFramingProtocol]
   public partial struct NestedPacket
   {
      [MagicBytes(0xAA, 0xBB, Order = 0)] public partial bool Magic { get; }

      [VarNumberField(Order = 1)] public ulong Id { get; set; }
   }
}

[GenerateFramingProtocol]
public partial struct VarNumberTypesPacket
{
   [MagicBytes(0x99, Order = 0)] public partial bool Magic { get; }

   [VarNumberField(Order = 1)] public ulong ULongVal { get; set; }

   [VarNumberField(Order = 2)] public long LongVal { get; set; }

   [VarNumberField(Order = 3)] public uint UIntVal { get; set; }

   [VarNumberField(Order = 4)] public int IntVal { get; set; }
}

[GenerateFramingProtocol]
public partial struct PacketWithSafeCopy
{
   [VarNumberField(Order = 0)] public int Length { get; set; }

   [ByteSequenceField(nameof(Length), true, Order = 1)]
   public ReadOnlySequence<byte> Payload { get; set; }
}

[GenerateFramingProtocol]
public partial struct PacketWithUnsafeCopy
{
   [VarNumberField(Order = 0)] public int Length { get; set; }

   [ByteSequenceField(nameof(Length), false, Order = 1)]
   public ReadOnlySequence<byte> Payload { get; set; }
}

public class BeskarPacketTests
{
   private static bool GenericRoundtripHelper<TPacket>(TPacket frame, out TPacket readBack)
      where TPacket : struct, IFramingProtocol<TPacket>
   {
      var buffer = new byte[frame.GetEncodedLength()];
      if (!frame.TryWrite(buffer, out var written) || written != buffer.Length)
      {
         readBack = default;
         return false;
      }

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      return TPacket.TryRead(ref reader, out readBack);
   }

   [Test]
   public async Task GenericFramingProtocol_Interface_Constraint_ShouldWorkPolymorphically()
   {
      var packet = new BeskarPacket
      {
         Version = 10,
         PacketType = BeskarPacketType.Message,
         PayloadLength = 4,
         Payload = new ReadOnlySequence<byte>("Data"u8.ToArray())
      };

      var success = GenericRoundtripHelper(packet, out var result);
      await Assert.That(success).IsTrue();
      await Assert.That(result.Version).IsEqualTo((byte)10);
      await Assert.That(result.PacketType).IsEqualTo(BeskarPacketType.Message);
      await Assert.That(result.PayloadLength).IsEqualTo(4);
   }

   [Test]
   public async Task ResilientFrameKind_GetFrameKind_And_CreateFrame_ShouldMapCorrectly()
   {
      var connectPkt = BeskarPacket.CreateFrame(ResilientFrameKind.Connect);
      await Assert.That(connectPkt.PacketType).IsEqualTo(BeskarPacketType.Connect);
      await Assert.That(connectPkt.GetFrameKind()).IsEqualTo(ResilientFrameKind.Connect);

      var pingPkt = BeskarPacket.CreateFrame(ResilientFrameKind.Ping);
      await Assert.That(pingPkt.PacketType).IsEqualTo(BeskarPacketType.Ping);
      await Assert.That(pingPkt.GetFrameKind()).IsEqualTo(ResilientFrameKind.Ping);

      var pongPkt = BeskarPacket.CreateFrame(ResilientFrameKind.Pong);
      await Assert.That(pongPkt.PacketType).IsEqualTo(BeskarPacketType.Pong);
      await Assert.That(pongPkt.GetFrameKind()).IsEqualTo(ResilientFrameKind.Pong);

      var disconnectPkt = BeskarPacket.CreateFrame(ResilientFrameKind.Disconnect);
      await Assert.That(disconnectPkt.PacketType).IsEqualTo(BeskarPacketType.Disconnect);
      await Assert.That(disconnectPkt.GetFrameKind()).IsEqualTo(ResilientFrameKind.Disconnect);
   }

   [Test]
   public async Task ControlPayloads_FastBinarySerialization_And_BeskarPacket_TryGetPayload_ShouldRoundtrip()
   {
      // ConnectPacketPayload
      var connectOriginal = new ConnectPacketPayload
      {
         KeepAliveSeconds = 60
      };

      var connectBuffer = new byte[connectOriginal.GetEncodedLength()];
      await Assert.That(connectOriginal.TryWrite(connectBuffer, out var writtenConnect)).IsTrue();
      await Assert.That(writtenConnect).IsEqualTo(2);

      var connectPacket = new BeskarPacket
      {
         PacketType = BeskarPacketType.Connect,
         PayloadLength = connectBuffer.Length,
         Payload = new ReadOnlySequence<byte>(connectBuffer)
      };

      await Assert.That(connectPacket.TryGetPayload<ConnectPacketPayload>(out var connectRead)).IsTrue();
      await Assert.That(connectRead).IsNotEqualTo(null);
      await Assert.That(connectRead!.KeepAliveSeconds).IsEqualTo((ushort)60);

      // DisconnectPacketPayload
      var disconnectOriginal = new DisconnectPacketPayload
      {
         ReasonCode = 0x80,
         ReasonString = "Normal Shutdown"
      };

      var disconnectBuffer = new byte[disconnectOriginal.GetEncodedLength()];
      await Assert.That(disconnectOriginal.TryWrite(disconnectBuffer, out var writtenDisconnect)).IsTrue();

      var disconnectPacket = new BeskarPacket
      {
         PacketType = BeskarPacketType.Disconnect,
         PayloadLength = disconnectBuffer.Length,
         Payload = new ReadOnlySequence<byte>(disconnectBuffer)
      };

      await Assert.That(disconnectPacket.TryGetPayload<DisconnectPacketPayload>(out var disconnectRead)).IsTrue();
      await Assert.That(disconnectRead).IsNotEqualTo(null);
      await Assert.That(disconnectRead!.ReasonCode).IsEqualTo((byte)0x80);
      await Assert.That(disconnectRead.ReasonString).IsEqualTo("Normal Shutdown");

      // AuthenticatePacketPayload
      var authOriginal = new AuthenticatePacketPayload
      {
         AuthMethod = "PLAIN",
         AuthData = "SecretData"u8.ToArray()
      };

      var authBuffer = new byte[authOriginal.GetEncodedLength()];
      await Assert.That(authOriginal.TryWrite(authBuffer, out var writtenAuth)).IsTrue();

      var authPacket = new BeskarPacket
      {
         PacketType = BeskarPacketType.Authenticate,
         PayloadLength = authBuffer.Length,
         Payload = new ReadOnlySequence<byte>(authBuffer)
      };

      await Assert.That(authPacket.TryGetPayload<AuthenticatePacketPayload>(out var authRead)).IsTrue();
      await Assert.That(authRead).IsNotEqualTo(null);
      await Assert.That(authRead!.AuthMethod).IsEqualTo("PLAIN");
      await Assert.That(authRead.AuthData).IsEquivalentTo("SecretData"u8.ToArray());
   }

   [Test]
   public async Task SafeCopyData_True_ShouldAllocateNewArray()
   {
      var sourceData = "SafeCopyTestPayload"u8.ToArray();
      var original = new PacketWithSafeCopy
      {
         Length = sourceData.Length,
         Payload = new ReadOnlySequence<byte>(sourceData)
      };

      var buffer = new byte[original.GetEncodedLength()];
      original.TryWrite(buffer, out _);

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      await Assert.That(PacketWithSafeCopy.TryRead(ref reader, out var readBack)).IsTrue();

      // Mutate the original buffer to verify the payload copy remains unaffected!
      Array.Fill<byte>(buffer, 0xFF);

      await Assert.That(readBack.Payload.ToArray()).IsEquivalentTo(sourceData);
   }

   [Test]
   public async Task SafeCopyData_False_ShouldSliceDirectly()
   {
      var sourceData = "UnsafeCopyTestPayload"u8.ToArray();
      var original = new PacketWithUnsafeCopy
      {
         Length = sourceData.Length,
         Payload = new ReadOnlySequence<byte>(sourceData)
      };

      var buffer = new byte[original.GetEncodedLength()];
      original.TryWrite(buffer, out _);

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      await Assert.That(PacketWithUnsafeCopy.TryRead(ref reader, out var readBack)).IsTrue();

      await Assert.That(readBack.Payload.ToArray()).IsEquivalentTo(sourceData);
   }

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
   public async Task BeskarPacket_EmptyPayload_ShouldRoundtrip()
   {
      var original = new BeskarPacket
      {
         Version = 1,
         PacketType = BeskarPacketType.Ping,
         Flags = default,
         PayloadLength = 0,
         Payload = ReadOnlySequence<byte>.Empty
      };

      var buffer = new byte[original.GetEncodedLength()];
      await Assert.That(original.TryWrite(buffer, out var written)).IsTrue();
      await Assert.That(written).IsEqualTo(8); // 2 magic + 1 ver + 2 ptype + 2 flags + 1 varint(0)

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      await Assert.That(BeskarPacket.TryRead(ref reader, out var readPkt)).IsTrue();
      await Assert.That(readPkt.PayloadLength).IsEqualTo(0);
      await Assert.That(readPkt.Payload.IsEmpty).IsTrue();
   }

   [Test]
   public async Task BeskarPacket_LargePayload_ShouldRoundtrip()
   {
      var largeBuffer = new byte[65536];
      new Random(42).NextBytes(largeBuffer);

      var original = new BeskarPacket
      {
         Version = 1,
         PacketType = BeskarPacketType.Message,
         Flags = default,
         PayloadLength = largeBuffer.Length,
         Payload = new ReadOnlySequence<byte>(largeBuffer)
      };

      var buffer = new byte[original.GetEncodedLength()];
      await Assert.That(original.TryWrite(buffer, out var written)).IsTrue();

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      await Assert.That(BeskarPacket.TryRead(ref reader, out var readPkt)).IsTrue();
      await Assert.That(readPkt.PayloadLength).IsEqualTo(largeBuffer.Length);
      await Assert.That(readPkt.Payload.ToArray()).IsEquivalentTo(largeBuffer);
   }

   [Test]
   public async Task BeskarPacket_BufferTooSmall_ShouldReturnFalse()
   {
      var packet = new BeskarPacket
      {
         Version = 1,
         PacketType = BeskarPacketType.Connect,
         PayloadLength = 10,
         Payload = new ReadOnlySequence<byte>(new byte[10])
      };

      var tooSmallBuffer = new byte[5];
      var success = packet.TryWrite(tooSmallBuffer, out var written);
      await Assert.That(success).IsFalse();
      await Assert.That(written).IsEqualTo(0);
   }

   [Test]
   public async Task BeskarPacket_InvalidMagicBytes_ShouldReturnFalse()
   {
      var packet = new BeskarPacket
      {
         Version = 1,
         PacketType = BeskarPacketType.Connect,
         PayloadLength = 0,
         Payload = ReadOnlySequence<byte>.Empty
      };

      var buffer = new byte[packet.GetEncodedLength()];
      packet.TryWrite(buffer, out _);
      buffer[0] = 0x00; // Corrupt magic byte

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      var success = BeskarPacket.TryRead(ref reader, out _);
      await Assert.That(success).IsFalse();
   }

   [Test]
   public async Task BeskarPacket_TruncatedBuffer_ShouldReturnFalse()
   {
      var packet = new BeskarPacket
      {
         Version = 1,
         PacketType = BeskarPacketType.Connect,
         PayloadLength = 10,
         Payload = new ReadOnlySequence<byte>(new byte[10])
      };

      var buffer = new byte[packet.GetEncodedLength()];
      packet.TryWrite(buffer, out _);

      // Truncate buffer to various incomplete lengths
      for (var i = 0; i < buffer.Length - 1; i++)
      {
         var truncatedSlice = new ReadOnlySequence<byte>(buffer, 0, i);
         var reader = new SequenceReader<byte>(truncatedSlice);
         var success = BeskarPacket.TryRead(ref reader, out _);
         await Assert.That(success).IsFalse();
      }
   }

   [Test]
   public async Task BeskarPacket_WriteTo_BufferWriter_ShouldWriteCompletePacket()
   {
      var payload = "ArrayBufferWriterTest"u8.ToArray();
      var original = new BeskarPacket
      {
         Version = 3,
         PacketType = BeskarPacketType.Disconnect,
         PayloadLength = payload.Length,
         Payload = new ReadOnlySequence<byte>(payload)
      };

      var writer = new ArrayBufferWriter<byte>();
      original.WriteTo(writer);

      await Assert.That(writer.WrittenCount).IsEqualTo(original.GetEncodedLength());

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray()));
      await Assert.That(BeskarPacket.TryRead(ref reader, out var readPkt)).IsTrue();
      await Assert.That(readPkt.Version).IsEqualTo((byte)3);
      await Assert.That(readPkt.PacketType).IsEqualTo(BeskarPacketType.Disconnect);
      await Assert.That(readPkt.Payload.ToArray()).IsEquivalentTo(payload);
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

   [Test]
   public async Task NestedPacket_ShouldGenerateAndRoundtrip()
   {
      var original = new OuterClass.NestedPacket { Id = 9876543210UL };
      var buffer = new byte[original.GetEncodedLength()];

      await Assert.That(original.TryWrite(buffer, out var written)).IsTrue();
      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      await Assert.That(OuterClass.NestedPacket.TryRead(ref reader, out var readPkt)).IsTrue();
      await Assert.That(readPkt.Magic).IsTrue();
      await Assert.That(readPkt.Id).IsEqualTo(9876543210UL);
   }

   [Test]
   public async Task VarNumberTypesPacket_AllPrimitiveIntTypes_ShouldRoundtrip()
   {
      var original = new VarNumberTypesPacket
      {
         ULongVal = 123456789012345UL,
         LongVal = 9876543210L,
         UIntVal = 4000000000U,
         IntVal = 1234567
      };

      var buffer = new byte[original.GetEncodedLength()];
      await Assert.That(original.TryWrite(buffer, out var written)).IsTrue();

      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer));
      await Assert.That(VarNumberTypesPacket.TryRead(ref reader, out var readPkt)).IsTrue();

      await Assert.That(readPkt.ULongVal).IsEqualTo(123456789012345UL);
      await Assert.That(readPkt.LongVal).IsEqualTo(9876543210L);
      await Assert.That(readPkt.UIntVal).IsEqualTo(4000000000U);
      await Assert.That(readPkt.IntVal).IsEqualTo(1234567);
   }

   [Test]
   public async Task VarNumber_Direct_BoundaryValues_ShouldRoundtrip()
   {
      ulong[] testValues = [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455, 268435456, ulong.MaxValue];
      var tempBuffer = new byte[16];

      foreach (var val in testValues)
      {
         var expectedLen = VarNumber.GetEncodedLength(val);
         var written = VarNumber.Write(tempBuffer, val);

         await Assert.That(written).IsEqualTo(expectedLen);

         var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(tempBuffer, 0, written));
         var success = VarNumber.TryRead(ref reader, out ulong readVal);

         await Assert.That(success).IsTrue();
         await Assert.That(readVal).IsEqualTo(val);
      }
   }
}
