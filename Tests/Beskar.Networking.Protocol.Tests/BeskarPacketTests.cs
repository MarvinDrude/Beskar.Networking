using System;
using System.Buffers;
using Beskar.Memory.Flags;
using Beskar.Networking.Protocol.Frames;

namespace Beskar.Networking.Protocol.Tests;

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
}
