using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Reliability;

namespace Beskar.Networking.Datagram.Tests;

public class ProtocolPrimitiveTests
{
   [Test]
   public async Task Crc32C_MatchesStandardCastagnoliTestVector()
   {
      var crc = Crc32C.Compute("123456789"u8);
      await Assert.That(crc).IsEqualTo(0xE3069283u);
   }

   [Test]
   public async Task SeqMath_HandlesWrapAroundAcross65535()
   {
      await Assert.That(SeqMath.IsGreater(0, 65535)).IsTrue();
      await Assert.That(SeqMath.IsGreater(5, 65530)).IsTrue();
      await Assert.That(SeqMath.IsGreater(65530, 5)).IsFalse();
      await Assert.That(SeqMath.Diff(5, 65530)).IsEqualTo(11);
      await Assert.That(SeqMath.Diff(65530, 5)).IsEqualTo(-11);
   }

   [Test]
   public async Task AckTracker_HandlesOutOfOrderExact64JumpAndReplayRejection()
   {
      var tracker = new AckTracker();

      await Assert.That(tracker.TryRecordReceivedPacket(1)).IsTrue();
      await Assert.That(tracker.TryRecordReceivedPacket(1)).IsFalse(); // duplicate

      // Packet 3 arrives before 2
      await Assert.That(tracker.TryRecordReceivedPacket(3)).IsTrue();
      await Assert.That(tracker.HighestReceivedSeq).IsEqualTo(3u);
      // Bit 0 = seq 2 (0), Bit 1 = seq 1 (1)
      await Assert.That(tracker.OutgoingAckBitfield).IsEqualTo(0b10UL);

      // Packet 2 now arrives
      await Assert.That(tracker.TryRecordReceivedPacket(2)).IsTrue();
      await Assert.That(tracker.OutgoingAckBitfield).IsEqualTo(0b11UL);
      await Assert.That(tracker.TryRecordReceivedPacket(2)).IsFalse(); // replay rejected

      // Jump by exactly 64 packets (from 3 to 67)
      await Assert.That(tracker.TryRecordReceivedPacket(67)).IsTrue();
      await Assert.That(tracker.HighestReceivedSeq).IsEqualTo(67u);
      // Bit 63 corresponds to 67 - 64 = 3!
      await Assert.That(tracker.OutgoingAckBitfield).IsEqualTo(1UL << 63);
   }

   [Test]
   public async Task WireHeaderAndMessageCodec_RoundTripsAcrossModesAndFragments()
   {
      var header = new DatagramWireHeader(
         DatagramPacketKind.Data,
         DatagramWireHeader.CurrentVersion,
         IsEncrypted: true,
         SessionToken: 0x12345678u,
         PacketSeq: 42,
         AckSeq: 40,
         AckBitfield: 0xDEAD_BEEF_CAFE_BABEUL);

      var wire = new byte[DatagramWireHeader.Size];
      header.Write(wire);

      await Assert.That(DatagramWireHeader.TryRead(wire, out var decodedHeader)).IsTrue();
      await Assert.That(decodedHeader).IsEqualTo(header);

      var opts = new DatagramOptions()
         .AddChannel(0, DatagramDeliveryMode.UnreliableUnordered)
         .AddChannel(1, DatagramDeliveryMode.ReliableOrdered);
      var table = opts.BuildValidatedChannelTable(out _, out _);

      var body = new byte[128];
      var payload = "Hello Datagram!"u8.ToArray();
      var len1 = DatagramMessageCodec.Write(body, 0, DatagramDeliveryMode.UnreliableUnordered, 0, false, 0, 0, 0, payload);
      var len2 = DatagramMessageCodec.Write(body.AsSpan(len1), 1, DatagramDeliveryMode.ReliableOrdered, 99, true, 7, 2, 5, payload);

      var combined = body.AsSpan(0, len1 + len2);

      var ok1 = DatagramMessageCodec.TryRead(combined, table, out var m1, out var c1);
      var m1Ch = m1.ChannelId;
      var m1Frag = m1.IsFragment;
      var m1Payload = m1.Payload.ToArray();

      var ok2 = DatagramMessageCodec.TryRead(combined[c1..], table, out var m2, out var c2);
      var m2Ch = m2.ChannelId;
      var m2Seq = m2.ChannelSeq;
      var m2Frag = m2.IsFragment;
      var m2Grp = m2.FragGroupId;
      var m2Idx = m2.FragIndex;
      var m2Cnt = m2.FragCount;
      var m2Payload = m2.Payload.ToArray();

      await Assert.That(ok1).IsTrue();
      await Assert.That(c1).IsEqualTo(len1);
      await Assert.That(m1Ch).IsEqualTo((byte)0);
      await Assert.That(m1Frag).IsFalse();
      await Assert.That(m1Payload).IsEquivalentTo(payload);

      await Assert.That(ok2).IsTrue();
      await Assert.That(c2).IsEqualTo(len2);
      await Assert.That(m2Ch).IsEqualTo((byte)1);
      await Assert.That(m2Seq).IsEqualTo((ushort)99);
      await Assert.That(m2Frag).IsTrue();
      await Assert.That(m2Grp).IsEqualTo((ushort)7);
      await Assert.That(m2Idx).IsEqualTo((ushort)2);
      await Assert.That(m2Cnt).IsEqualTo((ushort)5);
      await Assert.That(m2Payload).IsEquivalentTo(payload);
   }
}
