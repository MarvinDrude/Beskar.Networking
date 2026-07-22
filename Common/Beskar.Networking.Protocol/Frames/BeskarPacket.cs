using System.Buffers;
using System.Diagnostics;
using Beskar.Memory.Flags;
using Beskar.Networking.Protocol.Attributes;

namespace Beskar.Networking.Protocol.Frames;

/// <summary>
/// The main beskar packet structure for the default framing in Beskar.Networking.Resilient.
/// </summary>
[GenerateFramingProtocol]
[DebuggerDisplay("PacketType = {PacketType}")]
public partial struct BeskarPacket
{
   /// <summary>
   /// Whether the packet has valid magic bytes.
   /// 0xBE5C as in BESKAR
   /// </summary>
   [MagicBytes(0xBE, 0x5C, Order = 0)]
   public partial bool HasValidMagicBytes { get; }

   /// <summary>
   /// A small version field, just in case.
   /// </summary>
   [VersionField(Order = 1)]
   public byte Version { get; set; }

   /// <summary>
   /// The packet type.
   /// </summary>
   [ProtocolField(Order = 2)]
   public BeskarPacketType PacketType { get; set; }

   /// <summary>
   /// 2-Bytes of just flag space for all sorts of bools.
   /// </summary>
   [FlagsField(Order = 3)]
   public PackedBools16 Flags { get; set; }

   /// <summary>
   /// Length of the payload.
   /// Variable-length number encoded.
   /// </summary>
   [VarNumberField(Order = 4)]
   public int PayloadLength { get; set; }

   /// <summary>
   /// The main payload of the packet.
   /// </summary>
   [ByteSequenceField(nameof(PayloadLength), safeCopyData: false, Order = 5)]
   public ReadOnlySequence<byte> Payload { get; set; }
}

