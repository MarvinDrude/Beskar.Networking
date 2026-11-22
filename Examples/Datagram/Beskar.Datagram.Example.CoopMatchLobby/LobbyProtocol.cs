using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Datagram.Example.CoopMatchLobby;

/// <summary>
/// Shared 5-channel layout and compact binary helpers showcasing every <see cref="DatagramDeliveryMode"/>
/// on a single multiplexed UDP connection.
/// </summary>
internal static class LobbyProtocol
{
   // All 5 DatagramDeliveryModes prioritized highest-to-lowest for MTU packing:
   public const byte ChLifecycle = 0;  // ReliableOrdered     - phases, chat, 16 KB fragmented map sync
   public const byte ChLoot = 1;       // ReliableUnordered   - independent pickups/hits (no HoL blocking)
   public const byte ChBossHp = 2;     // ReliableSequenced   - latest-only authoritative Boss HP convergence
   public const byte ChPosition = 3;   // UnreliableSequenced - high-frequency transforms (drops stale)
   public const byte ChFx = 4;         // UnreliableUnordered - fire-and-forget visual/audio sparks

   public const byte OpMapChunk = 1;
   public const byte OpPhaseText = 2;
   public const byte OpSquadWhisper = 3;

   private static readonly byte[] LobbyPsk = "beskar-coop-lobby-example-psk-v1"u8.ToArray();

   public static DatagramOptions CreateMatchOptions(bool manualTick, DatagramEventDispatchMode dispatchMode)
   {
      return new DatagramOptions
      {
         MaxDatagramSize = 1200,
         ManualTick = manualTick,
         DispatchMode = dispatchMode,
         MaxPeers = 16,
         MaxBytesPerSecondPerPeer = 256 * 1024, // 256 KB/s per-peer token-bucket pacing
         PacingBurstBytes = 24 * 1024,
         ConnectTimeout = TimeSpan.FromSeconds(5),
         HandshakeRetryInterval = TimeSpan.FromMilliseconds(150),
         KeepAliveInterval = TimeSpan.FromMilliseconds(250),
         PeerTimeout = TimeSpan.FromSeconds(8),
         TimeSyncInterval = TimeSpan.FromMilliseconds(400),
         Security = new DatagramSecurityOptions
         {
            Mode = DatagramSecurityMode.Aead,
            AeadCipher = DatagramAeadCipher.Auto,
            PreSharedKey = LobbyPsk,
            PadConnectRequests = true,
            AllowEndpointMigration = true
         }
      }
      .AddChannel(ChLifecycle, DatagramDeliveryMode.ReliableOrdered, priority: 255, windowSize: 128, allowFragmentation: true)
      .AddChannel(ChLoot, DatagramDeliveryMode.ReliableUnordered, priority: 220, windowSize: 128, allowFragmentation: true)
      .AddChannel(ChBossHp, DatagramDeliveryMode.ReliableSequenced, priority: 180, windowSize: 32, allowFragmentation: false)
      .AddChannel(ChPosition, DatagramDeliveryMode.UnreliableSequenced, priority: 140, allowFragmentation: false)
      .AddChannel(ChFx, DatagramDeliveryMode.UnreliableUnordered, priority: 60, allowFragmentation: false);
   }

   public static byte[] BuildMapSnapshot(int byteCount, out ulong checksum)
   {
      var payload = new byte[1 + byteCount];
      payload[0] = OpMapChunk;

      new Random(2026).NextBytes(payload.AsSpan(1));
      checksum = ComputeFnv1A64(payload.AsSpan(1));

      return payload;
   }

   public static byte[] BuildTaggedText(byte opcode, string text)
   {
      var bytes = Encoding.UTF8.GetBytes(text);
      var packet = new byte[1 + bytes.Length];

      packet[0] = opcode;
      bytes.CopyTo(packet, 1);

      return packet;
   }

   public static int WriteInt32(Span<byte> dest, int value)
   {
      BinaryPrimitives.WriteInt32LittleEndian(dest, value);
      return 4;
   }

   public static int WritePosition(Span<byte> dest, uint tick, float x, float y)
   {
      BinaryPrimitives.WriteUInt32LittleEndian(dest[0..4], tick);
      BinaryPrimitives.WriteSingleLittleEndian(dest[4..8], x);
      BinaryPrimitives.WriteSingleLittleEndian(dest[8..12], y);
      return 12;
   }

   public static ulong ComputeFnv1A64(ReadOnlySpan<byte> data)
   {
      const ulong offset = 14695981039346656037UL;
      const ulong prime = 1099511628211UL;
      var hash = offset;

      for (var i = 0; i < data.Length; i++)
      {
         hash ^= data[i];
         hash *= prime;
      }

      return hash;
   }
}
