using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Datagram.Example.ArenaGame2D.Protocol;

/// <summary>
/// Shared channel configuration and compact binary codecs across all 4 DatagramDeliveryModes.
/// </summary>
public static class GameProtocol2D
{
   public const int DefaultPort = 7778;
   public const int TickRateHz = 60;
   public const int MaxRedundantInputs = 6; // Survives 5 consecutive lost Ch 2 UDP packets (80 bytes total)

   public const byte ChLifecycle = 0;   // ReliableOrdered     - welcome, join/leave, tags, emotes
   public const byte ChScoreboard = 1;  // ReliableSequenced   - latest-only authoritative scoreboard
   public const byte ChRealtime = 2;    // UnreliableSequenced - 60Hz redundant input bundles & 30Hz snapshots
   public const byte ChFx = 3;          // UnreliableUnordered - fire-and-forget visual rings/sparks

   public const byte OpWelcome = 1;
   public const byte OpEventLog = 2;
   public const byte OpEmote = 3;
   public const byte OpScoreboard = 10;
   public const byte OpInputBundle = 20;
   public const byte OpWorldSnapshot = 21;
   public const byte OpFxCue = 30;

   public static readonly string[] Emotes =
   [
      "GG!",
      "Nice shot!",
      "Lag?! Never!",
      "Beskar is mine!"
   ];

   private static readonly byte[] GamePsk = "beskar-arena-2d-v2"u8.ToArray();

   public static DatagramOptions CreateOptions() => new DatagramOptions
   {
      ManualTick = true,
      DispatchMode = DatagramEventDispatchMode.Poll,
      MaxPeers = 16,
      ConnectTimeout = TimeSpan.FromSeconds(8),
      HandshakeRetryInterval = TimeSpan.FromMilliseconds(200),
      KeepAliveInterval = TimeSpan.FromMilliseconds(350),
      PeerTimeout = TimeSpan.FromSeconds(10), // Resilient even at 300ms delay + 35% loss
      TimeSyncInterval = TimeSpan.FromSeconds(2),
      Security = new DatagramSecurityOptions
      {
         Mode = DatagramSecurityMode.Aead,
         AeadCipher = DatagramAeadCipher.Auto,
         PreSharedKey = GamePsk,
         AllowEndpointMigration = true
      }
   }
   .AddChannel(ChLifecycle, DatagramDeliveryMode.ReliableOrdered, priority: 255, windowSize: 64)
   .AddChannel(ChScoreboard, DatagramDeliveryMode.ReliableSequenced, priority: 200, windowSize: 16)
   .AddChannel(ChRealtime, DatagramDeliveryMode.UnreliableSequenced, priority: 180, allowFragmentation: false)
   .AddChannel(ChFx, DatagramDeliveryMode.UnreliableUnordered, priority: 100, allowFragmentation: false);

   // --- Channel 0: Lifecycle & Emotes ---

   public static int WriteWelcome(Span<byte> dest, byte playerId, byte color, float spawnX, float spawnY)
   {
      dest[0] = OpWelcome;
      dest[1] = playerId;
      dest[2] = color;

      BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(3, 4), spawnX);
      BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(7, 4), spawnY);

      return 11;
   }

   public static bool TryReadWelcome(ReadOnlySpan<byte> src, out byte playerId, out byte color, out float spawnX, out float spawnY)
   {
      playerId = 0;
      color = 0;
      spawnX = 0;
      spawnY = 0;

      if (src.Length < 11 || src[0] != OpWelcome) return false;

      playerId = src[1];
      color = src[2];
      spawnX = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(3, 4));
      spawnY = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(7, 4));

      return true;
   }

   public static int WriteEventLog(Span<byte> dest, string text)
   {
      dest[0] = OpEventLog;

      var written = Encoding.UTF8.GetBytes(text.AsSpan(0, Math.Min(text.Length, 120)), dest[2..]);
      dest[1] = (byte)written;

      return 2 + written;
   }

   public static bool TryReadEventLog(ReadOnlySpan<byte> src, out string text)
   {
      text = string.Empty;
      if (src.Length < 2 || src[0] != OpEventLog) return false;

      var len = src[1];
      if (src.Length < 2 + len) return false;

      text = Encoding.UTF8.GetString(src.Slice(2, len));
      return true;
   }

   public static int WriteEmote(Span<byte> dest, byte emoteIndex)
   {
      dest[0] = OpEmote;
      dest[1] = emoteIndex;

      return 2;
   }

   public static bool TryReadEmote(ReadOnlySpan<byte> src, out byte emoteIndex)
   {
      emoteIndex = 0;
      if (src.Length < 2 || src[0] != OpEmote) return false;

      emoteIndex = src[1];
      return true;
   }

   public static int WriteScoreboard(Span<byte> dest, IReadOnlyList<ScoreEntry2D> entries)
   {
      dest[0] = OpScoreboard;

      var count = (byte)Math.Min(entries.Count, 16);
      dest[1] = count;

      var offset = 2;

      for (var i = 0; i < count; i++)
      {
         var e = entries[i];

         dest[offset++] = e.Id;
         dest[offset++] = e.Color;

         BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(offset, 2), e.Score);
         offset += 2;

         var slice = e.Name.Length > 12 ? e.Name.AsSpan(0, 12) : e.Name.AsSpan();
         var nLen = Encoding.UTF8.GetBytes(slice, dest[(offset + 1)..]);

         dest[offset++] = (byte)nLen;
         offset += nLen;
      }

      return offset;
   }

   public static bool TryReadScoreboard(ReadOnlySpan<byte> src, List<ScoreEntry2D> output)
   {
      if (src.Length < 2 || src[0] != OpScoreboard) return false;

      var count = src[1];
      var offset = 2;
      output.Clear();

      for (var i = 0; i < count; i++)
      {
         if (offset + 5 > src.Length) return false;

         var id = src[offset++];
         var color = src[offset++];

         var score = BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(offset, 2));
         offset += 2;

         var nLen = src[offset++];
         if (offset + nLen > src.Length) return false;

         var name = Encoding.UTF8.GetString(src.Slice(offset, nLen));
         offset += nLen;

         output.Add(new ScoreEntry2D(id, color, score, name));
      }

      return true;
   }

   // --- Channel 2: 6x Redundant Input Bundles & Authoritative World Snapshots ---

   public static int WriteInputBundle(Span<byte> dest, ReadOnlySpan<ClientInputFrame> frames)
   {
      dest[0] = OpInputBundle;

      var count = (byte)Math.Min(frames.Length, MaxRedundantInputs);
      dest[1] = count;

      var offset = 2;

      for (var i = 0; i < count; i++)
      {
         var f = frames[i];

         BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(offset, 4), f.InputSeq);
         offset += 4;

         dest[offset++] = (byte)f.Buttons;

         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), f.AimAngle);
         offset += 4;

         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), f.ViewServerTick);
         offset += 4;
      }

      return offset;
   }

   public static int ReadInputBundle(ReadOnlySpan<byte> src, Span<ClientInputFrame> dest)
   {
      if (src.Length < 2 || src[0] != OpInputBundle) return 0;

      var count = Math.Min(src[1], dest.Length);
      if (src.Length < 2 + (count * 13)) return 0;

      var offset = 2;

      for (var i = 0; i < count; i++)
      {
         var seq = BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(offset, 4));
         offset += 4;

         var buttons = (InputButtons)src[offset++];

         var aim = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4));
         offset += 4;

         var viewServerTick = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4));
         offset += 4;

         dest[i] = new ClientInputFrame(seq, buttons, aim, viewServerTick);
      }

      return count;
   }

   public static int WriteWorldSnapshot(
      Span<byte> dest,
      uint serverTick,
      uint lastAckedInputSeq,
      IReadOnlyList<PlayerState2D> players,
      IReadOnlyList<OrbState2D> orbs,
      IReadOnlyList<LaserState2D> lasers)
   {
      dest[0] = OpWorldSnapshot;

      BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(1, 4), serverTick);
      BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(5, 4), lastAckedInputSeq);

      var pCount = (byte)Math.Min(players.Count, 16);
      var oCount = (byte)Math.Min(orbs.Count, 12);
      var lCount = (byte)Math.Min(lasers.Count, 24);

      dest[9] = pCount;
      dest[10] = oCount;
      dest[11] = lCount;

      var offset = 12;

      for (var i = 0; i < pCount; i++)
      {
         var p = players[i];

         dest[offset++] = p.Id;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), p.X); offset += 4;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), p.Y); offset += 4;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), p.FacingAngle); offset += 4;

         dest[offset++] = p.Color;
         dest[offset++] = (byte)((p.Stunned ? 1 : 0) | (p.IsBot ? 2 : 0));
      }

      for (var i = 0; i < oCount; i++)
      {
         var o = orbs[i];

         dest[offset++] = o.Id;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), o.X); offset += 4;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), o.Y); offset += 4;

         dest[offset++] = o.Value;
      }

      for (var i = 0; i < lCount; i++)
      {
         var l = lasers[i];

         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), l.X); offset += 4;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), l.Y); offset += 4;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), l.Vx); offset += 4;
         BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(offset, 4), l.Vy); offset += 4;

         dest[offset++] = l.Color;
         dest[offset++] = l.OwnerId;

         BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(offset, 4), l.ShotSeq);
         offset += 4;
      }

      return offset;
   }

   public static void PatchSnapshotAckedInput(Span<byte> snapshotBuf, uint lastAckedInputSeq) =>
      BinaryPrimitives.WriteUInt32LittleEndian(snapshotBuf.Slice(5, 4), lastAckedInputSeq);

   public static bool TryReadWorldSnapshot(
      ReadOnlySpan<byte> src,
      out uint serverTick,
      out uint lastAckedInputSeq,
      List<PlayerState2D> players,
      List<OrbState2D> orbs,
      List<LaserState2D> lasers)
   {
      serverTick = 0;
      lastAckedInputSeq = 0;

      if (src.Length < 12 || src[0] != OpWorldSnapshot) return false;

      serverTick = BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(1, 4));
      lastAckedInputSeq = BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(5, 4));

      var pCount = src[9];
      var oCount = src[10];
      var lCount = src[11];

      var expected = 12 + (pCount * 15) + (oCount * 10) + (lCount * 22);
      if (src.Length < expected) return false;

      players.Clear();
      orbs.Clear();
      lasers.Clear();

      var offset = 12;

      for (var i = 0; i < pCount; i++)
      {
         var id = src[offset++];

         var x = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;
         var y = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;
         var ang = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;

         var col = src[offset++];
         var flags = src[offset++];

         players.Add(new PlayerState2D(id, x, y, ang, col, (flags & 1) != 0, (flags & 2) != 0));
      }

      for (var i = 0; i < oCount; i++)
      {
         var id = src[offset++];

         var x = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;
         var y = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;

         var val = src[offset++];

         orbs.Add(new OrbState2D(id, x, y, val));
      }

      for (var i = 0; i < lCount; i++)
      {
         var x = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;
         var y = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;
         var vx = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;
         var vy = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(offset, 4)); offset += 4;

         var col = src[offset++];
         var ownerId = src[offset++];

         var shotSeq = BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(offset, 4));
         offset += 4;

         lasers.Add(new LaserState2D(x, y, vx, vy, col, ownerId, shotSeq));
      }

      return true;
   }

   // --- Channel 3: Ephemeral FX ---

   public static int WriteFxCue(Span<byte> dest, float x, float y, FxKind2D kind, byte color)
   {
      dest[0] = OpFxCue;

      BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(1, 4), x);
      BinaryPrimitives.WriteSingleLittleEndian(dest.Slice(5, 4), y);

      dest[9] = (byte)kind;
      dest[10] = color;

      return 11;
   }

   public static bool TryReadFxCue(ReadOnlySpan<byte> src, out FxCue2D fx)
   {
      fx = default;
      if (src.Length < 11 || src[0] != OpFxCue) return false;

      var x = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(1, 4));
      var y = BinaryPrimitives.ReadSingleLittleEndian(src.Slice(5, 4));

      fx = new FxCue2D(x, y, (FxKind2D)src[9], src[10]);
      return true;
   }
}
