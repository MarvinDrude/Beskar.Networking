using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Datagram.Example.ArenaGame;

public enum Direction : byte
{
   Up = 0,
   Down = 1,
   Left = 2,
   Right = 3
}

public enum FxKind : byte
{
   OrbPickup = 0,
   PlayerTag = 1,
   LaserShot = 2
}

public readonly record struct PlayerSnapshot(byte Id, byte X, byte Y, Direction Facing, byte Color, bool Stunned, bool IsBot);
public readonly record struct OrbSnapshot(byte X, byte Y, byte Value);
public readonly record struct LaserSnapshot(byte X, byte Y, Direction Dir, byte Color);
public readonly record struct ScoreEntry(byte Id, byte Color, ushort Score, string Name);
public readonly record struct FxCue(byte X, byte Y, FxKind Kind, byte Color);

/// <summary>
/// Shared channel layout and zero-allocation binary wire codecs used by both Host and Client.
/// </summary>
public static class GameProtocol
{
   public const int ArenaWidth = 48;
   public const int ArenaHeight = 16;
   public const int DefaultPort = 7777;
   public const int TickRateHz = 30;

   // Channel layout showcasing 4 distinct DatagramDeliveryModes:
   public const byte ChLifecycle = 0;   // ReliableOrdered     - welcome, join/leave, tag feed, emotes
   public const byte ChScoreboard = 1;  // ReliableSequenced   - latest-only authoritative scoreboard
   public const byte ChRealtime = 2;    // UnreliableSequenced - 30Hz player input & world snapshots
   public const byte ChFx = 3;          // UnreliableUnordered - fire-and-forget visual hit/pickup sparks

   // Message opcodes
   public const byte OpWelcome = 1;
   public const byte OpEventLog = 2;
   public const byte OpEmote = 3;
   public const byte OpScoreboard = 10;
   public const byte OpClientInput = 20;
   public const byte OpWorldSnapshot = 21;
   public const byte OpFxCue = 30;

   public static readonly string[] Emotes =
   [
      "GG!",
      "Nice shot!",
      "Watch out!",
      "Beskar is mine!"
   ];

   private static readonly byte[] GamePsk = "beskar-arena-example-v1"u8.ToArray();

   /// <summary>
   /// Builds identical DatagramOptions for host and clients (channel hash is validated during handshake).
   /// Uses ManualTick + Poll so all networking runs cleanly inside the fixed 30Hz game loop with zero locks.
   /// </summary>
   public static DatagramOptions CreateOptions() => new DatagramOptions
   {
      ManualTick = true,
      DispatchMode = DatagramEventDispatchMode.Poll,
      MaxPeers = 16,
      ConnectTimeout = TimeSpan.FromSeconds(4),
      HandshakeRetryInterval = TimeSpan.FromMilliseconds(150),
      KeepAliveInterval = TimeSpan.FromMilliseconds(400),
      PeerTimeout = TimeSpan.FromSeconds(5),
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

   // --- Channel 0 (ReliableOrdered): Welcome, EventLog, Emote ---

   public static int WriteWelcome(Span<byte> dest, byte playerId, byte color)
   {
      dest[0] = OpWelcome;
      dest[1] = playerId;
      dest[2] = color;
      return 3;
   }

   public static bool TryReadWelcome(ReadOnlySpan<byte> src, out byte playerId, out byte color)
   {
      playerId = 0;
      color = 0;
      if (src.Length < 3 || src[0] != OpWelcome) return false;
      playerId = src[1];
      color = src[2];
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

   // --- Channel 1 (ReliableSequenced): Authoritative Scoreboard ---

   public static int WriteScoreboard(Span<byte> dest, IReadOnlyList<ScoreEntry> entries)
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

         var nameSlice = e.Name.Length > 12 ? e.Name.AsSpan(0, 12) : e.Name.AsSpan();
         var nameBytes = Encoding.UTF8.GetBytes(nameSlice, dest[(offset + 1)..]);
         dest[offset++] = (byte)nameBytes;
         offset += nameBytes;
      }

      return offset;
   }

   public static bool TryReadScoreboard(ReadOnlySpan<byte> src, List<ScoreEntry> output)
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
         var nameLen = src[offset++];
         if (offset + nameLen > src.Length) return false;
         var name = Encoding.UTF8.GetString(src.Slice(offset, nameLen));
         offset += nameLen;
         output.Add(new ScoreEntry(id, color, score, name));
      }

      return true;
   }

   // --- Channel 2 (UnreliableSequenced): 30Hz Client Input & World Snapshot ---

   public static int WriteClientInput(Span<byte> dest, sbyte dx, sbyte dy, Direction facing, bool fire)
   {
      dest[0] = OpClientInput;
      dest[1] = unchecked((byte)dx);
      dest[2] = unchecked((byte)dy);
      dest[3] = (byte)facing;
      dest[4] = fire ? (byte)1 : (byte)0;
      return 5;
   }

   public static bool TryReadClientInput(ReadOnlySpan<byte> src, out sbyte dx, out sbyte dy, out Direction facing, out bool fire)
   {
      dx = 0;
      dy = 0;
      facing = Direction.Right;
      fire = false;
      if (src.Length < 5 || src[0] != OpClientInput) return false;

      dx = Math.Clamp(unchecked((sbyte)src[1]), (sbyte)-1, (sbyte)1);
      dy = Math.Clamp(unchecked((sbyte)src[2]), (sbyte)-1, (sbyte)1);
      facing = src[3] <= (byte)Direction.Right ? (Direction)src[3] : Direction.Right;
      fire = (src[4] & 1) != 0;
      return true;
   }

   public static int WriteWorldSnapshot(
      Span<byte> dest,
      uint tick,
      IReadOnlyList<PlayerSnapshot> players,
      IReadOnlyList<OrbSnapshot> orbs,
      IReadOnlyList<LaserSnapshot> lasers)
   {
      dest[0] = OpWorldSnapshot;
      BinaryPrimitives.WriteUInt32LittleEndian(dest.Slice(1, 4), tick);
      var pCount = (byte)Math.Min(players.Count, 16);
      var oCount = (byte)Math.Min(orbs.Count, 16);
      var lCount = (byte)Math.Min(lasers.Count, 32);

      dest[5] = pCount;
      dest[6] = oCount;
      dest[7] = lCount;
      var offset = 8;

      for (var i = 0; i < pCount; i++)
      {
         var p = players[i];
         dest[offset++] = p.Id;
         dest[offset++] = p.X;
         dest[offset++] = p.Y;
         dest[offset++] = (byte)p.Facing;
         dest[offset++] = p.Color;
         dest[offset++] = (byte)((p.Stunned ? 1 : 0) | (p.IsBot ? 2 : 0));
      }

      for (var i = 0; i < oCount; i++)
      {
         var o = orbs[i];
         dest[offset++] = o.X;
         dest[offset++] = o.Y;
         dest[offset++] = o.Value;
      }

      for (var i = 0; i < lCount; i++)
      {
         var l = lasers[i];
         dest[offset++] = l.X;
         dest[offset++] = l.Y;
         dest[offset++] = (byte)l.Dir;
         dest[offset++] = l.Color;
      }

      return offset;
   }

   public static bool TryReadWorldSnapshot(
      ReadOnlySpan<byte> src,
      out uint tick,
      List<PlayerSnapshot> players,
      List<OrbSnapshot> orbs,
      List<LaserSnapshot> lasers)
   {
      tick = 0;
      if (src.Length < 8 || src[0] != OpWorldSnapshot) return false;

      tick = BinaryPrimitives.ReadUInt32LittleEndian(src.Slice(1, 4));
      var pCount = src[5];
      var oCount = src[6];
      var lCount = src[7];

      var expected = 8 + (pCount * 6) + (oCount * 3) + (lCount * 4);
      if (src.Length < expected) return false;

      players.Clear();
      orbs.Clear();
      lasers.Clear();

      var offset = 8;
      for (var i = 0; i < pCount; i++)
      {
         var id = src[offset++];
         var x = src[offset++];
         var y = src[offset++];
         var facing = (Direction)src[offset++];
         var color = src[offset++];
         var flags = src[offset++];
         players.Add(new PlayerSnapshot(id, x, y, facing, color, (flags & 1) != 0, (flags & 2) != 0));
      }

      for (var i = 0; i < oCount; i++)
      {
         var x = src[offset++];
         var y = src[offset++];
         var val = src[offset++];
         orbs.Add(new OrbSnapshot(x, y, val));
      }

      for (var i = 0; i < lCount; i++)
      {
         var x = src[offset++];
         var y = src[offset++];
         var dir = (Direction)src[offset++];
         var color = src[offset++];
         lasers.Add(new LaserSnapshot(x, y, dir, color));
      }

      return true;
   }

   // --- Channel 3 (UnreliableUnordered): Ephemeral Visual FX ---

   public static int WriteFxCue(Span<byte> dest, byte x, byte y, FxKind kind, byte color)
   {
      dest[0] = OpFxCue;
      dest[1] = x;
      dest[2] = y;
      dest[3] = (byte)kind;
      dest[4] = color;
      return 5;
   }

   public static bool TryReadFxCue(ReadOnlySpan<byte> src, out FxCue fx)
   {
      fx = default;
      if (src.Length < 5 || src[0] != OpFxCue) return false;
      fx = new FxCue(src[1], src[2], (FxKind)src[3], src[4]);
      return true;
   }
}
