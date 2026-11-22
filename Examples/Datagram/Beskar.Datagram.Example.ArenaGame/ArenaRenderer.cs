using System.Runtime.InteropServices;
using System.Text;

namespace Beskar.Datagram.Example.ArenaGame;

public readonly record struct NetworkHudInfo(
   string RoleLabel,
   string EndPointLabel,
   int RemotePeers,
   double RttMs,
   double LossPercent,
   string CongestionLabel,
   long BytesSent,
   long BytesReceived,
   uint ServerTick,
   bool ShowBotHint);

/// <summary>
/// Flicker-free single-write ANSI console renderer for the 30 Hz arena game.
/// </summary>
public sealed class ArenaRenderer
{
   private readonly record struct ActiveSpark(byte X, byte Y, FxKind Kind, byte Color, int RemainingTicks);

   private static readonly string[] PlayerPalette =
   [
      "\x1b[96m", // Bright Cyan
      "\x1b[95m", // Bright Magenta
      "\x1b[92m", // Bright Green
      "\x1b[93m", // Bright Yellow
      "\x1b[91m", // Bright Red
      "\x1b[94m"  // Bright Blue
   ];

   private const string Reset = "\x1b[0m";
   private const string Dim = "\x1b[90m";
   private const string Bold = "\x1b[1m";
   private const string White = "\x1b[97m";
   private const string Gold = "\x1b[38;5;220m";
   private const string Cyan = "\x1b[36m";
   private const string ClearEol = "\x1b[K";

   private readonly StringBuilder _sb = new(4096);
   private readonly char[,] _glyphs = new char[GameProtocol.ArenaHeight, GameProtocol.ArenaWidth];
   private readonly string?[,] _colors = new string?[GameProtocol.ArenaHeight, GameProtocol.ArenaWidth];
   private readonly List<ActiveSpark> _sparks = [];
   private readonly Queue<string> _feed = new();
   private readonly Dictionary<byte, string> _namesById = new();

   public void BeginConsoleSession()
   {
      TryEnableAnsiOnWindows();
      try
      {
         Console.OutputEncoding = Encoding.UTF8;
         Console.CursorVisible = false;
         Console.Write("\x1b[2J\x1b[H");
      }
      catch
      {
      }
   }

   public void EndConsoleSession()
   {
      try
      {
         Console.Write(Reset);
         Console.CursorVisible = true;
         Console.WriteLine();
      }
      catch
      {
      }
   }

   public void AddFeedLine(string text)
   {
      _feed.Enqueue(text);
      while (_feed.Count > 4)
      {
         _feed.Dequeue();
      }
   }

   public void AddFx(FxCue fx)
   {
      var duration = fx.Kind switch
      {
         FxKind.PlayerTag => 10,
         FxKind.OrbPickup => 7,
         _ => 3
      };
      _sparks.Add(new ActiveSpark(fx.X, fx.Y, fx.Kind, fx.Color, duration));
   }

   public void RenderFrame(
      byte myPlayerId,
      IReadOnlyList<PlayerSnapshot> players,
      IReadOnlyList<OrbSnapshot> orbs,
      IReadOnlyList<LaserSnapshot> lasers,
      IReadOnlyList<ScoreEntry> scores,
      in NetworkHudInfo hud)
   {
      // Update name lookup from latest authoritative scoreboard
      for (var i = 0; i < scores.Count; i++)
      {
         _namesById[scores[i].Id] = scores[i].Name;
      }

      // Clear playfield buffer
      for (var y = 0; y < GameProtocol.ArenaHeight; y++)
      {
         for (var x = 0; x < GameProtocol.ArenaWidth; x++)
         {
            _glyphs[y, x] = ' ';
            _colors[y, x] = null;
         }
      }

      // Subtle arena grid dots
      for (var y = 2; y < GameProtocol.ArenaHeight; y += 4)
      {
         for (var x = 4; x < GameProtocol.ArenaWidth; x += 8)
         {
            _glyphs[y, x] = '·';
            _colors[y, x] = Dim;
         }
      }

      // Draw Beskar orbs
      for (var i = 0; i < orbs.Count; i++)
      {
         var o = orbs[i];
         if (o.Y < GameProtocol.ArenaHeight && o.X < GameProtocol.ArenaWidth)
         {
            _glyphs[o.Y, o.X] = o.Value > 1 ? '★' : '◆';
            _colors[o.Y, o.X] = o.Value > 1 ? Gold : Cyan;
         }
      }

      // Draw active ephemeral sparks (Channel 3 FX)
      for (var i = _sparks.Count - 1; i >= 0; i--)
      {
         var sp = _sparks[i];
         if (sp.Y < GameProtocol.ArenaHeight && sp.X < GameProtocol.ArenaWidth)
         {
            _glyphs[sp.Y, sp.X] = sp.Kind switch
            {
               FxKind.PlayerTag => '✦',
               FxKind.OrbPickup => '*',
               _ => '•'
            };
            _colors[sp.Y, sp.X] = sp.Kind == FxKind.PlayerTag ? "\x1b[91m" : Gold;
         }

         if (sp.RemainingTicks <= 1)
         {
            _sparks.RemoveAt(i);
         }
         else
         {
            _sparks[i] = sp with { RemainingTicks = sp.RemainingTicks - 1 };
         }
      }

      // Draw laser bolts
      for (var i = 0; i < lasers.Count; i++)
      {
         var l = lasers[i];
         if (l.Y < GameProtocol.ArenaHeight && l.X < GameProtocol.ArenaWidth)
         {
            _glyphs[l.Y, l.X] = l.Dir is Direction.Left or Direction.Right ? '─' : '│';
            _colors[l.Y, l.X] = Palette(l.Color);
         }
      }

      // Draw players
      for (var i = 0; i < players.Count; i++)
      {
         var p = players[i];
         if (p.Y >= GameProtocol.ArenaHeight || p.X >= GameProtocol.ArenaWidth) continue;

         char glyph;
         if (p.Stunned)
         {
            glyph = 'x';
         }
         else if (p.Id == myPlayerId)
         {
            glyph = '@';
         }
         else if (p.IsBot)
         {
            glyph = 'B';
         }
         else if (_namesById.TryGetValue(p.Id, out var name) && name.Length > 0)
         {
            glyph = char.ToUpperInvariant(name[0]);
         }
         else
         {
            glyph = 'P';
         }

         _glyphs[p.Y, p.X] = glyph;
         _colors[p.Y, p.X] = p.Stunned ? "\x1b[90m" : Palette(p.Color);
      }

      // Build right-side HUD panel lines (aligned with the 18 rows of the arena box)
      var sideLines = BuildSidePanel(myPlayerId, scores, in hud);

      _sb.Clear();
      _sb.Append("\x1b[H");
      _sb.Append($"{Bold}{Cyan} BESKAR ARENA{Reset} {Dim}|{Reset} {White}{hud.RoleLabel}{Reset} {Dim}({hud.EndPointLabel}){Reset} {Dim}| AEAD + PSK | Tick {hud.ServerTick}{Reset}{ClearEol}\n");

      // Top arena border + side panel line 0
      _sb.Append($"{Cyan}╔{new string('═', GameProtocol.ArenaWidth)}╗{Reset}  {sideLines[0]}{ClearEol}\n");

      for (var y = 0; y < GameProtocol.ArenaHeight; y++)
      {
         _sb.Append($"{Cyan}║{Reset}");
         string? activeColor = null;
         for (var x = 0; x < GameProtocol.ArenaWidth; x++)
         {
            var c = _colors[y, x];
            if (c != activeColor)
            {
               _sb.Append(c ?? Reset);
               activeColor = c;
            }
            _sb.Append(_glyphs[y, x]);
         }

         if (activeColor is not null) _sb.Append(Reset);
         _sb.Append($"{Cyan}║{Reset}  {sideLines[y + 1]}{ClearEol}\n");
      }

      // Bottom arena border + side panel line 17
      _sb.Append($"{Cyan}╚{new string('═', GameProtocol.ArenaWidth)}╝{Reset}  {sideLines[GameProtocol.ArenaHeight + 1]}{ClearEol}\n");

      // Event feed (Ch 0 ReliableOrdered)
      _sb.Append($"{Bold}{White} Event Feed{Reset} {Dim}(Ch 0 ReliableOrdered):{Reset}{ClearEol}\n");
      var feedIdx = 0;
      foreach (var line in _feed)
      {
         _sb.Append($"  {Dim}>{Reset} {line}{ClearEol}\n");
         feedIdx++;
      }
      for (; feedIdx < 4; feedIdx++)
      {
         _sb.Append($"{ClearEol}\n");
      }

      // Controls bar
      var botHint = hud.ShowBotHint ? " | [B] Cycle Bots" : "";
      _sb.Append($"{Dim} Controls: [WASD/Arrows] Move | [Space] Laser | [T] Emote{botHint} | [Q] Quit{Reset}{ClearEol}\n");
      _sb.Append($"{Dim} Legend: {White}@{Dim} You  {Cyan}◆{Dim} Orb (+1)  {Gold}★{Dim} Gold (+3)  {White}─│{Dim} Laser (+2 tag){Reset}{ClearEol}");

      Console.Write(_sb.ToString());
   }

   private static string[] BuildSidePanel(byte myPlayerId, IReadOnlyList<ScoreEntry> scores, in NetworkHudInfo hud)
   {
      var lines = new string[GameProtocol.ArenaHeight + 2];
      Array.Fill(lines, string.Empty);

      lines[0] = $"{Bold}{Gold}SCOREBOARD{Reset} {Dim}(Ch 1 RelSeq){Reset}";
      lines[1] = $"{Dim}──────────────────────────────{Reset}";

      var maxScoreRows = 6;
      for (var i = 0; i < Math.Min(scores.Count, maxScoreRows); i++)
      {
         var s = scores[i];
         var youTag = s.Id == myPlayerId ? " (you)" : "";
         var label = $"{s.Name}{youTag}";
         if (label.Length > 20) label = label[..20];
         lines[2 + i] = $"{Palette(s.Color)}● {label,-20}{Reset} {Bold}{White}{s.Score,4}{Reset}";
      }

      lines[9] = $"{Bold}{Cyan}NETWORK HUD{Reset} {Dim}(DatagramStats){Reset}";
      lines[10] = $"{Dim}──────────────────────────────{Reset}";
      lines[11] = $" Peers : {White}{hud.RemotePeers,-4}{Reset}  State: {White}{hud.CongestionLabel}{Reset}";
      lines[12] = $" RTT   : {White}{hud.RttMs,5:F1} ms{Reset}  Loss: {White}{hud.LossPercent,4:F1}%{Reset}";
      lines[13] = $" TX/RX : {White}{hud.BytesSent / 1024.0:F1} / {hud.BytesReceived / 1024.0:F1} KB{Reset}";
      lines[15] = $"{Dim}Channels: Ch0 RelOrd | Ch1 RelSeq{Reset}";
      lines[16] = $"{Dim}          Ch2 UnrelSeq (30Hz){Reset}";
      lines[17] = $"{Dim}          Ch3 UnrelUnord (FX){Reset}";

      return lines;
   }

   private static string Palette(byte index) => PlayerPalette[index % PlayerPalette.Length];

   private static void TryEnableAnsiOnWindows()
   {
      if (!OperatingSystem.IsWindows()) return;
      try
      {
         const int stdOutputHandle = -11;
         const uint enableVirtualTerminalProcessing = 0x0004;
         var handle = GetStdHandle(stdOutputHandle);
         if (GetConsoleMode(handle, out var mode))
         {
            SetConsoleMode(handle, mode | enableVirtualTerminalProcessing);
         }
      }
      catch
      {
         // ignored
      }
   }

   [DllImport("kernel32.dll", SetLastError = true)]
   private static extern IntPtr GetStdHandle(int nStdHandle);

   [DllImport("kernel32.dll", SetLastError = true)]
   private static extern bool GetConsoleMode(IntPtr hConsoleHandle, out uint lpMode);

   [DllImport("kernel32.dll", SetLastError = true)]
   private static extern bool SetConsoleMode(IntPtr hConsoleHandle, uint dwMode);
}
