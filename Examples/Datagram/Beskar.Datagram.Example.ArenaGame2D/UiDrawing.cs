using System.Drawing.Drawing2D;
using System.Runtime.InteropServices;

namespace Beskar.Datagram.Example.ArenaGame2D;

internal static class UiTheme
{
   public static readonly Color WindowBg = Color.FromArgb(24, 24, 27);       // #18181B
   public static readonly Color HeaderBg = Color.FromArgb(30, 30, 34);       // #1E1E22
   public static readonly Color CardBg = Color.FromArgb(34, 34, 39);         // #222227
   public static readonly Color ControlBg = Color.FromArgb(44, 44, 51);      // #2C2C33
   public static readonly Color ControlHover = Color.FromArgb(54, 54, 63);   // #36363F
   public static readonly Color BorderSubtle = Color.FromArgb(52, 52, 60);   // #34343C
   public static readonly Color BorderStrong = Color.FromArgb(72, 72, 82);   // #484852
   public static readonly Color TrackBg = Color.FromArgb(54, 54, 64);        // #363640

   public static readonly Color ArenaBg = Color.FromArgb(27, 29, 32);        // #1B1D20
   public static readonly Color ArenaGrid = Color.FromArgb(35, 38, 43);      // #23262B
   public static readonly Color ArenaGridMajor = Color.FromArgb(45, 49, 56); // #2D3138
   public static readonly Color ObstacleFill = Color.FromArgb(50, 54, 62);   // #32363E
   public static readonly Color ObstacleTop = Color.FromArgb(64, 69, 79);    // #40454F
   public static readonly Color ObstacleEdge = Color.FromArgb(80, 86, 98);   // #505662

   public static readonly Color TextPrimary = Color.FromArgb(232, 232, 236);   // #E8E8EC
   public static readonly Color TextSecondary = Color.FromArgb(164, 166, 176); // #A4A6B0
   public static readonly Color TextMuted = Color.FromArgb(116, 118, 128);     // #747680

   public static readonly Color AccentPrimary = Color.FromArgb(78, 140, 194);  // Muted steel blue
   public static readonly Color StatusOk = Color.FromArgb(86, 174, 114);       // Sage green
   public static readonly Color StatusWarn = Color.FromArgb(214, 158, 62);     // Warm ochre
   public static readonly Color StatusBad = Color.FromArgb(204, 82, 82);       // Brick red

   public static readonly Color[] PlayerPalette =
   [
      Color.FromArgb(72, 156, 186),  // Steel Teal
      Color.FromArgb(204, 96, 84),   // Terracotta
      Color.FromArgb(98, 168, 108),  // Olive Green
      Color.FromArgb(214, 164, 68),  // Ochre Gold
      Color.FromArgb(148, 120, 188), // Muted Plum
      Color.FromArgb(104, 138, 198)  // Slate Blue
   ];

   public static readonly Color TokenSilver = Color.FromArgb(182, 196, 212);
   public static readonly Color TokenGold = Color.FromArgb(222, 174, 72);

   public static Font MakeFont(string family, float pixelSize, FontStyle style = FontStyle.Regular) =>
      new(family, pixelSize, style, GraphicsUnit.Pixel);

   public static GraphicsPath CreateRoundedRect(RectangleF r, float radius)
   {
      var path = new GraphicsPath();
      var d = Math.Min(radius * 2f, Math.Min(r.Width, r.Height));
      if (d <= 0.5f)
      {
         path.AddRectangle(r);
         return path;
      }

      path.AddArc(r.X, r.Y, d, d, 180, 90);
      path.AddArc(r.Right - d, r.Y, d, d, 270, 90);
      path.AddArc(r.Right - d, r.Bottom - d, d, d, 0, 90);
      path.AddArc(r.X, r.Bottom - d, d, d, 90, 90);
      path.CloseFigure();
      return path;
   }

   public static void FillRoundedRect(Graphics g, RectangleF rect, float radius, Color fill)
   {
      using var path = CreateRoundedRect(rect, radius);
      using var brush = new SolidBrush(fill);
      g.FillPath(brush, path);
   }

   public static void DrawRoundedRect(Graphics g, RectangleF rect, float radius, Color border, float width = 1f)
   {
      using var path = CreateRoundedRect(rect, radius);
      using var pen = new Pen(border, width);
      g.DrawPath(pen, path);
   }

   public static void DrawCard(Graphics g, RectangleF rect, string title, string? rightTag, Font titleFont, Font tagFont)
   {
      FillRoundedRect(g, rect, 4f, CardBg);
      DrawRoundedRect(g, rect, 4f, BorderSubtle, 1f);

      using var tBrush = new SolidBrush(TextPrimary);
      g.DrawString(title, titleFont, tBrush, rect.X + 12f, rect.Y + 8f);

      if (!string.IsNullOrEmpty(rightTag))
      {
         var sz = g.MeasureString(rightTag, tagFont);
         using var mBrush = new SolidBrush(TextMuted);
         g.DrawString(rightTag, tagFont, mBrush, rect.Right - sz.Width - 11f, rect.Y + 9.5f);
      }

      using var sep = new Pen(Color.FromArgb(44, 44, 52), 1f);
      g.DrawLine(sep, rect.X + 10f, rect.Y + 29f, rect.Right - 10f, rect.Y + 29f);
   }

   public static void DrawFlatButton(Graphics g, RectangleF rect, string text, Font font, bool active, bool hovered, bool primary = false)
   {
      Color fill;
      Color border;
      Color textCol;

      if (primary)
      {
         fill = hovered ? Color.FromArgb(90, 152, 206) : AccentPrimary;
         border = Color.FromArgb(106, 164, 214);
         textCol = Color.White;
      }
      else if (active)
      {
         fill = hovered ? Color.FromArgb(62, 106, 148) : Color.FromArgb(52, 92, 130);
         border = AccentPrimary;
         textCol = Color.White;
      }
      else
      {
         fill = hovered ? ControlHover : ControlBg;
         border = hovered ? BorderStrong : BorderSubtle;
         textCol = hovered ? TextPrimary : TextSecondary;
      }

      FillRoundedRect(g, rect, 4f, fill);
      DrawRoundedRect(g, rect, 4f, border, 1f);

      var sz = g.MeasureString(text, font);
      using var brush = new SolidBrush(textCol);
      g.DrawString(text, font, brush, rect.X + ((rect.Width - sz.Width) * 0.5f), rect.Y + ((rect.Height - sz.Height) * 0.5f));
   }

   public static void DrawCheckbox(Graphics g, RectangleF boxRect, bool isChecked, bool hovered)
   {
      var fill = isChecked ? AccentPrimary : ControlBg;
      var border = hovered ? TextSecondary : (isChecked ? AccentPrimary : BorderStrong);
      FillRoundedRect(g, boxRect, 3f, fill);
      DrawRoundedRect(g, boxRect, 3f, border, 1f);

      if (isChecked)
      {
         using var checkPen = new Pen(Color.White, 1.8f) { StartCap = LineCap.Round, EndCap = LineCap.Round, LineJoin = LineJoin.Round };
         var x = boxRect.X;
         var y = boxRect.Y;
         var w = boxRect.Width;
         var h = boxRect.Height;
         g.DrawLines(checkPen, [
            new PointF(x + (w * 0.22f), y + (h * 0.52f)),
            new PointF(x + (w * 0.43f), y + (h * 0.74f)),
            new PointF(x + (w * 0.78f), y + (h * 0.28f))
         ]);
      }
   }

   public static void DrawSlider(
      Graphics g,
      RectangleF trackRect,
      int value,
      int min,
      int max,
      bool hovered,
      bool dragging)
   {
      var t = max > min ? (float)(value - min) / (max - min) : 0f;
      t = Math.Clamp(t, 0f, 1f);

      var barY = trackRect.Y + (trackRect.Height * 0.5f) - 2f;
      FillRoundedRect(g, new RectangleF(trackRect.X, barY, trackRect.Width, 4f), 2f, TrackBg);

      if (t > 0.001f)
      {
         var fillW = Math.Max(4f, trackRect.Width * t);
         FillRoundedRect(g, new RectangleF(trackRect.X, barY, fillW, 4f), 2f, AccentPrimary);
      }

      var thumbX = trackRect.X + (trackRect.Width * t);
      var thumbY = trackRect.Y + (trackRect.Height * 0.5f);
      var r = dragging ? 6.5f : (hovered ? 6f : 5.5f);

      using var fillBrush = new SolidBrush(dragging || hovered ? Color.White : TextPrimary);
      using var ringPen = new Pen(Color.FromArgb(20, 20, 24), 1.5f);
      g.FillEllipse(fillBrush, thumbX - r, thumbY - r, r * 2f, r * 2f);
      g.DrawEllipse(ringPen, thumbX - r, thumbY - r, r * 2f, r * 2f);
   }

   public static void EnableDarkTitleBar(IntPtr hwnd)
   {
      if (!OperatingSystem.IsWindows() || hwnd == IntPtr.Zero) return;
      try
      {
         var useDark = 1;
         _ = DwmSetWindowAttribute(hwnd, 20, ref useDark, sizeof(int));
      }
      catch
      {
      }
   }

   public static void BeginHighResTimer()
   {
      if (!OperatingSystem.IsWindows()) return;
      try { _ = TimeBeginPeriod(1); } catch { }
   }

   public static void EndHighResTimer()
   {
      if (!OperatingSystem.IsWindows()) return;
      try { _ = TimeEndPeriod(1); } catch { }
   }

   [DllImport("dwmapi.dll")]
   private static extern int DwmSetWindowAttribute(IntPtr hwnd, int attr, ref int attrValue, int attrSize);

   [DllImport("winmm.dll", EntryPoint = "timeBeginPeriod")]
   private static extern uint TimeBeginPeriod(uint period);

   [DllImport("winmm.dll", EntryPoint = "timeEndPeriod")]
   private static extern uint TimeEndPeriod(uint period);
}
