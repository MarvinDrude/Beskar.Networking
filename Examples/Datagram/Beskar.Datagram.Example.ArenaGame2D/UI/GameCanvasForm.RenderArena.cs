using System.Drawing.Drawing2D;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

public sealed partial class GameCanvasForm
{
   protected override void OnPaint(PaintEventArgs e)
   {
      PaintEntireWindow(e.Graphics);
   }

   private void PaintEntireWindow(Graphics g)
   {
      g.SmoothingMode = SmoothingMode.AntiAlias;
      g.TextRenderingHint = System.Drawing.Text.TextRenderingHint.ClearTypeGridFit;
      g.Clear(UiTheme.WindowBg);

      PaintHeader(g);
      PaintArenaViewport(g);
      PaintControlDeck(g);
      PaintFooter(g);
   }

   private void PaintHeader(Graphics g)
   {
      using (var bg = new SolidBrush(UiTheme.HeaderBg))
         g.FillRectangle(bg, _headerRect);

      using (var line = new Pen(UiTheme.BorderSubtle, 1f))
         g.DrawLine(line, 0, _headerRect.Bottom, _winW, _headerRect.Bottom);

      using var whiteBrush = new SolidBrush(UiTheme.TextPrimary);
      using var mutedBrush = new SolidBrush(UiTheme.TextSecondary);

      g.DrawString("Beskar Arena", _fonts.Brand, whiteBrush, 18f, 16f);

      // Subtle neutral role tag
      var roleText = _isHost ? "HOST" : "CLIENT";
      var roleRect = new RectangleF(134, 16, 62, 22);

      UiTheme.FillRoundedRect(g, roleRect, 3f, UiTheme.ControlBg);
      UiTheme.DrawRoundedRect(g, roleRect, 3f, UiTheme.BorderStrong, 1f);

      var rsz = g.MeasureString(roleText, _fonts.Badge);
      using (var rb = new SolidBrush(UiTheme.TextPrimary))
         g.DrawString(roleText, _fonts.Badge, rb, roleRect.X + ((roleRect.Width - rsz.Width) * 0.5f), roleRect.Y + ((roleRect.Height - rsz.Height) * 0.5f));

      // Endpoint & security info
      var epStr = _isHost
         ? $"UDP :{_datagramHost?.LocalEndPoint?.Port ?? _targetEndPoint.Port}  |  AEAD + PSK"
         : $"{_targetEndPoint}  |  AEAD + PSK";

      g.DrawString(epStr, _fonts.Mono, mutedBrush, 210f, 19f);

      // Clean connection status indicator
      var (healthLabel, healthCol) = GetConnectionHealthBadge();

      UiTheme.FillRoundedRect(g, _healthRect, 4f, UiTheme.CardBg);
      UiTheme.DrawRoundedRect(g, _healthRect, 4f, UiTheme.BorderSubtle, 1f);

      using (var dot = new SolidBrush(healthCol))
         g.FillEllipse(dot, _healthRect.X + 10f, _healthRect.Y + 10f, 8f, 8f);

      using (var hb = new SolidBrush(UiTheme.TextPrimary))
         g.DrawString(healthLabel, _fonts.MonoSmallBold, hb, _healthRect.X + 24f, _healthRect.Y + 7f);

      foreach (var btn in _buttons)
      {
         if (btn.Rect.Y < _headerRect.Bottom)
         {
            UiTheme.DrawFlatButton(g, btn.Rect, btn.Text(), _fonts.Button, btn.IsActive(), btn.Rect.Contains(_cursorPos), btn.Primary);
         }
      }
   }

   private void PaintArenaViewport(Graphics g)
   {
      var clipState = g.Save();
      using var clipPath = UiTheme.CreateRoundedRect(_arenaRect, 4f);
      g.SetClip(clipPath);

      var myId = _isHost ? (_hostLocalPlayer?.Id ?? 0) : _netcode.MyPlayerId;

      // --- WORLD-SPACE VECTOR PASS (scaled crisply to viewport resolution) ---
      var worldState = g.Save();
      g.TranslateTransform(_arenaRect.X, _arenaRect.Y);
      g.ScaleTransform(_arenaScale, _arenaScale);

      PaintArenaFloorAndGrid(g);
      PaintObstacles(g);
      PaintVisualRings(g);
      PaintOrbs(g);

      if (_debugOverlayEnabled)
      {
         if (!_isHost) PaintClientDebugUnderlayGeometry(g);
         else PaintHostRewindMarkersGeometry(g);
      }

      PaintLasers(g);

      foreach (var p in _renderPlayers)
      {
         PaintPlayerEntityGeometry(g, p, isLocal: p.Id == myId);
      }

      g.Restore(worldState);

      // --- 1:1 NATIVE PHYSICAL-PIXEL TEXT & HUD PASS (sharp ClearType at every window size) ---
      if (_debugOverlayEnabled && !_isHost)
      {
         PaintClientDebugUnderlayLabels(g);
      }

      foreach (var p in _renderPlayers)
      {
         PaintPlayerLabel(g, p, isLocal: p.Id == myId);
      }

      PaintFloatingPopups(g);

      if (_debugOverlayEnabled)
      {
         if (_isHost) PaintHostDebugOverlay(g);
         else PaintClientDebugTelemetryStrip(g);
      }

      PaintDisconnectBannerIfNeeded(g);

      g.Restore(clipState);

      UiTheme.DrawRoundedRect(g, _arenaRect, 4f, UiTheme.BorderStrong, 1f);
   }

   private void PaintArenaFloorAndGrid(Graphics g)
   {
      using (var bg = new SolidBrush(UiTheme.ArenaBg))
         g.FillRectangle(bg, 0, 0, GamePhysics2D.ArenaWidth, GamePhysics2D.ArenaHeight);

      var invScale = 1f / Math.Max(0.5f, _arenaScale);
      using var minorPen = new Pen(UiTheme.ArenaGrid, invScale);
      using var majorPen = new Pen(UiTheme.ArenaGridMajor, invScale);

      for (var x = 40; x < GamePhysics2D.ArenaWidth; x += 40)
      {
         g.DrawLine((x % 160 == 0) ? majorPen : minorPen, x, 0, x, GamePhysics2D.ArenaHeight);
      }

      for (var y = 40; y < GamePhysics2D.ArenaHeight; y += 40)
      {
         g.DrawLine((y % 160 == 0) ? majorPen : minorPen, 0, y, GamePhysics2D.ArenaWidth, y);
      }
   }

   private static void PaintObstacles(Graphics g)
   {
      foreach (var obs in GamePhysics2D.Obstacles)
      {
         var shadowRect = new RectangleF(obs.X + 3, obs.Y + 4, obs.W, obs.H);
         UiTheme.FillRoundedRect(g, shadowRect, 3f, Color.FromArgb(95, 0, 0, 0));

         var blockRect = new RectangleF(obs.X, obs.Y, obs.W, obs.H);
         UiTheme.FillRoundedRect(g, blockRect, 3f, UiTheme.ObstacleFill);

         using (var topBrush = new SolidBrush(UiTheme.ObstacleTop))
            g.FillRectangle(topBrush, obs.X + 2, obs.Y + 2, obs.W - 4, 6);

         using (var insetPen = new Pen(Color.FromArgb(40, 43, 50), 1f))
            g.DrawRectangle(insetPen, obs.X + 6, obs.Y + 12, obs.W - 12, obs.H - 22);

         UiTheme.DrawRoundedRect(g, blockRect, 3f, UiTheme.ObstacleEdge, 1.2f);
      }
   }

   private void PaintVisualRings(Graphics g)
   {
      foreach (var ring in _rings)
      {
         var t = ring.Age / ring.MaxAge;

         if (ring.Kind == FxKind2D.HitSpark)
         {
            var alpha = Math.Clamp((int)((1f - t) * 235), 0, 255);
            var inner = 3f + (t * 4f);
            var outer = 8f + (t * 10f);

            using var sparkPen = new Pen(Color.FromArgb(alpha, 244, 240, 224), 1.8f) { StartCap = LineCap.Round, EndCap = LineCap.Round };
            for (var k = 0; k < 4; k++)
            {
               var ang = (MathF.PI * 0.25f) + (k * MathF.PI * 0.5f);
               var ca = MathF.Cos(ang);
               var sa = MathF.Sin(ang);
               g.DrawLine(sparkPen, ring.X + (ca * inner), ring.Y + (sa * inner), ring.X + (ca * outer), ring.Y + (sa * outer));
            }

            var sr = 4f + (t * 9f);
            using var ringPen = new Pen(Color.FromArgb((int)(alpha * 0.65f), ring.Color), 1.3f);
            g.DrawEllipse(ringPen, ring.X - sr, ring.Y - sr, sr * 2f, sr * 2f);
            continue;
         }

         var radius = ring.Kind switch
         {
            FxKind2D.PlayerTag => 10f + (t * 36f),
            FxKind2D.OrbPickup => 7f + (t * 24f),
            _ => 3f + (t * 11f)
         };

         var ringAlpha = Math.Clamp((int)((1f - t) * 190), 0, 255);
         using var p1 = new Pen(Color.FromArgb(ringAlpha, ring.Color), ring.Kind == FxKind2D.PlayerTag ? 2f : 1.4f);
         g.DrawEllipse(p1, ring.X - radius, ring.Y - radius, radius * 2f, radius * 2f);
      }
   }

   private void PaintOrbs(Graphics g)
   {
      var timeSec = (float)_clock.Elapsed.TotalSeconds;

      foreach (var orb in _renderOrbs)
      {
         var isGold = orb.Value > 1;
         var col = isGold ? UiTheme.TokenGold : UiTheme.TokenSilver;
         var r = isGold ? 9.5f : 7.5f;

         using (var sh = new SolidBrush(Color.FromArgb(95, 0, 0, 0)))
            g.FillEllipse(sh, orb.X - r + 1.5f, orb.Y - r + 2.5f, r * 2f, r * 2f);

         var angle = (timeSec * (isGold ? 1.5f : 0.9f)) + orb.Id;
         var pts = new PointF[4];
         for (var k = 0; k < 4; k++)
         {
            var a = angle + (k * MathF.PI * 0.5f);
            pts[k] = new PointF(orb.X + (MathF.Cos(a) * r), orb.Y + (MathF.Sin(a) * r));
         }

         using (var fill = new SolidBrush(col)) g.FillPolygon(fill, pts);
         using (var outline = new Pen(Color.FromArgb(16, 18, 22), 1.4f)) g.DrawPolygon(outline, pts);

         if (isGold)
         {
            using var inner = new SolidBrush(Color.FromArgb(252, 232, 168));
            g.FillEllipse(inner, orb.X - 2.2f, orb.Y - 2.2f, 4.4f, 4.4f);
         }
      }
   }

   private void PaintLasers(Graphics g)
   {
      foreach (var l in _renderLasers)
      {
         var col = UiTheme.PlayerPalette[l.Color % UiTheme.PlayerPalette.Length];
         var spd = MathF.Sqrt((l.Vx * l.Vx) + (l.Vy * l.Vy));

         var tx = spd > 1f ? l.X - ((l.Vx / spd) * 15f) : l.X;
         var ty = spd > 1f ? l.Y - ((l.Vy / spd) * 15f) : l.Y;

         using var tailPen = new Pen(col, 3.6f) { StartCap = LineCap.Round, EndCap = LineCap.Round };
         using var corePen = new Pen(Color.FromArgb(245, 245, 240), 1.6f) { StartCap = LineCap.Round, EndCap = LineCap.Round };

         g.DrawLine(tailPen, tx, ty, l.X, l.Y);
         g.DrawLine(corePen, tx, ty, l.X, l.Y);

         if (_debugOverlayEnabled && !_isHost && _netcode.IsLaserUnconfirmedPredicted(l))
         {
            using var pPen = new Pen(Color.FromArgb(180, UiTheme.StatusWarn), 1f) { DashStyle = DashStyle.Dot };
            g.DrawEllipse(pPen, l.X - 6f, l.Y - 6f, 12f, 12f);
         }
      }
   }

   private void PaintPlayerEntityGeometry(Graphics g, PlayerState2D p, bool isLocal)
   {
      var baseCol = UiTheme.PlayerPalette[p.Color % UiTheme.PlayerPalette.Length];
      var fillCol = p.Stunned ? Color.FromArgb(108, 112, 120) : baseCol;

      // Subtle matte afterimage trail
      if (_trailsById.TryGetValue(p.Id, out var trail) && trail.Count > 1)
      {
         var idx = 0;
         foreach (var pt in trail)
         {
            idx++;
            if (idx == trail.Count) break;

            var ratio = (float)idx / trail.Count;
            var alpha = (int)(ratio * 24);
            var s = GamePhysics2D.PlayerSize * (0.65f + (ratio * 0.25f));

            UiTheme.FillRoundedRect(g, new RectangleF(pt.X - (s * 0.5f), pt.Y - (s * 0.5f), s, s), 3f, Color.FromArgb(alpha, fillCol));
         }
      }

      var x = p.X - GamePhysics2D.PlayerHalf;
      var y = p.Y - GamePhysics2D.PlayerHalf;
      var rect = new RectangleF(x, y, GamePhysics2D.PlayerSize, GamePhysics2D.PlayerSize);

      // Grounding drop shadow
      UiTheme.FillRoundedRect(g, new RectangleF(x + 2, y + 3, GamePhysics2D.PlayerSize, GamePhysics2D.PlayerSize), 3f, Color.FromArgb(110, 0, 0, 0));

      // Solid flat-shaded tactical 2D rectangle with crisp bevel & dark outline
      UiTheme.FillRoundedRect(g, rect, 3f, fillCol);

      using (var hiBrush = new SolidBrush(Color.FromArgb(48, 255, 255, 255)))
         g.FillRectangle(hiBrush, x + 2, y + 2, GamePhysics2D.PlayerSize - 4, 4);

      using (var darkCore = new SolidBrush(Color.FromArgb(55, 16, 18, 22)))
         g.FillRectangle(darkCore, x + 6, y + 7, GamePhysics2D.PlayerSize - 12, GamePhysics2D.PlayerSize - 13);

      UiTheme.DrawRoundedRect(g, rect, 3f, isLocal ? Color.FromArgb(240, 240, 244) : Color.FromArgb(18, 20, 24), isLocal ? 1.8f : 1.3f);

      // Clean solid turret pointer
      var cos = MathF.Cos(p.FacingAngle);
      var sin = MathF.Sin(p.FacingAngle);
      var tipX = p.X + (cos * (GamePhysics2D.PlayerHalf + 7f));
      var tipY = p.Y + (sin * (GamePhysics2D.PlayerHalf + 7f));

      using (var outline = new Pen(Color.FromArgb(18, 20, 24), 4.6f) { StartCap = LineCap.Round, EndCap = LineCap.Round })
      using (var barrel = new Pen(Color.FromArgb(236, 236, 240), 2.2f) { StartCap = LineCap.Round, EndCap = LineCap.Round })
      {
         g.DrawLine(outline, p.X, p.Y, tipX, tipY);
         g.DrawLine(barrel, p.X, p.Y, tipX, tipY);
      }

      if (p.Stunned)
      {
         using var stunRing = new Pen(UiTheme.StatusBad, 1.5f) { DashStyle = DashStyle.Dash };
         g.DrawEllipse(stunRing, x - 4, y - 4, GamePhysics2D.PlayerSize + 8, GamePhysics2D.PlayerSize + 8);

         using var xPen = new Pen(UiTheme.StatusBad, 2f);
         g.DrawLine(xPen, x + 7, y + 7, x + GamePhysics2D.PlayerSize - 7, y + GamePhysics2D.PlayerSize - 7);
         g.DrawLine(xPen, x + GamePhysics2D.PlayerSize - 7, y + 7, x + 7, y + GamePhysics2D.PlayerSize - 7);
      }
   }

   private void PaintPlayerLabel(Graphics g, PlayerState2D p, bool isLocal)
   {
      var label = _namesById.TryGetValue(p.Id, out var nm) ? nm : (p.IsBot ? "Bot" : $"P{p.Id}");
      if (isLocal) label += " (you)";

      var topCenter = WorldToScreen(p.X, p.Y - GamePhysics2D.PlayerHalf);
      var tsz = g.MeasureString(label, _fonts.MonoSmallBold);

      var tagW = tsz.Width + 10f;
      var tagX = Math.Clamp(topCenter.X - (tagW * 0.5f), _arenaRect.X + 4f, _arenaRect.Right - tagW - 4f);
      var tagY = Math.Max(_arenaRect.Y + 4f, topCenter.Y - 22f);
      var tagRect = new RectangleF(tagX, tagY, tagW, 16f);

      UiTheme.FillRoundedRect(g, tagRect, 3f, Color.FromArgb(185, 18, 19, 22));

      using var txtBrush = new SolidBrush(isLocal ? Color.White : UiTheme.TextPrimary);
      g.DrawString(label, _fonts.MonoSmallBold, txtBrush, tagRect.X + 5f, tagRect.Y + 1.5f);
   }

   private void PaintFloatingPopups(Graphics g)
   {
      foreach (var pop in _popups)
      {
         var sp = WorldToScreen(pop.X, pop.Y);
         var alpha = Math.Clamp((int)((1f - (pop.Age / pop.MaxAge)) * 255), 0, 255);
         var sz = g.MeasureString(pop.Text, _fonts.Popup);

         using var sh = new SolidBrush(Color.FromArgb(alpha, 12, 12, 14));
         using var br = new SolidBrush(Color.FromArgb(alpha, pop.Color));

         g.DrawString(pop.Text, _fonts.Popup, sh, sp.X - (sz.Width * 0.5f) + 1f, sp.Y + 1f);
         g.DrawString(pop.Text, _fonts.Popup, br, sp.X - (sz.Width * 0.5f), sp.Y);
      }
   }

   private void PaintDisconnectBannerIfNeeded(Graphics g)
   {
      if (_connected || string.IsNullOrEmpty(_statusBanner)) return;

      var bw = Math.Min(640f, _arenaRect.Width - 60f);
      var bh = 62f;
      var bx = _arenaRect.X + ((_arenaRect.Width - bw) * 0.5f);
      var by = _arenaRect.Y + ((_arenaRect.Height - bh) * 0.5f);

      UiTheme.FillRoundedRect(g, new RectangleF(bx, by, bw, bh), 4f, Color.FromArgb(230, 32, 20, 20));
      UiTheme.DrawRoundedRect(g, new RectangleF(bx, by, bw, bh), 4f, UiTheme.StatusBad, 1f);

      var sz = g.MeasureString(_statusBanner, _fonts.Brand);
      using var eb = new SolidBrush(UiTheme.TextPrimary);
      g.DrawString(_statusBanner, _fonts.Brand, eb, bx + ((bw - sz.Width) * 0.5f), by + ((bh - sz.Height) * 0.5f));
   }

   private void PaintFooter(Graphics g)
   {
      using var muted = new SolidBrush(UiTheme.TextMuted);
      using var secondary = new SolidBrush(UiTheme.TextSecondary);

      var footerY = _winH - 24f;
      var controlsLegend = _isHost
         ? "WASD / Arrows: Move   |   Mouse / Space: Aim & Fire   |   T: Emote   |   L: LagComp   |   B: Bots   |   F3: Debug"
         : "WASD / Arrows: Move   |   Mouse / Space: Aim & Fire   |   T: Emote   |   P / I / R / O: Netcode   |   F3: Debug";

      g.DrawString(controlsLegend, _fonts.MonoSmall, muted, 18f, footerY);

      const string channels = "Ch0 RelOrd  |  Ch1 RelSeq  |  Ch2 UnrelSeq  |  Ch3 UnrelUnord";
      var sz = g.MeasureString(channels, _fonts.MonoSmall);
      g.DrawString(channels, _fonts.MonoSmall, secondary, _winW - sz.Width - 18f, footerY);
   }
}
