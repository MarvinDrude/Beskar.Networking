using System.Drawing.Drawing2D;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

public sealed partial class GameCanvasForm
{
   private void PaintClientDebugUnderlayGeometry(Graphics g)
   {
      if (_netcode.PredictShotsAndPickupsEnabled && _netcode.PendingPickups.Count > 0)
      {
         using var pPen = new Pen(Color.FromArgb(170, UiTheme.StatusOk), 1.3f) { DashStyle = DashStyle.Dash };
         foreach (var pending in _netcode.PendingPickups)
         {
            var o = pending.Orb;
            g.DrawEllipse(pPen, o.X - 10f, o.Y - 10f, 20f, 20f);
         }
      }

      if (_netcode.InterpolationEnabled)
      {
         var rawCol = Color.FromArgb(170, 166, 174, 188);
         using var linkPen = new Pen(Color.FromArgb(110, 166, 174, 188), 1f) { DashStyle = DashStyle.Dot };

         foreach (var ghost in _rawRemoteGhosts)
         {
            if (!TryGetRenderedRemotePosition(ghost.Id, out var ix, out var iy)) continue;

            var sep = MathF.Sqrt(((ix - ghost.X) * (ix - ghost.X)) + ((iy - ghost.Y) * (iy - ghost.Y)));
            if (sep < 3f) continue;

            var gr = new RectangleF(ghost.X - GamePhysics2D.PlayerHalf, ghost.Y - GamePhysics2D.PlayerHalf, GamePhysics2D.PlayerSize, GamePhysics2D.PlayerSize);
            using (var gPath = UiTheme.CreateRoundedRect(gr, 3f))
            using (var gPen = new Pen(rawCol, 1.3f) { DashStyle = DashStyle.Dot })
               g.DrawPath(gPen, gPath);

            g.DrawLine(linkPen, ix, iy, ghost.X, ghost.Y);
         }
      }

      var trail = _netcode.UnackedTrail;
      for (var i = 0; i < trail.Count; i++)
      {
         var pt = trail[i];
         var alpha = 55 + (int)(155f * i / Math.Max(1, trail.Count));

         using var dot = new SolidBrush(Color.FromArgb(alpha, 212, 218, 228));
         g.FillEllipse(dot, pt.PredX - 2f, pt.PredY - 2f, 4f, 4f);
      }

      if (!_netcode.HasServerGhost) return;

      var gx = _netcode.ServerGhostX;
      var gy = _netcode.ServerGhostY;
      var lx = _netcode.RenderLocalX;
      var ly = _netcode.RenderLocalY;
      var flashing = _netcode.FlashReconcileTicks > 0;
      var ghostCol = flashing ? UiTheme.StatusBad : UiTheme.StatusWarn;

      var ghostRect = new RectangleF(gx - GamePhysics2D.PlayerHalf, gy - GamePhysics2D.PlayerHalf, GamePhysics2D.PlayerSize, GamePhysics2D.PlayerSize);
      UiTheme.FillRoundedRect(g, ghostRect, 3f, Color.FromArgb(26, ghostCol));

      using (var gPath = UiTheme.CreateRoundedRect(ghostRect, 3f))
      using (var gPen = new Pen(ghostCol, 1.8f) { DashStyle = DashStyle.Dash })
         g.DrawPath(gPen, gPath);

      if (_netcode.ServerGhostDistancePx > 4f)
      {
         using var tetherPen = new Pen(Color.FromArgb(195, ghostCol), 1.5f) { DashStyle = DashStyle.Dash };
         g.DrawLine(tetherPen, lx, ly, gx, gy);
      }
   }

   private void PaintClientDebugUnderlayLabels(Graphics g)
   {
      if (_netcode.PredictShotsAndPickupsEnabled && _netcode.PendingPickups.Count > 0)
      {
         using var pTxt = new SolidBrush(Color.FromArgb(195, UiTheme.StatusOk));
         foreach (var pending in _netcode.PendingPickups)
         {
            var sp = WorldToScreen(pending.Orb.X, pending.Orb.Y + 11f);
            var sz = g.MeasureString("PRED", _fonts.Badge);
            g.DrawString("PRED", _fonts.Badge, pTxt, sp.X - (sz.Width * 0.5f), sp.Y + 2f);
         }
      }

      if (_netcode.InterpolationEnabled)
      {
         using var rawText = new SolidBrush(Color.FromArgb(205, 196, 202, 214));
         foreach (var ghost in _rawRemoteGhosts)
         {
            if (!TryGetRenderedRemotePosition(ghost.Id, out var ix, out var iy)) continue;

            var sep = MathF.Sqrt(((ix - ghost.X) * (ix - ghost.X)) + ((iy - ghost.Y) * (iy - ghost.Y)));
            if (sep < 3f) continue;

            var bottom = WorldToScreen(ghost.X, ghost.Y + GamePhysics2D.PlayerHalf);
            var rsz = g.MeasureString("RAW", _fonts.Badge);
            var rPill = new RectangleF(bottom.X - (rsz.Width * 0.5f) - 4f, bottom.Y + 4f, rsz.Width + 8f, 16f);

            UiTheme.FillRoundedRect(g, rPill, 2.5f, Color.FromArgb(195, 20, 21, 24));
            g.DrawString("RAW", _fonts.Badge, rawText, rPill.X + 4f, rPill.Y + 2f);
         }
      }

      if (!_netcode.HasServerGhost) return;

      var gx = _netcode.ServerGhostX;
      var gy = _netcode.ServerGhostY;
      var lx = _netcode.RenderLocalX;
      var ly = _netcode.RenderLocalY;
      var flashing = _netcode.FlashReconcileTicks > 0;
      var ghostCol = flashing ? UiTheme.StatusBad : UiTheme.StatusWarn;

      var gBottom = WorldToScreen(gx, gy + GamePhysics2D.PlayerHalf);
      var ghostLabel = _netcode.PredictionEnabled ? "SERVER" : "NO PRED";
      var glSz = g.MeasureString(ghostLabel, _fonts.Badge);
      var sPill = new RectangleF(gBottom.X - (glSz.Width * 0.5f) - 5f, gBottom.Y + 4f, glSz.Width + 10f, 16f);

      UiTheme.FillRoundedRect(g, sPill, 2.5f, Color.FromArgb(205, 20, 21, 24));

      using var sBrush = new SolidBrush(ghostCol);
      g.DrawString(ghostLabel, _fonts.Badge, sBrush, sPill.X + 5f, sPill.Y + 2f);

      var dist = _netcode.ServerGhostDistancePx;
      if (dist > 4f)
      {
         var tag = flashing ? $"Reconcile Δ{dist:F0}px" : $"Lead Δ{dist:F0}px";
         var sz = g.MeasureString(tag, _fonts.Badge);
         var mid = WorldToScreen((lx + gx) * 0.5f, Math.Max(ly, gy) + GamePhysics2D.PlayerHalf);
         var mx = mid.X - (sz.Width * 0.5f);
         var my = mid.Y + 24f;

         UiTheme.FillRoundedRect(g, new RectangleF(mx - 6, my - 2, sz.Width + 12, 18), 3f, Color.FromArgb(225, 20, 21, 24));
         UiTheme.DrawRoundedRect(g, new RectangleF(mx - 6, my - 2, sz.Width + 12, 18), 3f, ghostCol, 1f);
         g.DrawString(tag, _fonts.Badge, sBrush, mx, my + 0.5f);
      }
   }

   private void PaintClientDebugTelemetryStrip(Graphics g)
   {
      var boxW = Math.Min(516f, _arenaRect.Width - 28f);
      var box = new RectangleF(_arenaRect.X + 14f, _arenaRect.Bottom - 102f, boxW, 88f);

      UiTheme.FillRoundedRect(g, box, 4f, Color.FromArgb(225, 22, 23, 27));
      UiTheme.DrawRoundedRect(g, box, 4f, UiTheme.BorderStrong, 1f);

      var predTag = _netcode.PredictionEnabled ? "pred:on" : "pred:off";
      var interpTag = _netcode.InterpolationEnabled ? $"interp:{_netcode.AdaptiveInterpDelaySec * 1000f:F0}ms" : "interp:off";
      var inTag = _netcode.InputRedundancyEnabled ? "in:6x" : "in:1x";
      var header = $"net_graph  |  {predTag}  {interpTag}  {inTag}  |  pending:{_netcode.PendingUnackedCount}  reconciles:{_netcode.ReconciliationCorrections}";

      using var hdrBrush = new SolidBrush(UiTheme.TextPrimary);
      g.DrawString(header, _fonts.Badge, hdrBrush, box.X + 10f, box.Y + 7f);

      var (rttHist, errHist, cursor) = _netcode.GetTelemetrySparklines();
      var rttCur = _serverPeer?.Stats.SmoothedRttMs ?? 0.0;
      var rttCol = rttCur < 80 ? UiTheme.StatusOk : (rttCur < 220 ? UiTheme.StatusWarn : UiTheme.StatusBad);
      var sparkW = (box.Width - 30f) * 0.5f;

      PaintSparkline(g, new RectangleF(box.X + 10, box.Y + 26, sparkW, 52), rttHist, cursor, 500f, rttCol, $"RTT {rttCur:F0} ms");
      PaintSparkline(g, new RectangleF(box.X + 20 + sparkW, box.Y + 26, sparkW, 52), errHist, cursor, 60f, UiTheme.StatusBad,
         $"Err {_netcode.LastReconcileErrorPx:F1} px (ghost {_netcode.ServerGhostDistancePx:F0}px)");
   }

   private void PaintHostRewindMarkersGeometry(Graphics g)
   {
      if (_sim is null) return;

      var markers = _sim.ActiveRewindMarkers;
      for (var i = 0; i < markers.Count; i++)
      {
         var m = markers[i];
         var fade = Math.Clamp(m.RemainingTicks / 42f, 0.15f, 1f);
         var col = Color.FromArgb((int)(fade * 215), UiTheme.StatusWarn);
         var fillCol = Color.FromArgb((int)(fade * 38), UiTheme.StatusWarn);

         var r = new RectangleF(m.RewoundX - GamePhysics2D.PlayerHalf, m.RewoundY - GamePhysics2D.PlayerHalf, GamePhysics2D.PlayerSize, GamePhysics2D.PlayerSize);
         UiTheme.FillRoundedRect(g, r, 3f, fillCol);

         using (var path = UiTheme.CreateRoundedRect(r, 3f))
         using (var pen = new Pen(col, 1.7f) { DashStyle = DashStyle.Dash })
            g.DrawPath(pen, path);

         var distSq = ((m.LiveX - m.RewoundX) * (m.LiveX - m.RewoundX)) + ((m.LiveY - m.RewoundY) * (m.LiveY - m.RewoundY));
         if (distSq > 9f)
         {
            using var linkPen = new Pen(Color.FromArgb((int)(fade * 170), UiTheme.StatusWarn), 1.3f) { DashStyle = DashStyle.Dot };
            g.DrawLine(linkPen, m.RewoundX, m.RewoundY, m.LiveX, m.LiveY);
         }
      }
   }

   private void PaintHostDebugOverlay(Graphics g)
   {
      if (_datagramHost is null || _sim is null) return;

      var lagCompText = _sim.LagCompensationEnabled ? $"LagComp: ON ({_sim.LagCompensatedHits} hits)" : "LagComp: OFF";
      var box = new RectangleF(_arenaRect.X + 14f, _arenaRect.Y + 14f, 470f, 50f);

      UiTheme.FillRoundedRect(g, box, 4f, Color.FromArgb(220, 22, 23, 27));
      UiTheme.DrawRoundedRect(g, box, 4f, UiTheme.BorderStrong, 1f);

      using var title = new SolidBrush(UiTheme.TextPrimary);
      using var body = new SolidBrush(UiTheme.TextSecondary);

      g.DrawString($"Host  |  Tick #{_sim.Tick}  |  {lagCompText}", _fonts.Badge, title, box.X + 11f, box.Y + 7f);
      g.DrawString($"Peers: {_datagramHost.PeerCount}  |  Drops: {_transport.DroppedPackets}  |  Sim: +{_transport.AddedLatencyMs}ms ±{_transport.JitterMs}ms / {_transport.PacketLossPercent}%",
         _fonts.MonoSmall, body, box.X + 11f, box.Y + 25f);

      var markers = _sim.ActiveRewindMarkers;
      for (var i = 0; i < markers.Count; i++)
      {
         var m = markers[i];
         var fade = Math.Clamp(m.RemainingTicks / 42f, 0.2f, 1f);
         var tag = $"REWIND -{m.RewindMs:F0}ms";
         var sz = g.MeasureString(tag, _fonts.Badge);
         var sp = WorldToScreen(m.RewoundX, m.RewoundY + GamePhysics2D.PlayerHalf);

         var pillW = sz.Width + 10f;
         var pillX = Math.Clamp(sp.X - (pillW * 0.5f), _arenaRect.X + 4f, _arenaRect.Right - pillW - 4f);
         var pillY = Math.Min(_arenaRect.Bottom - 20f, sp.Y + 5f);
         var pill = new RectangleF(pillX, pillY, pillW, 16f);

         UiTheme.FillRoundedRect(g, pill, 2.5f, Color.FromArgb((int)(fade * 215), 22, 23, 27));
         UiTheme.DrawRoundedRect(g, pill, 2.5f, Color.FromArgb((int)(fade * 220), UiTheme.StatusWarn), 1f);

         using var rb = new SolidBrush(Color.FromArgb((int)(fade * 245), UiTheme.StatusWarn));
         g.DrawString(tag, _fonts.Badge, rb, pill.X + 5f, pill.Y + 1.5f);
      }

      foreach (var peer in _datagramHost.Peers)
      {
         var entity = _sim.FindByPeer(peer.PeerId);
         if (entity is null) continue;

         var st = peer.Stats;
         var col = st.CongestionState switch
         {
            DatagramCongestionState.Healthy => UiTheme.StatusOk,
            DatagramCongestionState.Congested => UiTheme.StatusWarn,
            _ => UiTheme.StatusBad
         };

         var tag = $"#{peer.PeerId} · {st.SmoothedRttMs:F0}ms · {st.PacketLossRate * 100:F0}%";
         var sz = g.MeasureString(tag, _fonts.Badge);
         var sp = WorldToScreen(entity.X, entity.Y + GamePhysics2D.PlayerHalf);

         var pillW = sz.Width + 10f;
         var pillX = Math.Clamp(sp.X - (pillW * 0.5f), _arenaRect.X + 4f, _arenaRect.Right - pillW - 4f);
         var pillY = Math.Min(_arenaRect.Bottom - 21f, sp.Y + 5f);
         var pill = new RectangleF(pillX, pillY, pillW, 17f);

         UiTheme.FillRoundedRect(g, pill, 2.5f, Color.FromArgb(215, 22, 23, 27));
         UiTheme.DrawRoundedRect(g, pill, 2.5f, UiTheme.BorderStrong, 1f);

         using var tb = new SolidBrush(col);
         g.DrawString(tag, _fonts.Badge, tb, pill.X + 5f, pill.Y + 2f);
      }
   }

   private void PaintSparkline(Graphics g, RectangleF rect, float[] samples, int cursor, float maxScale, Color color, string title)
   {
      UiTheme.FillRoundedRect(g, rect, 3f, Color.FromArgb(200, 17, 18, 21));
      UiTheme.DrawRoundedRect(g, rect, 3f, UiTheme.BorderSubtle, 1f);

      var count = samples.Length;
      var stepX = (rect.Width - 6f) / Math.Max(1, count - 1);
      var pts = new PointF[count];

      for (var i = 0; i < count; i++)
      {
         var idx = (cursor + i) % count;
         var norm = Math.Clamp(samples[idx] / maxScale, 0f, 1f);
         pts[i] = new PointF(rect.X + 3f + (i * stepX), rect.Bottom - 4f - (norm * (rect.Height - 20f)));
      }

      var poly = new PointF[count + 2];
      poly[0] = new PointF(pts[0].X, rect.Bottom - 2f);
      Array.Copy(pts, 0, poly, 1, count);
      poly[^1] = new PointF(pts[^1].X, rect.Bottom - 2f);

      using (var fill = new SolidBrush(Color.FromArgb(28, color)))
         g.FillPolygon(fill, poly);

      using (var linePen = new Pen(color, 1.4f))
         g.DrawLines(linePen, pts);

      using var tb = new SolidBrush(UiTheme.TextSecondary);
      g.DrawString(title, _fonts.Badge, tb, rect.X + 6f, rect.Y + 3f);
   }

   private bool TryGetRenderedRemotePosition(byte playerId, out float x, out float y)
   {
      for (var i = 0; i < _renderPlayers.Count; i++)
      {
         if (_renderPlayers[i].Id == playerId)
         {
            x = _renderPlayers[i].X;
            y = _renderPlayers[i].Y;
            return true;
         }
      }

      x = 0f;
      y = 0f;
      return false;
   }
}
