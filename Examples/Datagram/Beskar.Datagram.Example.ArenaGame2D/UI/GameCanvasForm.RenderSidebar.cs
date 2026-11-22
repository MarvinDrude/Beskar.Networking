using Beskar.Networking.Datagram.Enums;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

public sealed partial class GameCanvasForm
{
   private void PaintControlDeck(Graphics g)
   {
      // --- CARD 1: Network Simulation ---
      UiTheme.DrawCard(g, _cardFaults, "Network Simulation", "Real UDP", _fonts.CardTitle, _fonts.Badge);
      PaintNetworkSimulationCard(g);

      // --- CARD 2: Client Netcode / Host Authority & Peers ---
      if (!_isHost)
      {
         UiTheme.DrawCard(g, _cardEngine, "Client Netcode", "Local", _fonts.CardTitle, _fonts.Badge);
         PaintToggleList(g);
      }
      else
      {
         UiTheme.DrawCard(g, _cardEngine, "Host Authority & Peers", $"{_datagramHost?.PeerCount ?? 0} online", _fonts.CardTitle, _fonts.Badge);
         PaintToggleList(g);

         using (var sep = new Pen(Color.FromArgb(44, 44, 52), 1f))
            g.DrawLine(sep, _cardEngine.X + 10f, _cardEngine.Y + 63f, _cardEngine.Right - 10f, _cardEngine.Y + 63f);

         PaintHostPeerListCard(g);
      }

      // --- CARD 3: Scoreboard ---
      UiTheme.DrawCard(g, _cardScores, "Scoreboard", "Ch 1 · RelSeq", _fonts.CardTitle, _fonts.Badge);
      PaintScoreboardCard(g);

      // --- CARD 4: Telemetry & Log ---
      UiTheme.DrawCard(g, _cardTelemetry, "Telemetry & Log", "Ch 0 · RelOrd", _fonts.CardTitle, _fonts.Badge);
      PaintTelemetryAndFeedCard(g);
   }

   private void PaintNetworkSimulationCard(Graphics g)
   {
      foreach (var btn in _buttons)
      {
         if (_cardFaults.Contains(btn.Rect.Location))
         {
            UiTheme.DrawFlatButton(g, btn.Rect, btn.Text(), _fonts.MonoSmallBold, btn.IsActive(), btn.Rect.Contains(_cursorPos));
         }
      }

      using var labelBrush = new SolidBrush(UiTheme.TextSecondary);
      using var valActiveBrush = new SolidBrush(UiTheme.TextPrimary);
      using var valZeroBrush = new SolidBrush(UiTheme.TextMuted);

      foreach (var sld in _sliders)
      {
         var val = sld.Getter();
         var valStr = sld.FormatValue(val);
         var titleY = sld.TrackRect.Y - 18f;

         g.DrawString(sld.Title, _fonts.Body, labelBrush, sld.TrackRect.X, titleY);

         var vsz = g.MeasureString(valStr, _fonts.MonoSmallBold);
         g.DrawString(valStr, _fonts.MonoSmallBold, val == 0 ? valZeroBrush : valActiveBrush, sld.TrackRect.Right - vsz.Width, titleY + 1f);

         UiTheme.DrawSlider(
            g, sld.TrackRect, val, sld.Min, sld.Max,
            hovered: sld.HitRect.Contains(_cursorPos),
            dragging: ReferenceEquals(_activeSlider, sld));
      }
   }

   private void PaintToggleList(Graphics g)
   {
      using var primaryBrush = new SolidBrush(UiTheme.TextPrimary);
      using var keyBrush = new SolidBrush(UiTheme.TextMuted);

      foreach (var tog in _toggles)
      {
         var hovered = tog.RowRect.Contains(_cursorPos);
         if (hovered)
         {
            UiTheme.FillRoundedRect(g, tog.RowRect, 3f, Color.FromArgb(24, 255, 255, 255));
         }

         UiTheme.DrawCheckbox(g, tog.CheckRect, tog.Getter(), hovered);
         g.DrawString(tog.Label, _fonts.Body, primaryBrush, tog.CheckRect.Right + 9f, tog.RowRect.Y + ((tog.RowRect.Height - 17f) * 0.5f));

         var keyRect = new RectangleF(tog.RowRect.Right - 22f, tog.RowRect.Y + ((tog.RowRect.Height - 18f) * 0.5f), 20f, 18f);
         UiTheme.FillRoundedRect(g, keyRect, 3f, UiTheme.ControlBg);
         UiTheme.DrawRoundedRect(g, keyRect, 3f, UiTheme.BorderSubtle, 1f);

         var ksz = g.MeasureString(tog.Hotkey, _fonts.Badge);
         g.DrawString(tog.Hotkey, _fonts.Badge, keyBrush, keyRect.X + ((keyRect.Width - ksz.Width) * 0.5f), keyRect.Y + ((keyRect.Height - ksz.Height) * 0.5f));
      }
   }

   private void PaintHostPeerListCard(Graphics g)
   {
      if (_datagramHost is null || _datagramHost.PeerCount == 0)
      {
         using var muted = new SolidBrush(UiTheme.TextMuted);
         g.DrawString("No remote clients connected.", _fonts.Body, muted, _cardEngine.X + 12f, _cardEngine.Y + 72f);
         g.DrawString("Click '+ Spawn Client' in the top bar.", _fonts.MonoSmall, muted, _cardEngine.X + 12f, _cardEngine.Y + 94f);
         return;
      }

      var y = _cardEngine.Y + 70f;
      var maxPeers = Math.Max(2, (int)((_cardEngine.Bottom - y - 4f) / 27f));
      var shown = 0;

      foreach (var peer in _datagramHost.Peers)
      {
         if (shown >= maxPeers) break;

         var st = peer.Stats;
         var name = peer.Tag as string ?? $"Peer#{peer.PeerId}";
         var col = st.CongestionState switch
         {
            DatagramCongestionState.Healthy => UiTheme.StatusOk,
            DatagramCongestionState.Congested => UiTheme.StatusWarn,
            _ => UiTheme.StatusBad
         };

         var row = new RectangleF(_cardEngine.X + 12f, y, _cardEngine.Width - 24f, 24f);
         UiTheme.FillRoundedRect(g, row, 3f, UiTheme.ControlBg);

         using (var dot = new SolidBrush(col)) g.FillEllipse(dot, row.X + 8f, row.Y + 8f, 8f, 8f);
         using (var nb = new SolidBrush(UiTheme.TextPrimary)) g.DrawString(name, _fonts.BodyBold, nb, row.X + 23f, row.Y + 3.5f);

         var statText = $"{st.SmoothedRttMs,4:F0}ms  ·  {st.PacketLossRate * 100,2:F0}% loss";
         var ssz = g.MeasureString(statText, _fonts.MonoSmallBold);
         using (var sb = new SolidBrush(UiTheme.TextSecondary)) g.DrawString(statText, _fonts.MonoSmallBold, sb, row.Right - ssz.Width - 8f, row.Y + 4.5f);

         y += 28f;
         shown++;
      }
   }

   private void PaintScoreboardCard(Graphics g)
   {
      if (_scoreboard.Count == 0)
      {
         using var muted = new SolidBrush(UiTheme.TextMuted);
         g.DrawString("Waiting for scoreboard...", _fonts.Body, muted, _cardScores.X + 12f, _cardScores.Y + 46f);
         return;
      }

      var myId = _isHost ? (_hostLocalPlayer?.Id ?? 0) : _netcode.MyPlayerId;
      var maxScore = Math.Max(1, (int)_scoreboard[0].Score);
      var y = _cardScores.Y + 36f;
      var maxRows = Math.Clamp((int)((_cardScores.Bottom - y - 4f) / 24f), 4, 6);

      for (var i = 0; i < Math.Min(_scoreboard.Count, maxRows); i++)
      {
         var entry = _scoreboard[i];
         var col = UiTheme.PlayerPalette[entry.Color % UiTheme.PlayerPalette.Length];
         var rowX = _cardScores.X + 12f;
         var rowW = _cardScores.Width - 24f;

         var barW = Math.Max(4f, (rowW - 4f) * (entry.Score / (float)maxScore));
         UiTheme.FillRoundedRect(g, new RectangleF(rowX, y, rowW, 21f), 3f, Color.FromArgb(28, 28, 33));
         UiTheme.FillRoundedRect(g, new RectangleF(rowX, y, barW, 21f), 3f, Color.FromArgb(46, col));

         using (var sw = new SolidBrush(col)) g.FillRectangle(sw, rowX + 7f, y + 6f, 9f, 9f);

         var name = entry.Name + (entry.Id == myId ? " (you)" : "");
         if (name.Length > 22) name = name[..22];

         using (var nb = new SolidBrush(entry.Id == myId ? Color.White : UiTheme.TextPrimary))
            g.DrawString($"{i + 1}. {name}", _fonts.MonoSmallBold, nb, rowX + 22f, y + 3.5f);

         var ptsText = $"{entry.Score} pts";
         var psz = g.MeasureString(ptsText, _fonts.MonoSmallBold);

         using (var pb = new SolidBrush(UiTheme.TextPrimary))
            g.DrawString(ptsText, _fonts.MonoSmallBold, pb, rowX + rowW - psz.Width - 7f, y + 3.5f);

         y += 24f;
      }
   }

   private void PaintTelemetryAndFeedCard(Graphics g)
   {
      var (rttText, lossText, bwText, dropText) = GatherTelemetryCells();

      var cx = _cardTelemetry.X + 12f;
      var cy = _cardTelemetry.Y + 36f;
      const float cg = 5f;
      var cw = (_cardTelemetry.Width - 24f - (cg * 3f)) / 4f;
      const float ch = 36f;

      DrawMiniStatCell(g, new RectangleF(cx, cy, cw, ch), "RTT", rttText);
      DrawMiniStatCell(g, new RectangleF(cx + (cw + cg), cy, cw, ch), "LOSS", lossText);
      DrawMiniStatCell(g, new RectangleF(cx + ((cw + cg) * 2f), cy, cw, ch), "TRAFFIC", bwText);
      DrawMiniStatCell(g, new RectangleF(cx + ((cw + cg) * 3f), cy, cw, ch), "DROPS", dropText);

      var fy = cy + ch + 10f;
      var linePitch = 18.5f;
      var maxLines = Math.Max(2, (int)((_cardTelemetry.Bottom - fy - 6f) / linePitch));
      var maxChars = Math.Max(34, (int)((_cardTelemetry.Width - 36f) / 6.7f));
      var feedLines = _eventFeed.TakeLast(maxLines);

      using var arrowBrush = new SolidBrush(UiTheme.TextMuted);
      using var textBrush = new SolidBrush(UiTheme.TextSecondary);

      foreach (var line in feedLines)
      {
         var clipped = line.Length > maxChars ? line[..(maxChars - 1)] + "…" : line;
         g.DrawString("-", _fonts.MonoSmallBold, arrowBrush, _cardTelemetry.X + 12f, fy);
         g.DrawString(clipped, _fonts.MonoSmall, textBrush, _cardTelemetry.X + 23f, fy);
         fy += linePitch;
      }
   }

   private void DrawMiniStatCell(Graphics g, RectangleF rect, string label, string value)
   {
      UiTheme.FillRoundedRect(g, rect, 3f, UiTheme.ControlBg);

      using var lb = new SolidBrush(UiTheme.TextMuted);
      using var vb = new SolidBrush(UiTheme.TextPrimary);

      g.DrawString(label, _fonts.Badge, lb, rect.X + 7f, rect.Y + 4f);
      g.DrawString(value, _fonts.MonoSmallBold, vb, rect.X + 7f, rect.Y + 18f);
   }

   private (string Label, Color Color) GetConnectionHealthBadge()
   {
      if (!_connected) return ("Disconnected", UiTheme.StatusBad);

      if (_isHost)
      {
         var peers = _datagramHost?.Peers;
         if (peers is null || peers.Count == 0) return ("Listening · 0 peers", UiTheme.TextSecondary);

         var rttSum = 0.0;
         var worst = DatagramCongestionState.Healthy;

         foreach (var p in peers)
         {
            var st = p.Stats;
            rttSum += st.SmoothedRttMs;
            if (st.CongestionState > worst) worst = st.CongestionState;
         }

         var avg = rttSum / peers.Count;
         var col = worst switch
         {
            DatagramCongestionState.Healthy => UiTheme.StatusOk,
            DatagramCongestionState.Congested => UiTheme.StatusWarn,
            _ => UiTheme.StatusBad
         };

         return ($"{worst} · {avg:F0}ms", col);
      }

      if (_serverPeer is null) return ("Connecting...", UiTheme.StatusWarn);

      var cst = _serverPeer.Stats;
      var cCol = cst.CongestionState switch
      {
         DatagramCongestionState.Healthy => UiTheme.StatusOk,
         DatagramCongestionState.Congested => UiTheme.StatusWarn,
         _ => UiTheme.StatusBad
      };

      return ($"{cst.CongestionState} · {cst.SmoothedRttMs:F0}ms", cCol);
   }

   private (string Rtt, string Loss, string Bw, string Drops) GatherTelemetryCells()
   {
      if (_isHost && _datagramHost is not null)
      {
         var rtt = 0.0;
         var loss = 0.0;
         var tx = 0L;
         var rx = 0L;
         var n = 0;

         foreach (var p in _datagramHost.Peers)
         {
            var st = p.Stats;
            n++;
            rtt += st.SmoothedRttMs;
            loss += st.PacketLossRate;
            tx += st.BytesSent;
            rx += st.BytesReceived;
         }

         var kb = (tx + rx) / 1024.0;
         return (n > 0 ? $"{rtt / n:F0}ms" : "0ms", n > 0 ? $"{loss / n * 100:F1}%" : "0.0%", $"{kb:F0} KB", _transport.DroppedPackets.ToString());
      }

      if (_serverPeer is not null)
      {
         var st = _serverPeer.Stats;
         var kb = (st.BytesSent + st.BytesReceived) / 1024.0;
         return ($"{st.SmoothedRttMs:F0}ms", $"{st.PacketLossRate * 100:F1}%", $"{kb:F0} KB", _transport.DroppedPackets.ToString());
      }

      return ("--", "--", "0 KB", "0");
   }
}
