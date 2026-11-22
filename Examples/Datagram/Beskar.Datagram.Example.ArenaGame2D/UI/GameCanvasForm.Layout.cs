using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

public sealed partial class GameCanvasForm
{
   private void RecalculateLayout(int w, int h)
   {
      _winW = Math.Max(800, w);
      _winH = Math.Max(500, h);

      const float headerH = 54f;
      const float footerH = 30f;
      const float margin = 16f;
      const float gap = 16f;

      _headerRect = new RectangleF(0, 0, _winW, headerH);

      var sidebarW = Math.Clamp(MathF.Round(_winW * 0.255f), 376f, 430f);
      var sideX = _winW - margin - sidebarW;
      var contentTop = headerH + 14f;
      var contentBottom = _winH - footerH - 8f;
      var contentH = Math.Max(420f, contentBottom - contentTop);

      // Fit 800:560 logical arena cleanly into left viewport at exact aspect ratio
      var availW = Math.Max(320f, sideX - gap - margin);
      var availH = contentH;

      _arenaScale = Math.Min(availW / GamePhysics2D.ArenaWidth, availH / GamePhysics2D.ArenaHeight);

      var vpW = GamePhysics2D.ArenaWidth * _arenaScale;
      var vpH = GamePhysics2D.ArenaHeight * _arenaScale;

      _arenaRect = new RectangleF(
         margin + ((availW - vpW) * 0.5f),
         contentTop + ((availH - vpH) * 0.5f),
         vpW,
         vpH);

      // Balanced vertical stack of the 4 sidebar cards (scales smoothly from 720p to 4K)
      var expandT = Math.Clamp((contentH - 610f) / 184f, 0f, 1f);
      var cardGap = 10f + (2f * expandT);

      var faultsH = 186f + (14f * expandT);
      var engineH = 142f + (12f * expandT);
      var scoresH = 140f + (24f * expandT);

      var y = contentTop;
      _cardFaults = new RectangleF(sideX, y, sidebarW, faultsH);
      y = _cardFaults.Bottom + cardGap;

      _cardEngine = new RectangleF(sideX, y, sidebarW, engineH);
      y = _cardEngine.Bottom + cardGap;

      _cardScores = new RectangleF(sideX, y, sidebarW, scoresH);
      y = _cardScores.Bottom + cardGap;

      _cardTelemetry = new RectangleF(sideX, y, sidebarW, Math.Max(118f, contentBottom - y));

      RebuildWidgets();
   }

   private void RebuildWidgets()
   {
      _buttons.Clear();
      _sliders.Clear();
      _toggles.Clear();
      _activeSlider = null;

      BuildHeaderWidgets();
      BuildFaultSimulationWidgets();
      BuildEngineToggleWidgets();
   }

   private void BuildHeaderWidgets()
   {
      var bx = _winW - 16f;
      const float by = 13f;
      const float bh = 28f;

      bx -= 104f;
      _buttons.Add(new ButtonWidget(
         "btn-debug",
         new RectangleF(bx, by, 104f, bh),
         () => _debugOverlayEnabled ? "Debug: On" : "Debug: Off",
         () => _debugOverlayEnabled,
         () => _debugOverlayEnabled = !_debugOverlayEnabled));

      bx -= 8f + 86f;
      _buttons.Add(new ButtonWidget(
         "btn-emote",
         new RectangleF(bx, by, 86f, bh),
         () => "Emote [T]",
         () => false,
         SendEmote));

      if (_isHost)
      {
         bx -= 8f + 86f;
         _buttons.Add(new ButtonWidget(
            "btn-bots",
            new RectangleF(bx, by, 86f, bh),
            () => $"Bots: {_botCount}",
            () => false,
            CycleBots));

         bx -= 8f + 144f;
         _buttons.Add(new ButtonWidget(
            "btn-spawn",
            new RectangleF(bx, by, 144f, bh),
            () => "+ Spawn Client",
            () => false,
            SpawnSecondClientWindow,
            primary: true));
      }

      const float healthW = 178f;
      _healthRect = new RectangleF(bx - 12f - healthW, by, healthW, bh);
   }

   private void BuildFaultSimulationWidgets()
   {
      var py = _cardFaults.Y + 37f;
      var px = _cardFaults.X + 12f;
      const float pg = 6f;
      var pw = (_cardFaults.Width - 24f - (pg * 3f)) / 4f;

      _buttons.Add(new ButtonWidget("pre-lan", new RectangleF(px, py, pw, 26f), () => "LAN 0ms",
         () => _transport.AddedLatencyMs == 0 && _transport.PacketLossPercent == 0, () => SetFaultProfile(0, 0, 0)));

      _buttons.Add(new ButtonWidget("pre-40", new RectangleF(px + (pw + pg), py, pw, 26f), () => "40ms",
         () => _transport.AddedLatencyMs == 40 && _transport.PacketLossPercent == 1, () => SetFaultProfile(40, 8, 1)));

      _buttons.Add(new ButtonWidget("pre-120", new RectangleF(px + ((pw + pg) * 2), py, pw, 26f), () => "120ms",
         () => _transport.AddedLatencyMs == 120 && _transport.PacketLossPercent == 8, () => SetFaultProfile(120, 20, 8)));

      _buttons.Add(new ButtonWidget("pre-220", new RectangleF(px + ((pw + pg) * 3), py, pw, 26f), () => "220ms",
         () => _transport.AddedLatencyMs == 220 && _transport.PacketLossPercent == 18, () => SetFaultProfile(220, 40, 18)));

      var sx = _cardFaults.X + 14f;
      var sw = _cardFaults.Width - 28f;
      var sliderAreaTop = py + 32f;
      var sliderStep = (_cardFaults.Bottom - sliderAreaTop - 8f) / 3f;

      _sliders.Add(new SliderWidget(
         "sld-delay", "One-way delay",
         new RectangleF(sx - 6, sliderAreaTop, sw + 12, sliderStep),
         new RectangleF(sx, sliderAreaTop + 18f, sw, 14f),
         0, 300,
         () => _transport.AddedLatencyMs, v => _transport.AddedLatencyMs = v,
         v => v == 0 ? "0 ms (LAN)" : $"{v} ms (~{v * 2}ms RTT)"));

      _sliders.Add(new SliderWidget(
         "sld-jitter", "Jitter (+/-)",
         new RectangleF(sx - 6, sliderAreaTop + sliderStep, sw + 12, sliderStep),
         new RectangleF(sx, sliderAreaTop + sliderStep + 18f, sw, 14f),
         0, 80,
         () => _transport.JitterMs, v => _transport.JitterMs = v,
         v => $"±{v} ms"));

      _sliders.Add(new SliderWidget(
         "sld-loss", "Packet loss",
         new RectangleF(sx - 6, sliderAreaTop + (sliderStep * 2f), sw + 12, sliderStep),
         new RectangleF(sx, sliderAreaTop + (sliderStep * 2f) + 18f, sw, 14f),
         0, 35,
         () => _transport.PacketLossPercent, v => _transport.PacketLossPercent = v,
         v => $"{v} %"));
   }

   private void BuildEngineToggleWidgets()
   {
      var tx = _cardEngine.X + 12f;
      var tw = _cardEngine.Width - 24f;

      if (!_isHost)
      {
         var togTop = _cardEngine.Y + 36f;
         var togPitch = (_cardEngine.Bottom - togTop - 6f) / 4f;

         AddToggle("tog-pred", "Client-side prediction & reconcile", "P", tx, togTop, tw, togPitch,
            () => _netcode.PredictionEnabled, v => _netcode.PredictionEnabled = v);

         AddToggle("tog-interp", "Remote entity interpolation", "I", tx, togTop + togPitch, tw, togPitch,
            () => _netcode.InterpolationEnabled, v => _netcode.InterpolationEnabled = v);

         AddToggle("tog-redund", "Input redundancy (6x, Ch 2)", "R", tx, togTop + (togPitch * 2f), tw, togPitch,
            () => _netcode.InputRedundancyEnabled, v => _netcode.InputRedundancyEnabled = v);

         AddToggle("tog-shots", "Predict shots & item pickups", "O", tx, togTop + (togPitch * 3f), tw, togPitch,
            () => _netcode.PredictShotsAndPickupsEnabled, v => _netcode.PredictShotsAndPickupsEnabled = v);
      }
      else if (_sim is not null)
      {
         AddToggle("tog-lagcomp", "Lag compensation (hitbox rewind)", "L", tx, _cardEngine.Y + 35f, tw, 25f,
            () => _sim.LagCompensationEnabled, v => _sim.LagCompensationEnabled = v);
      }
   }

   private void AddToggle(string id, string label, string hotkey, float x, float y, float w, float rowH, Func<bool> get, Action<bool> set)
   {
      var h = Math.Min(25f, rowH - 1f);
      var checkY = y + ((h - 16f) * 0.5f);

      _toggles.Add(new ToggleWidget(
         id, label, hotkey,
         new RectangleF(x, y, w, h),
         new RectangleF(x + 2f, checkY, 16f, 16f),
         get, set));
   }

   private PointF WorldToScreen(float wx, float wy) =>
      new(_arenaRect.X + (wx * _arenaScale), _arenaRect.Y + (wy * _arenaScale));

   private void WireEvents()
   {
      Shown += async (_, _) => await InitializeNetworkingAsync();
      FormClosing += async (_, _) => await ShutdownNetworkingAsync();
      Deactivate += (_, _) => { _heldKeys.Clear(); _mouseFireHeld = false; _activeSlider = null; };

      ClientSizeChanged += (_, _) =>
      {
         if (ClientSize.Width > 0 && ClientSize.Height > 0)
         {
            RecalculateLayout(ClientSize.Width, ClientSize.Height);
            Invalidate();
         }
      };

      KeyDown += (_, e) =>
      {
         _heldKeys.Add(e.KeyCode);

         switch (e.KeyCode)
         {
            case Keys.T: SendEmote(); break;
            case Keys.F3 or Keys.G: _debugOverlayEnabled = !_debugOverlayEnabled; break;
            case Keys.P when !_isHost: _netcode.PredictionEnabled = !_netcode.PredictionEnabled; break;
            case Keys.I when !_isHost: _netcode.InterpolationEnabled = !_netcode.InterpolationEnabled; break;
            case Keys.R when !_isHost: _netcode.InputRedundancyEnabled = !_netcode.InputRedundancyEnabled; break;
            case Keys.O when !_isHost: _netcode.PredictShotsAndPickupsEnabled = !_netcode.PredictShotsAndPickupsEnabled; break;
            case Keys.B when _isHost: CycleBots(); break;
            case Keys.L when _isHost && _sim is not null: _sim.LagCompensationEnabled = !_sim.LagCompensationEnabled; break;
         }
      };

      KeyUp += (_, e) => _heldKeys.Remove(e.KeyCode);
      MouseMove += HandleMouseMove;
      MouseDown += HandleMouseDown;

      MouseUp += (_, e) =>
      {
         if (e.Button == MouseButtons.Left)
         {
            _activeSlider = null;
            _mouseFireHeld = false;
         }
      };
   }

   private void HandleMouseMove(object? sender, MouseEventArgs e)
   {
      _cursorPos = new PointF(e.X, e.Y);

      if (_activeSlider is not null)
      {
         UpdateSliderFromMouse(_activeSlider, e.X);
         return;
      }

      if (_arenaRect.Contains(_cursorPos))
      {
         _mouseAimActive = true;
         _mouseArenaX = (e.X - _arenaRect.X) / _arenaScale;
         _mouseArenaY = (e.Y - _arenaRect.Y) / _arenaScale;
         Cursor = Cursors.Cross;
         return;
      }

      var overInteractive = _buttons.Any(b => b.Rect.Contains(_cursorPos))
                            || _sliders.Any(s => s.HitRect.Contains(_cursorPos))
                            || _toggles.Any(t => t.RowRect.Contains(_cursorPos));

      Cursor = overInteractive ? Cursors.Hand : Cursors.Default;
   }

   private void HandleMouseDown(object? sender, MouseEventArgs e)
   {
      _cursorPos = new PointF(e.X, e.Y);
      if (e.Button != MouseButtons.Left) return;

      foreach (var btn in _buttons)
      {
         if (btn.Rect.Contains(_cursorPos))
         {
            btn.OnClick();
            return;
         }
      }

      foreach (var sld in _sliders)
      {
         if (sld.HitRect.Contains(_cursorPos))
         {
            _activeSlider = sld;
            UpdateSliderFromMouse(sld, e.X);
            return;
         }
      }

      foreach (var tog in _toggles)
      {
         if (tog.RowRect.Contains(_cursorPos))
         {
            tog.Setter(!tog.Getter());
            return;
         }
      }

      if (_arenaRect.Contains(_cursorPos))
      {
         _mouseAimActive = true;
         _mouseArenaX = (e.X - _arenaRect.X) / _arenaScale;
         _mouseArenaY = (e.Y - _arenaRect.Y) / _arenaScale;
         _mouseFireHeld = true;
      }
   }

   private static void UpdateSliderFromMouse(SliderWidget sld, int mouseX)
   {
      var t = Math.Clamp((mouseX - sld.TrackRect.X) / Math.Max(1f, sld.TrackRect.Width), 0f, 1f);
      var value = sld.Min + (int)MathF.Round(t * (sld.Max - sld.Min));
      sld.Setter(value);
   }

   protected override bool ProcessCmdKey(ref Message msg, Keys keyData)
   {
      var key = keyData & Keys.KeyCode;
      if (key is Keys.Up or Keys.Down or Keys.Left or Keys.Right or Keys.Space or Keys.W or Keys.A or Keys.S or Keys.D)
      {
         _heldKeys.Add(key);
         return true;
      }

      return base.ProcessCmdKey(ref msg, keyData);
   }
}
