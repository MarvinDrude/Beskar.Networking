using System.Diagnostics;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Net;
using System.Text;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Datagram.Example.ArenaGame2D;

/// <summary>
/// Studio-grade, 100% custom-drawn DPI-independent 60 FPS 2D Arena window.
/// Runs either as an authoritative Listen Server (Host) or as a UDP Client with
/// live fault sliders, prediction/reconciliation/interpolation toggles, and visual debug overlay.
/// </summary>
public sealed class GameCanvasForm : Form
{
   private readonly record struct VisualRing(float X, float Y, FxKind2D Kind, Color Color, float Age, float MaxAge);
   private readonly record struct FloatingPopup(float X, float Y, string Text, Color Color, float Age, float MaxAge);
   private readonly record struct TrailPoint(float X, float Y);

   private sealed class ButtonWidget(string id, RectangleF rect, Func<string> text, Func<bool> isActive, Action onClick, bool primary = false)
   {
      public string Id { get; } = id;
      public RectangleF Rect { get; } = rect;
      public Func<string> Text { get; } = text;
      public Func<bool> IsActive { get; } = isActive;
      public Action OnClick { get; } = onClick;
      public bool Primary { get; } = primary;
   }

   private sealed class SliderWidget(string id, string title, RectangleF hitRect, RectangleF trackRect, int min, int max, Func<int> getter, Action<int> setter, Func<int, string> formatValue)
   {
      public string Id { get; } = id;
      public string Title { get; } = title;
      public RectangleF HitRect { get; } = hitRect;
      public RectangleF TrackRect { get; } = trackRect;
      public int Min { get; } = min;
      public int Max { get; } = max;
      public Func<int> Getter { get; } = getter;
      public Action<int> Setter { get; } = setter;
      public Func<int, string> FormatValue { get; } = formatValue;
   }

   private sealed class ToggleWidget(string id, string label, string hotkey, RectangleF rowRect, RectangleF checkRect, Func<bool> getter, Action<bool> setter)
   {
      public string Id { get; } = id;
      public string Label { get; } = label;
      public string Hotkey { get; } = hotkey;
      public RectangleF RowRect { get; } = rowRect;
      public RectangleF CheckRect { get; } = checkRect;
      public Func<bool> Getter { get; } = getter;
      public Action<bool> Setter { get; } = setter;
   }

   // Default high-resolution layout (1600 x 900, 16:9) + dynamic resizing up to 4K/maximized
   private const int DefaultWinW = 1600;
   private const int DefaultWinH = 900;
   private int _winW = DefaultWinW;
   private int _winH = DefaultWinH;
   private RectangleF _headerRect;
   private RectangleF _arenaRect;
   private float _arenaScale = 1f;
   private RectangleF _healthRect;
   private RectangleF _cardFaults;
   private RectangleF _cardEngine;
   private RectangleF _cardScores;
   private RectangleF _cardTelemetry;

   private readonly bool _isHost;
   private readonly string _playerName;
   private readonly IPEndPoint _targetEndPoint;
   private int _botCount;

   private readonly FaultInjectingUdpTransport _transport = new();
   private DatagramHost? _datagramHost;
   private DatagramPeer? _serverPeer;

   // Host-only simulation state
   private readonly ArenaSimulation2D? _sim;
   private ArenaSimulation2D.PlayerEntity? _hostLocalPlayer;

   // Client-only netcode state
   private readonly ClientNetcodeState _netcode = new();

   // Render & scratch state
   private readonly List<PlayerState2D> _renderPlayers = new(16);
   private readonly List<PlayerState2D> _rawRemoteGhosts = new(16);
   private readonly List<OrbState2D> _renderOrbs = new(16);
   private readonly List<LaserState2D> _renderLasers = new(32);
   private readonly List<PlayerState2D> _snapScratchPlayers = new(16);
   private readonly List<OrbState2D> _snapScratchOrbs = new(12);
   private readonly List<LaserState2D> _snapScratchLasers = new(24);
   private readonly List<ScoreEntry2D> _scoreboard = new(16);
   private readonly Dictionary<byte, string> _namesById = new();
   private readonly Dictionary<byte, Queue<TrailPoint>> _trailsById = new();
   private readonly List<VisualRing> _rings = new(48);
   private readonly List<FloatingPopup> _popups = new(24);
   private readonly Queue<string> _eventFeed = new();
   private readonly byte[] _packetBuf = new byte[1400];

   // Custom interactive widgets
   private readonly List<ButtonWidget> _buttons = [];
   private readonly List<SliderWidget> _sliders = [];
   private readonly List<ToggleWidget> _toggles = [];
   private PointF _cursorPos;
   private SliderWidget? _activeSlider;

   // Gameplay input state
   private readonly HashSet<Keys> _heldKeys = [];
   private bool _mouseAimActive;
   private float _mouseArenaX = GamePhysics2D.ArenaWidth * 0.5f;
   private float _mouseArenaY = GamePhysics2D.ArenaHeight * 0.5f;
   private bool _mouseFireHeld;
   private float _lastAimAngle;
   private int _emoteCursor;

   // Automated test override input (optional, null during normal manual play)
   private Func<(InputButtons Buttons, float AimAngle)>? _scriptedInputProvider;

   // Fixed-timestep loop state (10ms high-res timer + sub-frame alpha interpolation = silky 60-100Hz glide!)
   private readonly System.Windows.Forms.Timer _frameTimer = new() { Interval = 10 };
   private readonly Stopwatch _clock = Stopwatch.StartNew();
   private readonly TaskCompletionSource _readyTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
   private double _lastFrameTimeSec;
   private double _accumulatorSec;
   private bool _highResTimerActive;
   private bool _debugOverlayEnabled = true;
   private bool _connected = true;
   private bool _isInitialized;
   private string _statusBanner = string.Empty;
   private int _snapshotsReceivedCount;
   private int _scoreboardsReceivedCount;
   private int _fxReceivedCount;
   private int _orderedEventsReceivedCount;

   // Pixel-exact fonts (zero DPI distortion, tuned for 1600x900+ crisp legibility)
   private readonly Font _fBrand = UiTheme.MakeFont("Segoe UI", 16f, FontStyle.Bold);
   private readonly Font _fCardTitle = UiTheme.MakeFont("Segoe UI", 12f, FontStyle.Bold);
   private readonly Font _fBody = UiTheme.MakeFont("Segoe UI", 12.5f, FontStyle.Regular);
   private readonly Font _fBodyBold = UiTheme.MakeFont("Segoe UI", 12.5f, FontStyle.Bold);
   private readonly Font _fBtn = UiTheme.MakeFont("Segoe UI", 12f, FontStyle.Bold);
   private readonly Font _fBadge = UiTheme.MakeFont("Consolas", 10.5f, FontStyle.Bold);
   private readonly Font _fMono = UiTheme.MakeFont("Consolas", 12f, FontStyle.Regular);
   private readonly Font _fMonoBold = UiTheme.MakeFont("Consolas", 12f, FontStyle.Bold);
   private readonly Font _fMonoSmall = UiTheme.MakeFont("Consolas", 11f, FontStyle.Regular);
   private readonly Font _fMonoSmallBold = UiTheme.MakeFont("Consolas", 11f, FontStyle.Bold);
   private readonly Font _fPopup = UiTheme.MakeFont("Segoe UI", 13f, FontStyle.Bold);

   public Task ReadyTask => _readyTcs.Task;
   public IPEndPoint? BoundEndPoint => _datagramHost?.LocalEndPoint;
   public bool IsClientConnected => _connected && (_isHost || _serverPeer?.State == DatagramPeerState.Connected);

   public ClientNetcodeState Netcode => _netcode;
   public FaultInjectingUdpTransport FaultTransport => _transport;
   public ArenaSimulation2D? HostSimulation => _sim;
   public DatagramHost? DatagramHostInstance => _datagramHost;
   public DatagramPeer? ClientServerPeer => _serverPeer;

   public int SnapshotsReceivedCount => _snapshotsReceivedCount;
   public int ScoreboardsReceivedCount => _scoreboardsReceivedCount;
   public int FxReceivedCount => _fxReceivedCount;
   public int OrderedEventsReceivedCount => _orderedEventsReceivedCount;

   public IReadOnlyList<ScoreEntry2D> CurrentScoreboard => _scoreboard;
   public IReadOnlyList<PlayerState2D> RenderedPlayers => _renderPlayers;
   public IReadOnlyList<LaserState2D> RenderedLasers => _renderLasers;

   public event Action<double, IReadOnlyList<PlayerState2D>>? FrameRendered;

   public GameCanvasForm(bool isHost, IPEndPoint endPoint, string playerName, int initialBots = 2, bool showOnScreen = true)
   {
      _isHost = isHost;
      _targetEndPoint = endPoint;
      _playerName = string.IsNullOrWhiteSpace(playerName) ? (isHost ? "Host" : "Pilot") : playerName.Trim();
      _botCount = Math.Clamp(initialBots, 0, 4);

      if (_isHost)
      {
         _sim = new ArenaSimulation2D();
         _hostLocalPlayer = _sim.AddPlayer(_playerName, peerId: null);
         _sim.SetBotCount(_botCount);
      }

      AutoScaleMode = AutoScaleMode.None;
      DoubleBuffered = true;
      SetStyle(ControlStyles.AllPaintingInWmPaint | ControlStyles.UserPaint | ControlStyles.OptimizedDoubleBuffer, true);

      Text = _isHost
         ? $"Beskar Arena 2D — HOST ({_playerName})"
         : $"Beskar Arena 2D — CLIENT ({_playerName})";

      var targetW = DefaultWinW;
      var targetH = DefaultWinH;
      if (showOnScreen)
      {
         var work = Screen.FromPoint(Cursor.Position).WorkingArea;
         if (work.Width > 0 && work.Height > 0)
         {
            targetW = Math.Clamp(DefaultWinW, 1200, Math.Max(1200, work.Width - 40));
            targetH = Math.Clamp(DefaultWinH, 720, Math.Max(720, work.Height - 70));
         }
      }

      FormBorderStyle = FormBorderStyle.Sizable;
      MaximizeBox = true;
      MinimumSize = new Size(1240, 760);
      ClientSize = new Size(targetW, targetH);
      StartPosition = _isHost ? FormStartPosition.CenterScreen : FormStartPosition.WindowsDefaultLocation;
      BackColor = UiTheme.WindowBg;
      ForeColor = UiTheme.TextPrimary;
      KeyPreview = true;

      if (!showOnScreen)
      {
         ShowInTaskbar = false;
         StartPosition = FormStartPosition.Manual;
         Location = new Point(-2600, -2600);
      }

      RecalculateLayout(ClientSize.Width, ClientSize.Height);
      WireEvents();
   }

   public void SetFaultProfile(int delayMs, int jitterMs, int lossPct)
   {
      _transport.AddedLatencyMs = delayMs;
      _transport.JitterMs = jitterMs;
      _transport.PacketLossPercent = lossPct;
   }

   public void SetNetcodeOptions(bool prediction, bool interpolation, bool redundancy, bool predictShotsAndPickups = true)
   {
      _netcode.PredictionEnabled = prediction;
      _netcode.InterpolationEnabled = interpolation;
      _netcode.InputRedundancyEnabled = redundancy;
      _netcode.PredictShotsAndPickupsEnabled = predictShotsAndPickups;
   }

   public void SetDebugOverlay(bool enabled) => _debugOverlayEnabled = enabled;
   public void SetLagCompensation(bool enabled)
   {
      if (_sim is not null) _sim.LagCompensationEnabled = enabled;
   }
   public void SetScriptedInput(Func<(InputButtons Buttons, float AimAngle)>? provider) => _scriptedInputProvider = provider;
   public void TriggerEmote() => SendEmote();

   public void CaptureScreenshotToFile(string path)
   {
      var w = Math.Max(1, _winW);
      var h = Math.Max(1, _winH);
      using var bmp = new Bitmap(w, h, PixelFormat.Format32bppArgb);
      using (var g = Graphics.FromImage(bmp))
      {
         PaintEntireWindow(g);
      }
      Directory.CreateDirectory(Path.GetDirectoryName(path)!);
      bmp.Save(path, ImageFormat.Png);
   }

   protected override void OnHandleCreated(EventArgs e)
   {
      base.OnHandleCreated(e);
      UiTheme.EnableDarkTitleBar(Handle);
   }

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

      if (!_isHost)
      {
         var tx = _cardEngine.X + 12f;
         var tw = _cardEngine.Width - 24f;
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
         var tx = _cardEngine.X + 12f;
         var tw = _cardEngine.Width - 24f;
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

      MouseMove += (_, e) =>
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
      };

      MouseDown += (_, e) =>
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
      };

      MouseUp += (_, e) =>
      {
         if (e.Button == MouseButtons.Left)
         {
            _activeSlider = null;
            _mouseFireHeld = false;
         }
      };
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

   private async Task InitializeNetworkingAsync()
   {
      if (_isInitialized) return;
      _isInitialized = true;

      var opts = GameProtocol2D.CreateOptions();

      if (_isHost)
      {
         _datagramHost = new DatagramHost(new IPEndPoint(IPAddress.Any, _targetEndPoint.Port), opts, _transport);
         _datagramHost.Events.ValidateConnection = ctx =>
         {
            var rawName = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span).Trim();
            if (string.IsNullOrWhiteSpace(rawName)) rawName = $"Pilot{Random.Shared.Next(10, 99)}";
            ctx.PeerTag = rawName.Length > 12 ? rawName[..12] : rawName;
            return ValueTask.CompletedTask;
         };

         _datagramHost.Start();
         var boundPort = _datagramHost.LocalEndPoint?.Port ?? _targetEndPoint.Port;
         AddFeed($"Hosting on UDP :{boundPort} (AEAD + PSK).");
      }
      else
      {
         _datagramHost = new DatagramHost(new IPEndPoint(IPAddress.Any, 0), opts, _transport);
         _datagramHost.Start();

         try
         {
            var connectTask = _datagramHost.ConnectAsync(_targetEndPoint, Encoding.UTF8.GetBytes(_playerName));
            while (!connectTask.IsCompleted)
            {
               _datagramHost.Tick();
               await Task.Delay(15);
            }

            _serverPeer = await connectTask;
            AddFeed($"Connected to {_targetEndPoint}!");
         }
         catch (Exception ex)
         {
            _connected = false;
            _statusBanner = $"Could not connect to {_targetEndPoint}: {ex.Message}";
            AddFeed(_statusBanner);
            _readyTcs.TrySetResult();
            Invalidate();
            return;
         }
      }

      UiTheme.BeginHighResTimer();
      _highResTimerActive = true;
      _lastFrameTimeSec = _clock.Elapsed.TotalSeconds;
      _frameTimer.Tick += OnFrameTimerTick;
      _frameTimer.Start();
      _readyTcs.TrySetResult();
   }

   private async Task ShutdownNetworkingAsync()
   {
      _frameTimer.Stop();
      if (_highResTimerActive)
      {
         UiTheme.EndHighResTimer();
         _highResTimerActive = false;
      }

      if (_datagramHost is not null)
      {
         if (_isHost)
         {
            foreach (var p in _datagramHost.Peers)
            {
               p.Disconnect("Host closed arena"u8);
            }
            _datagramHost.Tick();
         }
         else if (_serverPeer is not null && _serverPeer.State == DatagramPeerState.Connected)
         {
            _serverPeer.Disconnect("Client quit"u8);
            _datagramHost.Tick();
         }

         await Task.Delay(30);
         await _datagramHost.DisposeAsync();
         _datagramHost = null;
      }

      _fBrand.Dispose();
      _fCardTitle.Dispose();
      _fBody.Dispose();
      _fBodyBold.Dispose();
      _fBtn.Dispose();
      _fBadge.Dispose();
      _fMono.Dispose();
      _fMonoBold.Dispose();
      _fMonoSmall.Dispose();
      _fMonoSmallBold.Dispose();
      _fPopup.Dispose();
   }

   private void OnFrameTimerTick(object? sender, EventArgs e)
   {
      if (_datagramHost is null) return;

      var nowSec = _clock.Elapsed.TotalSeconds;
      var dt = Math.Clamp(nowSec - _lastFrameTimeSec, 0.0, 0.05);
      _lastFrameTimeSec = nowSec;
      _accumulatorSec += dt;

      var steps = 0;
      while (_accumulatorSec >= GamePhysics2D.FixedDt && steps < 3)
      {
         _accumulatorSec -= GamePhysics2D.FixedDt;
         steps++;

         if (_isHost) StepHostTick();
         else if (_connected) StepClientTick(ClientNetcodeState.NowSeconds());
      }

      for (var i = _rings.Count - 1; i >= 0; i--)
      {
         var r = _rings[i];
         var next = r.Age + (float)dt;

         if (next >= r.MaxAge) _rings.RemoveAt(i);
         else _rings[i] = r with { Age = next };
      }

      for (var i = _popups.Count - 1; i >= 0; i--)
      {
         var p = _popups[i];
         var next = p.Age + (float)dt;

         if (next >= p.MaxAge) _popups.RemoveAt(i);
         else _popups[i] = p with { Age = next, Y = p.Y - (34f * (float)dt) };
      }

      var subFrameAlpha = (float)Math.Clamp(_accumulatorSec / GamePhysics2D.FixedDt, 0.0, 1.0);

      if (_isHost && _sim is not null)
      {
         _sim.FillSnapshotLists(_renderPlayers, _renderOrbs, _renderLasers, renderAlpha: subFrameAlpha);
      }
      else
      {
         _netcode.BuildRenderWorld(ClientNetcodeState.NowSeconds(), subFrameAlpha, _renderPlayers, _rawRemoteGhosts, _renderOrbs, _renderLasers);
      }

      FrameRendered?.Invoke(ClientNetcodeState.NowSeconds(), _renderPlayers);

      for (var i = 0; i < _renderPlayers.Count; i++)
      {
         var p = _renderPlayers[i];
         if (!_trailsById.TryGetValue(p.Id, out var q))
         {
            q = new Queue<TrailPoint>(7);
            _trailsById[p.Id] = q;
         }
         q.Enqueue(new TrailPoint(p.X, p.Y));
         while (q.Count > 6) q.Dequeue();
      }

      Invalidate();
   }

   private void StepHostTick()
   {
      var host = _datagramHost!;
      var sim = _sim!;
      var local = _hostLocalPlayer!;

      host.Tick();

      host.PollEvents(evt =>
      {
         switch (evt.Type)
         {
            case DatagramEventType.PeerConnected:
            {
               var name = evt.Peer.Tag as string ?? $"Pilot{evt.Peer.PeerId}";
               var entity = sim.AddPlayer(name, evt.Peer.PeerId);
               var wLen = GameProtocol2D.WriteWelcome(_packetBuf, entity.Id, entity.Color, entity.X, entity.Y);
               evt.Peer.TrySend(GameProtocol2D.ChLifecycle, _packetBuf.AsSpan(0, wLen));
               sim.ScoreboardDirty = true;
               break;
            }

            case DatagramEventType.PeerDisconnected:
               sim.RemovePlayerByPeer(evt.Peer.PeerId, evt.DisconnectReason.ToString());
               break;

            case DatagramEventType.MessageReceived:
            {
               var player = sim.FindByPeer(evt.Peer.PeerId);
               if (player is null) break;

               if (evt.ChannelId == GameProtocol2D.ChRealtime)
               {
                  Span<ClientInputFrame> inputScratch = stackalloc ClientInputFrame[GameProtocol2D.MaxRedundantInputs];
                  var count = GameProtocol2D.ReadInputBundle(evt.Payload.Span, inputScratch);
                  if (count > 0) sim.ReceiveClientInputBundle(player, inputScratch[..count]);
               }
               else if (evt.ChannelId == GameProtocol2D.ChLifecycle
                        && GameProtocol2D.TryReadEmote(evt.Payload.Span, out var emoteIdx))
               {
                  var emoteText = GameProtocol2D.Emotes[emoteIdx % GameProtocol2D.Emotes.Length];
                  BroadcastEvent($"{player.Name}: \"{emoteText}\"");
               }
               break;
            }
         }
      });

      var (buttons, aim) = SampleCurrentControls(local.X, local.Y);
      sim.ApplyLocalHostInput(local, buttons, aim);
      sim.Step();

      foreach (var ev in sim.PendingEvents)
      {
         BroadcastEvent(ev);
      }
      sim.PendingEvents.Clear();

      foreach (var fx in sim.PendingFx)
      {
         SpawnFxVisuals(fx);
         var fxLen = GameProtocol2D.WriteFxCue(_packetBuf, fx.X, fx.Y, fx.Kind, fx.Color);
         host.Broadcast(GameProtocol2D.ChFx, _packetBuf.AsSpan(0, fxLen));
      }
      sim.PendingFx.Clear();

      sim.FillScoreboard(_scoreboard);
      UpdateNameMap();
      if (sim.ScoreboardDirty)
      {
         sim.ScoreboardDirty = false;
         var sbLen = GameProtocol2D.WriteScoreboard(_packetBuf, _scoreboard);
         host.Broadcast(GameProtocol2D.ChScoreboard, _packetBuf.AsSpan(0, sbLen));
      }

      if ((sim.Tick & 1) == 0 && host.PeerCount > 0)
      {
         sim.FillSnapshotLists(_renderPlayers, _renderOrbs, _renderLasers);
         var snapLen = GameProtocol2D.WriteWorldSnapshot(_packetBuf, sim.Tick, 0, _renderPlayers, _renderOrbs, _renderLasers);

         foreach (var peer in host.Peers)
         {
            if (peer.State != DatagramPeerState.Connected) continue;
            var pEntity = sim.FindByPeer(peer.PeerId);
            GameProtocol2D.PatchSnapshotAckedInput(_packetBuf, pEntity?.LastProcessedInputSeq ?? 0);
            peer.TrySend(GameProtocol2D.ChRealtime, _packetBuf.AsSpan(0, snapLen));
         }
      }

      host.Tick();
   }

   private void StepClientTick(double nowSec)
   {
      var client = _datagramHost!;
      var peer = _serverPeer!;

      var (buttons, aim) = SampleCurrentControls(_netcode.RenderLocalX, _netcode.RenderLocalY);
      Span<ClientInputFrame> bundle = stackalloc ClientInputFrame[GameProtocol2D.MaxRedundantInputs];
      var count = _netcode.RecordAndPredictInput(buttons, aim, bundle, nowSec, (fx, orbVal) => SpawnFxVisuals(fx, orbVal));

      var inLen = GameProtocol2D.WriteInputBundle(_packetBuf, bundle[..count]);
      peer.TrySend(GameProtocol2D.ChRealtime, _packetBuf.AsSpan(0, inLen));

      client.Tick();

      client.PollEvents(evt =>
      {
         switch (evt.Type)
         {
            case DatagramEventType.PeerDisconnected:
               _connected = false;
               var reason = evt.Payload.Length > 0 ? Encoding.UTF8.GetString(evt.Payload.Span) : evt.DisconnectReason.ToString();
               _statusBanner = $"Disconnected: {reason}";
               AddFeed(_statusBanner);
               break;

            case DatagramEventType.MessageReceived:
            {
               var span = evt.Payload.Span;
               switch (evt.ChannelId)
               {
                  case GameProtocol2D.ChLifecycle:
                     if (GameProtocol2D.TryReadWelcome(span, out var pid, out var col, out var sx, out var sy))
                     {
                        _netcode.HandleWelcome(pid, col, sx, sy);
                     }
                     else if (GameProtocol2D.TryReadEventLog(span, out var text))
                     {
                        _orderedEventsReceivedCount++;
                        AddFeed(text);
                     }
                     break;

                  case GameProtocol2D.ChScoreboard:
                     if (GameProtocol2D.TryReadScoreboard(span, _scoreboard))
                     {
                        _scoreboardsReceivedCount++;
                        UpdateNameMap();
                     }
                     break;

                  case GameProtocol2D.ChRealtime:
                     if (GameProtocol2D.TryReadWorldSnapshot(span, out var tick, out var ackSeq, _snapScratchPlayers, _snapScratchOrbs, _snapScratchLasers))
                     {
                        _snapshotsReceivedCount++;
                        _netcode.ApplyAuthoritativeSnapshot(tick, ackSeq, _snapScratchPlayers, _snapScratchOrbs, _snapScratchLasers, nowSec);
                     }
                     break;

                  case GameProtocol2D.ChFx:
                     if (GameProtocol2D.TryReadFxCue(span, out var fx))
                     {
                        _fxReceivedCount++;
                        if (!_netcode.ShouldSuppressServerFx(fx, nowSec))
                        {
                           SpawnFxVisuals(fx);
                        }
                     }
                     break;
               }
               break;
            }
         }
      });

      if ((_netcode.LatestServerTick & 1) == 0)
      {
         _netcode.RecordTelemetrySample((float)peer.Stats.SmoothedRttMs);
      }
   }

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
      g.DrawString("Beskar Arena", _fBrand, whiteBrush, 18f, 16f);

      // Subtle neutral role tag
      var roleText = _isHost ? "HOST" : "CLIENT";
      var roleRect = new RectangleF(134, 16, 62, 22);

      UiTheme.FillRoundedRect(g, roleRect, 3f, UiTheme.ControlBg);
      UiTheme.DrawRoundedRect(g, roleRect, 3f, UiTheme.BorderStrong, 1f);

      var rsz = g.MeasureString(roleText, _fBadge);
      using (var rb = new SolidBrush(UiTheme.TextPrimary))
         g.DrawString(roleText, _fBadge, rb, roleRect.X + ((roleRect.Width - rsz.Width) * 0.5f), roleRect.Y + ((roleRect.Height - rsz.Height) * 0.5f));

      // Endpoint & security info
      var epStr = _isHost
         ? $"UDP :{_datagramHost?.LocalEndPoint?.Port ?? _targetEndPoint.Port}  |  AEAD + PSK"
         : $"{_targetEndPoint}  |  AEAD + PSK";
      g.DrawString(epStr, _fMono, mutedBrush, 210f, 19f);

      // Clean connection status indicator
      var (healthLabel, healthCol) = GetConnectionHealthBadge();
      UiTheme.FillRoundedRect(g, _healthRect, 4f, UiTheme.CardBg);
      UiTheme.DrawRoundedRect(g, _healthRect, 4f, UiTheme.BorderSubtle, 1f);
      using (var dot = new SolidBrush(healthCol))
         g.FillEllipse(dot, _healthRect.X + 10f, _healthRect.Y + 10f, 8f, 8f);
      using (var hb = new SolidBrush(UiTheme.TextPrimary))
         g.DrawString(healthLabel, _fMonoSmallBold, hb, _healthRect.X + 24f, _healthRect.Y + 7f);

      // Header flat buttons
      foreach (var btn in _buttons)
      {
         if (btn.Rect.Y < _headerRect.Bottom)
         {
            UiTheme.DrawFlatButton(g, btn.Rect, btn.Text(), _fBtn, btn.IsActive(), btn.Rect.Contains(_cursorPos), btn.Primary);
         }
      }
   }

   private void PaintArenaViewport(Graphics g)
   {
      var clipState = g.Save();
      using var clipPath = UiTheme.CreateRoundedRect(_arenaRect, 4f);
      g.SetClip(clipPath);

      // WORLD-SPACE VECTOR PASS (scaled crisply to viewport resolution)
      var worldState = g.Save();
      g.TranslateTransform(_arenaRect.X, _arenaRect.Y);
      g.ScaleTransform(_arenaScale, _arenaScale);

      // Matte dark drafting/concrete arena floor
      using (var bg = new SolidBrush(UiTheme.ArenaBg))
         g.FillRectangle(bg, 0, 0, GamePhysics2D.ArenaWidth, GamePhysics2D.ArenaHeight);

      // Clean subtle floor grid (40px tiles, 160px major lines)
      var invScale = 1f / Math.Max(0.5f, _arenaScale);
      using (var minorPen = new Pen(UiTheme.ArenaGrid, invScale))
      using (var majorPen = new Pen(UiTheme.ArenaGridMajor, invScale))
      {
         for (var x = 40; x < GamePhysics2D.ArenaWidth; x += 40)
            g.DrawLine((x % 160 == 0) ? majorPen : minorPen, x, 0, x, GamePhysics2D.ArenaHeight);
         for (var y = 40; y < GamePhysics2D.ArenaHeight; y += 40)
            g.DrawLine((y % 160 == 0) ? majorPen : minorPen, 0, y, GamePhysics2D.ArenaWidth, y);
      }

      // Solid matte concrete/steel cover blocks with contact shadow & crisp top bevel
      foreach (var obs in GamePhysics2D.Obstacles)
      {
         var shadowRect = new RectangleF(obs.X + 3, obs.Y + 4, obs.W, obs.H);
         UiTheme.FillRoundedRect(g, shadowRect, 3f, Color.FromArgb(95, 0, 0, 0));

         var blockRect = new RectangleF(obs.X, obs.Y, obs.W, obs.H);
         UiTheme.FillRoundedRect(g, blockRect, 3f, UiTheme.ObstacleFill);

         // Subtle matte top cap highlight & inner surface inset
         using (var topBrush = new SolidBrush(UiTheme.ObstacleTop))
            g.FillRectangle(topBrush, obs.X + 2, obs.Y + 2, obs.W - 4, 6);
         using (var insetPen = new Pen(Color.FromArgb(40, 43, 50), 1f))
            g.DrawRectangle(insetPen, obs.X + 6, obs.Y + 12, obs.W - 12, obs.H - 22);

         UiTheme.DrawRoundedRect(g, blockRect, 3f, UiTheme.ObstacleEdge, 1.2f);
      }

      // Clean understated impact/pickup rings & local cosmetic hit sparks
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

      // Crisp solid metallic Beskar pickup tokens
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

      // Debug Underlay geometry (Client RAW/SERVER ghosts OR Host lag-comp rewound hitboxes!)
      if (_debugOverlayEnabled)
      {
         if (!_isHost) PaintClientDebugUnderlayGeometry(g);
         else PaintHostRewindMarkersGeometry(g);
      }

      // Crisp sharp projectiles
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

      // Grounded flat-shaded 2D player rectangles
      var myId = _isHost ? (_hostLocalPlayer?.Id ?? 0) : _netcode.MyPlayerId;
      foreach (var p in _renderPlayers)
      {
         PaintPlayerEntityGeometry(g, p, isLocal: p.Id == myId);
      }

      g.Restore(worldState);

      // NATIVE PHYSICAL-PIXEL TEXT & HUD PASS (razor-sharp ClearType at every window size)
      if (_debugOverlayEnabled && !_isHost)
      {
         PaintClientDebugUnderlayLabels(g);
      }

      foreach (var p in _renderPlayers)
      {
         PaintPlayerLabel(g, p, isLocal: p.Id == myId);
      }

      // Clean floating combat/pickup text
      foreach (var pop in _popups)
      {
         var sp = WorldToScreen(pop.X, pop.Y);
         var alpha = Math.Clamp((int)((1f - (pop.Age / pop.MaxAge)) * 255), 0, 255);
         var sz = g.MeasureString(pop.Text, _fPopup);

         using var sh = new SolidBrush(Color.FromArgb(alpha, 12, 12, 14));
         using var br = new SolidBrush(Color.FromArgb(alpha, pop.Color));

         g.DrawString(pop.Text, _fPopup, sh, sp.X - (sz.Width * 0.5f) + 1f, sp.Y + 1f);
         g.DrawString(pop.Text, _fPopup, br, sp.X - (sz.Width * 0.5f), sp.Y);
      }

      // Valve net_graph-style grounded Debug Overlay
      if (_debugOverlayEnabled)
      {
         if (_isHost) PaintHostDebugOverlay(g);
         else PaintClientDebugTelemetryStrip(g);
      }

      // Disconnect banner
      if (!_connected && !string.IsNullOrEmpty(_statusBanner))
      {
         var bw = Math.Min(640f, _arenaRect.Width - 60f);
         var bh = 62f;
         var bx = _arenaRect.X + ((_arenaRect.Width - bw) * 0.5f);
         var by = _arenaRect.Y + ((_arenaRect.Height - bh) * 0.5f);

         UiTheme.FillRoundedRect(g, new RectangleF(bx, by, bw, bh), 4f, Color.FromArgb(230, 32, 20, 20));
         UiTheme.DrawRoundedRect(g, new RectangleF(bx, by, bw, bh), 4f, UiTheme.StatusBad, 1f);

         var sz = g.MeasureString(_statusBanner, _fBrand);
         using var eb = new SolidBrush(UiTheme.TextPrimary);
         g.DrawString(_statusBanner, _fBrand, eb, bx + ((bw - sz.Width) * 0.5f), by + ((bh - sz.Height) * 0.5f));
      }

      g.Restore(clipState);

      // Crisp 1px arena wall frame
      UiTheme.DrawRoundedRect(g, _arenaRect, 4f, UiTheme.BorderStrong, 1f);
   }

   private void PaintPlayerEntityGeometry(Graphics g, PlayerState2D p, bool isLocal)
   {
      var baseCol = UiTheme.PlayerPalette[p.Color % UiTheme.PlayerPalette.Length];
      var fillCol = p.Stunned ? Color.FromArgb(108, 112, 120) : baseCol;

      // Subtle understated matte afterimage trail
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

      // Stun indicator
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
      var tsz = g.MeasureString(label, _fMonoSmallBold);
      var tagW = tsz.Width + 10f;
      var tagX = Math.Clamp(topCenter.X - (tagW * 0.5f), _arenaRect.X + 4f, _arenaRect.Right - tagW - 4f);
      var tagY = Math.Max(_arenaRect.Y + 4f, topCenter.Y - 22f);
      var tagRect = new RectangleF(tagX, tagY, tagW, 16f);

      UiTheme.FillRoundedRect(g, tagRect, 3f, Color.FromArgb(185, 18, 19, 22));

      using var txtBrush = new SolidBrush(isLocal ? Color.White : UiTheme.TextPrimary);
      g.DrawString(label, _fMonoSmallBold, txtBrush, tagRect.X + 5f, tagRect.Y + 1.5f);
   }

   private void PaintClientDebugUnderlayGeometry(Graphics g)
   {
      // Pending optimistic orb pickups (dashed outline awaiting server confirm)
      if (_netcode.PredictShotsAndPickupsEnabled && _netcode.PendingPickups.Count > 0)
      {
         using var pPen = new Pen(Color.FromArgb(170, UiTheme.StatusOk), 1.3f) { DashStyle = DashStyle.Dash };
         foreach (var pending in _netcode.PendingPickups)
         {
            var o = pending.Orb;
            g.DrawEllipse(pPen, o.X - 10f, o.Y - 10f, 20f, 20f);
         }
      }

      // Remote RAW un-interpolated snapshot ghosts (neutral slate dashed box)
      if (_netcode.InterpolationEnabled)
      {
         var rawCol = Color.FromArgb(170, 166, 174, 188);
         using var linkPen = new Pen(Color.FromArgb(110, 166, 174, 188), 1f) { DashStyle = DashStyle.Dot };

         foreach (var ghost in _rawRemoteGhosts)
         {
            var sep = 999f;
            float ix = ghost.X, iy = ghost.Y;
            foreach (var interp in _renderPlayers)
            {
               if (interp.Id == ghost.Id)
               {
                  ix = interp.X; iy = interp.Y;
                  sep = MathF.Sqrt(((ix - ghost.X) * (ix - ghost.X)) + ((iy - ghost.Y) * (iy - ghost.Y)));
                  break;
               }
            }

            if (sep < 3f) continue;

            var gr = new RectangleF(ghost.X - GamePhysics2D.PlayerHalf, ghost.Y - GamePhysics2D.PlayerHalf, GamePhysics2D.PlayerSize, GamePhysics2D.PlayerSize);
            using (var gPath = UiTheme.CreateRoundedRect(gr, 3f))
            using (var gPen = new Pen(rawCol, 1.3f) { DashStyle = DashStyle.Dot })
               g.DrawPath(gPen, gPath);

            g.DrawLine(linkPen, ix, iy, ghost.X, ghost.Y);
         }
      }

      // Subtle monochrome breadcrumb dots for unconfirmed predicted inputs in flight
      var trail = _netcode.UnackedTrail;
      if (trail.Count > 0)
      {
         for (var i = 0; i < trail.Count; i++)
         {
            var pt = trail[i];
            var alpha = 55 + (int)(155f * i / Math.Max(1, trail.Count));
            using var dot = new SolidBrush(Color.FromArgb(alpha, 212, 218, 228));
            g.FillEllipse(dot, pt.PredX - 2f, pt.PredY - 2f, 4f, 4f);
         }
      }

      // Authoritative SERVER Ghost Rectangle + tether for local player
      if (_netcode.HasServerGhost)
      {
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
   }

   private void PaintClientDebugUnderlayLabels(Graphics g)
   {
      if (_netcode is { PredictShotsAndPickupsEnabled: true, PendingPickups.Count: > 0 })
      {
         using var pTxt = new SolidBrush(Color.FromArgb(195, UiTheme.StatusOk));
         foreach (var pending in _netcode.PendingPickups)
         {
            var sp = WorldToScreen(pending.Orb.X, pending.Orb.Y + 11f);
            var sz = g.MeasureString("PRED", _fBadge);
            g.DrawString("PRED", _fBadge, pTxt, sp.X - (sz.Width * 0.5f), sp.Y + 2f);
         }
      }

      if (_netcode.InterpolationEnabled)
      {
         using var rawText = new SolidBrush(Color.FromArgb(205, 196, 202, 214));
         foreach (var ghost in _rawRemoteGhosts)
         {
            var sep = 999f;
            foreach (var interp in _renderPlayers)
            {
               if (interp.Id == ghost.Id)
               {
                  sep = MathF.Sqrt(((interp.X - ghost.X) * (interp.X - ghost.X)) + ((interp.Y - ghost.Y) * (interp.Y - ghost.Y)));
                  break;
               }
            }

            if (sep < 3f) continue;

            var bottom = WorldToScreen(ghost.X, ghost.Y + GamePhysics2D.PlayerHalf);
            var rsz = g.MeasureString("RAW", _fBadge);
            var rPill = new RectangleF(bottom.X - (rsz.Width * 0.5f) - 4f, bottom.Y + 4f, rsz.Width + 8f, 16f);

            UiTheme.FillRoundedRect(g, rPill, 2.5f, Color.FromArgb(195, 20, 21, 24));
            g.DrawString("RAW", _fBadge, rawText, rPill.X + 4f, rPill.Y + 2f);
         }
      }

      if (_netcode.HasServerGhost)
      {
         var gx = _netcode.ServerGhostX;
         var gy = _netcode.ServerGhostY;
         var lx = _netcode.RenderLocalX;
         var ly = _netcode.RenderLocalY;
         var flashing = _netcode.FlashReconcileTicks > 0;
         var ghostCol = flashing ? UiTheme.StatusBad : UiTheme.StatusWarn;

         var gBottom = WorldToScreen(gx, gy + GamePhysics2D.PlayerHalf);
         var ghostLabel = _netcode.PredictionEnabled ? "SERVER" : "NO PRED";
         var glSz = g.MeasureString(ghostLabel, _fBadge);
         var sPill = new RectangleF(gBottom.X - (glSz.Width * 0.5f) - 5f, gBottom.Y + 4f, glSz.Width + 10f, 16f);
         UiTheme.FillRoundedRect(g, sPill, 2.5f, Color.FromArgb(205, 20, 21, 24));

         using var sBrush = new SolidBrush(ghostCol);
         g.DrawString(ghostLabel, _fBadge, sBrush, sPill.X + 5f, sPill.Y + 2f);

         var dist = _netcode.ServerGhostDistancePx;
         if (dist > 4f)
         {
            var tag = flashing ? $"Reconcile Δ{dist:F0}px" : $"Lead Δ{dist:F0}px";
            var sz = g.MeasureString(tag, _fBadge);
            var mid = WorldToScreen((lx + gx) * 0.5f, Math.Max(ly, gy) + GamePhysics2D.PlayerHalf);
            var mx = mid.X - (sz.Width * 0.5f);
            var my = mid.Y + 24f;

            UiTheme.FillRoundedRect(g, new RectangleF(mx - 6, my - 2, sz.Width + 12, 18), 3f, Color.FromArgb(225, 20, 21, 24));
            UiTheme.DrawRoundedRect(g, new RectangleF(mx - 6, my - 2, sz.Width + 12, 18), 3f, ghostCol, 1f);

            g.DrawString(tag, _fBadge, sBrush, mx, my + 0.5f);
         }
      }
   }

   private void PaintClientDebugTelemetryStrip(Graphics g)
   {
      // Valve net_graph inspired matte charcoal telemetry strip (native physical pixels inside arena)
      var boxW = Math.Min(516f, _arenaRect.Width - 28f);
      var box = new RectangleF(_arenaRect.X + 14f, _arenaRect.Bottom - 102f, boxW, 88f);

      UiTheme.FillRoundedRect(g, box, 4f, Color.FromArgb(225, 22, 23, 27));
      UiTheme.DrawRoundedRect(g, box, 4f, UiTheme.BorderStrong, 1f);

      var predTag = _netcode.PredictionEnabled ? "pred:on" : "pred:off";
      var interpTag = _netcode.InterpolationEnabled ? $"interp:{_netcode.AdaptiveInterpDelaySec * 1000f:F0}ms" : "interp:off";
      var inTag = _netcode.InputRedundancyEnabled ? "in:6x" : "in:1x";
      var header = $"net_graph  |  {predTag}  {interpTag}  {inTag}  |  pending:{_netcode.PendingUnackedCount}  reconciles:{_netcode.ReconciliationCorrections}";

      using var hdrBrush = new SolidBrush(UiTheme.TextPrimary);
      g.DrawString(header, _fBadge, hdrBrush, box.X + 10f, box.Y + 7f);

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

         var r = new RectangleF(
            m.RewoundX - GamePhysics2D.PlayerHalf,
            m.RewoundY - GamePhysics2D.PlayerHalf,
            GamePhysics2D.PlayerSize,
            GamePhysics2D.PlayerSize);

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
      g.DrawString($"Host  |  Tick #{_sim.Tick}  |  {lagCompText}", _fBadge, title, box.X + 11f, box.Y + 7f);
      g.DrawString($"Peers: {_datagramHost.PeerCount}  |  Drops: {_transport.DroppedPackets}  |  Sim: +{_transport.AddedLatencyMs}ms ±{_transport.JitterMs}ms / {_transport.PacketLossPercent}%",
         _fMonoSmall, body, box.X + 11f, box.Y + 25f);

      var markers = _sim.ActiveRewindMarkers;
      for (var i = 0; i < markers.Count; i++)
      {
         var m = markers[i];
         var fade = Math.Clamp(m.RemainingTicks / 42f, 0.2f, 1f);
         var tag = $"REWIND -{m.RewindMs:F0}ms";

         var sz = g.MeasureString(tag, _fBadge);
         var sp = WorldToScreen(m.RewoundX, m.RewoundY + GamePhysics2D.PlayerHalf);

         var pillW = sz.Width + 10f;
         var pillX = Math.Clamp(sp.X - (pillW * 0.5f), _arenaRect.X + 4f, _arenaRect.Right - pillW - 4f);
         var pillY = Math.Min(_arenaRect.Bottom - 20f, sp.Y + 5f);
         var pill = new RectangleF(pillX, pillY, pillW, 16f);

         UiTheme.FillRoundedRect(g, pill, 2.5f, Color.FromArgb((int)(fade * 215), 22, 23, 27));
         UiTheme.DrawRoundedRect(g, pill, 2.5f, Color.FromArgb((int)(fade * 220), UiTheme.StatusWarn), 1f);

         using var rb = new SolidBrush(Color.FromArgb((int)(fade * 245), UiTheme.StatusWarn));
         g.DrawString(tag, _fBadge, rb, pill.X + 5f, pill.Y + 1.5f);
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
         var sz = g.MeasureString(tag, _fBadge);
         var sp = WorldToScreen(entity.X, entity.Y + GamePhysics2D.PlayerHalf);
         var pillW = sz.Width + 10f;
         var pillX = Math.Clamp(sp.X - (pillW * 0.5f), _arenaRect.X + 4f, _arenaRect.Right - pillW - 4f);
         var pillY = Math.Min(_arenaRect.Bottom - 21f, sp.Y + 5f);
         var pill = new RectangleF(pillX, pillY, pillW, 17f);

         UiTheme.FillRoundedRect(g, pill, 2.5f, Color.FromArgb(215, 22, 23, 27));
         UiTheme.DrawRoundedRect(g, pill, 2.5f, UiTheme.BorderStrong, 1f);

         using var tb = new SolidBrush(col);
         g.DrawString(tag, _fBadge, tb, pill.X + 5f, pill.Y + 2f);
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
      g.DrawString(title, _fBadge, tb, rect.X + 6f, rect.Y + 3f);
   }

   private void PaintControlDeck(Graphics g)
   {
      UiTheme.DrawCard(g, _cardFaults, "Network Simulation", "Real UDP", _fCardTitle, _fBadge);

      foreach (var btn in _buttons)
      {
         if (_cardFaults.Contains(btn.Rect.Location))
         {
            UiTheme.DrawFlatButton(g, btn.Rect, btn.Text(), _fMonoSmallBold, btn.IsActive(), btn.Rect.Contains(_cursorPos));
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

         g.DrawString(sld.Title, _fBody, labelBrush, sld.TrackRect.X, titleY);

         var vsz = g.MeasureString(valStr, _fMonoSmallBold);
         g.DrawString(valStr, _fMonoSmallBold, val == 0 ? valZeroBrush : valActiveBrush, sld.TrackRect.Right - vsz.Width, titleY + 1f);

         UiTheme.DrawSlider(
            g, sld.TrackRect, val, sld.Min, sld.Max,
            hovered: sld.HitRect.Contains(_cursorPos),
            dragging: ReferenceEquals(_activeSlider, sld));
      }

      if (!_isHost)
      {
         UiTheme.DrawCard(g, _cardEngine, "Client Netcode", "Local", _fCardTitle, _fBadge);
         PaintToggleList(g);
      }
      else
      {
         UiTheme.DrawCard(g, _cardEngine, "Host Authority & Peers", $"{_datagramHost?.PeerCount ?? 0} online", _fCardTitle, _fBadge);
         PaintToggleList(g);
         using (var sep = new Pen(Color.FromArgb(44, 44, 52), 1f))
            g.DrawLine(sep, _cardEngine.X + 10f, _cardEngine.Y + 63f, _cardEngine.Right - 10f, _cardEngine.Y + 63f);
         PaintHostPeerListCard(g);
      }

      UiTheme.DrawCard(g, _cardScores, "Scoreboard", "Ch 1 · RelSeq", _fCardTitle, _fBadge);
      PaintScoreboardCard(g);

      UiTheme.DrawCard(g, _cardTelemetry, "Telemetry & Log", "Ch 0 · RelOrd", _fCardTitle, _fBadge);
      PaintTelemetryAndFeedCard(g);
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
         g.DrawString(tog.Label, _fBody, primaryBrush, tog.CheckRect.Right + 9f, tog.RowRect.Y + ((tog.RowRect.Height - 17f) * 0.5f));

         var keyRect = new RectangleF(tog.RowRect.Right - 22f, tog.RowRect.Y + ((tog.RowRect.Height - 18f) * 0.5f), 20f, 18f);
         UiTheme.FillRoundedRect(g, keyRect, 3f, UiTheme.ControlBg);
         UiTheme.DrawRoundedRect(g, keyRect, 3f, UiTheme.BorderSubtle, 1f);
         var ksz = g.MeasureString(tog.Hotkey, _fBadge);
         g.DrawString(tog.Hotkey, _fBadge, keyBrush, keyRect.X + ((keyRect.Width - ksz.Width) * 0.5f), keyRect.Y + ((keyRect.Height - ksz.Height) * 0.5f));
      }
   }

   private void PaintHostPeerListCard(Graphics g)
   {
      if (_datagramHost is null || _datagramHost.PeerCount == 0)
      {
         using var muted = new SolidBrush(UiTheme.TextMuted);
         g.DrawString("No remote clients connected.", _fBody, muted, _cardEngine.X + 12f, _cardEngine.Y + 72f);
         g.DrawString("Click '+ Spawn Client' in the top bar.", _fMonoSmall, muted, _cardEngine.X + 12f, _cardEngine.Y + 94f);
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
         using (var nb = new SolidBrush(UiTheme.TextPrimary)) g.DrawString(name, _fBodyBold, nb, row.X + 23f, row.Y + 3.5f);

         var statText = $"{st.SmoothedRttMs,4:F0}ms  ·  {st.PacketLossRate * 100,2:F0}% loss";
         var ssz = g.MeasureString(statText, _fMonoSmallBold);
         using (var sb = new SolidBrush(UiTheme.TextSecondary)) g.DrawString(statText, _fMonoSmallBold, sb, row.Right - ssz.Width - 8f, row.Y + 4.5f);

         y += 28f;
         shown++;
      }
   }

   private void PaintScoreboardCard(Graphics g)
   {
      if (_scoreboard.Count == 0)
      {
         using var muted = new SolidBrush(UiTheme.TextMuted);
         g.DrawString("Waiting for scoreboard...", _fBody, muted, _cardScores.X + 12f, _cardScores.Y + 46f);
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
            g.DrawString($"{i + 1}. {name}", _fMonoSmallBold, nb, rowX + 22f, y + 3.5f);

         var ptsText = $"{entry.Score} pts";
         var psz = g.MeasureString(ptsText, _fMonoSmallBold);
         using (var pb = new SolidBrush(UiTheme.TextPrimary))
            g.DrawString(ptsText, _fMonoSmallBold, pb, rowX + rowW - psz.Width - 7f, y + 3.5f);

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
         g.DrawString("-", _fMonoSmallBold, arrowBrush, _cardTelemetry.X + 12f, fy);
         g.DrawString(clipped, _fMonoSmall, textBrush, _cardTelemetry.X + 23f, fy);
         fy += linePitch;
      }
   }

   private void DrawMiniStatCell(Graphics g, RectangleF rect, string label, string value)
   {
      UiTheme.FillRoundedRect(g, rect, 3f, UiTheme.ControlBg);
      using var lb = new SolidBrush(UiTheme.TextMuted);
      using var vb = new SolidBrush(UiTheme.TextPrimary);
      g.DrawString(label, _fBadge, lb, rect.X + 7f, rect.Y + 4f);
      g.DrawString(value, _fMonoSmallBold, vb, rect.X + 7f, rect.Y + 18f);
   }

   private void PaintFooter(Graphics g)
   {
      using var muted = new SolidBrush(UiTheme.TextMuted);
      using var secondary = new SolidBrush(UiTheme.TextSecondary);

      var footerY = _winH - 24f;
      var controlsLegend = _isHost
         ? "WASD / Arrows: Move   |   Mouse / Space: Aim & Fire   |   T: Emote   |   L: LagComp   |   B: Bots   |   F3: Debug"
         : "WASD / Arrows: Move   |   Mouse / Space: Aim & Fire   |   T: Emote   |   P / I / R / O: Netcode   |   F3: Debug";
      g.DrawString(controlsLegend, _fMonoSmall, muted, 18f, footerY);

      const string channels = "Ch0 RelOrd  |  Ch1 RelSeq  |  Ch2 UnrelSeq  |  Ch3 UnrelUnord";
      var sz = g.MeasureString(channels, _fMonoSmall);
      g.DrawString(channels, _fMonoSmall, secondary, _winW - sz.Width - 18f, footerY);
   }

   private (string Label, Color Color) GetConnectionHealthBadge()
   {
      if (!_connected) return ("Disconnected", UiTheme.StatusBad);

      if (_isHost)
      {
         var peers = _datagramHost?.Peers;
         if (peers is null || peers.Count == 0) return ("Listening · 0 peers", UiTheme.TextSecondary);
         double rttSum = 0;
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
         double rtt = 0, loss = 0;
         long tx = 0, rx = 0;
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

   private (InputButtons Buttons, float AimAngle) SampleCurrentControls(float playerX, float playerY)
   {
      if (_scriptedInputProvider is not null)
      {
         return _scriptedInputProvider();
      }

      var b = InputButtons.None;
      if (_heldKeys.Contains(Keys.W) || _heldKeys.Contains(Keys.Up)) b |= InputButtons.Up;
      if (_heldKeys.Contains(Keys.S) || _heldKeys.Contains(Keys.Down)) b |= InputButtons.Down;
      if (_heldKeys.Contains(Keys.A) || _heldKeys.Contains(Keys.Left)) b |= InputButtons.Left;
      if (_heldKeys.Contains(Keys.D) || _heldKeys.Contains(Keys.Right)) b |= InputButtons.Right;
      if (_heldKeys.Contains(Keys.Space) || _heldKeys.Contains(Keys.F) || _mouseFireHeld) b |= InputButtons.Fire;

      if (_mouseAimActive)
      {
         _lastAimAngle = MathF.Atan2(_mouseArenaY - playerY, _mouseArenaX - playerX);
         return (b, _lastAimAngle);
      }

      float dx = 0, dy = 0;
      if ((b & InputButtons.Left) != 0) dx -= 1;
      if ((b & InputButtons.Right) != 0) dx += 1;
      if ((b & InputButtons.Up) != 0) dy -= 1;
      if ((b & InputButtons.Down) != 0) dy += 1;
      if (dx != 0 || dy != 0) _lastAimAngle = MathF.Atan2(dy, dx);

      return (b, _lastAimAngle);
   }

   private void SendEmote()
   {
      if (_datagramHost is null) return;
      var idx = (byte)(_emoteCursor++ % GameProtocol2D.Emotes.Length);

      if (_isHost && _hostLocalPlayer is not null)
      {
         BroadcastEvent($"{_hostLocalPlayer.Name}: \"{GameProtocol2D.Emotes[idx]}\"");
      }
      else if (_serverPeer is not null && _serverPeer.State == DatagramPeerState.Connected)
      {
         var len = GameProtocol2D.WriteEmote(_packetBuf, idx);
         _serverPeer.TrySend(GameProtocol2D.ChLifecycle, _packetBuf.AsSpan(0, len));
      }
   }

   private void CycleBots()
   {
      if (_sim is null) return;
      _botCount = (_botCount + 1) % 5;
      _sim.SetBotCount(_botCount);
   }

   private void SpawnSecondClientWindow()
   {
      var exePath = Environment.ProcessPath;
      if (string.IsNullOrEmpty(exePath)) return;

      var port = _datagramHost?.LocalEndPoint?.Port ?? _targetEndPoint.Port;
      var clientName = $"Client{Random.Shared.Next(10, 99)}";
      Process.Start(new ProcessStartInfo
      {
         FileName = exePath,
         Arguments = $"--join 127.0.0.1:{port} --name {clientName}",
         UseShellExecute = false
      });
   }

   private void BroadcastEvent(string message)
   {
      AddFeed(message);
      if (_datagramHost is null) return;
      var len = GameProtocol2D.WriteEventLog(_packetBuf, message);
      _datagramHost.Broadcast(GameProtocol2D.ChLifecycle, _packetBuf.AsSpan(0, len));
   }

   private void AddFeed(string text)
   {
      _eventFeed.Enqueue(text);
      while (_eventFeed.Count > 16) _eventFeed.Dequeue();
   }

   private void SpawnFxVisuals(FxCue2D fx, byte orbValue = 1)
   {
      var col = fx.Kind == FxKind2D.PlayerTag
         ? UiTheme.StatusBad
         : UiTheme.PlayerPalette[fx.Color % UiTheme.PlayerPalette.Length];
      var maxAge = fx.Kind switch
      {
         FxKind2D.PlayerTag => 0.40f,
         FxKind2D.OrbPickup => 0.30f,
         FxKind2D.HitSpark => 0.15f,
         _ => 0.14f
      };
      _rings.Add(new VisualRing(fx.X, fx.Y, fx.Kind, col, 0f, maxAge));

      if (fx.Kind == FxKind2D.PlayerTag)
      {
         _popups.Add(new FloatingPopup(fx.X, fx.Y - 30f, "Tag +2", UiTheme.StatusBad, 0f, 0.80f));
      }
      else if (fx.Kind == FxKind2D.OrbPickup)
      {
         var label = orbValue > 1 ? $"+{orbValue}" : "+1";
         _popups.Add(new FloatingPopup(fx.X, fx.Y - 24f, label, UiTheme.TokenGold, 0f, 0.60f));
      }
   }

   private void UpdateNameMap()
   {
      for (var i = 0; i < _scoreboard.Count; i++)
      {
         _namesById[_scoreboard[i].Id] = _scoreboard[i].Name;
      }
   }
}
