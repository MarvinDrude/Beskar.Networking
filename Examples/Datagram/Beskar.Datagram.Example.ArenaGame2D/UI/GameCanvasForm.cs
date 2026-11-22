using System.Diagnostics;
using System.Drawing.Imaging;
using System.Net;
using Beskar.Datagram.Example.ArenaGame2D.Netcode;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;
using Beskar.Datagram.Example.ArenaGame2D.Simulation;
using Beskar.Datagram.Example.ArenaGame2D.Transport;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Datagram.Example.ArenaGame2D.UI;

/// <summary>
/// DPI-independent, custom-drawn 60 FPS 2D Arena window.
/// Runs either as an authoritative Listen Server (Host) or as a UDP Client with
/// live fault sliders, prediction/reconciliation/interpolation toggles, and visual debug overlay.
/// </summary>
public sealed partial class GameCanvasForm : Form
{
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
   private PlayerEntity? _hostLocalPlayer;

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
   private readonly UiFonts _fonts = new();

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

   // Automated test override input (null during normal manual play)
   private Func<(InputButtons Buttons, float AimAngle)>? _scriptedInputProvider;

   // Fixed-timestep loop state (10ms high-res timer + sub-frame alpha interpolation)
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

      var (targetW, targetH) = ResolveInitialClientSize(showOnScreen);

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

   // --- Public control hooks for automated visual & functional self-testing ---

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

      AdvanceVisualEffects((float)dt);

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

      UpdateMotionTrails();
      Invalidate();
   }

   private void AdvanceVisualEffects(float dt)
   {
      for (var i = _rings.Count - 1; i >= 0; i--)
      {
         var r = _rings[i];
         var next = r.Age + dt;
         if (next >= r.MaxAge) _rings.RemoveAt(i);
         else _rings[i] = r with { Age = next };
      }

      for (var i = _popups.Count - 1; i >= 0; i--)
      {
         var p = _popups[i];
         var next = p.Age + dt;
         if (next >= p.MaxAge) _popups.RemoveAt(i);
         else _popups[i] = p with { Age = next, Y = p.Y - (34f * dt) };
      }
   }

   private void UpdateMotionTrails()
   {
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

      var dx = 0f;
      var dy = 0f;

      if ((b & InputButtons.Left) != 0) dx -= 1f;
      if ((b & InputButtons.Right) != 0) dx += 1f;
      if ((b & InputButtons.Up) != 0) dy -= 1f;
      if ((b & InputButtons.Down) != 0) dy += 1f;

      if (dx != 0f || dy != 0f)
      {
         _lastAimAngle = MathF.Atan2(dy, dx);
      }

      return (b, _lastAimAngle);
   }

   private static (int Width, int Height) ResolveInitialClientSize(bool showOnScreen)
   {
      if (!showOnScreen) return (DefaultWinW, DefaultWinH);

      var work = Screen.FromPoint(Cursor.Position).WorkingArea;
      if (work.Width <= 0 || work.Height <= 0) return (DefaultWinW, DefaultWinH);

      var width = Math.Clamp(DefaultWinW, 1200, Math.Max(1200, work.Width - 40));
      var height = Math.Clamp(DefaultWinH, 720, Math.Max(720, work.Height - 70));

      return (width, height);
   }
}
