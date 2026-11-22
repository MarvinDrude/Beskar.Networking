using System.Diagnostics;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Netcode;

/// <summary>
/// Manages Client-Side Prediction, Server Reconciliation, PLL-synced Server-Tick Remote Interpolation,
/// 6x Input Redundancy, optimistic shot/pickup prediction, and debug telemetry for the 2D Arena client.
/// </summary>
public sealed partial class ClientNetcodeState
{
   private readonly List<PredictedStep> _unackedInputs = new(160);
   private readonly Queue<ClientInputFrame> _recentInputHistory = new(GameProtocol2D.MaxRedundantInputs + 1);
   private readonly List<TimedSnapshot> _snapshotBuffer = new(32);

   // Client-side predicted projectiles & optimistic item pickups
   private readonly List<PredictedLaser> _predictedLasers = new(16);
   private readonly Queue<uint> _retiredPredictedShots = new(32);
   private readonly Dictionary<byte, PendingOptimisticPickup> _optimisticPickups = new(8);
   private readonly List<RecentLocalFx> _recentLocalFx = new(16);
   private readonly List<PlayerState2D> _lastRenderedRemotes = new(16);

   private int _localFireCooldownTicks;
   private double _lastKnownRttSec = 0.05;

   // Rolling debug telemetry sparklines
   private readonly float[] _rttHistory = new float[90];
   private readonly float[] _errorHistory = new float[90];
   private int _historyCursor;

   // Jitter-free Server-Tick PLL playback clock for silky-smooth remote interpolation
   private bool _clockInitialized;
   private double _serverMinusLocalOffsetSec;
   private double _interpServerTimeSec;
   private double _lastRenderNowSec;
   private double _lastSnapshotArrivalSec;
   private double _avgSnapIntervalSec = 1.0 / 30.0;
   private double _snapJitterSec;
   private double _smoothInterpDelaySec = 0.068;

   private uint _nextInputSeq = 1;
   private bool _hasSpawned;

   // Local predicted state (previous & current tick for sub-frame render alpha interpolation)
   private float _prevPredX;
   private float _prevPredY;
   private float _prevPredFacing;

   // Smooth visual correction offset (decays quickly so reconciliation corrections feel crisp)
   private float _visualOffsetX;
   private float _visualOffsetY;

   // Last rendered local position (so debug tether matches visible sprite exactly)
   private float _lastRenderedLocalX;
   private float _lastRenderedLocalY;

   // Live toggleable netcode switches
   public bool PredictionEnabled { get; set; } = true;
   public bool InterpolationEnabled { get; set; } = true;
   public bool InputRedundancyEnabled { get; set; } = true;
   public bool PredictShotsAndPickupsEnabled { get; set; } = true;

   public float AdaptiveInterpDelaySec => (float)_smoothInterpDelaySec;

   public byte MyPlayerId { get; private set; }
   public byte MyColor { get; private set; }
   public uint LatestServerTick { get; private set; }
   public uint LastAckedInputSeq { get; private set; }

   public float PredX { get; private set; }
   public float PredY { get; private set; }
   public float PredFacing { get; private set; }
   public bool LocalStunned { get; private set; }

   // Last authoritative server state for local player (rendered as the dashed "SERVER" ghost in Debug Mode)
   public bool HasServerGhost { get; private set; }
   public float ServerGhostX { get; private set; }
   public float ServerGhostY { get; private set; }

   // Debug counters & metrics
   public float LastReconcileErrorPx { get; private set; }
   public float MaxReconcileErrorPx { get; private set; }

   public float ServerGhostDistancePx => HasServerGhost
      ? MathF.Sqrt(((_lastRenderedLocalX - ServerGhostX) * (_lastRenderedLocalX - ServerGhostX))
                   + ((_lastRenderedLocalY - ServerGhostY) * (_lastRenderedLocalY - ServerGhostY)))
      : 0f;

   public int ReconciliationCorrections { get; private set; }
   public int FlashReconcileTicks { get; private set; }
   public int PendingUnackedCount => _unackedInputs.Count;
   public int BufferedSnapshotCount => _snapshotBuffer.Count;
   public int StaleSnapshotViolations { get; private set; }

   public int PredictedShotsFired { get; private set; }
   public int PredictedShotsConfirmed { get; private set; }
   public int PredictedCosmeticHits { get; private set; }
   public int ActivePredictedShotCount => _predictedLasers.Count;

   public int OptimisticPickupsTriggered { get; private set; }
   public int OptimisticPickupsConfirmed { get; private set; }
   public int OptimisticPickupsRolledBack { get; private set; }

   public IReadOnlyList<PredictedStep> UnackedTrail => _unackedInputs;
   public IReadOnlyCollection<PendingOptimisticPickup> PendingPickups => _optimisticPickups.Values;

   public float RenderLocalX => _lastRenderedLocalX;
   public float RenderLocalY => _lastRenderedLocalY;

   public void HandleWelcome(byte playerId, byte color, float spawnX, float spawnY)
   {
      MyPlayerId = playerId;
      MyColor = color;

      _prevPredX = PredX = _lastRenderedLocalX = spawnX;
      _prevPredY = PredY = _lastRenderedLocalY = spawnY;
      _prevPredFacing = PredFacing = 0f;

      ServerGhostX = spawnX;
      ServerGhostY = spawnY;
      HasServerGhost = true;
      _hasSpawned = true;

      _unackedInputs.Clear();
      _recentInputHistory.Clear();
      _predictedLasers.Clear();
      _retiredPredictedShots.Clear();
      _optimisticPickups.Clear();
      _recentLocalFx.Clear();
      _lastRenderedRemotes.Clear();

      _localFireCooldownTicks = 0;
      _visualOffsetX = 0f;
      _visualOffsetY = 0f;
   }

   public void RecordTelemetrySample(float rttMs)
   {
      if (rttMs > 1f) _lastKnownRttSec = rttMs / 1000.0;

      _rttHistory[_historyCursor] = rttMs;
      _errorHistory[_historyCursor] = LastReconcileErrorPx;
      _historyCursor = (_historyCursor + 1) % _rttHistory.Length;
   }

   /// <summary>
   /// Suppresses duplicate delayed server Ch 3 FX cues that were already played optimistically locally.
   /// </summary>
   public bool ShouldSuppressServerFx(FxCue2D fx, double nowSec)
   {
      if (!PredictShotsAndPickupsEnabled) return false;

      var ttl = Math.Max(0.45, _lastKnownRttSec + 0.25);
      for (var i = _recentLocalFx.Count - 1; i >= 0; i--)
      {
         if (nowSec - _recentLocalFx[i].TimeSec > ttl)
         {
            _recentLocalFx.RemoveAt(i);
         }
      }

      if (fx.Kind is not (FxKind2D.LaserShot or FxKind2D.OrbPickup)) return false;

      for (var i = 0; i < _recentLocalFx.Count; i++)
      {
         var r = _recentLocalFx[i];
         if (r.Kind != fx.Kind) continue;

         var dx = r.X - fx.X;
         var dy = r.Y - fx.Y;

         if ((dx * dx) + (dy * dy) <= 42f * 42f)
         {
            _recentLocalFx.RemoveAt(i);
            return true;
         }
      }

      return false;
   }

   public bool IsLaserUnconfirmedPredicted(LaserState2D laser)
   {
      if (laser.OwnerId != MyPlayerId) return false;

      for (var i = 0; i < _predictedLasers.Count; i++)
      {
         if (_predictedLasers[i].ShotSeq == laser.ShotSeq)
         {
            return !_predictedLasers[i].ConfirmedByServer;
         }
      }

      return false;
   }

   public (float[] RttSamples, float[] ErrorSamples, int Cursor) GetTelemetrySparklines() =>
      (_rttHistory, _errorHistory, _historyCursor);

   public static double NowSeconds() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;

   public static float LerpAngle(float from, float to, float t)
   {
      var diff = (to - from) % (MathF.PI * 2f);
      if (diff > MathF.PI) diff -= MathF.PI * 2f;
      else if (diff < -MathF.PI) diff += MathF.PI * 2f;

      return from + (diff * t);
   }
}
