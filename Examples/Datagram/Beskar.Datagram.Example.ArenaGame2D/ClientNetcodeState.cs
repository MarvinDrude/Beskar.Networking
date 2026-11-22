using System.Diagnostics;

namespace Beskar.Datagram.Example.ArenaGame2D;

/// <summary>
/// Manages Client-Side Prediction, Server Reconciliation, PLL-synced Server-Tick Remote Interpolation,
/// 6x Input Redundancy, and Debug Visualization telemetry for the 2D Arena client.
/// </summary>
public sealed class ClientNetcodeState
{
   public readonly record struct PredictedStep(ClientInputFrame Input, float PredX, float PredY);
   public readonly record struct PendingOptimisticPickup(OrbState2D Orb, double PredictedAtSec);

   private sealed class PredictedLaser
   {
      public uint ShotSeq;

      public float X;
      public float Y;

      public float PrevX;
      public float PrevY;

      public float Vx;
      public float Vy;

      public byte Color { get; init; }
      public int LifeTicks;
      public bool ConfirmedByServer;
   }

   private readonly record struct RecentLocalFx(float X, float Y, FxKind2D Kind, double TimeSec);

   private sealed class TimedSnapshot
   {
      public required double ServerTimeSec { get; init; }
      public required double ReceivedTimeSec { get; init; }

      public required uint ServerTick { get; init; }

      public required List<PlayerState2D> Players { get; init; }
      public required List<OrbState2D> Orbs { get; init; }
      public required List<LaserState2D> Lasers { get; init; }
   }

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

   // Local predicted state (previous & current tick for sub-frame render alpha interpolation)
   private float _prevPredX;
   private float _prevPredY;
   private float _prevPredFacing;
   public float PredX { get; private set; }
   public float PredY { get; private set; }
   public float PredFacing { get; private set; }
   public bool LocalStunned { get; private set; }

   // Last authoritative server state for local player (rendered as the dashed "SERVER" ghost in Debug Mode!)
   public bool HasServerGhost { get; private set; }
   public float ServerGhostX { get; private set; }
   public float ServerGhostY { get; private set; }

   // Smooth visual correction offset (decays quickly so reconciliation corrections feel crisp)
   private float _visualOffsetX;
   private float _visualOffsetY;

   // Last rendered local position (so debug tether matches visible sprite exactly)
   private float _lastRenderedLocalX;
   private float _lastRenderedLocalY;

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

   /// <summary>
   /// Records one 60 Hz client input tick, predicts local movement, projectiles, and optimistic orb pickups
   /// (when enabled), and fills <paramref name="bundleToSend"/> with 1 or up to 6 redundant recent input frames.
   /// </summary>
   public int RecordAndPredictInput(
      InputButtons buttons,
      float aimAngle,
      Span<ClientInputFrame> bundleToSend,
      double nowSec = 0,
      Action<FxCue2D, byte>? onLocalFx = null)
   {
      if (nowSec <= 0) nowSec = NowSeconds();
      var seq = _nextInputSeq++;
      var viewServerTick = InterpolationEnabled && _clockInitialized
         ? (float)(_interpServerTimeSec / GamePhysics2D.FixedDt)
         : (float)LatestServerTick;
      var frame = new ClientInputFrame(seq, buttons, aimAngle, viewServerTick);

      _prevPredX = PredX;
      _prevPredY = PredY;
      _prevPredFacing = PredFacing;

      if (_localFireCooldownTicks > 0) _localFireCooldownTicks--;

      if (_hasSpawned && PredictionEnabled)
      {
         var x = PredX;
         var y = PredY;
         var f = PredFacing;
         GamePhysics2D.StepPlayerMovement(ref x, ref y, ref f, buttons, aimAngle, LocalStunned);
         PredX = x;
         PredY = y;
         PredFacing = f;

         _unackedInputs.Add(new PredictedStep(frame, PredX, PredY));
         if (_unackedInputs.Count > 140)
         {
            _unackedInputs.RemoveAt(0);
         }
      }
      else
      {
         PredFacing = aimAngle;
      }

      // Instant client-side projectile visual prediction (matches server order: move -> spawn -> step lasers!)
      if (_hasSpawned && PredictShotsAndPickupsEnabled && !LocalStunned
          && (buttons & InputButtons.Fire) != 0 && _localFireCooldownTicks == 0)
      {
         var cos = MathF.Cos(PredFacing);
         var sin = MathF.Sin(PredFacing);
         var sx = PredX + (cos * (GamePhysics2D.PlayerHalf + 6f));
         var sy = PredY + (sin * (GamePhysics2D.PlayerHalf + 6f));

         if (!GamePhysics2D.PointInAnyObstacle(sx, sy))
         {
            _localFireCooldownTicks = 15;
            _predictedLasers.Add(new PredictedLaser
            {
               ShotSeq = seq,
               X = sx,
               Y = sy,
               PrevX = sx,
               PrevY = sy,
               Vx = cos * GamePhysics2D.LaserSpeed,
               Vy = sin * GamePhysics2D.LaserSpeed,
               Color = MyColor,
               LifeTicks = 65
            });
            PredictedShotsFired++;
            RememberLocalFx(sx, sy, FxKind2D.LaserShot, nowSec);
            onLocalFx?.Invoke(new FxCue2D(sx, sy, FxKind2D.LaserShot, MyColor), 0);
         }
      }

      // Step all client-predicted projectiles at 60Hz (identical order to ArenaSimulation2D.Step!)
      for (var i = _predictedLasers.Count - 1; i >= 0; i--)
      {
         var pl = _predictedLasers[i];
         pl.PrevX = pl.X;
         pl.PrevY = pl.Y;
         pl.LifeTicks--;
         pl.X += pl.Vx * GamePhysics2D.FixedDt;
         pl.Y += pl.Vy * GamePhysics2D.FixedDt;

         var hitWall = pl.X < 6f || pl.X > GamePhysics2D.ArenaWidth - 6f
                       || pl.Y < 6f || pl.Y > GamePhysics2D.ArenaHeight - 6f
                       || GamePhysics2D.PointInAnyObstacle(pl.X, pl.Y);

         // Local cosmetic impact on enemy contact: stop tracer & emit subtle spark instead of passing through
         var hitEnemy = false;
         if (!hitWall)
         {
            for (var r = 0; r < _lastRenderedRemotes.Count; r++)
            {
               var remote = _lastRenderedRemotes[r];
               if (remote.Stunned) continue;
               if (MathF.Abs(remote.X - pl.X) <= GamePhysics2D.PlayerHalf + 4f
                   && MathF.Abs(remote.Y - pl.Y) <= GamePhysics2D.PlayerHalf + 4f)
               {
                  hitEnemy = true;
                  break;
               }
            }
         }

         if (hitEnemy)
         {
            PredictedCosmeticHits++;
            RetirePredictedShot(pl.ShotSeq);
            _predictedLasers.RemoveAt(i);
            onLocalFx?.Invoke(new FxCue2D(pl.X, pl.Y, FxKind2D.HitSpark, MyColor), 0);
         }
         else if (hitWall || pl.LifeTicks <= 0)
         {
            RetirePredictedShot(pl.ShotSeq);
            _predictedLasers.RemoveAt(i);
         }
      }

      // Instant optimistic visual orb pickup prediction (hides orb immediately + plays local popup; rolls back if lost!)
      if (_hasSpawned && PredictShotsAndPickupsEnabled && _snapshotBuffer.Count > 0)
      {
         const float pickupDistSq = (GamePhysics2D.PlayerHalf + GamePhysics2D.OrbRadius + 3f) * (GamePhysics2D.PlayerHalf + GamePhysics2D.OrbRadius + 3f);
         var latestOrbs = _snapshotBuffer[^1].Orbs;
         for (var i = 0; i < latestOrbs.Count; i++)
         {
            var orb = latestOrbs[i];
            if (_optimisticPickups.ContainsKey(orb.Id)) continue;

            var dx = PredX - orb.X;
            var dy = PredY - orb.Y;
            if ((dx * dx) + (dy * dy) <= pickupDistSq)
            {
               _optimisticPickups[orb.Id] = new PendingOptimisticPickup(orb, nowSec);
               OptimisticPickupsTriggered++;
               RememberLocalFx(orb.X, orb.Y, FxKind2D.OrbPickup, nowSec);
               onLocalFx?.Invoke(new FxCue2D(orb.X, orb.Y, FxKind2D.OrbPickup, MyColor), orb.Value);
            }
         }
      }

      if (!PredictShotsAndPickupsEnabled)
      {
         _predictedLasers.Clear();
         _optimisticPickups.Clear();
      }

      _visualOffsetX *= 0.72f;
      _visualOffsetY *= 0.72f;

      if (MathF.Abs(_visualOffsetX) < 0.2f) _visualOffsetX = 0f;
      if (MathF.Abs(_visualOffsetY) < 0.2f) _visualOffsetY = 0f;

      if (FlashReconcileTicks > 0) FlashReconcileTicks--;

      // Maintain 6-frame sliding redundancy window for Ch 2 UnreliableSequenced input packets
      _recentInputHistory.Enqueue(frame);
      while (_recentInputHistory.Count > GameProtocol2D.MaxRedundantInputs)
      {
         _recentInputHistory.Dequeue();
      }

      if (!InputRedundancyEnabled)
      {
         bundleToSend[0] = frame;
         return 1;
      }

      var count = 0;
      foreach (var item in _recentInputHistory)
      {
         if (count < bundleToSend.Length)
         {
            bundleToSend[count++] = item;
         }
      }

      return count;
   }

   public void ApplyAuthoritativeSnapshot(
      uint serverTick,
      uint lastAckedInputSeq,
      List<PlayerState2D> players,
      List<OrbState2D> orbs,
      List<LaserState2D> lasers,
      double nowSec)
   {
      if (serverTick < LatestServerTick)
      {
         StaleSnapshotViolations++;
         return;
      }

      var serverTimeSec = serverTick * (double)GamePhysics2D.FixedDt;

      if (_lastSnapshotArrivalSec > 0)
      {
         var dt = Math.Clamp(nowSec - _lastSnapshotArrivalSec, 0.005, 0.40);
         var jitterSample = Math.Abs(dt - _avgSnapIntervalSec);
         _avgSnapIntervalSec = (_avgSnapIntervalSec * 0.92) + (dt * 0.08);
         _snapJitterSec = (_snapJitterSec * 0.88) + (jitterSample * 0.12);
      }
      _lastSnapshotArrivalSec = nowSec;

      // Update smoothed server-to-local clock offset (filters out OS timer / network arrival jitter!)
      var observedOffset = serverTimeSec - nowSec;
      if (!_clockInitialized)
      {
         _clockInitialized = true;
         _serverMinusLocalOffsetSec = observedOffset;
         _interpServerTimeSec = serverTimeSec - _smoothInterpDelaySec;
         _lastRenderNowSec = nowSec;
      }
      else
      {
         var offsetDelta = observedOffset - _serverMinusLocalOffsetSec;
         if (Math.Abs(offsetDelta) > 0.35)
         {
            _serverMinusLocalOffsetSec = observedOffset;
         }
         else
         {
            // Asymmetric bias: track lower-latency arrivals slightly faster to avoid permanent latency inflation
            var gain = offsetDelta > 0 ? 0.06 : 0.025;
            _serverMinusLocalOffsetSec += offsetDelta * gain;
         }
      }

      LatestServerTick = serverTick;
      LastAckedInputSeq = lastAckedInputSeq;

      // Avoid duplicate entries if same ServerTick ever arrives twice
      if (_snapshotBuffer.Count == 0 || _snapshotBuffer[^1].ServerTick < serverTick)
      {
         _snapshotBuffer.Add(new TimedSnapshot
         {
            ServerTimeSec = serverTimeSec,
            ReceivedTimeSec = nowSec,
            ServerTick = serverTick,
            Players = [.. players],
            Orbs = [.. orbs],
            Lasers = [.. lasers]
         });
      }

      while (_snapshotBuffer.Count > 32)
      {
         _snapshotBuffer.RemoveAt(0);
      }

      ReconcilePredictedShotsAndPickups(lastAckedInputSeq, serverTimeSec, orbs, lasers, nowSec);

      var foundLocal = false;
      PlayerState2D authLocal = default;
      for (var i = 0; i < players.Count; i++)
      {
         if (players[i].Id == MyPlayerId)
         {
            authLocal = players[i];
            foundLocal = true;
            break;
         }
      }

      if (!foundLocal) return;

      HasServerGhost = true;
      ServerGhostX = authLocal.X;
      ServerGhostY = authLocal.Y;
      LocalStunned = authLocal.Stunned;

      if (!PredictionEnabled)
      {
         _unackedInputs.Clear();
         _prevPredX = PredX = _lastRenderedLocalX = authLocal.X;
         _prevPredY = PredY = _lastRenderedLocalY = authLocal.Y;
         _prevPredFacing = PredFacing = authLocal.FacingAngle;
         _visualOffsetX = 0f;
         _visualOffsetY = 0f;
         LastReconcileErrorPx = 0f;
         return;
      }

      // --- SERVER RECONCILIATION ---
      var removeCount = 0;
      while (removeCount < _unackedInputs.Count && _unackedInputs[removeCount].Input.InputSeq <= lastAckedInputSeq)
      {
         removeCount++;
      }

      if (removeCount > 0)
      {
         _unackedInputs.RemoveRange(0, removeCount);
      }

      var prevRenderX = PredX + _visualOffsetX;
      var prevRenderY = PredY + _visualOffsetY;

      var rx = authLocal.X;
      var ry = authLocal.Y;
      var rf = authLocal.FacingAngle;

      for (var i = 0; i < _unackedInputs.Count; i++)
      {
         var step = _unackedInputs[i];
         GamePhysics2D.StepPlayerMovement(
            ref rx,
            ref ry,
            ref rf,
            step.Input.Buttons,
            step.Input.AimAngle,
            authLocal.Stunned);

         _unackedInputs[i] = step with { PredX = rx, PredY = ry };
      }

      var errX = rx - PredX;
      var errY = ry - PredY;
      var errDist = MathF.Sqrt((errX * errX) + (errY * errY));
      LastReconcileErrorPx = errDist;
      if (errDist > MaxReconcileErrorPx) MaxReconcileErrorPx = errDist;

      if (errDist > 1.2f)
      {
         ReconciliationCorrections++;
         FlashReconcileTicks = 22;

         if (errDist < 95f)
         {
            _visualOffsetX = prevRenderX - rx;
            _visualOffsetY = prevRenderY - ry;
         }
         else
         {
            _visualOffsetX = 0f;
            _visualOffsetY = 0f;
         }
      }

      // Preserve smooth sub-frame delta on _prevPred if error was negligible
      _prevPredX += rx - PredX;
      _prevPredY += ry - PredY;
      PredX = rx;
      PredY = ry;
      if (_unackedInputs.Count == 0)
      {
         PredFacing = rf;
      }
   }

   /// <summary>
   /// Produces silky-smooth render lists for the current frame:
   /// - Local player uses sub-frame <paramref name="subFrameAlpha"/> interpolation between previous and current predicted tick.
   /// - Remote players & lasers are evaluated continuously along the PLL-synced exact Server-Tick timeline,
   ///   completely immune to OS timer quantum wobble and UDP arrival jitter!
   /// </summary>
   public void BuildRenderWorld(
      double nowSec,
      float subFrameAlpha,
      List<PlayerState2D> outPlayers,
      List<PlayerState2D> outRawRemoteGhosts,
      List<OrbState2D> outOrbs,
      List<LaserState2D> outLasers)
   {
      outPlayers.Clear();
      outRawRemoteGhosts.Clear();
      outOrbs.Clear();
      outLasers.Clear();

      var alpha = Math.Clamp(subFrameAlpha, 0f, 1f);
      if (_hasSpawned)
      {
         var lx = PredictionEnabled
            ? (_prevPredX + ((PredX - _prevPredX) * alpha)) + _visualOffsetX
            : PredX;
         var ly = PredictionEnabled
            ? (_prevPredY + ((PredY - _prevPredY) * alpha)) + _visualOffsetY
            : PredY;
         var lf = PredictionEnabled ? LerpAngle(_prevPredFacing, PredFacing, alpha) : PredFacing;
         _lastRenderedLocalX = lx;
         _lastRenderedLocalY = ly;
         outPlayers.Add(new PlayerState2D(MyPlayerId, lx, ly, lf, MyColor, LocalStunned, IsBot: false));
      }

      if (_snapshotBuffer.Count == 0) return;

      // Advance the jitter-free PLL Server-Tick playback clock smoothly
      var frameDt = _lastRenderNowSec > 0 ? Math.Clamp(nowSec - _lastRenderNowSec, 0.0, 0.05) : 0.0;
      _lastRenderNowSec = nowSec;

      var targetDelay = Math.Clamp(Math.Max(0.068, (_avgSnapIntervalSec * 2.0) + (_snapJitterSec * 2.6)), 0.068, 0.240);
      var maxDelayStep = 0.025 * frameDt; // Slew interpolation delay gently (<= 25ms/sec) so it never causes speed jerks
      _smoothInterpDelaySec += Math.Clamp(targetDelay - _smoothInterpDelaySec, -maxDelayStep, maxDelayStep);

      if (_clockInitialized && frameDt > 0)
      {
         var estServerNow = nowSec + _serverMinusLocalOffsetSec;
         var targetPlayback = estServerNow - _smoothInterpDelaySec;
         var drift = targetPlayback - _interpServerTimeSec;

         if (Math.Abs(drift) > 0.30)
         {
            _interpServerTimeSec = targetPlayback;
         }
         else
         {
            // Imperceptible +/-6% proportional PLL speed nudge keeps playback locked cleanly inside the buffer
            var rate = Math.Clamp(1.0 + (drift * 1.6), 0.94, 1.06);
            _interpServerTimeSec += frameDt * rate;
         }
      }

      // Prune snapshots far behind playback window while keeping at least 2
      while (_snapshotBuffer.Count > 3 && _snapshotBuffer[1].ServerTimeSec < _interpServerTimeSec - 0.15)
      {
         _snapshotBuffer.RemoveAt(0);
      }

      var latest = _snapshotBuffer[^1];
      for (var i = 0; i < latest.Orbs.Count; i++)
      {
         var orb = latest.Orbs[i];
         if (PredictShotsAndPickupsEnabled && _optimisticPickups.ContainsKey(orb.Id))
         {
            continue; // Optimistically hidden locally while awaiting server confirmation
         }
         outOrbs.Add(orb);
      }

      foreach (var p in latest.Players)
      {
         if (p.Id != MyPlayerId) outRawRemoteGhosts.Add(p);
      }

      // Render locally predicted projectiles (sub-frame alpha interpolated, 0ms latency, pure constant velocity!)
      if (PredictShotsAndPickupsEnabled)
      {
         for (var i = 0; i < _predictedLasers.Count; i++)
         {
            var pl = _predictedLasers[i];
            var lx = pl.PrevX + ((pl.X - pl.PrevX) * alpha);
            var ly = pl.PrevY + ((pl.Y - pl.PrevY) * alpha);
            outLasers.Add(new LaserState2D(lx, ly, pl.Vx, pl.Vy, pl.Color, MyPlayerId, pl.ShotSeq));
         }
      }

      if (!InterpolationEnabled || _snapshotBuffer.Count < 2)
      {
         foreach (var l in latest.Lasers)
         {
            if (PredictShotsAndPickupsEnabled && l.OwnerId == MyPlayerId && IsShotPredictedOrRetired(l.ShotSeq)) continue;
            outLasers.Add(l);
         }

         _lastRenderedRemotes.Clear();
         foreach (var p in latest.Players)
         {
            if (p.Id == MyPlayerId) continue;
            outPlayers.Add(p);
            _lastRenderedRemotes.Add(p);
         }
         return;
      }

      var renderServerTime = _interpServerTimeSec;
      TimedSnapshot a = _snapshotBuffer[0];
      TimedSnapshot b = _snapshotBuffer[1];
      var extrapolating = false;

      if (renderServerTime >= latest.ServerTimeSec)
      {
         a = _snapshotBuffer[^2];
         b = latest;
         extrapolating = true;
      }
      else if (renderServerTime <= _snapshotBuffer[0].ServerTimeSec)
      {
         a = _snapshotBuffer[0];
         b = _snapshotBuffer[1];
      }
      else
      {
         for (var i = 0; i < _snapshotBuffer.Count - 1; i++)
         {
            if (_snapshotBuffer[i].ServerTimeSec <= renderServerTime && _snapshotBuffer[i + 1].ServerTimeSec >= renderServerTime)
            {
               a = _snapshotBuffer[i];
               b = _snapshotBuffer[i + 1];
               break;
            }
         }
      }

      var span = b.ServerTimeSec - a.ServerTimeSec;
      var maxExtraT = span > 0.001 ? 1.0 + Math.Min(0.09 / span, 1.8) : 1.0;
      var t = span > 0.0001
         ? (float)Math.Clamp((renderServerTime - a.ServerTimeSec) / span, 0.0, extrapolating ? maxExtraT : 1.0)
         : 1f;

      // Interpolate remote players' lasers smoothly along the exact same PLL Server-Tick timeline!
      foreach (var lb in b.Lasers)
      {
         if (PredictShotsAndPickupsEnabled && lb.OwnerId == MyPlayerId && IsShotPredictedOrRetired(lb.ShotSeq))
         {
            continue;
         }

         var hasLa = false;
         LaserState2D la = default;
         for (var i = 0; i < a.Lasers.Count; i++)
         {
            if (a.Lasers[i].OwnerId == lb.OwnerId && a.Lasers[i].ShotSeq == lb.ShotSeq)
            {
               la = a.Lasers[i];
               hasLa = true;
               break;
            }
         }

         if (hasLa)
         {
            var lx = la.X + ((lb.X - la.X) * t);
            var ly = la.Y + ((lb.Y - la.Y) * t);
            outLasers.Add(lb with { X = lx, Y = ly });
         }
         else
         {
            // Newly spawned laser in snapshot b: back-project cleanly by (t - 1) * span so it never jumps
            var dtFromB = (t - 1f) * (float)span;
            outLasers.Add(lb with { X = lb.X + (lb.Vx * dtFromB), Y = lb.Y + (lb.Vy * dtFromB) });
         }
      }

      _lastRenderedRemotes.Clear();
      foreach (var pb in b.Players)
      {
         if (pb.Id == MyPlayerId) continue;

         // Uncouple hit/stun state from position interpolation: apply latest Stunned flag immediately
         // while keeping movement smoothly interpolated on the PLL timeline (shaves ~70-130ms off kill feel!)
         var latestStunned = pb.Stunned;
         for (var i = 0; i < latest.Players.Count; i++)
         {
            if (latest.Players[i].Id == pb.Id)
            {
               latestStunned = latest.Players[i].Stunned;
               break;
            }
         }

         var hasA = false;
         PlayerState2D pa = default;
         for (var i = 0; i < a.Players.Count; i++)
         {
            if (a.Players[i].Id == pb.Id)
            {
               pa = a.Players[i];
               hasA = true;
               break;
            }
         }

         PlayerState2D rendered;
         if (hasA)
         {
            var ix = Math.Clamp(pa.X + ((pb.X - pa.X) * t), GamePhysics2D.PlayerHalf, GamePhysics2D.ArenaWidth - GamePhysics2D.PlayerHalf);
            var iy = Math.Clamp(pa.Y + ((pb.Y - pa.Y) * t), GamePhysics2D.PlayerHalf, GamePhysics2D.ArenaHeight - GamePhysics2D.PlayerHalf);
            var ia = LerpAngle(pa.FacingAngle, pb.FacingAngle, Math.Clamp(t, 0f, 1f));
            rendered = pb with { X = ix, Y = iy, FacingAngle = ia, Stunned = latestStunned };
         }
         else
         {
            rendered = pb with { Stunned = latestStunned };
         }

         outPlayers.Add(rendered);
         _lastRenderedRemotes.Add(rendered);
      }
   }

   public void RecordTelemetrySample(float rttMs)
   {
      if (rttMs > 1f) _lastKnownRttSec = rttMs / 1000.0;
      _rttHistory[_historyCursor] = rttMs;
      _errorHistory[_historyCursor] = LastReconcileErrorPx;
      _historyCursor = (_historyCursor + 1) % _rttHistory.Length;
   }

   /// <summary>
   /// Suppresses duplicate delayed server Ch 3 FX cues that we already played optimistically locally.
   /// </summary>
   public bool ShouldSuppressServerFx(FxCue2D fx, double nowSec)
   {
      if (!PredictShotsAndPickupsEnabled) return false;

      // Prune expired local FX records
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

   private void ReconcilePredictedShotsAndPickups(
      uint lastAckedInputSeq,
      double serverTimeSec,
      List<OrbState2D> serverOrbs,
      List<LaserState2D> serverLasers,
      double nowSec)
   {
      // reconcile predicted projectiles against authoritative server lasers.
      // Cosmetic predicted projectiles fly forward at strictly constant velocity (never pulled backward!),
      // and are removed immediately if the server confirms hit/impact or misprediction.
      for (var i = _predictedLasers.Count - 1; i >= 0; i--)
      {
         var pl = _predictedLasers[i];
         var matchedIdx = -1;

         for (var j = 0; j < serverLasers.Count; j++)
         {
            var sl = serverLasers[j];
            if (sl.OwnerId == MyPlayerId && sl.ShotSeq == pl.ShotSeq)
            {
               matchedIdx = j;
               break;
            }
         }

         if (matchedIdx < 0 && !pl.ConfirmedByServer)
         {
            for (var j = 0; j < serverLasers.Count; j++)
            {
               var sl = serverLasers[j];
               if (sl.OwnerId == MyPlayerId && Math.Abs((long)sl.ShotSeq - pl.ShotSeq) <= 2 && !IsShotSeqOwnedByOtherPredicted(sl.ShotSeq, pl))
               {
                  pl.ShotSeq = sl.ShotSeq;
                  matchedIdx = j;
                  break;
               }
            }
         }

         if (matchedIdx >= 0)
         {
            if (!pl.ConfirmedByServer)
            {
               pl.ConfirmedByServer = true;
               PredictedShotsConfirmed++;
            }
         }
         else if (pl.ShotSeq + 2 <= lastAckedInputSeq)
         {
            RetirePredictedShot(pl.ShotSeq);
            _predictedLasers.RemoveAt(i);
         }
      }

      if (_optimisticPickups.Count == 0) return;

      var rollbackTimeout = Math.Max(0.32, _lastKnownRttSec + 0.18);
      List<byte>? toRemove = null;

      foreach (var (orbId, pending) in _optimisticPickups)
      {
         var stillOnServer = false;
         for (var i = 0; i < serverOrbs.Count; i++)
         {
            if (serverOrbs[i].Id == orbId)
            {
               stillOnServer = true;
               break;
            }
         }

         if (!stillOnServer)
         {
            OptimisticPickupsConfirmed++;
            (toRemove ??= []).Add(orbId);
         }
         else if (nowSec - pending.PredictedAtSec > rollbackTimeout)
         {
            OptimisticPickupsRolledBack++;
            (toRemove ??= []).Add(orbId);
         }
      }

      if (toRemove is not null)
      {
         for (var i = 0; i < toRemove.Count; i++)
         {
            _optimisticPickups.Remove(toRemove[i]);
         }
      }
   }

   private bool IsShotSeqOwnedByOtherPredicted(uint shotSeq, PredictedLaser exclude)
   {
      for (var i = 0; i < _predictedLasers.Count; i++)
      {
         var other = _predictedLasers[i];
         if (!ReferenceEquals(other, exclude) && other.ShotSeq == shotSeq) return true;
      }
      return false;
   }

   private void RetirePredictedShot(uint shotSeq)
   {
      _retiredPredictedShots.Enqueue(shotSeq);
      while (_retiredPredictedShots.Count > 32)
      {
         _retiredPredictedShots.Dequeue();
      }
   }

   private bool IsShotPredictedOrRetired(uint shotSeq)
   {
      if (shotSeq == 0) return false;

      for (var i = 0; i < _predictedLasers.Count; i++)
      {
         if (_predictedLasers[i].ShotSeq == shotSeq) return true;
      }

      foreach (var r in _retiredPredictedShots)
      {
         if (r == shotSeq) return true;
      }

      return false;
   }

   private void RememberLocalFx(float x, float y, FxKind2D kind, double nowSec)
   {
      _recentLocalFx.Add(new RecentLocalFx(x, y, kind, nowSec));
      if (_recentLocalFx.Count > 24)
      {
         _recentLocalFx.RemoveAt(0);
      }
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
