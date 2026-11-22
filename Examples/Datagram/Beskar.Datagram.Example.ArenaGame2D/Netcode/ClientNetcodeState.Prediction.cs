using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Netcode;

public sealed partial class ClientNetcodeState
{
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

      PredictLocalMovement(frame, buttons, aimAngle);
      TrySpawnPredictedShot(seq, buttons, nowSec, onLocalFx);
      StepPredictedLasers(onLocalFx);
      TryPredictOptimisticPickups(nowSec, onLocalFx);

      if (!PredictShotsAndPickupsEnabled)
      {
         _predictedLasers.Clear();
         _optimisticPickups.Clear();
      }

      DecayVisualOffsets();

      return BuildRedundantInputBundle(frame, bundleToSend);
   }

   private void PredictLocalMovement(ClientInputFrame frame, InputButtons buttons, float aimAngle)
   {
      if (!_hasSpawned || !PredictionEnabled)
      {
         PredFacing = aimAngle;
         return;
      }

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

   private void TrySpawnPredictedShot(
      uint seq,
      InputButtons buttons,
      double nowSec,
      Action<FxCue2D, byte>? onLocalFx)
   {
      // Matches server simulation order: move -> spawn -> step lasers
      if (!_hasSpawned || !PredictShotsAndPickupsEnabled || LocalStunned
          || (buttons & InputButtons.Fire) == 0 || _localFireCooldownTicks != 0)
      {
         return;
      }

      var cos = MathF.Cos(PredFacing);
      var sin = MathF.Sin(PredFacing);

      var sx = PredX + (cos * (GamePhysics2D.PlayerHalf + 6f));
      var sy = PredY + (sin * (GamePhysics2D.PlayerHalf + 6f));

      if (GamePhysics2D.PointInAnyObstacle(sx, sy)) return;

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

   private void StepPredictedLasers(Action<FxCue2D, byte>? onLocalFx)
   {
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

         // Local cosmetic impact on enemy contact: stop tracer & emit spark instead of passing through
         var hitEnemy = !hitWall && HitsRenderedRemotePlayer(pl.X, pl.Y);

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
   }

   private bool HitsRenderedRemotePlayer(float lx, float ly)
   {
      for (var r = 0; r < _lastRenderedRemotes.Count; r++)
      {
         var remote = _lastRenderedRemotes[r];
         if (remote.Stunned) continue;

         if (MathF.Abs(remote.X - lx) <= GamePhysics2D.PlayerHalf + 4f
             && MathF.Abs(remote.Y - ly) <= GamePhysics2D.PlayerHalf + 4f)
         {
            return true;
         }
      }

      return false;
   }

   private void TryPredictOptimisticPickups(double nowSec, Action<FxCue2D, byte>? onLocalFx)
   {
      if (!_hasSpawned || !PredictShotsAndPickupsEnabled || _snapshotBuffer.Count == 0) return;

      const float pickupDistSq = (GamePhysics2D.PlayerHalf + GamePhysics2D.OrbRadius + 3f)
                                 * (GamePhysics2D.PlayerHalf + GamePhysics2D.OrbRadius + 3f);

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

   private void DecayVisualOffsets()
   {
      _visualOffsetX *= 0.72f;
      _visualOffsetY *= 0.72f;

      if (MathF.Abs(_visualOffsetX) < 0.2f) _visualOffsetX = 0f;
      if (MathF.Abs(_visualOffsetY) < 0.2f) _visualOffsetY = 0f;

      if (FlashReconcileTicks > 0) FlashReconcileTicks--;
   }

   private int BuildRedundantInputBundle(ClientInputFrame frame, Span<ClientInputFrame> bundleToSend)
   {
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
}
