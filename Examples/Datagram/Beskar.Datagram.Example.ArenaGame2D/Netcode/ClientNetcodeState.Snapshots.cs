using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Netcode;

public sealed partial class ClientNetcodeState
{
   /// <summary>
   /// Processes an authoritative Ch 2 World Snapshot from the Host:
   /// updates the jitter-free Server-Tick PLL clock, updates the Server Ghost, performs Server Reconciliation,
   /// and stores the snapshot keyed by exact ServerTimeSec (<c>serverTick * FixedDt</c>).
   /// </summary>
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

      UpdateSnapshotJitterAndClock(serverTimeSec, nowSec);

      LatestServerTick = serverTick;
      LastAckedInputSeq = lastAckedInputSeq;

      BufferSnapshot(serverTick, serverTimeSec, nowSec, players, orbs, lasers);
      ReconcilePredictedShotsAndPickups(lastAckedInputSeq, orbs, lasers, nowSec);
      ReconcileLocalPlayerState(lastAckedInputSeq, players);
   }

   private void UpdateSnapshotJitterAndClock(double serverTimeSec, double nowSec)
   {
      if (_lastSnapshotArrivalSec > 0)
      {
         var dt = Math.Clamp(nowSec - _lastSnapshotArrivalSec, 0.005, 0.40);
         var jitterSample = Math.Abs(dt - _avgSnapIntervalSec);

         _avgSnapIntervalSec = (_avgSnapIntervalSec * 0.92) + (dt * 0.08);
         _snapJitterSec = (_snapJitterSec * 0.88) + (jitterSample * 0.12);
      }

      _lastSnapshotArrivalSec = nowSec;

      var observedOffset = serverTimeSec - nowSec;
      if (!_clockInitialized)
      {
         _clockInitialized = true;
         _serverMinusLocalOffsetSec = observedOffset;
         _interpServerTimeSec = serverTimeSec - _smoothInterpDelaySec;
         _lastRenderNowSec = nowSec;
         return;
      }

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

   private void BufferSnapshot(
      uint serverTick,
      double serverTimeSec,
      double nowSec,
      List<PlayerState2D> players,
      List<OrbState2D> orbs,
      List<LaserState2D> lasers)
   {
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
   }

   private void ReconcileLocalPlayerState(uint lastAckedInputSeq, List<PlayerState2D> players)
   {
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

      // Discard acknowledged inputs
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

      // Replay unacknowledged local inputs on top of the authoritative server position
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

      // Preserve smooth sub-frame delta on _prevPred when error was negligible
      _prevPredX += rx - PredX;
      _prevPredY += ry - PredY;
      PredX = rx;
      PredY = ry;

      if (_unackedInputs.Count == 0)
      {
         PredFacing = rf;
      }
   }

   private void ReconcilePredictedShotsAndPickups(
      uint lastAckedInputSeq,
      List<OrbState2D> serverOrbs,
      List<LaserState2D> serverLasers,
      double nowSec)
   {
      ReconcilePredictedLasers(lastAckedInputSeq, serverLasers);
      ReconcileOptimisticPickups(serverOrbs, nowSec);
   }

   private void ReconcilePredictedLasers(uint lastAckedInputSeq, List<LaserState2D> serverLasers)
   {
      // Cosmetic predicted projectiles fly forward at strictly constant velocity (never pulled backward),
      // and are removed immediately if the server confirms hit/impact or misprediction.
      for (var i = _predictedLasers.Count - 1; i >= 0; i--)
      {
         var pl = _predictedLasers[i];
         var matchedIdx = FindMatchingServerLaser(pl, serverLasers);

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
   }

   private int FindMatchingServerLaser(PredictedLaser pl, List<LaserState2D> serverLasers)
   {
      for (var j = 0; j < serverLasers.Count; j++)
      {
         var sl = serverLasers[j];
         if (sl.OwnerId == MyPlayerId && sl.ShotSeq == pl.ShotSeq)
         {
            return j;
         }
      }

      if (pl.ConfirmedByServer) return -1;

      for (var j = 0; j < serverLasers.Count; j++)
      {
         var sl = serverLasers[j];
         if (sl.OwnerId == MyPlayerId
             && Math.Abs((long)sl.ShotSeq - pl.ShotSeq) <= 2
             && !IsShotSeqOwnedByOtherPredicted(sl.ShotSeq, pl))
         {
            pl.ShotSeq = sl.ShotSeq;
            return j;
         }
      }

      return -1;
   }

   private void ReconcileOptimisticPickups(List<OrbState2D> serverOrbs, double nowSec)
   {
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

      if (toRemove is null) return;

      for (var i = 0; i < toRemove.Count; i++)
      {
         _optimisticPickups.Remove(toRemove[i]);
      }
   }
}
