using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Netcode;

public sealed partial class ClientNetcodeState
{
   /// <summary>
   /// Produces silky-smooth render lists for the current frame:
   /// - Local player uses sub-frame <paramref name="subFrameAlpha"/> interpolation between previous and current predicted tick.
   /// - Remote players & lasers are evaluated continuously along the PLL-synced exact Server-Tick timeline,
   ///   completely immune to OS timer quantum wobble and UDP arrival jitter.
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

      AppendLocalPlayer(alpha, outPlayers);

      if (_snapshotBuffer.Count == 0) return;

      AdvancePllPlaybackClock(nowSec);

      var latest = _snapshotBuffer[^1];

      AppendVisibleOrbs(latest, outOrbs);
      AppendRawRemoteGhosts(latest, outRawRemoteGhosts);
      AppendPredictedLasers(alpha, outLasers);

      if (!InterpolationEnabled || _snapshotBuffer.Count < 2)
      {
         AppendUninterpolatedRemoteState(latest, outPlayers, outLasers);
         return;
      }

      var (a, b, t, span) = SelectInterpolationPair(latest);

      AppendInterpolatedLasers(a, b, t, span, outLasers);
      AppendInterpolatedRemotePlayers(a, b, latest, t, outPlayers);
   }

   private void AppendLocalPlayer(float alpha, List<PlayerState2D> outPlayers)
   {
      if (!_hasSpawned) return;

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

   private void AdvancePllPlaybackClock(double nowSec)
   {
      var frameDt = _lastRenderNowSec > 0 ? Math.Clamp(nowSec - _lastRenderNowSec, 0.0, 0.05) : 0.0;
      _lastRenderNowSec = nowSec;

      var targetDelay = Math.Clamp(Math.Max(0.068, (_avgSnapIntervalSec * 2.0) + (_snapJitterSec * 2.6)), 0.068, 0.240);
      var maxDelayStep = 0.025 * frameDt; // Slew interpolation delay gently (<= 25ms/sec)
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
   }

   private void AppendVisibleOrbs(TimedSnapshot latest, List<OrbState2D> outOrbs)
   {
      for (var i = 0; i < latest.Orbs.Count; i++)
      {
         var orb = latest.Orbs[i];
         if (PredictShotsAndPickupsEnabled && _optimisticPickups.ContainsKey(orb.Id))
         {
            continue; // Optimistically hidden locally while awaiting server confirmation
         }

         outOrbs.Add(orb);
      }
   }

   private void AppendRawRemoteGhosts(TimedSnapshot latest, List<PlayerState2D> outRawRemoteGhosts)
   {
      foreach (var p in latest.Players)
      {
         if (p.Id != MyPlayerId) outRawRemoteGhosts.Add(p);
      }
   }

   private void AppendPredictedLasers(float alpha, List<LaserState2D> outLasers)
   {
      if (!PredictShotsAndPickupsEnabled) return;

      for (var i = 0; i < _predictedLasers.Count; i++)
      {
         var pl = _predictedLasers[i];
         var lx = pl.PrevX + ((pl.X - pl.PrevX) * alpha);
         var ly = pl.PrevY + ((pl.Y - pl.PrevY) * alpha);

         outLasers.Add(new LaserState2D(lx, ly, pl.Vx, pl.Vy, pl.Color, MyPlayerId, pl.ShotSeq));
      }
   }

   private void AppendUninterpolatedRemoteState(
      TimedSnapshot latest,
      List<PlayerState2D> outPlayers,
      List<LaserState2D> outLasers)
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
   }

   private (TimedSnapshot A, TimedSnapshot B, float T, double Span) SelectInterpolationPair(TimedSnapshot latest)
   {
      var renderServerTime = _interpServerTimeSec;
      var a = _snapshotBuffer[0];
      var b = _snapshotBuffer[1];
      var extrapolating = false;

      if (renderServerTime >= latest.ServerTimeSec)
      {
         a = _snapshotBuffer[^2];
         b = latest;
         extrapolating = true;
      }
      else if (renderServerTime > _snapshotBuffer[0].ServerTimeSec)
      {
         for (var i = 0; i < _snapshotBuffer.Count - 1; i++)
         {
            if (_snapshotBuffer[i].ServerTimeSec <= renderServerTime
                && _snapshotBuffer[i + 1].ServerTimeSec >= renderServerTime)
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

      return (a, b, t, span);
   }

   private void AppendInterpolatedLasers(
      TimedSnapshot a,
      TimedSnapshot b,
      float t,
      double span,
      List<LaserState2D> outLasers)
   {
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
   }

   private void AppendInterpolatedRemotePlayers(
      TimedSnapshot a,
      TimedSnapshot b,
      TimedSnapshot latest,
      float t,
      List<PlayerState2D> outPlayers)
   {
      _lastRenderedRemotes.Clear();

      foreach (var pb in b.Players)
      {
         if (pb.Id == MyPlayerId) continue;

         // Uncouple hit/stun state from position interpolation: apply latest Stunned flag immediately
         // while keeping movement smoothly interpolated on the PLL timeline (shaves ~70-130ms off kill feel)
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
}
