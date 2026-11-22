using Beskar.Datagram.Example.ArenaGame2D.Netcode;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Simulation;

/// <summary>
/// Authoritative 60 Hz 2D arena simulation executed on the Host.
/// Uses the exact same <see cref="GamePhysics2D.StepPlayerMovement"/> as client-side prediction.
/// </summary>
public sealed partial class ArenaSimulation2D
{
   public const float MaxLagCompRewindTicks = 30f; // 500ms anti-abuse rewind cap

   private readonly List<PlayerEntity> _players = [];
   private readonly List<OrbState2D> _orbs = [];
   private readonly List<LaserEntity> _lasers = [];
   private readonly PoseHistoryBuffer _poseHistory = new();
   private readonly List<RewindHitMarker> _rewindMarkers = new(12);
   private readonly Random _rng = new();

   private byte _nextPlayerId = 1;
   private byte _nextOrbId = 1;
   private byte _nextColor;
   private uint _tick;

   public uint Tick => _tick;

   public IReadOnlyList<PlayerEntity> Players => _players;
   public IReadOnlyList<RewindHitMarker> ActiveRewindMarkers => _rewindMarkers;

   public bool LagCompensationEnabled { get; set; } = true;
   public int LagCompensatedHits { get; private set; }
   public float LastRewindMs { get; private set; }

   public bool ScoreboardDirty { get; set; } = true;
   public List<string> PendingEvents { get; } = [];
   public List<FxCue2D> PendingFx { get; } = [];

   public ArenaSimulation2D()
   {
      EnsureOrbs();
   }

   public PlayerEntity AddPlayer(string name, uint? peerId, bool isBot = false)
   {
      var id = _nextPlayerId++;
      if (_nextPlayerId == 0) _nextPlayerId = 1;

      var (sx, sy) = FindSafeSpawnPoint();

      var entity = new PlayerEntity
      {
         Id = id,
         PeerId = peerId,
         Name = string.IsNullOrWhiteSpace(name) ? $"Pilot{id}" : name.Trim(),
         Color = (byte)(_nextColor++ % 6),
         IsBot = isBot,
         X = sx,
         Y = sy,
         PrevX = sx,
         PrevY = sy,
         FacingAngle = 0f,
         PrevFacingAngle = 0f
      };

      _players.Add(entity);
      ScoreboardDirty = true;
      PendingEvents.Add($"{entity.Name} joined the arena!");

      return entity;
   }

   public void RemovePlayerByPeer(uint peerId, string reason)
   {
      var idx = _players.FindIndex(p => p.PeerId == peerId);
      if (idx < 0) return;

      var removed = _players[idx];
      _players.RemoveAt(idx);
      ScoreboardDirty = true;
      PendingEvents.Add($"{removed.Name} left ({reason}).");
   }

   public PlayerEntity? FindByPeer(uint peerId) => _players.FirstOrDefault(p => p.PeerId == peerId);
   public PlayerEntity? FindById(byte id) => _players.FirstOrDefault(p => p.Id == id);

   public void SpawnOrbAt(float x, float y, byte value = 3)
   {
      _orbs.Add(new OrbState2D(_nextOrbId++, x, y, value));
   }

   public void ClearLasers() => _lasers.Clear();

   /// <summary>
   /// Accepts a 1x..6x redundant input bundle from a remote client, enqueuing only unseen InputSeqs
   /// so lost/jittered UDP packets recover cleanly without ever double-applying an input.
   /// </summary>
   public void ReceiveClientInputBundle(PlayerEntity player, ReadOnlySpan<ClientInputFrame> frames)
   {
      var highestQueued = player.LastProcessedInputSeq;
      foreach (var q in player.PendingInputs)
      {
         if (q.InputSeq > highestQueued) highestQueued = q.InputSeq;
      }

      for (var i = 0; i < frames.Length; i++)
      {
         var f = frames[i];
         if (f.InputSeq > highestQueued)
         {
            if (player.PendingInputs.Count >= 24)
            {
               player.PendingInputs.Dequeue();
            }

            player.PendingInputs.Enqueue(f);
            highestQueued = f.InputSeq;
         }
      }
   }

   public void ApplyLocalHostInput(PlayerEntity localPlayer, InputButtons buttons, float aimAngle)
   {
      var nextSeq = localPlayer.LastProcessedInputSeq + 1;
      localPlayer.PendingInputs.Enqueue(new ClientInputFrame(nextSeq, buttons, aimAngle));
   }

   /// <summary>
   /// Triggers an authoritative knockback impulse on a player (used by lasers and self-tests).
   /// </summary>
   public void TriggerAuthoritativeTag(PlayerEntity target, PlayerEntity? shooter, float dirX, float dirY, byte fxColor)
   {
      target.StunTicks = 34;
      GamePhysics2D.ApplyKnockback(ref target.X, ref target.Y, dirX * 48f, dirY * 48f);

      if (shooter is not null)
      {
         shooter.Score += 2;
         ScoreboardDirty = true;
         PendingEvents.Add($"{shooter.Name} tagged {target.Name}! (+2)");
      }

      PendingFx.Add(new FxCue2D(target.X, target.Y, FxKind2D.PlayerTag, fxColor));
   }

   public void Step()
   {
      _tick++;

      StepPlayers();
      _poseHistory.Record(_tick, _players);
      DecayRewindMarkers();
      StepLasers();
      StepOrbPickups();
      EnsureOrbs();
   }

   public void FillSnapshotLists(
      List<PlayerState2D> players,
      List<OrbState2D> orbs,
      List<LaserState2D> lasers,
      float renderAlpha = 1f)
   {
      var a = Math.Clamp(renderAlpha, 0f, 1f);

      players.Clear();
      foreach (var p in _players)
      {
         var x = a >= 0.999f ? p.X : p.PrevX + ((p.X - p.PrevX) * a);
         var y = a >= 0.999f ? p.Y : p.PrevY + ((p.Y - p.PrevY) * a);
         var ang = a >= 0.999f ? p.FacingAngle : ClientNetcodeState.LerpAngle(p.PrevFacingAngle, p.FacingAngle, a);

         players.Add(new PlayerState2D(p.Id, x, y, ang, p.Color, p.StunTicks > 0, p.IsBot));
      }

      orbs.Clear();
      orbs.AddRange(_orbs);

      lasers.Clear();
      foreach (var l in _lasers)
      {
         var lx = a >= 0.999f ? l.X : l.PrevX + ((l.X - l.PrevX) * a);
         var ly = a >= 0.999f ? l.Y : l.PrevY + ((l.Y - l.PrevY) * a);

         lasers.Add(new LaserState2D(lx, ly, l.Vx, l.Vy, l.Color, l.OwnerId, l.ShotSeq));
      }
   }

   public void FillScoreboard(List<ScoreEntry2D> scores)
   {
      scores.Clear();

      foreach (var p in _players.OrderByDescending(p => p.Score).ThenBy(p => p.Id))
      {
         scores.Add(new ScoreEntry2D(p.Id, p.Color, p.Score, p.Name));
      }
   }

   private void StepPlayers()
   {
      foreach (var p in _players)
      {
         p.PrevX = p.X;
         p.PrevY = p.Y;
         p.PrevFacingAngle = p.FacingAngle;

         var stunned = p.StunTicks > 0;
         if (stunned) p.StunTicks--;

         if (p.IsBot)
         {
            if (p.FireCooldownTicks > 0) p.FireCooldownTicks--;
            StepBot(p, stunned);
            continue;
         }

         var stepsToProcess = ComputeInputStepsToProcess(p);

         for (var s = 0; s < stepsToProcess; s++)
         {
            var frame = p.PendingInputs.Dequeue();
            p.LastProcessedInputSeq = frame.InputSeq;

            // Decrement fire cooldown per processed InputSeq so rapid-fire matches client prediction 1-to-1
            if (p.FireCooldownTicks > 0) p.FireCooldownTicks--;

            GamePhysics2D.StepPlayerMovement(
               ref p.X,
               ref p.Y,
               ref p.FacingAngle,
               frame.Buttons,
               frame.AimAngle,
               stunned);

            if (!stunned && (frame.Buttons & InputButtons.Fire) != 0 && p.FireCooldownTicks == 0)
            {
               var rewindTicks = ComputeRewindOffsetTicks(p, frame);
               if (SpawnLaser(p, frame.InputSeq, rewindTicks))
               {
                  p.FireCooldownTicks = 15; // ~250ms cooldown (matches client _localFireCooldownTicks)
               }
            }
         }
      }
   }

   private static int ComputeInputStepsToProcess(PlayerEntity p)
   {
      if (!p.PeerId.HasValue)
      {
         // Local host player: process immediately
         return Math.Min(p.PendingInputs.Count, 2);
      }

      // Remote UDP client: prime a tiny 2-tick jitter buffer and pace cleanly at 1 input/tick
      // so the remote client moves with zero 0px -> 8px -> 0px stop-go jitter.
      var q = p.PendingInputs.Count;
      if (!p.InputBufferPrimed)
      {
         if (q >= 2)
         {
            p.InputBufferPrimed = true;
            p.EmptyInputTicks = 0;
            return 1;
         }

         return 0;
      }

      if (q == 0)
      {
         p.EmptyInputTicks++;
         if (p.EmptyInputTicks >= 3) p.InputBufferPrimed = false;
         return 0;
      }

      p.EmptyInputTicks = 0;
      return q >= 12 ? 3 : (q >= 5 ? 2 : 1);
   }

   private float ComputeRewindOffsetTicks(PlayerEntity shooter, ClientInputFrame frame)
   {
      if (!LagCompensationEnabled
          || !shooter.PeerId.HasValue
          || !float.IsFinite(frame.ViewServerTick)
          || frame.ViewServerTick <= 0f)
      {
         return 0f;
      }

      return Math.Clamp(_tick - frame.ViewServerTick, 0f, MaxLagCompRewindTicks);
   }

   private void DecayRewindMarkers()
   {
      for (var i = _rewindMarkers.Count - 1; i >= 0; i--)
      {
         var m = _rewindMarkers[i];
         if (m.RemainingTicks <= 1) _rewindMarkers.RemoveAt(i);
         else _rewindMarkers[i] = m with { RemainingTicks = m.RemainingTicks - 1 };
      }
   }

   private void StepLasers()
   {
      for (var i = _lasers.Count - 1; i >= 0; i--)
      {
         var l = _lasers[i];
         l.PrevX = l.X;
         l.PrevY = l.Y;
         l.LifeTicks--;
         l.X += l.Vx * GamePhysics2D.FixedDt;
         l.Y += l.Vy * GamePhysics2D.FixedDt;

         var hit = l.X < 6f || l.X > GamePhysics2D.ArenaWidth - 6f
                   || l.Y < 6f || l.Y > GamePhysics2D.ArenaHeight - 6f
                   || GamePhysics2D.PointInAnyObstacle(l.X, l.Y);

         if (!hit)
         {
            hit = TryResolveLaserPlayerHit(l);
         }

         if (hit || l.LifeTicks <= 0)
         {
            _lasers.RemoveAt(i);
         }
      }
   }

   private bool TryResolveLaserPlayerHit(LaserEntity l)
   {
      foreach (var target in _players)
      {
         if (target.Id == l.OwnerId || target.StunTicks > 0) continue;

         var testX = target.X;
         var testY = target.Y;
         var usedRewind = false;

         if (LagCompensationEnabled && l.RewindOffsetTicks > 0.05f)
         {
            var evalTick = _tick - l.RewindOffsetTicks;
            if (_poseHistory.TrySample(target.Id, evalTick, _tick, out var hx, out var hy, out var wasStunned))
            {
               if (wasStunned) continue;
               testX = hx;
               testY = hy;
               usedRewind = true;
            }
         }

         if (MathF.Abs(testX - l.X) <= GamePhysics2D.PlayerHalf + 4f
             && MathF.Abs(testY - l.Y) <= GamePhysics2D.PlayerHalf + 4f)
         {
            var speed = MathF.Sqrt((l.Vx * l.Vx) + (l.Vy * l.Vy));
            var nx = speed > 1f ? l.Vx / speed : 1f;
            var ny = speed > 1f ? l.Vy / speed : 0f;

            if (usedRewind)
            {
               var rewindMs = l.RewindOffsetTicks * GamePhysics2D.FixedDt * 1000f;
               LagCompensatedHits++;
               LastRewindMs = rewindMs;

               _rewindMarkers.Add(new RewindHitMarker(target.Id, testX, testY, target.X, target.Y, rewindMs, RemainingTicks: 42));
               if (_rewindMarkers.Count > 12) _rewindMarkers.RemoveAt(0);
            }

            TriggerAuthoritativeTag(target, FindById(l.OwnerId), nx, ny, l.Color);
            return true;
         }
      }

      return false;
   }

   private void StepOrbPickups()
   {
      const float pickupDistSq = (GamePhysics2D.PlayerHalf + GamePhysics2D.OrbRadius + 3f)
                                 * (GamePhysics2D.PlayerHalf + GamePhysics2D.OrbRadius + 3f);

      for (var i = _orbs.Count - 1; i >= 0; i--)
      {
         var orb = _orbs[i];

         foreach (var p in _players)
         {
            var dx = p.X - orb.X;
            var dy = p.Y - orb.Y;

            if ((dx * dx) + (dy * dy) <= pickupDistSq)
            {
               p.Score += orb.Value;
               ScoreboardDirty = true;

               PendingFx.Add(new FxCue2D(orb.X, orb.Y, FxKind2D.OrbPickup, p.Color));

               if (orb.Value > 1)
               {
                  PendingEvents.Add($"{p.Name} grabbed Golden Beskar! (+{orb.Value})");
               }

               _orbs.RemoveAt(i);
               break;
            }
         }
      }
   }

   private bool SpawnLaser(PlayerEntity shooter, uint shotSeq = 0, float rewindOffsetTicks = 0f)
   {
      var cos = MathF.Cos(shooter.FacingAngle);
      var sin = MathF.Sin(shooter.FacingAngle);

      var sx = shooter.X + (cos * (GamePhysics2D.PlayerHalf + 6f));
      var sy = shooter.Y + (sin * (GamePhysics2D.PlayerHalf + 6f));

      if (GamePhysics2D.PointInAnyObstacle(sx, sy)) return false;

      _lasers.Add(new LaserEntity
      {
         OwnerId = shooter.Id,
         ShotSeq = shotSeq,
         Color = shooter.Color,
         X = sx,
         Y = sy,
         PrevX = sx,
         PrevY = sy,
         Vx = cos * GamePhysics2D.LaserSpeed,
         Vy = sin * GamePhysics2D.LaserSpeed,
         LifeTicks = 65,
         RewindOffsetTicks = rewindOffsetTicks
      });

      PendingFx.Add(new FxCue2D(sx, sy, FxKind2D.LaserShot, shooter.Color));
      return true;
   }

   private void EnsureOrbs()
   {
      const int targetOrbs = 7;

      while (_orbs.Count < targetOrbs)
      {
         var x = 44f + ((float)_rng.NextDouble() * (GamePhysics2D.ArenaWidth - 88f));
         var y = 44f + ((float)_rng.NextDouble() * (GamePhysics2D.ArenaHeight - 88f));

         if (GamePhysics2D.CollidesAnyObstacle(x, y, halfSize: 22f)) continue;

         var val = (byte)(_rng.Next(5) == 0 ? 3 : 1);
         _orbs.Add(new OrbState2D(_nextOrbId++, x, y, val));
      }
   }

   private (float X, float Y) FindSafeSpawnPoint()
   {
      for (var i = 0; i < 40; i++)
      {
         var x = 64f + ((float)_rng.NextDouble() * (GamePhysics2D.ArenaWidth - 128f));
         var y = 64f + ((float)_rng.NextDouble() * (GamePhysics2D.ArenaHeight - 128f));

         if (!GamePhysics2D.CollidesAnyObstacle(x, y, halfSize: 32f))
         {
            return (x, y);
         }
      }

      return (GamePhysics2D.ArenaWidth * 0.5f, GamePhysics2D.ArenaHeight * 0.5f);
   }
}
