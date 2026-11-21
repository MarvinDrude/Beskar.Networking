namespace Beskar.Datagram.Example.ArenaGame;

/// <summary>
/// Minimal authoritative 2D arena simulation executed exclusively on the Host at 30 Hz.
/// </summary>
public sealed class ArenaSimulation
{
   public sealed class PlayerEntity
   {
      public byte Id { get; init; }
      public uint? PeerId { get; init; } // null for local host player or AI bot
      public required string Name { get; init; }

      public byte Color { get; init; }
      public bool IsBot { get; init; }

      public int X { get; set; }
      public int Y { get; set; }
      public Direction Facing { get; set; } = Direction.Right;

      public ushort Score { get; set; }

      public int FireCooldownTicks { get; set; }
      public int StunTicks { get; set; }
      public int BotThinkCooldown { get; set; }

      public sbyte PendingDx { get; set; }
      public sbyte PendingDy { get; set; }
      public bool PendingFire { get; set; }
   }

   private sealed class LaserEntity
   {
      public byte OwnerId { get; init; }
      public byte Color { get; init; }

      public int X { get; set; }
      public int Y { get; set; }

      public Direction Dir { get; init; }
      public int LifeTicks { get; set; }
   }

   private readonly List<PlayerEntity> _players = [];
   private readonly List<OrbSnapshot> _orbs = [];
   private readonly List<LaserEntity> _lasers = [];
   private readonly Random _rng = new();

   private byte _nextPlayerId = 1;
   private byte _nextColor;
   private uint _tick;

   public uint Tick => _tick;
   public IReadOnlyList<PlayerEntity> Players => _players;
   public bool ScoreboardDirty { get; set; } = true;

   public List<string> PendingEvents { get; } = [];
   public List<FxCue> PendingFx { get; } = [];

   public ArenaSimulation()
   {
      EnsureOrbs();
   }

   public PlayerEntity AddPlayer(string name, uint? peerId, bool isBot = false)
   {
      var id = _nextPlayerId++;
      if (_nextPlayerId == 0) _nextPlayerId = 1;

      var color = (byte)(_nextColor++ % 6);
      var entity = new PlayerEntity
      {
         Id = id,
         PeerId = peerId,
         Name = string.IsNullOrWhiteSpace(name) ? $"Player{id}" : name.Trim(),
         Color = color,
         IsBot = isBot,
         X = _rng.Next(2, GameProtocol.ArenaWidth - 2),
         Y = _rng.Next(2, GameProtocol.ArenaHeight - 2),
         Facing = (Direction)_rng.Next(4)
      };

      _players.Add(entity);
      ScoreboardDirty = true;

      PendingEvents.Add($"{entity.Name} entered the arena!");
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

   public void SetBotCount(int targetBots)
   {
      var currentBots = _players.Count(p => p.IsBot);
      while (currentBots < targetBots)
      {
         AddPlayer($"Bot-{currentBots + 1}", peerId: null, isBot: true);
         currentBots++;
      }

      while (currentBots > targetBots)
      {
         var idx = _players.FindLastIndex(p => p.IsBot);

         if (idx < 0) break;
         var bot = _players[idx];

         _players.RemoveAt(idx);
         ScoreboardDirty = true;
         PendingEvents.Add($"{bot.Name} powered down.");
         currentBots--;
      }
   }

   public PlayerEntity? FindByPeer(uint peerId) => _players.FirstOrDefault(p => p.PeerId == peerId);
   public PlayerEntity? FindById(byte id) => _players.FirstOrDefault(p => p.Id == id);

   public void QueueInput(PlayerEntity player, sbyte dx, sbyte dy, Direction facing, bool fire)
   {
      player.PendingDx = dx;
      player.PendingDy = dy;
      player.Facing = facing;
      if (fire) player.PendingFire = true;
   }

   public void Step()
   {
      _tick++;

      // 1. Update AI bots & apply all player inputs
      foreach (var p in _players)
      {
         if (p.FireCooldownTicks > 0) p.FireCooldownTicks--;
         if (p.StunTicks > 0)
         {
            p.StunTicks--;
            p.PendingDx = 0;
            p.PendingDy = 0;
            p.PendingFire = false;
            continue;
         }

         if (p.IsBot)
         {
            UpdateBotDecision(p);
         }

         if (p.PendingDx != 0 || p.PendingDy != 0)
         {
            p.X = Math.Clamp(p.X + p.PendingDx, 0, GameProtocol.ArenaWidth - 1);
            p.Y = Math.Clamp(p.Y + p.PendingDy, 0, GameProtocol.ArenaHeight - 1);
            p.PendingDx = 0;
            p.PendingDy = 0;
         }

         if (p.PendingFire && p.FireCooldownTicks == 0)
         {
            p.PendingFire = false;
            p.FireCooldownTicks = 8; // ~260ms fire cooldown
            SpawnLaser(p);
         }
         else
         {
            p.PendingFire = false;
         }
      }

      // 2. Step lasers & check hits
      for (var i = _lasers.Count - 1; i >= 0; i--)
      {
         var l = _lasers[i];
         l.LifeTicks--;

         var steps = l.Dir is Direction.Left or Direction.Right ? 2 : 1;
         var hit = false;

         for (var s = 0; s < steps; s++)
         {
            switch (l.Dir)
            {
               case Direction.Up: l.Y--; break;
               case Direction.Down: l.Y++; break;
               case Direction.Left: l.X--; break;
               case Direction.Right: l.X++; break;
            }

            if (l.X < 0 || l.X >= GameProtocol.ArenaWidth || l.Y < 0 || l.Y >= GameProtocol.ArenaHeight)
            {
               hit = true;
               break;
            }

            foreach (var target in _players)
            {
               if (target.Id == l.OwnerId || target.StunTicks > 0) continue;
               if (target.X == l.X && target.Y == l.Y)
               {
                  hit = true;
                  target.StunTicks = 24; // ~0.8s stun
                  // Small knockback
                  var (kx, ky) = l.Dir switch
                  {
                     Direction.Up => (0, -2),
                     Direction.Down => (0, 2),
                     Direction.Left => (-3, 0),
                     _ => (3, 0)
                  };
                  target.X = Math.Clamp(target.X + kx, 0, GameProtocol.ArenaWidth - 1);
                  target.Y = Math.Clamp(target.Y + ky, 0, GameProtocol.ArenaHeight - 1);

                  var shooter = FindById(l.OwnerId);
                  if (shooter is not null)
                  {
                     shooter.Score += 2;
                     ScoreboardDirty = true;
                     PendingEvents.Add($"{shooter.Name} tagged {target.Name}! (+2)");
                  }
                  PendingFx.Add(new FxCue((byte)target.X, (byte)target.Y, FxKind.PlayerTag, l.Color));
                  break;
               }
            }

            if (hit) break;
         }

         if (hit || l.LifeTicks <= 0)
         {
            _lasers.RemoveAt(i);
         }
      }

      // 3. Check Beskar orb pickups
      for (var i = _orbs.Count - 1; i >= 0; i--)
      {
         var orb = _orbs[i];
         foreach (var p in _players)
         {
            if (p.X == orb.X && p.Y == orb.Y)
            {
               p.Score += orb.Value;
               ScoreboardDirty = true;
               PendingFx.Add(new FxCue(orb.X, orb.Y, FxKind.OrbPickup, p.Color));

               if (orb.Value > 1)
               {
                  PendingEvents.Add($"{p.Name} grabbed a Golden Beskar! (+{orb.Value})");
               }

               _orbs.RemoveAt(i);
               break;
            }
         }
      }

      EnsureOrbs();
   }

   public void FillSnapshotLists(List<PlayerSnapshot> players, List<OrbSnapshot> orbs, List<LaserSnapshot> lasers)
   {
      players.Clear();
      foreach (var p in _players)
      {
         players.Add(new PlayerSnapshot(p.Id, (byte)p.X, (byte)p.Y, p.Facing, p.Color, p.StunTicks > 0, p.IsBot));
      }

      orbs.Clear();
      orbs.AddRange(_orbs);

      lasers.Clear();
      foreach (var l in _lasers)
      {
         lasers.Add(new LaserSnapshot((byte)l.X, (byte)l.Y, l.Dir, l.Color));
      }
   }

   public void FillScoreboard(List<ScoreEntry> scores)
   {
      scores.Clear();
      foreach (var p in _players.OrderByDescending(p => p.Score).ThenBy(p => p.Id))
      {
         scores.Add(new ScoreEntry(p.Id, p.Color, p.Score, p.Name));
      }
   }

   private void SpawnLaser(PlayerEntity shooter)
   {
      var (dx, dy) = shooter.Facing switch
      {
         Direction.Up => (0, -1),
         Direction.Down => (0, 1),
         Direction.Left => (-1, 0),
         _ => (1, 0)
      };

      var sx = shooter.X + dx;
      var sy = shooter.Y + dy;
      if (sx < 0 || sx >= GameProtocol.ArenaWidth || sy < 0 || sy >= GameProtocol.ArenaHeight) return;

      _lasers.Add(new LaserEntity
      {
         OwnerId = shooter.Id,
         Color = shooter.Color,
         X = sx,
         Y = sy,
         Dir = shooter.Facing,
         LifeTicks = 18
      });

      PendingFx.Add(new FxCue((byte)sx, (byte)sy, FxKind.LaserShot, shooter.Color));
   }

   private void UpdateBotDecision(PlayerEntity bot)
   {
      if (bot.BotThinkCooldown > 0)
      {
         bot.BotThinkCooldown--;
         return;
      }

      bot.BotThinkCooldown = _rng.Next(2, 5); // ~7-12 moves/sec

      // Occasionally shoot if lined up with another player
      if (bot.FireCooldownTicks == 0 && _rng.Next(5) == 0)
      {
         foreach (var other in _players)
         {
            if (other.Id == bot.Id || other.StunTicks > 0) continue;
            if (other.Y == bot.Y && Math.Abs(other.X - bot.X) <= 18)
            {
               bot.Facing = other.X > bot.X ? Direction.Right : Direction.Left;
               bot.PendingFire = true;
               return;
            }
            if (other.X == bot.X && Math.Abs(other.Y - bot.Y) <= 8)
            {
               bot.Facing = other.Y > bot.Y ? Direction.Down : Direction.Up;
               bot.PendingFire = true;
               return;
            }
         }
      }

      // Otherwise steer toward nearest Beskar orb
      if (_orbs.Count == 0) return;

      var nearest = _orbs[0];
      var bestDist = int.MaxValue;
      foreach (var orb in _orbs)
      {
         var dist = Math.Abs(orb.X - bot.X) + Math.Abs(orb.Y - bot.Y);
         if (dist < bestDist)
         {
            bestDist = dist;
            nearest = orb;
         }
      }

      var dx = Math.Sign(nearest.X - bot.X);
      var dy = Math.Sign(nearest.Y - bot.Y);

      if (dx != 0 && (dy == 0 || _rng.Next(2) == 0))
      {
         bot.PendingDx = (sbyte)dx;
         bot.Facing = dx > 0 ? Direction.Right : Direction.Left;
      }
      else if (dy != 0)
      {
         bot.PendingDy = (sbyte)dy;
         bot.Facing = dy > 0 ? Direction.Down : Direction.Up;
      }
   }

   private void EnsureOrbs()
   {
      const int targetOrbs = 6;
      while (_orbs.Count < targetOrbs)
      {
         var x = (byte)_rng.Next(1, GameProtocol.ArenaWidth - 1);
         var y = (byte)_rng.Next(1, GameProtocol.ArenaHeight - 1);
         if (_orbs.Any(o => o.X == x && o.Y == y)) continue;

         // 20% chance for a 3-point Golden Beskar orb!
         var value = (byte)(_rng.Next(5) == 0 ? 3 : 1);
         _orbs.Add(new OrbSnapshot(x, y, value));
      }
   }
}
