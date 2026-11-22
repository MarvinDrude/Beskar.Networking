using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Simulation;

public sealed partial class ArenaSimulation2D
{
   public void SetBotCount(int targetBots)
   {
      var current = _players.Count(p => p.IsBot);

      while (current < targetBots)
      {
         AddPlayer($"Bot-{current + 1}", peerId: null, isBot: true);
         current++;
      }

      while (current > targetBots)
      {
         var idx = _players.FindLastIndex(p => p.IsBot);
         if (idx < 0) break;

         var bot = _players[idx];
         _players.RemoveAt(idx);
         ScoreboardDirty = true;
         PendingEvents.Add($"{bot.Name} powered down.");
         current--;
      }
   }

   private void StepBot(PlayerEntity bot, bool stunned)
   {
      if (stunned) return;

      var buttons = InputButtons.None;
      var aim = bot.FacingAngle;

      if (bot.BotDecisionTimer > 0)
      {
         bot.BotDecisionTimer--;

         var wx = MathF.Cos(bot.BotWanderAngle);
         var wy = MathF.Sin(bot.BotWanderAngle);

         if (wx < -0.3f) buttons |= InputButtons.Left;
         else if (wx > 0.3f) buttons |= InputButtons.Right;

         if (wy < -0.3f) buttons |= InputButtons.Up;
         else if (wy > 0.3f) buttons |= InputButtons.Down;

         aim = bot.BotWanderAngle;
      }
      else if (_orbs.Count > 0)
      {
         var nearest = FindNearestOrb(bot);
         var dx = nearest.X - bot.X;
         var dy = nearest.Y - bot.Y;

         if (dx < -8f) buttons |= InputButtons.Left;
         else if (dx > 8f) buttons |= InputButtons.Right;

         if (dy < -8f) buttons |= InputButtons.Up;
         else if (dy > 8f) buttons |= InputButtons.Down;

         aim = MathF.Atan2(dy, dx);
      }

      var prevX = bot.X;
      var prevY = bot.Y;

      GamePhysics2D.StepPlayerMovement(ref bot.X, ref bot.Y, ref bot.FacingAngle, buttons, aim, isStunned: false);

      // If blocked by a pillar, briefly steer around it on subsequent ticks
      if (MathF.Abs(bot.X - prevX) < 0.25f && MathF.Abs(bot.Y - prevY) < 0.25f)
      {
         bot.BotDecisionTimer = 22;
         bot.BotWanderAngle = (float)(_rng.NextDouble() * MathF.PI * 2f);
      }

      TryBotFire(bot);
   }

   private OrbState2D FindNearestOrb(PlayerEntity bot)
   {
      var nearest = _orbs[0];
      var bestDist = float.MaxValue;

      foreach (var orb in _orbs)
      {
         var dx = orb.X - bot.X;
         var dy = orb.Y - bot.Y;
         var distSq = (dx * dx) + (dy * dy);

         if (distSq < bestDist)
         {
            bestDist = distSq;
            nearest = orb;
         }
      }

      return nearest;
   }

   private void TryBotFire(PlayerEntity bot)
   {
      if (bot.FireCooldownTicks != 0 || _rng.Next(45) != 0) return;

      foreach (var other in _players)
      {
         if (other.Id == bot.Id || other.StunTicks > 0) continue;

         var dx = other.X - bot.X;
         var dy = other.Y - bot.Y;

         if ((dx * dx) + (dy * dy) < 290f * 290f)
         {
            bot.FacingAngle = MathF.Atan2(dy, dx);
            if (SpawnLaser(bot, ++bot.LastProcessedInputSeq))
            {
               bot.FireCooldownTicks = 32;
            }
            break;
         }
      }
   }
}
