namespace Beskar.Datagram.Example.ArenaGame2D.Protocol;

/// <summary>
/// Deterministic 2D movement & AABB obstacle collision shared verbatim by both
/// the authoritative Host simulation and Client-Side Prediction / Reconciliation.
/// </summary>
public static class GamePhysics2D
{
   public const float ArenaWidth = 800f;
   public const float ArenaHeight = 560f;

   public const float PlayerSize = 28f;
   public const float PlayerHalf = PlayerSize * 0.5f;

   public const float MoveSpeed = 246f; // px/sec
   public const float FixedDt = 1f / 60f;
   public const float LaserSpeed = 580f;
   public const float OrbRadius = 9f;

   public static readonly ArenaObstacle[] Obstacles =
   [
      new(224f, 165f, 44f, 230f),
      new(532f, 165f, 44f, 230f)
   ];

   public static void StepPlayerMovement(
      ref float x,
      ref float y,
      ref float facingAngle,
      InputButtons buttons,
      float aimAngle,
      bool isStunned,
      float dt = FixedDt)
   {
      if (isStunned) return;

      var dx = 0f;
      var dy = 0f;

      if ((buttons & InputButtons.Left) != 0) dx -= 1f;
      if ((buttons & InputButtons.Right) != 0) dx += 1f;
      if ((buttons & InputButtons.Up) != 0) dy -= 1f;
      if ((buttons & InputButtons.Down) != 0) dy += 1f;

      if (dx != 0f || dy != 0f)
      {
         var invLen = 1f / MathF.Sqrt((dx * dx) + (dy * dy));
         dx *= invLen;
         dy *= invLen;

         // Resolve X then Y against arena bounds & cover obstacles (clean sliding)
         var nextX = Math.Clamp(x + (dx * MoveSpeed * dt), PlayerHalf + 4f, ArenaWidth - PlayerHalf - 4f);
         if (!CollidesAnyObstacle(nextX, y))
         {
            x = nextX;
         }

         var nextY = Math.Clamp(y + (dy * MoveSpeed * dt), PlayerHalf + 4f, ArenaHeight - PlayerHalf - 4f);
         if (!CollidesAnyObstacle(x, nextY))
         {
            y = nextY;
         }
      }

      facingAngle = aimAngle;
   }

   /// <summary>
   /// Applies knockback in small sub-steps so players never tunnel or clip into cover pillars.
   /// </summary>
   public static void ApplyKnockback(ref float x, ref float y, float kx, float ky)
   {
      const int subSteps = 4;
      var stepX = kx / subSteps;
      var stepY = ky / subSteps;

      for (var i = 0; i < subSteps; i++)
      {
         var nx = Math.Clamp(x + stepX, PlayerHalf + 4f, ArenaWidth - PlayerHalf - 4f);
         if (!CollidesAnyObstacle(nx, y)) x = nx;

         var ny = Math.Clamp(y + stepY, PlayerHalf + 4f, ArenaHeight - PlayerHalf - 4f);
         if (!CollidesAnyObstacle(x, ny)) y = ny;
      }
   }

   public static bool CollidesAnyObstacle(float cx, float cy, float halfSize = PlayerHalf)
   {
      foreach (var obs in Obstacles)
      {
         if (cx + halfSize > obs.X && cx - halfSize < obs.X + obs.W
             && cy + halfSize > obs.Y && cy - halfSize < obs.Y + obs.H)
         {
            return true;
         }
      }

      return false;
   }

   public static bool PointInAnyObstacle(float px, float py) => CollidesAnyObstacle(px, py, halfSize: 3f);
}
