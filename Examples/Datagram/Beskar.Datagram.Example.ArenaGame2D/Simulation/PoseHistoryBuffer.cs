namespace Beskar.Datagram.Example.ArenaGame2D.Simulation;

/// <summary>
/// Zero-allocation 60 Hz ring buffer storing historical player poses for bounded server-side
/// lag compensation (hitbox rewind) with exact sub-tick linear interpolation.
/// </summary>
internal sealed class PoseHistoryBuffer
{
   private readonly record struct HistoricalPose(byte PlayerId, float X, float Y, bool Stunned);

   private sealed class PoseHistoryFrame
   {
      public uint Tick;
      public int Count;
      public readonly HistoricalPose[] Poses = new HistoricalPose[MaxTrackedPlayersPerFrame];
   }

   private const int Capacity = 48; // 800ms of 60Hz snapshots
   private const int MaxTrackedPlayersPerFrame = 16;

   private readonly PoseHistoryFrame[] _frames = new PoseHistoryFrame[Capacity];

   public PoseHistoryBuffer()
   {
      for (var i = 0; i < Capacity; i++)
      {
         _frames[i] = new PoseHistoryFrame();
      }
   }

   public void Record(uint tick, IReadOnlyList<PlayerEntity> players)
   {
      var slot = _frames[tick % Capacity];
      slot.Tick = tick;

      var count = Math.Min(players.Count, MaxTrackedPlayersPerFrame);
      slot.Count = count;

      for (var i = 0; i < count; i++)
      {
         var p = players[i];
         slot.Poses[i] = new HistoricalPose(p.Id, p.X, p.Y, p.StunTicks > 0);
      }
   }

   public bool TrySample(
      byte playerId,
      float evalTick,
      uint currentTick,
      out float x,
      out float y,
      out bool wasStunned)
   {
      x = 0f;
      y = 0f;
      wasStunned = false;

      if (evalTick <= 0f || currentTick == 0) return false;

      var clampedTick = Math.Clamp(evalTick, 1f, currentTick);
      var tickA = (uint)MathF.Floor(clampedTick);
      var tickB = (uint)MathF.Ceiling(clampedTick);

      if (tickA < 1) tickA = 1;
      if (tickB > currentTick) tickB = currentTick;

      var frameA = _frames[tickA % Capacity];
      var frameB = _frames[tickB % Capacity];

      HistoricalPose poseA = default;
      HistoricalPose poseB = default;

      var hasA = frameA.Tick == tickA && TryFindPose(frameA, playerId, out poseA);
      var hasB = frameB.Tick == tickB && TryFindPose(frameB, playerId, out poseB);

      if (hasA && hasB)
      {
         var frac = tickB > tickA ? Math.Clamp(clampedTick - tickA, 0f, 1f) : 0f;

         x = poseA.X + ((poseB.X - poseA.X) * frac);
         y = poseA.Y + ((poseB.Y - poseA.Y) * frac);
         wasStunned = poseA.Stunned && poseB.Stunned;

         return true;
      }

      if (hasB)
      {
         x = poseB.X;
         y = poseB.Y;
         wasStunned = poseB.Stunned;

         return true;
      }

      if (hasA)
      {
         x = poseA.X;
         y = poseA.Y;
         wasStunned = poseA.Stunned;

         return true;
      }

      return false;
   }

   private static bool TryFindPose(PoseHistoryFrame frame, byte playerId, out HistoricalPose pose)
   {
      for (var i = 0; i < frame.Count; i++)
      {
         if (frame.Poses[i].PlayerId == playerId)
         {
            pose = frame.Poses[i];
            return true;
         }
      }

      pose = default;
      return false;
   }
}
