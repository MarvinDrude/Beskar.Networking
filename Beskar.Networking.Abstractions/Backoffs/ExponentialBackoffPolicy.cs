using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

public sealed class ExponentialBackoffPolicy(TimeSpan initial) : IBackoffPolicy
{
   public TimeSpan InitialDelay { get; } = initial;

   public TimeSpan GetNextDelay(int attempt)
   {
      var clampedAttempt = Math.Clamp(attempt, 1, 30);
      var multiplier = Math.Pow(2, clampedAttempt - 1);
      var delayMs = InitialDelay.TotalMilliseconds * multiplier;
      var maxMs = (double)(int.MaxValue - 1);

      return TimeSpan.FromMilliseconds(Math.Min(Math.Max(0, delayMs), maxMs));
   }
}
