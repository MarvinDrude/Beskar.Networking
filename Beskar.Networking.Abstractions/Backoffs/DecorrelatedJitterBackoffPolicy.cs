using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

/// <summary>
/// Implements AWS Decorrelated Jitter backoff policy.
/// Calculates next delay using sleep = min(maxDelay, random(initialDelay, previousDelay * 3)).
/// </summary>
public sealed class DecorrelatedJitterBackoffPolicy : IBackoffPolicy
{
   private readonly TimeSpan _initialDelay;
   private readonly TimeSpan _maxDelay;
   private readonly Func<double>? _randomProvider;
   private TimeSpan _lastDelay;

   public TimeSpan InitialDelay => _initialDelay;
   public TimeSpan MaxDelay => _maxDelay;

   public DecorrelatedJitterBackoffPolicy(
      TimeSpan initialDelay,
      TimeSpan maxDelay,
      Func<double>? randomProvider = null)
   {
      _initialDelay = initialDelay <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(100) : initialDelay;
      _maxDelay = maxDelay < _initialDelay ? _initialDelay : maxDelay;
      _randomProvider = randomProvider;
      _lastDelay = _initialDelay;
   }
   public TimeSpan GetNextDelay(int attempt)
   {
      if (attempt <= 1)
      {
         _lastDelay = _initialDelay;
      }

      var randomValue = _randomProvider?.Invoke() ?? Random.Shared.NextDouble();
      randomValue = Math.Clamp(randomValue, 0.0, 1.0);

      var minTicks = _initialDelay.Ticks;
      var lastTicks = _lastDelay.Ticks;
      var tripleTicks = lastTicks > long.MaxValue / 3 ? long.MaxValue : lastTicks * 3;
      var maxRangeTicks = Math.Min(_maxDelay.Ticks, tripleTicks);

      if (maxRangeTicks <= minTicks)
      {
         _lastDelay = _initialDelay;
         return _initialDelay;
      }

      var nextTicks = minTicks + (long)((maxRangeTicks - minTicks) * randomValue);
      _lastDelay = TimeSpan.FromTicks(nextTicks);
      return _lastDelay;
   }
}
