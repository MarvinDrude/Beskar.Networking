using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

/// <summary>
/// A decorator backoff policy that adds randomized jitter to an inner <see cref="IBackoffPolicy"/>
/// to prevent thundering herd reconnection spikes.
/// </summary>
public sealed class JitterBackoffPolicy(
   IBackoffPolicy innerPolicy,
   JitterMode mode = JitterMode.Full,
   double jitterFactor = 0.2,
   Func<double>? randomProvider = null)
   : IBackoffPolicy
{
   public IBackoffPolicy InnerPolicy { get; } = innerPolicy ?? throw new ArgumentNullException(nameof(innerPolicy));

   public JitterMode Mode { get; } = mode;

   public double JitterFactor { get; } = Math.Clamp(jitterFactor, 0.0, 1.0);

   private readonly Func<double>? _randomProvider = randomProvider;

   public TimeSpan GetNextDelay(int attempt)
   {
      var baseDelay = InnerPolicy.GetNextDelay(attempt);
      if (baseDelay <= TimeSpan.Zero)
      {
         return TimeSpan.Zero;
      }

      var randomValue = _randomProvider?.Invoke() ?? Random.Shared.NextDouble();
      randomValue = Math.Clamp(randomValue, 0.0, 1.0);

      return Mode switch
      {
         JitterMode.Full => TimeSpan.FromTicks((long)(baseDelay.Ticks * randomValue)),
         JitterMode.Equal => TimeSpan.FromTicks((long)(baseDelay.Ticks / 2.0 + (baseDelay.Ticks / 2.0) * randomValue)),
         JitterMode.Proportional => ApplyProportional(baseDelay, randomValue),
         _ => baseDelay
      };
   }

   private TimeSpan ApplyProportional(TimeSpan baseDelay, double randomValue)
   {
      var factor = (randomValue * 2.0 - 1.0) * JitterFactor;
      var ticks = (long)(baseDelay.Ticks * (1.0 + factor));

      return ticks < 0 ? TimeSpan.Zero : TimeSpan.FromTicks(ticks);
   }
}
