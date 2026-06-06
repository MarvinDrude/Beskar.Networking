using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

public sealed class LinearBackoffPolicy(TimeSpan initial, TimeSpan increment) : IBackoffPolicy
{
   public TimeSpan InitialDelay { get; } = initial;
   public TimeSpan Increment { get; } = increment;

   public TimeSpan GetNextDelay(int attempt)
   {
      return InitialDelay + (attempt - 1) * Increment;
   }
}
