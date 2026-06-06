using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

public sealed class ExponentialBackoffPolicy(TimeSpan initial) : IBackoffPolicy
{
   public TimeSpan InitialDelay { get; } = initial;

   public TimeSpan GetNextDelay(int attempt)
   {
      return InitialDelay * (int)Math.Pow(2, attempt - 1);
   }
}
