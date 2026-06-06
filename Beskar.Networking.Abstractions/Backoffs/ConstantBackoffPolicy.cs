using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

public sealed class ConstantBackoffPolicy(TimeSpan constant) : IBackoffPolicy
{
   public TimeSpan ConstantDelay { get; } = constant;

   public TimeSpan GetNextDelay(int attempt)
   {
      return ConstantDelay;
   }
}
