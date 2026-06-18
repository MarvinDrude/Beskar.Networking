using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Options;

public sealed class AutoReconnectOptions
{
   public bool IsEnabled { get; init; } = true;

   public int MaxRetryAttempts { get; init; } = 5;

   public IBackoffPolicy BackoffPolicy { get; init; } = new LinearBackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
}
