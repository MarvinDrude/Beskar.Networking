using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Options;

public sealed class AutoReconnectOptions
{
   public bool IsEnabled { get; set; } = true;

   public int MaxRetryAttempts { get; set; } = 5;

   public IBackoffPolicy BackoffPolicy { get; set; } = new LinearBackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
}
