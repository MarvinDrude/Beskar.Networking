using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Configures retry attempts and backoff intervals for failed or orphaned job runs.
/// </summary>
public sealed class RetryPolicy
{
   /// <summary>
   /// Default retry policy: up to 3 attempts with exponential backoff starting at 250ms.
   /// </summary>
   public static RetryPolicy Default { get; } = new();

   /// <summary>
   /// Maximum number of execution attempts before moving the job to <c>DeadLettered</c>. Defaults to 3.
   /// </summary>
   public int MaxAttempts { get; set; } = 3;

   /// <summary>
   /// Backoff policy used to compute the delay before retry attempt <c>N</c>.
   /// </summary>
   public IBackoffPolicy Backoff { get; set; } = new ExponentialBackoffPolicy(TimeSpan.FromMilliseconds(250));

   /// <summary>
   /// Maximum cap on retry delay. Defaults to 5 minutes.
   /// </summary>
   public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);

   /// <summary>
   /// Calculates the retry delay for the given retry attempt (1-based).
   /// </summary>
   public TimeSpan GetRetryDelay(int retryAttempt)
   {
      var delay = Backoff.GetNextDelay(Math.Max(1, retryAttempt));
      if (delay < TimeSpan.Zero)
      {
         return TimeSpan.Zero;
      }

      return delay > MaxDelay ? MaxDelay : delay;
   }
}
