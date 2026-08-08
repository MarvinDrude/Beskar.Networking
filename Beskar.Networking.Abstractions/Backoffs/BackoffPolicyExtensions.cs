using Beskar.Networking.Abstractions.Interfaces.Misc;

namespace Beskar.Networking.Abstractions.Backoffs;

/// <summary>
/// Extension methods for configuring <see cref="IBackoffPolicy"/> instances with jitter.
/// </summary>
public static class BackoffPolicyExtensions
{
   /// <summary>
   /// Wraps the backoff policy with randomized jitter.
   /// </summary>
   public static IBackoffPolicy WithJitter(
      this IBackoffPolicy policy,
      JitterMode mode = JitterMode.Full,
      double jitterFactor = 0.2)
   {
      ArgumentNullException.ThrowIfNull(policy);
      return new JitterBackoffPolicy(policy, mode, jitterFactor);
   }

   /// <summary>
   /// Wraps the backoff policy with Full Jitter.
   /// </summary>
   public static IBackoffPolicy WithFullJitter(this IBackoffPolicy policy)
   {
      return policy.WithJitter(JitterMode.Full);
   }

   /// <summary>
   /// Wraps the backoff policy with Equal Jitter.
   /// </summary>
   public static IBackoffPolicy WithEqualJitter(this IBackoffPolicy policy)
   {
      return policy.WithJitter(JitterMode.Equal);
   }
}
