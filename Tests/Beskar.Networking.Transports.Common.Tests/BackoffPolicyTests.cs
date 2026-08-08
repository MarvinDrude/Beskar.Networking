using Beskar.Networking.Abstractions.Backoffs;

namespace Beskar.Networking.Transports.Common.Tests;

public class BackoffPolicyTests
{
   [Test]
   public async Task FullJitter_ScalesDelayUniformly()
   {
      // Arrange
      var inner = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(2));
      // Base delay for attempt 1 = 2 seconds
      var jittered = new JitterBackoffPolicy(inner, JitterMode.Full, randomProvider: () => 0.5);

      // Act
      var delay = jittered.GetNextDelay(1);

      // Assert
      await Assert.That(delay).IsEqualTo(TimeSpan.FromSeconds(1));
   }

   [Test]
   public async Task EqualJitter_HasHalfConstantAndHalfRandom()
   {
      // Arrange
      var inner = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(4));
      // Base delay for attempt 1 = 4 seconds. Half = 2s, half random with r=0.5 -> 2s + 1s = 3s
      var jittered = new JitterBackoffPolicy(inner, JitterMode.Equal, randomProvider: () => 0.5);

      // Act
      var delay = jittered.GetNextDelay(1);

      // Assert
      await Assert.That(delay).IsEqualTo(TimeSpan.FromSeconds(3));
   }

   [Test]
   public async Task ProportionalJitter_VariesWithinFactorRange()
   {
      // Arrange
      var inner = new ConstantBackoffPolicy(TimeSpan.FromSeconds(10));
      // JitterFactor 0.2 (+/- 20%). With r=1.0, factor = +0.2 -> 12 seconds
      var jitteredUpper = new JitterBackoffPolicy(inner, JitterMode.Proportional, jitterFactor: 0.2, randomProvider: () => 1.0);
      // With r=0.0, factor = -0.2 -> 8 seconds
      var jitteredLower = new JitterBackoffPolicy(inner, JitterMode.Proportional, jitterFactor: 0.2, randomProvider: () => 0.0);

      // Act & Assert
      await Assert.That(jitteredUpper.GetNextDelay(1)).IsEqualTo(TimeSpan.FromSeconds(12));
      await Assert.That(jitteredLower.GetNextDelay(1)).IsEqualTo(TimeSpan.FromSeconds(8));
   }

   [Test]
   public async Task DecorrelatedJitter_RespectsMinAndMaxBounds()
   {
      // Arrange
      var initial = TimeSpan.FromMilliseconds(100);
      var max = TimeSpan.FromSeconds(5);
      var policy = new DecorrelatedJitterBackoffPolicy(initial, max, randomProvider: () => 1.0);

      // Act
      var delay1 = policy.GetNextDelay(1); // attempt 1 -> max bound is min(5s, 100ms * 3 = 300ms), r=1.0 -> 300ms
      var delay2 = policy.GetNextDelay(2); // previous = 300ms -> max bound is min(5s, 300ms * 3 = 900ms), r=1.0 -> 900ms

      // Assert
      await Assert.That(delay1).IsEqualTo(TimeSpan.FromMilliseconds(300));
      await Assert.That(delay2).IsEqualTo(TimeSpan.FromMilliseconds(900));
   }

   [Test]
   public async Task ExtensionMethods_CreateConfiguredJitterPolicies()
   {
      // Arrange
      var inner = new ConstantBackoffPolicy(TimeSpan.FromSeconds(10));

      // Act
      var fullJitter = inner.WithFullJitter();
      var equalJitter = inner.WithEqualJitter();
      var customJitter = inner.WithJitter(JitterMode.Proportional, 0.5);

      // Assert
      await Assert.That(fullJitter).IsTypeOf<JitterBackoffPolicy>();
      await Assert.That(equalJitter).IsTypeOf<JitterBackoffPolicy>();
      await Assert.That(customJitter).IsTypeOf<JitterBackoffPolicy>();

      var customTyped = (JitterBackoffPolicy)customJitter;
      await Assert.That(customTyped.Mode).IsEqualTo(JitterMode.Proportional);
      await Assert.That(customTyped.JitterFactor).IsEqualTo(0.5);
   }
}
