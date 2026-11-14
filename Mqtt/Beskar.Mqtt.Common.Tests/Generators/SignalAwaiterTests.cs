using System.Reflection;
using Beskar.Mqtt.Common.Generators;

namespace Beskar.Mqtt.Common.Tests.Generators;

[NotInParallel(nameof(SignalBrokerTests))]
public class SignalAwaiterTests
{
   [Test]
   public async Task WaitOneAsync_AfterCompletionOrReset_ShouldNotRegisterCancellationCallback()
   {
      var broker = new SignalBroker();
      var regField = typeof(SignalAwaiter<int>).GetField("_cancellationRegistration", BindingFlags.NonPublic | BindingFlags.Instance)!;

      var completedAwaiter = broker.AddAwaitable<int>(1);
      var dispatched = broker.TryDispatch<int>(42, 1);
      await Assert.That(dispatched).IsTrue();

      using var cts = new CancellationTokenSource();
      var value = await completedAwaiter.WaitOneAsync(cts.Token);
      await Assert.That(value).IsEqualTo(42);

      var registration = (CancellationTokenRegistration)regField.GetValue(completedAwaiter)!;
      try
      {
         await Assert.That(registration).IsEqualTo(default(CancellationTokenRegistration));
      }
      finally
      {
         completedAwaiter.Dispose();
      }
   }
}
