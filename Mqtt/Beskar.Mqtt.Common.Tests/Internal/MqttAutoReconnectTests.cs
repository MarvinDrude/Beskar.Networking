using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Interfaces.Misc;
using Beskar.Networking.Abstractions.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttAutoReconnectTests
{
   [Test]
   public async Task Client_AutoReconnect_UsesCustomBackoffPolicy()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var attemptsEvaluated = new List<int>();
      var testBackoff = new TestBackoffPolicy((attempt) =>
      {
         lock (attemptsEvaluated) attemptsEvaluated.Add(attempt);
         return TimeSpan.FromMilliseconds(50);
      });

      var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithCleanSession()
         .WithClientId($"auto-reconnect-backoff-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 5,
            BackoffPolicy = testBackoff
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      var reconnectedTcs = new TaskCompletionSource();
      var connectCount = 0;

      client.Events.OnClientConnected.Add((_, _) =>
      {
         if (Interlocked.Increment(ref connectCount) == 2)
         {
            reconnectedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Simulate ungraceful server drop by disposing session on server
      using (var clients = await server.ClientSessions.GetClients())
      {
         await clients.WrittenSpan[0].Session.DisposeAsync();
      }

      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(client.IsConnected).IsTrue();

      int evaluatedCount;
      int firstAttempt;
      lock (attemptsEvaluated)
      {
         evaluatedCount = attemptsEvaluated.Count;
         firstAttempt = evaluatedCount > 0 ? attemptsEvaluated[0] : -1;
      }

      await Assert.That(evaluatedCount).IsGreaterThan(0);
      await Assert.That(firstAttempt).IsEqualTo(1);

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Client_AutoReconnect_ExplicitDisconnect_CancelsReconnectLoop()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithCleanSession()
         .WithClientId($"auto-reconnect-cancel-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 10,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromSeconds(30)) // Long delay
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectedTcs = new TaskCompletionSource();

      client.Events.OnClientDisconnected.Add((_, _) =>
      {
         disconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Explicit disconnect should not trigger auto-reconnect
      await client.DisconnectAsync(new DisconnectOptions());
      await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

      await Task.Delay(200);
      await Assert.That(client.IsConnected).IsFalse();

      await server.StopAsync();
   }

   private class TestBackoffPolicy(Func<int, TimeSpan> getDelay) : IBackoffPolicy
   {
      public TimeSpan GetNextDelay(int attempt) => getDelay(attempt);
   }
}
