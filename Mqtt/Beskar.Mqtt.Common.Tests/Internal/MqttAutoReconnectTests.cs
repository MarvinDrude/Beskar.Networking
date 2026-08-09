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
      var reconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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
      var disconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

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

      await client.DisconnectAsync(new DisconnectOptions());
      await server.StopAsync();
   }

   [Test]
   public async Task Client_AutoReconnect_MaxRetriesExceeded_TransitionsToDisconnected()
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
         .WithClientId($"auto-reconnect-maxretries-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 2,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(10))
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Stop server permanently so all 2 retry attempts fail
      await server.StopAsync();

      // Drop active session
      using (var clients = await server.ClientSessions.GetClients())
      {
         if (!clients.WrittenSpan.IsEmpty)
         {
            await clients.WrittenSpan[0].Session.DisposeAsync();
         }
      }

      // Wait for max retries to be exhausted
      await Task.Delay(500);

      await Assert.That(client.IsConnected).IsFalse();
   }

   [Test]
   public async Task Client_AutoReconnect_DisposeAsyncDuringRetry_CancelsImmediatelyWithoutHang()
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
         .WithClientId($"auto-reconnect-dispose-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 10,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromSeconds(30)) // 30s delay
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Stop server to trigger reconnection loop
      await server.StopAsync();

      using (var clients = await server.ClientSessions.GetClients())
      {
         if (!clients.WrittenSpan.IsEmpty)
         {
            await clients.WrittenSpan[0].Session.DisposeAsync();
         }
      }

      await Task.Delay(100);

      var sw = System.Diagnostics.Stopwatch.StartNew();
      await client.DisposeAsync();
      sw.Stop();

      // DisposeAsync must cancel backoff delay instantly and exit within 2 seconds
      await Assert.That(sw.ElapsedMilliseconds).IsLessThan(2000);
      await Assert.That(client.IsConnected).IsFalse();
   }

   [Test]
   public async Task Client_AutoReconnect_MultipleSequentialDrops_ReconnectsRepeatedly()
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
         .WithClientId($"auto-reconnect-multi-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 5,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(20))
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Perform 3 sequential ungraceful drops
      for (var i = 0; i < 3; i++)
      {
         var reconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         using var handlerToken = client.Events.OnClientConnected.Add((_, _) =>
         {
            reconnectedTcs.TrySetResult();
            return ValueTask.CompletedTask;
         });

         await client.NetworkClient.DisconnectAsync();

         await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));
         await Assert.That(client.IsConnected).IsTrue();
      }

      await client.DisconnectAsync(new DisconnectOptions());
      await server.StopAsync();
   }

   [Test]
   public async Task Client_ConnectEvent_CalledAfterReconnect()
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
         .WithClientId($"auto-reconnect-event-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 5,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(50))
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      var connectedCount = 0;
      var reconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      client.Events.OnClientConnected.Add((_, _) =>
      {
         var count = Interlocked.Increment(ref connectedCount);
         if (count == 2)
         {
            reconnectedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(connectedCount).IsEqualTo(1);

      // Trigger ungraceful disconnect from server side
      using (var clients = await server.ClientSessions.GetClients())
      {
         await clients.WrittenSpan[0].Session.DisposeAsync();
      }

      await reconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(connectedCount).IsEqualTo(2);
      await Assert.That(client.IsConnected).IsTrue();

      await client.DisconnectAsync(new DisconnectOptions());
      await server.StopAsync();
   }

   [Test]
   public async Task Client_InitialConnectFails_RetriesInBackgroundWhenAutoReconnectEnabled()
   {
      // Use an unused port first
      var portFinderListener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
      portFinderListener.Start();
      var port = ((IPEndPoint)portFinderListener.LocalEndpoint).Port;
      portFinderListener.Stop();

      var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithCleanSession()
         .WithClientId($"auto-reconnect-init-fail-{Guid.NewGuid():N}")
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 10,
            BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(50))
         })
         .Build();

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      var connectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      client.Events.OnClientConnected.Add((_, _) =>
      {
         connectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Initial connect fails because server is not running yet
      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();

      // Now start the server on that port
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // Background auto-reconnect loop should retry and connect successfully
      await connectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));
      await Assert.That(client.IsConnected).IsTrue();

      await client.DisconnectAsync(new DisconnectOptions());
      await server.StopAsync();
   }

   private class TestBackoffPolicy(Func<int, TimeSpan> getDelay) : IBackoffPolicy
   {
      public TimeSpan GetNextDelay(int attempt) => getDelay(attempt);
   }
}
