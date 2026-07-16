using System.Net;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Managed;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Options;
using ConnectionState = Beskar.Networking.Abstractions.Managed.ConnectionState;

namespace Beskar.Networking.Transports.Common.Tests;

public class NetworkClientTests
{
   private readonly EndPoint _testEndPoint = new IPEndPoint(IPAddress.Loopback, 12345);

   [Test]
   public async Task ConnectAsync_SuccessfulConnection_TransitionsToConnectedAndInvokesHandlers()
   {
      // Arrange
      var fakeSession = new FakeNetworkSession();
      var fakeInnerClient = new FakeNetworkClient
      {
         OnConnectAsync = (ep, ct) => ValueTask.FromResult(new Result<INetworkSession, NetworkCodeError>(fakeSession))
      };

      var client = new NetworkClient(fakeInnerClient, new AutoReconnectOptions { IsEnabled = false });

      var connectedHandlerInvoked = false;
      INetworkSession? receivedSession = null;

      client.Connected.Add((ev, ct) =>
      {
         connectedHandlerInvoked = true;
         receivedSession = ev.Session;
         return ValueTask.CompletedTask;
      });

      // Act
      await Assert.That(client.IsConnected).IsFalse();
      var result = await client.ConnectAsync(_testEndPoint);

      // Assert
      await Assert.That(result.IsSuccess).IsTrue();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.State).IsEqualTo(ConnectionState.Connected);
      await Assert.That(client.Session).IsEqualTo(fakeSession);
      await Assert.That(connectedHandlerInvoked).IsTrue();
      await Assert.That(receivedSession).IsEqualTo(fakeSession);
      await Assert.That(fakeInnerClient.ConnectCount).IsEqualTo(1);
   }

   [Test]
   public async Task Connected_MultipleHandlers_AllExecutedSequentially()
   {
      // Arrange
      var fakeSession = new FakeNetworkSession();
      var fakeInnerClient = new FakeNetworkClient
      {
         OnConnectAsync = (ep, ct) => ValueTask.FromResult(new Result<INetworkSession, NetworkCodeError>(fakeSession))
      };

      var client = new NetworkClient(fakeInnerClient, new AutoReconnectOptions { IsEnabled = false });

      var orderString = "";

      client.Connected.Add((ev, ct) =>
      {
         orderString += "A";
         return ValueTask.CompletedTask;
      });

      client.Connected.Add(async (ev, ct) =>
      {
         await Task.Delay(10, ct);
         orderString += "B";
      });

      client.Connected.Add((ev, ct) =>
      {
         orderString += "C";
         return ValueTask.CompletedTask;
      });

      // Act
      await client.ConnectAsync(_testEndPoint);

      // Assert
      await Assert.That(orderString).IsEqualTo("ABC");
   }

   [Test]
   public async Task AutoReconnect_ClientFailsInitiallyThenSucceeds_ReconnectsSuccessfully()
   {
      // Arrange
      var fakeSession = new FakeNetworkSession();
      var attemptCount = 0;
      var fakeInnerClient = new FakeNetworkClient
      {
         OnConnectAsync = (ep, ct) =>
         {
            attemptCount++;
            if (attemptCount <= 2)
               return ValueTask.FromResult(
                  new Result<INetworkSession, NetworkCodeError>(new NetworkCodeError(-1, "Failed to connect")));
            return ValueTask.FromResult(new Result<INetworkSession, NetworkCodeError>(fakeSession));
         }
      };

      var options = new AutoReconnectOptions
      {
         IsEnabled = true,
         MaxRetryAttempts = 5,
         BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(5))
      };

      var client = new NetworkClient(fakeInnerClient, options);

      var reconnectAttempts = 0;
      client.Reconnecting.Add((ev, ct) =>
      {
         reconnectAttempts++;
         return ValueTask.CompletedTask;
      });

      var connectedTcs = new TaskCompletionSource();
      client.Connected.Add((ev, ct) =>
      {
         connectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Act
      var initialResult = await client.ConnectAsync(_testEndPoint);

      // Assert Initial failure return, but triggers background reconnection
      await Assert.That(initialResult.Failed).IsTrue();
      await Assert.That(client.State).IsEqualTo(ConnectionState.Reconnecting);

      // Wait until connection succeeds (using TCS)
      await connectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(client.State).IsEqualTo(ConnectionState.Connected);
      await Assert.That(fakeInnerClient.ConnectCount).IsEqualTo(3); // 1 initial + 2 reconnect attempts
      await Assert.That(reconnectAttempts).IsEqualTo(2);
   }

   [Test]
   public async Task AutoReconnect_ReachesMaxAttempts_TransitionsToFailed()
   {
      // Arrange
      var fakeInnerClient = new FakeNetworkClient
      {
         OnConnectAsync = (ep, ct) =>
            ValueTask.FromResult(new Result<INetworkSession, NetworkCodeError>(new NetworkCodeError(-1, "Always fail")))
      };

      var options = new AutoReconnectOptions
      {
         IsEnabled = true,
         MaxRetryAttempts = 2,
         BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(5))
      };

      var client = new NetworkClient(fakeInnerClient, options);

      var failedTcs = new TaskCompletionSource();
      client.ConnectionFailed.Add((ev, ct) =>
      {
         failedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Act
      await client.ConnectAsync(_testEndPoint);

      // Wait for event to fire (using TCS)
      await failedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Assert
      await Assert.That(client.State).IsEqualTo(ConnectionState.Failed);
      await Assert.That(fakeInnerClient.ConnectCount).IsEqualTo(3); // 1 initial + 2 reconnect attempts
   }

   [Test]
   public async Task SessionClosed_TriggersReconnectionLoop()
   {
      // Arrange
      var fakeSession = new FakeNetworkSession();
      var fakeSession2 = new FakeNetworkSession();
      var attemptCount = 0;
      var fakeInnerClient = new FakeNetworkClient
      {
         OnConnectAsync = (ep, ct) =>
         {
            attemptCount++;
            return ValueTask.FromResult(
               new Result<INetworkSession, NetworkCodeError>(attemptCount == 1 ? fakeSession : fakeSession2));
         }
      };

      var options = new AutoReconnectOptions
      {
         IsEnabled = true,
         MaxRetryAttempts = 3,
         BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(5))
      };

      var client = new NetworkClient(fakeInnerClient, options);

      // Act
      var connectResult = await client.ConnectAsync(_testEndPoint);
      await Assert.That(connectResult.IsSuccess).IsTrue();
      await Assert.That(client.State).IsEqualTo(ConnectionState.Connected);

      var reconnectConnectedTcs = new TaskCompletionSource();
      client.Connected.Add((ev, ct) =>
      {
         reconnectConnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Trigger session closed
      fakeSession.Close();

      // Wait for reconnect to complete (using TCS)
      await reconnectConnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Assert
      await Assert.That(client.State).IsEqualTo(ConnectionState.Connected);
      await Assert.That(client.Session).IsEqualTo(fakeSession2);
      await Assert.That(fakeInnerClient.ConnectCount).IsEqualTo(2); // 1 initial + 1 reconnect
   }

   [Test]
   public async Task DisconnectAsync_AbortsReconnectionLoop()
   {
      // Arrange
      var fakeInnerClient = new FakeNetworkClient
      {
         OnConnectAsync = (ep, ct) =>
            ValueTask.FromResult(new Result<INetworkSession, NetworkCodeError>(new NetworkCodeError(-1, "Always fail")))
      };

      var options = new AutoReconnectOptions
      {
         IsEnabled = true,
         MaxRetryAttempts = 100, // Large number of attempts
         BackoffPolicy = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(10))
      };

      var client = new NetworkClient(fakeInnerClient, options);

      var disconnectedTcs = new TaskCompletionSource();
      client.Disconnected.Add((ev, ct) =>
      {
         disconnectedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Act
      await client.ConnectAsync(_testEndPoint);
      await Task.Delay(50); // Let reconnection start

      await Assert.That(client.State).IsEqualTo(ConnectionState.Reconnecting);

      await client.DisconnectAsync();

      await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Assert
      await Assert.That(client.State).IsEqualTo(ConnectionState.Disconnected);

      var countAfterDisconnect = fakeInnerClient.ConnectCount;
      await Task.Delay(100); // Wait more time

      // Verify no more connect attempts occur
      await Assert.That(fakeInnerClient.ConnectCount).IsEqualTo(countAfterDisconnect);
   }
}

public class FakeNetworkSession : INetworkSession, IAsyncDisposable
{
   private readonly CancellationTokenSource _cts = new();

   public bool Disposed { get; private set; }
   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
   public TransportKind Transport => TransportKind.Unknown;

   public Guid Id { get; } = Guid.NewGuid();
   public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
   public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;
   public CancellationToken SessionClosedToken => _cts.Token;
   public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
   public NetworkStats Stats => default;

   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask DisposeAsync()
   {
      Disposed = true;
      _cts.Dispose();
      return ValueTask.CompletedTask;
   }

   public void Close()
   {
      _cts.Cancel();
   }
}

public class FakeNetworkClient : INetworkClient
{
   private int _connectCount;

   private int _disconnectCount;
   public int ConnectCount => Volatile.Read(ref _connectCount);
   public int DisconnectCount => Volatile.Read(ref _disconnectCount);

   public Func<EndPoint, CancellationToken, ValueTask<Result<INetworkSession, NetworkCodeError>>>? OnConnectAsync
   {
      get;
      set;
   }

   public Func<CancellationToken, ValueTask>? OnDisconnectAsync { get; set; }

   public TransportKind Transport => TransportKind.Unknown;

   public bool IsConnected => ConnectCount > DisconnectCount;

   public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint,
      CancellationToken ct = default)
   {
      Interlocked.Increment(ref _connectCount);
      if (OnConnectAsync is not null) return OnConnectAsync(endPoint, ct);
      return ValueTask.FromResult(new Result<INetworkSession, NetworkCodeError>(
         new NetworkCodeError(-1, "Not configured")));
   }

   public ValueTask DisconnectAsync(CancellationToken ct = default)
   {
      Interlocked.Increment(ref _disconnectCount);
      if (OnDisconnectAsync is not null) return OnDisconnectAsync(ct);
      return ValueTask.CompletedTask;
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
   }
}
