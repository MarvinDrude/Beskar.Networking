using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Common.Options;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Tests;

[NotInParallel]
public class MemoryLeakAndPoolTests
{
   [Test]
   public async Task TcpRegistryDisposal_DisposesConnectionPools()
   {
      var options = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions { MaxConnectionPoolSize = 10 },
         StreamOptions = new StreamTransportOptions { MaxConnectionPoolSize = 10 },
         ForceStreamBased = false
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var client = new TcpNetworkClient(options);

      await listener.BindAsync();
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      var acceptResult = await listener.AcceptSessionAsync();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientRegistry = typeof(TcpNetworkClient)
         .GetField("_ioQueueRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
         .GetValue(client)!;

      var listenerRegistry = typeof(TcpNetworkListener)
         .GetField("_ioQueueRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
         .GetValue(listener)!;

      var clientSocketPool = GetPool(clientRegistry, "_socketConnectionPool");
      var listenerSocketPool = GetPool(listenerRegistry, "_socketConnectionPool");

      await Assert.That(IsPoolDisposed(clientSocketPool)).IsFalse();
      await Assert.That(IsPoolDisposed(listenerSocketPool)).IsFalse();

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();

      await client.DisposeAsync();
      await listener.DisposeAsync();

      await Assert.That(IsPoolDisposed(clientSocketPool)).IsTrue();
      await Assert.That(IsPoolDisposed(listenerSocketPool)).IsTrue();
   }

    [Test]
    public async Task TcpConnections_AreGarbageCollected_AfterDispose()
    {
       var options = new TcpTransportOptions();
       var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
       await listener.BindAsync();

       var client = new TcpNetworkClient(options);

       var (sessionRef, connectionRef) = await ExecuteAndGetRefs(client, listener);

       await client.DisposeAsync();
       await listener.DisposeAsync();

       VerifyGC(sessionRef, connectionRef);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void VerifyGC(WeakReference sessionRef, WeakReference connectionRef)
    {
       for (var i = 0; i < 10; i++)
       {
          GC.Collect(2, GCCollectionMode.Forced, blocking: true);
          GC.WaitForPendingFinalizers();
          GC.Collect(2, GCCollectionMode.Forced, blocking: true);
       }

       if (sessionRef.IsAlive)
       {
          throw new Exception("Session was not garbage collected.");
       }
       if (connectionRef.IsAlive)
       {
          throw new Exception("Connection was not garbage collected.");
       }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<(WeakReference Session, WeakReference Connection)> ExecuteAndGetRefs(TcpNetworkClient client, TcpNetworkListener listener)
    {
       var task = Task.Run(async () =>
       {
          var connectResult = await client.ConnectAsync(listener.LocalAddress);
          var acceptResult = await listener.AcceptSessionAsync();

          var clientSession = connectResult.Success!;
          var serverSession = acceptResult.Success!;

          var sessionRef = new WeakReference(clientSession);

          var connectionField = typeof(TcpNetworkSession)
             .GetField("_connection", BindingFlags.NonPublic | BindingFlags.Instance)!;
          var connectionObj = connectionField.GetValue(clientSession);
          var connectionRef = new WeakReference(connectionObj);

          var streamResult = await clientSession.OpenStreamAsync();
          var serverStreamResult = await serverSession.AcceptStreamAsync();

          var clientStream = streamResult.Success!;
          var payload = "Test payload"u8.ToArray();
          await clientStream.Transport.Output.WriteAsync(payload);
          await clientStream.Transport.Output.FlushAsync();

          await clientSession.DisposeAsync();
          await serverSession.DisposeAsync();

          return (sessionRef, connectionRef);
       });

       return await task;
    }

   [Test]
   public async Task TcpConnectionPool_ReusesConnectionsCorrectly_WithoutStarvation()
   {
      var options = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions { MaxConnectionPoolSize = 2 },
         ForceStreamBased = false
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);

      var listenerRegistry = typeof(TcpNetworkListener)
         .GetField("_ioQueueRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
         .GetValue(listener)!;

      var listenerSocketPool = GetPool(listenerRegistry, "_socketConnectionPool")!;

      var initialCached = PoolDiagnostics.GetCachedCount(listenerSocketPool);
      await Assert.That(initialCached).IsEqualTo(0);

      object? firstConnectionObj = null;

      for (int i = 0; i < 5; i++)
      {
         var connectResult = await client.ConnectAsync(listener.LocalAddress);
         var acceptResult = await listener.AcceptSessionAsync();

         var clientSession = connectResult.Success!;
         var serverSession = acceptResult.Success!;

         var serverConnectionField = typeof(TcpNetworkSession)
            .GetField("_connection", BindingFlags.NonPublic | BindingFlags.Instance)!;
         var serverConnection = serverConnectionField.GetValue(serverSession);

         if (i == 0)
         {
            firstConnectionObj = serverConnection;
         }
         else
         {
            await Assert.That(serverConnection).IsSameReferenceAs(firstConnectionObj);
         }

         await Assert.That(PoolDiagnostics.GetCachedCount(listenerSocketPool)).IsEqualTo(0);

         await clientSession.DisposeAsync();
         await serverSession.DisposeAsync();

         await Assert.That(PoolDiagnostics.GetCachedCount(listenerSocketPool)).IsEqualTo(1);
      }

      await client.DisposeAsync();
      await listener.DisposeAsync();
   }

   private static object? GetPool(object registry, string fieldName)
   {
      var field = registry.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
      return field?.GetValue(registry);
   }

   private static bool IsPoolDisposed(object? pool)
   {
      if (pool == null) return true;
      var field = pool.GetType().GetField("_isDisposed", BindingFlags.NonPublic | BindingFlags.Instance);
      return (bool)field?.GetValue(pool)!;
   }

   [Test]
   public async Task ListenerDisposal_DrainsAndDisposesQueuedSessions()
   {
      var options = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions { MaxConnectionPoolSize = 10 },
         ForceStreamBased = false
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);

      // Let the accept loop run and enqueue the session into listener's channel.
      await Task.Delay(200);

      var listenerRegistry = typeof(TcpNetworkListener)
         .GetField("_ioQueueRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
         .GetValue(listener)!;

      var listenerSocketPool = GetPool(listenerRegistry, "_socketConnectionPool")!;

      var cachedBefore = PoolDiagnostics.GetCachedCount(listenerSocketPool);
      await Assert.That(cachedBefore).IsEqualTo(0);

      // Now, unbind/dispose the listener. This should drain and dispose the queued session.
      await listener.UnbindAsync();

      // Draining and disposing the session returns the socket connection to the pool.
      await Task.Delay(100);

      var cachedAfter = PoolDiagnostics.GetCachedCount(listenerSocketPool);
      await Assert.That(cachedAfter).IsEqualTo(1);

      await listener.DisposeAsync();
      await client.DisposeAsync();
   }

   [Test]
   public async Task MemoryPool_DoesNotLeakBlocks_AfterSessionDispose()
   {
      var options = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions { MaxConnectionPoolSize = 10 },
         ForceStreamBased = false
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);

      var listenerRegistry = typeof(TcpNetworkListener)
         .GetField("_ioQueueRegistry", BindingFlags.NonPublic | BindingFlags.Instance)!
         .GetValue(listener)!;

      var ioQueues = listenerRegistry.GetType()
         .GetField("_ioQueues", BindingFlags.NonPublic | BindingFlags.Instance)!
         .GetValue(listenerRegistry) as Array;

      var ioQueue = ioQueues!.GetValue(0)!;
      var socketSettings = ioQueue.GetType()
         .GetProperty("SocketSettings", BindingFlags.Public | BindingFlags.Instance)!
         .GetValue(ioQueue)!;

      var memoryPool = socketSettings.GetType()
         .GetProperty("MemoryPool", BindingFlags.Public | BindingFlags.Instance)!
         .GetValue(socketSettings) as PinnedBlockMemoryPool;

      await Assert.That(memoryPool).IsNotNull();

      // Warmup cycle to allocate buffers
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      var acceptResult = await listener.AcceptSessionAsync();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStreamResult = await clientSession.OpenStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      // Send and receive some data to force pool allocations
      var payload = new byte[16384]; // 16 KB (4 blocks)
      new Random().NextBytes(payload);

      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var readResult = await serverStream.Transport.Input.ReadAsync();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      // Dispose streams and sessions
      await clientStream.DisposeAsync();
      await serverStream.DisposeAsync();
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();

      // Perform GC to ensure everything is returned
      for (var gc = 0; gc < 10; gc++)
      {
         GC.Collect(2, GCCollectionMode.Forced, blocking: true);
         GC.WaitForPendingFinalizers();
         GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      }

      var cachedWarmup = -1;

      // Run 5 more cycles and verify cached blocks count remains stable (no leaks)
      for (var i = 0; i < 5; i++)
      {
         connectResult = await client.ConnectAsync(listener.LocalAddress);
         acceptResult = await listener.AcceptSessionAsync();

         clientSession = connectResult.Success!;
         serverSession = acceptResult.Success!;

         clientStreamResult = await clientSession.OpenStreamAsync();
         serverStreamResult = await serverSession.AcceptStreamAsync();

         clientStream = clientStreamResult.Success!;
         serverStream = serverStreamResult.Success!;

         await clientStream.Transport.Output.WriteAsync(payload);
         await clientStream.Transport.Output.FlushAsync();

         readResult = await serverStream.Transport.Input.ReadAsync();
         serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

         await clientStream.DisposeAsync();
         await serverStream.DisposeAsync();
         await clientSession.DisposeAsync();
         await serverSession.DisposeAsync();

         for (var gc = 0; gc < 10; gc++)
         {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
         }

         var cachedCurrent = PoolDiagnostics.GetCachedBlocksCount(memoryPool!);
         if (i == 0)
         {
            cachedWarmup = cachedCurrent;
         }
         else
         {
            await Assert.That(cachedCurrent).IsEqualTo(cachedWarmup);
         }
      }

      await client.DisposeAsync();
      await listener.DisposeAsync();
   }

   [Test]
   public async Task RegistryDisposalOrPoolFull_DisposesRejectedConnections()
   {
      var options = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions { MaxConnectionPoolSize = 2 },
         StreamOptions = new StreamTransportOptions { MaxConnectionPoolSize = 2 },
         ForceStreamBased = false
      };

      var registry = new TcpIoQueueRegistry(options);

      var socket1 = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      var socket2 = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      var socket3 = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      var socket4 = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);

      var conn1 = (Beskar.Networking.Transports.Common.Sockets.SocketConnection)registry.Create(socket1);
      var conn2 = (Beskar.Networking.Transports.Common.Sockets.SocketConnection)registry.Create(socket2);
      var conn3 = (Beskar.Networking.Transports.Common.Sockets.SocketConnection)registry.Create(socket3);
      var conn4 = (Beskar.Networking.Transports.Common.Sockets.SocketConnection)registry.Create(socket4);

      var registrySocketPool = (AsyncDisposableObjectPool<Beskar.Networking.Transports.Common.Sockets.SocketConnection>)GetPool(registry, "_socketConnectionPool")!;

      await conn1.StopAsync();
      await conn2.StopAsync();
      await conn3.StopAsync();
      await conn4.StopAsync();

      await registry.ReturnAsync(conn1);
      await registry.ReturnAsync(conn2);
      await registry.ReturnAsync(conn3);
      await registry.ReturnAsync(conn4);

      var isDisposedField = typeof(Beskar.Networking.Transports.Common.Sockets.SocketConnection).GetField("_isDisposed", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

      var isConn1Disposed = (bool)isDisposedField.GetValue(conn1)!;
      var isConn2Disposed = (bool)isDisposedField.GetValue(conn2)!;
      var isConn3Disposed = (bool)isDisposedField.GetValue(conn3)!;
      var isConn4Disposed = (bool)isDisposedField.GetValue(conn4)!;

      // conn1, conn2, conn3 should NOT be disposed because they fit in the pool (capacity MaxSize + 1)
      await Assert.That(isConn1Disposed).IsFalse();
      await Assert.That(isConn2Disposed).IsFalse();
      await Assert.That(isConn3Disposed).IsFalse();

      // conn4 should be automatically disposed by the registry because the pool was full!
      await Assert.That(isConn4Disposed).IsTrue();

      await registry.DisposeAsync();
   }

   [Test]
   public async Task PoolDisposal_DisposesCachedConnections()
   {
      var options = new TcpTransportOptions
      {
         SocketOptions = new SocketTransportOptions { MaxConnectionPoolSize = 10 },
         StreamOptions = new StreamTransportOptions { MaxConnectionPoolSize = 10 },
         ForceStreamBased = false
      };

      var registry = new TcpIoQueueRegistry(options);
      var socket1 = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      var conn1 = (Beskar.Networking.Transports.Common.Sockets.SocketConnection)registry.Create(socket1);

      await conn1.StopAsync();
      await registry.ReturnAsync(conn1);

      var isDisposedField = typeof(Beskar.Networking.Transports.Common.Sockets.SocketConnection).GetField("_isDisposed", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
      
      // Before registry disposal, conn1 should NOT be disposed (it's cached in the pool)
      await Assert.That((bool)isDisposedField.GetValue(conn1)!).IsFalse();

      // Dispose the registry, which disposes the pool
      await registry.DisposeAsync();

      // After registry disposal, conn1 MUST be disposed!
      await Assert.That((bool)isDisposedField.GetValue(conn1)!).IsTrue();
   }
}
