using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Common.Options;

namespace Beskar.Networking.Transports.Common.Tests;

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
}
