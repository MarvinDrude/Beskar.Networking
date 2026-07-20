using System.Buffers;
using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Transports.Memory.Tests;

public class MemoryTransportTests
{
   [Test]
   public async Task MemoryClientServer_StandardConnection_DataExchangedSuccessfully()
   {
      var options = new MemoryTransportOptions();
      var address = $"test-channel-{Guid.NewGuid():N}";
      var endpoint = new MemoryEndPoint(address);

      var listener = new MemoryNetworkListener(endpoint, options);

      // Assert initially unbound
      await Assert.That(listener.IsBound).IsFalse();

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();
      await Assert.That(listener.IsBound).IsTrue();
      await Assert.That(listener.Stats.Binds).IsEqualTo(1);

      // Connect client
      var client = new MemoryNetworkClient(options);
      await Assert.That(client.IsConnected).IsFalse();
      var connectResult = await client.ConnectAsync(endpoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.Stats.ConnectionsEstablished).IsEqualTo(1);

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      await Assert.That(listener.Stats.SessionsAccepted).IsEqualTo(1);

      // Accept / Open streams
      var clientStreamResult = await clientSession.OpenStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      await Assert.That(clientStreamResult.Failed).IsFalse();
      await Assert.That(serverStreamResult.Failed).IsFalse();

      await Assert.That(clientSession.SessionStats.StreamsOpened).IsEqualTo(1);
      await Assert.That(serverSession.SessionStats.StreamsAccepted).IsEqualTo(1);

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      // Client writes to server
      var payload = "Hello from Memory Client!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server writes to client
      var serverPayload = "Hello from Memory Server!"u8.ToArray();
      await serverStream.Transport.Output.WriteAsync(serverPayload);
      await serverStream.Transport.Output.FlushAsync();

      // Client reads from server
      var clientReadResult = await clientStream.Transport.Input.ReadAsync();
      var clientReadBytes = clientReadResult.Buffer.ToArray();
      clientStream.Transport.Input.AdvanceTo(clientReadResult.Buffer.End);

      await Assert.That(clientReadBytes).IsEquivalentTo(serverPayload);

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();

      await Assert.That(client.Stats.ConnectionsLost).IsEqualTo(1);
      await Assert.That(listener.Stats.Unbinds).IsEqualTo(1);
   }

   [Test]
   public async Task MemoryClientServer_StandardConnection_StatsTrackedCorrectly()
   {
      var options = new MemoryTransportOptions();
      var endpoint = new MemoryEndPoint($"stats-channel-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, options);

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      // Connect client
      var client = new MemoryNetworkClient(options);
      var connectResult = await client.ConnectAsync(endpoint);
      await Assert.That(connectResult.Failed).IsFalse();

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      // Accept / Open streams
      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      await Assert.That(clientStreamResult.Failed).IsFalse();
      await Assert.That(serverStreamResult.Failed).IsFalse();

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      // Verify initial stats are 0
      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(clientStream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(clientSession.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(clientSession.Stats.BytesReceived).IsEqualTo(0);

      // Client writes to server
      var payload = "Hello stats!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Verify stats after client -> server write
      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(clientSession.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(serverStream.Stats.BytesReceived).IsEqualTo(payload.Length);
      await Assert.That(serverSession.Stats.BytesReceived).IsEqualTo(payload.Length);

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task MemoryListener_BindUnbindBindUnbind_SuccessiveCallsWork()
   {
      var options = new MemoryTransportOptions();
      var listener = new MemoryNetworkListener(new MemoryEndPoint("successive-bind"), options);

      for (var i = 0; i < 3; i++)
      {
         var bindResult = await listener.BindAsync();
         await Assert.That(bindResult.Failed).IsFalse();
         await Assert.That(listener.IsBound).IsTrue();

         await listener.UnbindAsync();
         await Assert.That(listener.IsBound).IsFalse();
      }
   }

   [Test]
   public async Task MemoryClient_ConnectToNonExistentListener_ReturnsError()
   {
      var options = new MemoryTransportOptions();
      var client = new MemoryNetworkClient(options);

      var connectResult = await client.ConnectAsync(new MemoryEndPoint("does-not-exist"));
      await Assert.That(connectResult.Failed).IsTrue();
   }

   [Test]
   public async Task MemoryListener_BindSameAddressTwice_ReturnsError()
   {
      var options = new MemoryTransportOptions();
      var address = $"duplicate-{Guid.NewGuid():N}";
      var endpoint = new MemoryEndPoint(address);

      var listener1 = new MemoryNetworkListener(endpoint, options);
      var listener2 = new MemoryNetworkListener(endpoint, options);

      var bindResult1 = await listener1.BindAsync();
      await Assert.That(bindResult1.Failed).IsFalse();

      var bindResult2 = await listener2.BindAsync();
      await Assert.That(bindResult2.Failed).IsTrue();

      await listener1.UnbindAsync();
   }

   [Test]
   public async Task MemorySession_DisposingClientSession_DisposesPeerSession()
   {
      var options = new MemoryTransportOptions();
      var endpoint = new MemoryEndPoint($"dispose-peer-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, options);

      await listener.BindAsync();

      var client = new MemoryNetworkClient(options);
      var connectResult = await client.ConnectAsync(endpoint);
      var acceptResult = await listener.AcceptSessionAsync();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientClosedToken = clientSession.SessionClosedToken;
      var serverClosedToken = serverSession.SessionClosedToken;

      await Assert.That(clientClosedToken.IsCancellationRequested).IsFalse();
      await Assert.That(serverClosedToken.IsCancellationRequested).IsFalse();

      // Dispose client session
      await clientSession.DisposeAsync();

      // Both should be closed
      await Assert.That(clientClosedToken.IsCancellationRequested).IsTrue();
      await Assert.That(serverClosedToken.IsCancellationRequested).IsTrue();

      await listener.UnbindAsync(serverClosedToken);
   }
}
