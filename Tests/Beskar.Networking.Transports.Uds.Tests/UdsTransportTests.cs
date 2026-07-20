using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Uds;
using Beskar.Networking.Transports.Uds.Extensions;

namespace Beskar.Networking.Transports.Uds.Tests;

public class UdsTransportTests
{
   private static string GetTempSocketPath()
   {
      var name = $"beskar-test-{Guid.NewGuid():N}.sock";
      return Path.Combine(Path.GetTempPath(), name);
   }

   [Test]
   public async Task UdsClientServer_StandardConnection_DataExchangedSuccessfully()
   {
      var socketPath = GetTempSocketPath();
      var localEndPoint = new UnixDomainSocketEndPoint(socketPath);
      var options = new UdsTransportOptions();
      var listener = new UdsNetworkListener(localEndPoint, options);

      // Assert initially unbound
      await Assert.That(listener.IsBound).IsFalse();

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();
      await Assert.That(listener.IsBound).IsTrue();
      await Assert.That(listener.Stats.Binds).IsEqualTo(1);

      // Connect client
      var client = new UdsNetworkClient(options);
      await Assert.That(client.IsConnected).IsFalse();
      var connectResult = await client.ConnectAsync(localEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.Stats.ConnectionsEstablished).IsEqualTo(1);

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      await Assert.That(listener.Stats.SessionsAccepted).IsEqualTo(1);

      // Assert session security info
      await Assert.That(clientSession.SecurityInfo.IsEncrypted).IsFalse();
      await Assert.That(serverSession.SecurityInfo.IsEncrypted).IsFalse();

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
      var payload = "Hello from UDS Client!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server writes to client
      var serverPayload = "Hello from UDS Server!"u8.ToArray();
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

      // Verify file is deleted on unbind
      await Assert.That(File.Exists(socketPath)).IsFalse();
   }

   [Test]
   public async Task UdsClient_DisconnectAsync_ClosesSessionAndCancelsSessionClosedToken()
   {
      var socketPath = GetTempSocketPath();
      var localEndPoint = new UnixDomainSocketEndPoint(socketPath);
      var options = new UdsTransportOptions();
      var listener = new UdsNetworkListener(localEndPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new UdsNetworkClient(options);
      var connectResult = await client.ConnectAsync(localEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var sessionClosedToken = clientSession.SessionClosedToken;

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsFalse();

      await client.DisconnectAsync(sessionClosedToken);

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsTrue();

      await listener.UnbindAsync(sessionClosedToken);
   }

   [Test]
   public async Task UdsClientServer_StandardConnection_StatsTrackedCorrectly()
   {
      var socketPath = GetTempSocketPath();
      var localEndPoint = new UnixDomainSocketEndPoint(socketPath);
      var options = new UdsTransportOptions();
      var listener = new UdsNetworkListener(localEndPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new UdsNetworkClient(options);
      var connectResult = await client.ConnectAsync(localEndPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      await Assert.That(clientStreamResult.Failed).IsFalse();
      await Assert.That(serverStreamResult.Failed).IsFalse();

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(clientStream.Stats.BytesReceived).IsEqualTo(0);

      var payload = "Hello UDS Stats"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var readResult = await serverStream.Transport.Input.ReadAsync();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(clientSession.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(serverStream.Stats.BytesReceived).IsEqualTo(payload.Length);
      await Assert.That(serverSession.Stats.BytesReceived).IsEqualTo(payload.Length);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task UdsListener_BindUnbindBindUnbind_SuccessiveCallsWork()
   {
      var socketPath = GetTempSocketPath();
      var localEndPoint = new UnixDomainSocketEndPoint(socketPath);
      var options = new UdsTransportOptions();
      var listener = new UdsNetworkListener(localEndPoint, options);

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
   public async Task UdsClientServer_MultipleClients_DataExchangedWithoutLeakage()
   {
      var socketPath = GetTempSocketPath();
      var localEndPoint = new UnixDomainSocketEndPoint(socketPath);
      var options = new UdsTransportOptions();
      var listener = new UdsNetworkListener(localEndPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client1 = new UdsNetworkClient(options);
      var client2 = new UdsNetworkClient(options);

      var connectResult1 = await client1.ConnectAsync(localEndPoint);
      var connectResult2 = await client2.ConnectAsync(localEndPoint);

      await Assert.That(connectResult1.Failed).IsFalse();
      await Assert.That(connectResult2.Failed).IsFalse();

      var clientSession1 = connectResult1.Success!;
      var clientSession2 = connectResult2.Success!;

      var acceptResult1 = await listener.AcceptSessionAsync();
      var acceptResult2 = await listener.AcceptSessionAsync();

      await Assert.That(acceptResult1.Failed).IsFalse();
      await Assert.That(acceptResult2.Failed).IsFalse();

      var serverSession1 = acceptResult1.Success!;
      var serverSession2 = acceptResult2.Success!;

      var clientStream1 = (await clientSession1.OpenStreamAsync()).Success!;
      var clientStream2 = (await clientSession2.OpenStreamAsync()).Success!;

      var identity1 = "Client1-Identity"u8.ToArray();
      await clientStream1.Transport.Output.WriteAsync(identity1);
      await clientStream1.Transport.Output.FlushAsync();

      var identity2 = "Client2-Identity"u8.ToArray();
      await clientStream2.Transport.Output.WriteAsync(identity2);
      await clientStream2.Transport.Output.FlushAsync();

      var serverStream1 = (await serverSession1.AcceptStreamAsync()).Success!;
      var serverStream2 = (await serverSession2.AcceptStreamAsync()).Success!;

      var serverReadResult1 = await serverStream1.Transport.Input.ReadAsync();
      var serverReadBytes1 = serverReadResult1.Buffer.ToArray();
      serverStream1.Transport.Input.AdvanceTo(serverReadResult1.Buffer.End);

      var serverReadResult2 = await serverStream2.Transport.Input.ReadAsync();
      var serverReadBytes2 = serverReadResult2.Buffer.ToArray();
      serverStream2.Transport.Input.AdvanceTo(serverReadResult2.Buffer.End);

      INetworkStream serverStreamForClient1;
      INetworkStream serverStreamForClient2;

      if (serverReadBytes1.SequenceEqual(identity1))
      {
         serverStreamForClient1 = serverStream1;
         serverStreamForClient2 = serverStream2;
         await Assert.That(serverReadBytes2).IsEquivalentTo(identity2);
      }
      else
      {
         serverStreamForClient1 = serverStream2;
         serverStreamForClient2 = serverStream1;
         await Assert.That(serverReadBytes1).IsEquivalentTo(identity2);
         await Assert.That(serverReadBytes2).IsEquivalentTo(identity1);
      }

      var msg1 = "Payload For Client 1"u8.ToArray();
      var msg2 = "Payload For Client 2"u8.ToArray();

      await serverStreamForClient1.Transport.Output.WriteAsync(msg1);
      await serverStreamForClient1.Transport.Output.FlushAsync();

      await serverStreamForClient2.Transport.Output.WriteAsync(msg2);
      await serverStreamForClient2.Transport.Output.FlushAsync();

      var clientReadResult1 = await clientStream1.Transport.Input.ReadAsync();
      var clientReadBytes1 = clientReadResult1.Buffer.ToArray();
      clientStream1.Transport.Input.AdvanceTo(clientReadResult1.Buffer.End);
      await Assert.That(clientReadBytes1).IsEquivalentTo(msg1);

      var clientReadResult2 = await clientStream2.Transport.Input.ReadAsync();
      var clientReadBytes2 = clientReadResult2.Buffer.ToArray();
      clientStream2.Transport.Input.AdvanceTo(clientReadResult2.Buffer.End);
      await Assert.That(clientReadBytes2).IsEquivalentTo(msg2);

      await clientSession1.DisposeAsync();
      await clientSession2.DisposeAsync();
      await serverSession1.DisposeAsync();
      await serverSession2.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task UdsClientServer_PathExceedsLengthLimit_ReturnsFailedResult()
   {
      var baseTempPath = Path.GetTempPath();
      var remainingLength = 106 - baseTempPath.Length;
      if (remainingLength < 5)
      {
         return;
      }
      var longSocketPath = baseTempPath + new string('a', remainingLength);
      
      var localEndPoint = new UnixDomainSocketEndPoint(longSocketPath);
      var options = new UdsTransportOptions();
      var listener = new UdsNetworkListener(localEndPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsTrue();
      await Assert.That(bindResult.Error!.Message).Contains("exceeds the maximum allowed length of 104 characters");

      var client = new UdsNetworkClient(options);
      var connectResult = await client.ConnectAsync(localEndPoint);
      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(connectResult.Error!.Message).Contains("exceeds the maximum allowed length of 104 characters");
   }

   [Test]
   public async Task UdsExtensions_RegisterCorrectly()
   {
      // Test ServerBuilder extension
      var builder = new MockServerBuilder();
      builder.UseUds(12345);

      await Assert.That(builder.Listener).IsNotNull();
      await Assert.That(builder.Listener!.Transport).IsEqualTo(TransportKind.UnixDomainSocket);

      // Test ClientFactory extension
      var client = MockClientFactory.UseUds<MockClientFactory, INetworkClient>();
      await Assert.That(client).IsNotNull();
      await Assert.That(client.Transport).IsEqualTo(TransportKind.UnixDomainSocket);
   }
}

public class MockServerBuilder : IServerBuilder<MockServerBuilder>
{
   public INetworkListener? Listener { get; private set; }

   public MockServerBuilder Use(INetworkListener listener)
   {
      Listener = listener;
      return this;
   }
}

public class MockClientFactory : IClientFactory<INetworkClient>
{
   public static INetworkClient Create(INetworkClient client) => client;
}
