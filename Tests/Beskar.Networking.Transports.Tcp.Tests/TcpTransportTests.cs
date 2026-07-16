using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Transports.Quic;

namespace Beskar.Networking.Transports.Tcp.Tests;

public class TcpTransportTests
{
   private static int GetFreePort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));

      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task TcpClientServer_StandardConnection_DataExchangedSuccessfully()
   {
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(endPoint, options);

      // Assert initially unbound
      await Assert.That(listener.IsBound).IsFalse();

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();
      await Assert.That(listener.IsBound).IsTrue();
      await Assert.That(listener.Stats.Binds).IsEqualTo(1);

      // Connect client
      var client = new TcpNetworkClient(options);
      await Assert.That(client.IsConnected).IsFalse();
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.Stats.ConnectionsEstablished).IsEqualTo(1);

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      await Assert.That(listener.Stats.SessionsAccepted).IsEqualTo(1);

      // Assert session security info for non-SSL connection
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
      var payload = "Hello from TCP Client!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server writes to client
      var serverPayload = "Hello from TCP Server!"u8.ToArray();
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
   public async Task TcpClientServer_SslConnection_DataExchangedSuccessfully()
   {
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      using var certificate = CertificateUtility.GenerateSelfSignedCertificate();

      var serverSslOptions = new SslServerAuthenticationOptions
      {
         ServerCertificate = certificate,
         ClientCertificateRequired = false
      };

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         TargetHost = "localhost",
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var options = new TcpTransportOptions
      {
         UseSsl = true,
         SslServerOptions = serverSslOptions,
         SslClientOptions = clientSslOptions
      };

      var listener = new TcpNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      // Assert session security info for SSL connection
      await Assert.That(clientSession.SecurityInfo.IsEncrypted).IsTrue();
      await Assert.That(serverSession.SecurityInfo.IsEncrypted).IsTrue();
      await Assert.That(clientSession.SecurityInfo.Protocol).IsNotNull();
      await Assert.That(serverSession.SecurityInfo.Protocol).IsNotNull();
      await Assert.That(clientSession.SecurityInfo.RemoteCertificate).IsNotNull();
      await Assert.That(serverSession.SecurityInfo.LocalCertificate).IsNotNull();

      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      await Assert.That(clientStreamResult.Failed).IsFalse();
      await Assert.That(serverStreamResult.Failed).IsFalse();

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      var payload = "Secure message"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task TcpClient_DisconnectAsync_ClosesSessionAndCancelsSessionClosedToken()
   {
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(endPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var sessionClosedToken = clientSession.SessionClosedToken;

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsFalse();

      await client.DisconnectAsync(sessionClosedToken);

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsTrue();

      await listener.UnbindAsync(sessionClosedToken);
   }

   [Test]
   public async Task TcpClientServer_StandardConnection_StatsTrackedCorrectly()
   {
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(endPoint, options);

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      // Connect client
      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
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

      await Assert.That(serverStream.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(serverStream.Stats.BytesReceived).IsEqualTo(0);
      await Assert.That(serverSession.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(serverSession.Stats.BytesReceived).IsEqualTo(0);

      // Client writes to server
      var payload = "Hello from TCP Client!"u8.ToArray();
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

      // Server writes to client
      var serverPayload = "Hello from TCP Server!"u8.ToArray();
      await serverStream.Transport.Output.WriteAsync(serverPayload);
      await serverStream.Transport.Output.FlushAsync();

      // Client reads from server
      var clientReadResult = await clientStream.Transport.Input.ReadAsync();
      var clientReadBytes = clientReadResult.Buffer.ToArray();
      clientStream.Transport.Input.AdvanceTo(clientReadResult.Buffer.End);

      await Assert.That(clientReadBytes).IsEquivalentTo(serverPayload);

      // Verify stats after server -> client write
      await Assert.That(serverStream.Stats.BytesSent).IsEqualTo(serverPayload.Length);
      await Assert.That(serverSession.Stats.BytesSent).IsEqualTo(serverPayload.Length);
      await Assert.That(clientStream.Stats.BytesReceived).IsEqualTo(serverPayload.Length);
      await Assert.That(clientSession.Stats.BytesReceived).IsEqualTo(serverPayload.Length);

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }
}
