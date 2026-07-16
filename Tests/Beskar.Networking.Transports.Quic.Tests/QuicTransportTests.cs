using System.Buffers;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;

namespace Beskar.Networking.Transports.Quic.Tests;

public class QuicTransportTests
{
   private static int GetFreePort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task QuicClientServer_BidirectionalStream_DataExchangedSuccessfully()
   {
      if (!QuicConnection.IsSupported)
         // Skip the test if QUIC is not supported on the host platform
         return;

      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var options = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };

      var listener = new QuicNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new QuicNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      // Assert security info is populated from the actual QUIC connection
      await Assert.That(clientSession.SecurityInfo.IsEncrypted).IsTrue();
      await Assert.That(serverSession.SecurityInfo.IsEncrypted).IsTrue();
      await Assert.That(clientSession.SecurityInfo.Protocol).IsNotNull();
      await Assert.That(serverSession.SecurityInfo.Protocol).IsNotNull();

      // Client opens a bidirectional stream
      var clientStreamResult = await clientSession.OpenStreamAsync();
      await Assert.That(clientStreamResult.Failed).IsFalse();
      var clientStream = clientStreamResult.Success!;

      // Client writes to server FIRST so that the stream is actually created and sent to the server in QUIC
      var payload = "Hello via QUIC!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server accepts the inbound stream
      var serverStreamResult = await serverSession.AcceptStreamAsync();
      await Assert.That(serverStreamResult.Failed).IsFalse();
      var serverStream = serverStreamResult.Success!;

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server responds back to client
      var responsePayload = "QUIC works beautifully!"u8.ToArray();
      await serverStream.Transport.Output.WriteAsync(responsePayload);
      await serverStream.Transport.Output.FlushAsync();

      // Client reads response
      var clientReadResult = await clientStream.Transport.Input.ReadAsync();
      var clientReadBytes = clientReadResult.Buffer.ToArray();
      clientStream.Transport.Input.AdvanceTo(clientReadResult.Buffer.End);

      await Assert.That(clientReadBytes).IsEquivalentTo(responsePayload);

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task QuicClientServer_MultipleStreamsOverOneConnection_DataExchangedSuccessfully()
   {
      if (!QuicConnection.IsSupported)
         // Skip the test if QUIC is not supported on the host platform
         return;

      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var options = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };

      var listener = new QuicNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new QuicNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      // 1. Open first bidirectional stream
      var clientStreamResult1 = await clientSession.OpenStreamAsync();
      await Assert.That(clientStreamResult1.Failed).IsFalse();
      var clientStream1 = clientStreamResult1.Success!;

      // 2. Open second bidirectional stream
      var clientStreamResult2 = await clientSession.OpenStreamAsync();
      await Assert.That(clientStreamResult2.Failed).IsFalse();
      var clientStream2 = clientStreamResult2.Success!;

      // 3. Write data from client on both streams (flushing to notify the server connection)
      var clientPayload1 = "Data from Client Stream 1"u8.ToArray();
      await clientStream1.Transport.Output.WriteAsync(clientPayload1);
      await clientStream1.Transport.Output.FlushAsync();

      var clientPayload2 = "Data from Client Stream 2"u8.ToArray();
      await clientStream2.Transport.Output.WriteAsync(clientPayload2);
      await clientStream2.Transport.Output.FlushAsync();

      // 4. Accept both streams on the server side
      var serverStreamResult1 = await serverSession.AcceptStreamAsync();
      await Assert.That(serverStreamResult1.Failed).IsFalse();
      var serverStream1 = serverStreamResult1.Success!;

      var serverStreamResult2 = await serverSession.AcceptStreamAsync();
      await Assert.That(serverStreamResult2.Failed).IsFalse();
      var serverStream2 = serverStreamResult2.Success!;

      // 5. Server reads data from client from both streams
      var readResult1 = await serverStream1.Transport.Input.ReadAsync();
      var readBytes1 = readResult1.Buffer.ToArray();
      serverStream1.Transport.Input.AdvanceTo(readResult1.Buffer.End);
      await Assert.That(readBytes1).IsEquivalentTo(clientPayload1);

      var readResult2 = await serverStream2.Transport.Input.ReadAsync();
      var readBytes2 = readResult2.Buffer.ToArray();
      serverStream2.Transport.Input.AdvanceTo(readResult2.Buffer.End);
      await Assert.That(readBytes2).IsEquivalentTo(clientPayload2);

      // 6. Server responds back on both streams
      var serverPayload1 = "Response from Server Stream 1"u8.ToArray();
      await serverStream1.Transport.Output.WriteAsync(serverPayload1);
      await serverStream1.Transport.Output.FlushAsync();

      var serverPayload2 = "Response from Server Stream 2"u8.ToArray();
      await serverStream2.Transport.Output.WriteAsync(serverPayload2);
      await serverStream2.Transport.Output.FlushAsync();

      // 7. Client reads responses on both streams
      var clientReadResult1 = await clientStream1.Transport.Input.ReadAsync();
      var clientReadBytes1 = clientReadResult1.Buffer.ToArray();
      clientStream1.Transport.Input.AdvanceTo(clientReadResult1.Buffer.End);
      await Assert.That(clientReadBytes1).IsEquivalentTo(serverPayload1);

      var clientReadResult2 = await clientStream2.Transport.Input.ReadAsync();
      var clientReadBytes2 = clientReadResult2.Buffer.ToArray();
      clientStream2.Transport.Input.AdvanceTo(clientReadResult2.Buffer.End);
      await Assert.That(clientReadBytes2).IsEquivalentTo(serverPayload2);

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task QuicListener_DynamicPortBinding_ResolvesActualLocalAddress()
   {
      if (!QuicConnection.IsSupported)
         return;

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };
      var options = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };
      var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      await Assert.That(listener.LocalAddress).IsEqualTo(new IPEndPoint(IPAddress.Loopback, 0));

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();
      await Assert.That(listener.IsBound).IsTrue();

      var localAddress = listener.LocalAddress as IPEndPoint;
      await Assert.That(localAddress).IsNotNull();
      await Assert.That(localAddress!.Port).IsGreaterThan(0);

      await listener.UnbindAsync();
   }

   [Test]
   public async Task QuicClientSessionProperties_VerifyExposedCorrectly()
   {
      if (!QuicConnection.IsSupported)
         return;

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };
      var options = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };
      var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new QuicNetworkClient(options);
      
      // Verify initial client state
      await Assert.That(client.Session).IsNull();
      await Assert.That(client.LocalAddress).IsNull();
      await Assert.That(client.RemoteAddress).IsNull();

      // Connect
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();
      var clientSession = connectResult.Success!;

      // Verify client session reference & addresses
      await Assert.That(client.Session).IsEqualTo(clientSession);
      await Assert.That(client.LocalAddress).IsEqualTo(clientSession.LocalAddress);
      await Assert.That(client.RemoteAddress).IsEqualTo(clientSession.RemoteAddress);

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();
      var serverSession = acceptResult.Success!;

      // Verify initial active streams
      await Assert.That(clientSession.ActiveStreams).IsEmpty();
      await Assert.That(serverSession.ActiveStreams).IsEmpty();

      // Open stream and verify active streams
      var clientStreamResult = await clientSession.OpenStreamAsync();
      await Assert.That(clientStreamResult.Failed).IsFalse();
      var clientStream = clientStreamResult.Success!;

      await Assert.That(clientSession.ActiveStreams).Count().IsEqualTo(1);
      await Assert.That(clientSession.ActiveStreams).Contains(clientStream);

      // Write data to trigger stream acceptance on the server
      var payload = "Hi"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Accept stream and verify server active streams
      var serverStreamResult = await serverSession.AcceptStreamAsync();
      await Assert.That(serverStreamResult.Failed).IsFalse();
      var serverStream = serverStreamResult.Success!;

      await Assert.That(serverSession.ActiveStreams).Count().IsEqualTo(1);
      await Assert.That(serverSession.ActiveStreams).Contains(serverStream);

      // Cleanup
      await client.DisconnectAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();

      // Verify client properties are cleared
      await Assert.That(client.Session).IsNull();
      await Assert.That(client.LocalAddress).IsNull();
      await Assert.That(client.RemoteAddress).IsNull();
   }
}
