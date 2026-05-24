using System.Buffers;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using TUnit.Assertions;

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
      {
         // Skip the test if QUIC is not supported on the host platform
         return;
      }

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

      // Client opens a bidirectional stream
      var clientStreamResult = await clientSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional);
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
      await ((IAsyncDisposable)clientSession).DisposeAsync();
      await ((IAsyncDisposable)serverSession).DisposeAsync();
      await listener.UnbindAsync();
   }
}
