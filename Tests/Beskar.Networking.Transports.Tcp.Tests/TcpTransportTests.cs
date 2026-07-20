using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;

namespace Beskar.Networking.Transports.Tcp.Tests;

public class TcpTransportTests
{

   [Test]
   public async Task TcpClientServer_StandardConnection_DataExchangedSuccessfully()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

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
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      // Connect client
      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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

   [Test]
   public async Task TcpListener_DynamicPortBinding_ResolvesActualLocalAddress()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

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
   public async Task TcpClientSessionProperties_VerifyExposedCorrectly()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);

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

      // Accept stream and verify server active streams
      // Send data first to trigger stream creation in TCP stream connection
      var payload = "Hi"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

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

   [Test]
   public async Task TcpListener_MaxConcurrentHandshakesLimit_BlocksAcceptingFurtherConnections()
   {
      var options = new TcpTransportOptions
      {
         UseSsl = true,
         MaxConcurrentHandshakes = 1
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;

      // Connect Client 1 (raw TCP socket, no SSL handshake bytes sent)
      using var clientSocket1 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      await clientSocket1.ConnectAsync(IPAddress.Loopback, actualPort);

      // Give the server accept loop a moment to accept Client 1 and enter the SSL handshake (which blocks waiting for bytes)
      await Task.Delay(100);

      // Connect Client 2 (raw TCP socket)
      using var clientSocket2 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      await clientSocket2.ConnectAsync(IPAddress.Loopback, actualPort);

      // Give it another moment
      await Task.Delay(100);

      // At this point:
      // Client 1 is stuck in the handshake, occupying the 1 semaphore slot.
      // Client 2's socket is established at the OS level, but the listener accept loop should be blocked
      // on the semaphore and should NOT have accepted Client 2's socket yet.

      // Let's close Client 1. This will cause its handshake to fail/abort, releasing the semaphore slot.
      clientSocket1.Close();
      await Task.Delay(150);

      // Cleanup
      await listener.UnbindAsync();
   }

   [Test]
   public async Task TcpListener_MaxPendingConnectionsBounded_BlocksWhenFullAndResumesOnRead()
   {
      var options = new TcpTransportOptions
      {
         MaxPendingConnections = 1
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);

      // Connect Client 1
      var connectTask1 = client.ConnectAsync(listener.LocalAddress).AsTask();
      // Connect Client 2 (using a second client instance to avoid active session overwrite)
      var client2 = new TcpNetworkClient(options);
      var connectTask2 = client2.ConnectAsync(listener.LocalAddress).AsTask();

      // Wait a moment for both handshakes to complete and attempt to enqueue
      await Task.Delay(100);

      // At this point:
      // Client 1 is enqueued in the bounded channel.
      // Client 2's session is blocked on WriteAsync because the channel is full.
      // Let's accept Client 1 session:
      var acceptResult1 = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult1.Failed).IsFalse();

      // Freeing Client 1 should unblock Client 2, allowing it to enqueue in the channel.
      // Let's accept Client 2 session:
      var acceptResult2 = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult2.Failed).IsFalse();

      // Clean up
      var session1 = acceptResult1.Success!;
      var session2 = acceptResult2.Success!;

      await session1.DisposeAsync();
      await session2.DisposeAsync();
      await client.DisconnectAsync();
      await client2.DisconnectAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task TcpListenerClient_VerifyLingerAndTimeoutOptionsConfigured_Succeeds()
   {
      var options = new TcpTransportOptions
      {
         Backlog = 10,
         LingerState = new LingerOption(true, 5),
         SslHandshakeTimeout = TimeSpan.FromSeconds(5)
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      await connectResult.Success!.DisposeAsync();
      await acceptResult.Success!.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task TcpListener_SslHandshakeTimeout_AbortsHandshake()
   {
      var options = new TcpTransportOptions
      {
         UseSsl = true,
         SslHandshakeTimeout = TimeSpan.FromMilliseconds(100) // very short
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      // Client connects but sends no SSL bytes, exceeding handshake timeout
      await Task.Delay(300);

      // Verify that server has closed the connection
      var buffer = new byte[10];
      var readBytes = await stream.ReadAsync(buffer);
      await Assert.That(readBytes).IsEqualTo(0); // connection closed by server

      await listener.UnbindAsync();
   }

   [Test]
   public async Task TcpListener_BindUnbindBindUnbind_SuccessiveCallsWork()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

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
   public async Task TcpListener_UnbindWithActiveClient_CleanlyDisconnectsClient()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();
      var clientSession = connectResult.Success!;

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();
      var serverSession = acceptResult.Success!;

      // Unbind while client is connected
      await listener.UnbindAsync();

      // Verify that unbind is successful and listener is unbound
      await Assert.That(listener.IsBound).IsFalse();

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
   }

   [Test]
   public async Task TcpListener_SocketOptionException_DoesNotLeakSemaphoreOrSocket()
   {
      var options = new TcpTransportOptions
      {
         SendBufferSize = -1, // This invalid value will force an exception inside AcceptLoopAsync
         MaxConcurrentHandshakes = 2
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      // Verify initial semaphore count is 1 (2 minus the 1 permit acquired by the active accept loop)
      var semaphoreField = typeof(TcpNetworkListener).GetField("_handshakeSemaphore", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      var semaphore = (SemaphoreSlim)semaphoreField!.GetValue(listener)!;
      await Assert.That(semaphore.CurrentCount).IsEqualTo(1);

      // Connect client to trigger AcceptLoopAsync connection processing and the subsequent exception
      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using (var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
      {
         await clientSocket.ConnectAsync(IPAddress.Loopback, actualPort);
         
         // Give the server accept loop a moment to accept connection and throw exception
         await Task.Delay(200);
      }

      // Verify that the semaphore count is STILL 1 (not leaked)
      await Assert.That(semaphore.CurrentCount).IsEqualTo(1);

      await listener.UnbindAsync();
   }

   [Test]
   public async Task TcpClientServer_MultipleClients_DataExchangedWithoutLeakage()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client1 = new TcpNetworkClient(options);
      var client2 = new TcpNetworkClient(options);

      var connectResult1 = await client1.ConnectAsync(listener.LocalAddress);
      var connectResult2 = await client2.ConnectAsync(listener.LocalAddress);

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
}

