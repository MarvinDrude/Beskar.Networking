using System.Buffers;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.Ws.Tests;

public class WsTransportTests
{
   [Test]
   public async Task ComputeAcceptKey_WithStandardRfcKey_ReturnsExpectedBase64Hash()
   {
      var clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
      var expectedAcceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

      var acceptKey = WsHandshake.ComputeAcceptKey(clientKey);

      await Assert.That(acceptKey).IsEqualTo(expectedAcceptKey);
   }

   [Test]
   public async Task ComputeAcceptKey_WithTooLongKey_ThrowsArgumentException()
   {
      var longKey = new string('A', 129);
      await Assert.That(() => WsHandshake.ComputeAcceptKey(longKey)).Throws<ArgumentException>();
   }

   [Test]
   public async Task WsHandshake_WithTooLongSecWebSocketKey_AbortsConnection()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat"
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var tooLongKey = new string('A', 130);
      var request = $"GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {tooLongKey}\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(request));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("400 Bad Request");

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsClientServer_LoopbackConnection_HandshakeAndDataExchangedSuccessfully()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat",
         Subprotocol = "chat-proto"
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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

      // Client writes a WebSocket message to server
      var payload = "Hi from WebSocket Client!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads the WebSocket message
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server responds back to client
      var responsePayload = "Welcome! Handshake was completely zero-allocation!"u8.ToArray();
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
   public async Task WsListener_DynamicPortBinding_ResolvesActualLocalAddress()
   {
      var options = new WsTransportOptions();
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

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
   public async Task WsClientSessionProperties_VerifyExposedCorrectly()
   {
      var options = new WsTransportOptions();
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);

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

      // Verify active streams are pre-populated (WebSockets have exactly 1 stream per session)
      await Assert.That(clientSession.ActiveStreams).Count().IsEqualTo(1);
      await Assert.That(serverSession.ActiveStreams).Count().IsEqualTo(1);

      // Verify AcceptStreamAsync / OpenStreamAsync return the pre-populated active stream
      var clientStreamResult = await clientSession.AcceptStreamAsync();
      await Assert.That(clientStreamResult.Failed).IsFalse();
      var clientStream = clientStreamResult.Success!;

      var serverStreamResult = await serverSession.AcceptStreamAsync();
      await Assert.That(serverStreamResult.Failed).IsFalse();
      var serverStream = serverStreamResult.Success!;

      await Assert.That(clientSession.ActiveStreams).Contains(clientStream);
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
   public async Task WsHandshake_WithHeadersExceedingLimit_AbortsConnection()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat",
         MaxHeaderSize = 120 // Keep it very small
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var longHeaderRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSome-Long-Header: " + new string('A', 200) + "\r\n\r\n";
      var bytes = System.Text.Encoding.ASCII.GetBytes(longHeaderRequest);
      await stream.WriteAsync(bytes);
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      await Assert.That(read).IsEqualTo(0);

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsFrameParser_WithFrameSizeExceedingLimit_ClosesSession()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat",
         MaxFrameSize = 100 // Keep it very small
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      var largePayload = new byte[150];
      Array.Fill(largePayload, (byte)0x41);

      await clientStream.Transport.Output.WriteAsync(largePayload);
      await clientStream.Transport.Output.FlushAsync();

      var readResult = await serverStream.Transport.Input.ReadAsync();
      await Assert.That(readResult.IsCompleted).IsTrue();
      await Assert.That(readResult.Buffer.Length).IsEqualTo(0);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsClient_ReceivesMaskedFrame_ClosesSession()
   {
      using var tcpListener = new TcpListener(new IPEndPoint(IPAddress.Loopback, 0));
      tcpListener.Start();
      var actualEndPoint = tcpListener.LocalEndpoint;

      var clientOptions = new WsTransportOptions
      {
         Path = "/mock"
      };
      var client = new WsNetworkClient(clientOptions);

      var connectTask = client.ConnectAsync(actualEndPoint).AsTask();
      using var serverSocket = await tcpListener.AcceptSocketAsync();

      var buffer = new byte[1024];
      var read = await serverSocket.ReceiveAsync(buffer, SocketFlags.None);
      var requestText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);

      var keyLine = requestText.Split("\r\n").First(l => l.StartsWith("Sec-WebSocket-Key:", StringComparison.OrdinalIgnoreCase));
      var clientKey = keyLine.Split(':')[1].Trim();
      var acceptKey = WsHandshake.ComputeAcceptKey(clientKey);

      var responseText = $"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {acceptKey}\r\n\r\n";
      await serverSocket.SendAsync(System.Text.Encoding.ASCII.GetBytes(responseText), SocketFlags.None);

      var clientSession = (await connectTask).Success!;
      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var clientStream = clientStreamResult.Success!;

      var maskedFrame = new byte[] { 0x81, 0x85, 0x00, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F };
      await serverSocket.SendAsync(maskedFrame, SocketFlags.None);

      var readResult = await clientStream.Transport.Input.ReadAsync();
      await Assert.That(readResult.IsCompleted).IsTrue();
      await Assert.That(readResult.Buffer.Length).IsEqualTo(0);

      await clientSession.DisposeAsync();
      tcpListener.Stop();
   }

   [Test]
   public async Task WsServer_ReceivesUnmaskedFrame_ClosesSession()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var handshakeRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(handshakeRequest));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      var unmaskedFrame = new byte[] { 0x81, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F };
      await stream.WriteAsync(unmaskedFrame);
      await stream.FlushAsync();

      var readBytes = await stream.ReadAsync(buffer);
      await Assert.That(readBytes).IsEqualTo(0);

      var acceptResult = await listener.AcceptSessionAsync();
      if (!acceptResult.Failed)
      {
         await acceptResult.Success!.DisposeAsync();
      }

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsServer_ReceivesFragmentedFrame_ConcatenatesSuccessfully()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var handshakeRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(handshakeRequest));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      var acceptSessionTask = listener.AcceptSessionAsync().AsTask();

      // Frame 1: Fin = false, Opcode = Binary (2), Length = 6, Masked, Mask key = 0,0,0,0
      var frame1 = new byte[] { 0x02, 0x86, 0x00, 0x00, 0x00, 0x00, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20 }; // "Hello "
      await stream.WriteAsync(frame1);
      await stream.FlushAsync();

      // Frame 2: Fin = true, Opcode = Continuation (0), Length = 6, Masked, Mask key = 0,0,0,0
      var frame2 = new byte[] { 0x80, 0x86, 0x00, 0x00, 0x00, 0x00, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21 }; // "World!"
      await stream.WriteAsync(frame2);
      await stream.FlushAsync();

      var serverSession = (await acceptSessionTask).Success!;
      var serverStreamResult = await serverSession.AcceptStreamAsync();
      var serverStream = serverStreamResult.Success!;

      var receivedBytes = new List<byte>();
      while (receivedBytes.Count < 12)
      {
         var readResult = await serverStream.Transport.Input.ReadAsync();
         receivedBytes.AddRange(readResult.Buffer.ToArray());
         serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);
      }

      var receivedText = System.Text.Encoding.ASCII.GetString(receivedBytes.ToArray());
      await Assert.That(receivedText).IsEqualTo("Hello World!");

      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsServer_ReceivesUnexpectedContinuationFrame_ClosesSession()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var handshakeRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(handshakeRequest));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      // Send unexpected continuation frame (Opcode = 0) without prior start frame
      var continuationFrame = new byte[] { 0x80, 0x85, 0x00, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F }; // "hello"
      await stream.WriteAsync(continuationFrame);
      await stream.FlushAsync();

      var readBytes = await stream.ReadAsync(buffer);
      await Assert.That(readBytes).IsEqualTo(0); // Expect connection closed by server due to protocol error

      var acceptResult = await listener.AcceptSessionAsync();
      if (!acceptResult.Failed)
      {
         await acceptResult.Success!.DisposeAsync();
      }

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsServer_ReceivesInterleavedStartingFrame_ClosesSession()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var handshakeRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(handshakeRequest));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      // Frame 1: Fin = false, Opcode = Binary (2)
      var frame1 = new byte[] { 0x02, 0x85, 0x00, 0x00, 0x00, 0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F }; // "hello"
      await stream.WriteAsync(frame1);
      await stream.FlushAsync();

      // Frame 2: Fin = false, Opcode = Text (1) - Violates RFC by starting new message before finishing previous
      var frame2 = new byte[] { 0x01, 0x85, 0x00, 0x00, 0x00, 0x00, 0x77, 0x6F, 0x72, 0x6C, 0x64 }; // "world"
      await stream.WriteAsync(frame2);
      await stream.FlushAsync();

      var readBytes = await stream.ReadAsync(buffer);
      await Assert.That(readBytes).IsEqualTo(0); // Expect connection closed by server due to protocol error

      var acceptResult = await listener.AcceptSessionAsync();
      if (!acceptResult.Failed)
      {
         await acceptResult.Success!.DisposeAsync();
      }

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsClientServer_KeepAliveEnabled_PingsAndPongsSuccessfully()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat",
         KeepAliveInterval = TimeSpan.FromMilliseconds(100) // Ping frequently for test
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      // Just wait long enough for multiple pings and pongs to be exchanged in the background
      await Task.Delay(500);

      // Verify that no connections were closed/errored due to the background pings/pongs
      await Assert.That(clientSession.SessionClosedToken.IsCancellationRequested).IsFalse();
      await Assert.That(serverSession.SessionClosedToken.IsCancellationRequested).IsFalse();

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsHandshake_WithAllowedOriginsMatching_HandshakeSucceeds()
   {
      var serverOptions = new WsTransportOptions
      {
         Path = "/chat",
         AllowedOrigins = new[] { "http://localhost:8080", "https://app.example.com" }
      };

      var clientOptions = new WsTransportOptions
      {
         Path = "/chat",
         Origin = "https://app.example.com"
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), serverOptions);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(clientOptions);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      await connectResult.Success!.DisposeAsync();
      await acceptResult.Success!.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsHandshake_WithAllowedOriginsMismatch_HandshakeFails()
   {
      var serverOptions = new WsTransportOptions
      {
         Path = "/chat",
         AllowedOrigins = new[] { "https://app.example.com" }
      };

      var clientOptions = new WsTransportOptions
      {
         Path = "/chat",
         Origin = "https://evil.com"
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), serverOptions);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(clientOptions);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);

      // Client handshake should fail because server rejects origin with 403 Forbidden
      await Assert.That(connectResult.Failed).IsTrue();

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsHandshake_WithAllowedOriginsConfiguredButClientOmitsOrigin_HandshakeFails()
   {
      var serverOptions = new WsTransportOptions
      {
         Path = "/chat",
         AllowedOrigins = new[] { "https://app.example.com" }
      };

      var clientOptions = new WsTransportOptions
      {
         Path = "/chat",
         Origin = null // No origin
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), serverOptions);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(clientOptions);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);

      await Assert.That(connectResult.Failed).IsTrue();

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsHandshake_WithHandshakeTimeoutExceeded_HandshakeFails()
   {
      var serverOptions = new WsTransportOptions
      {
         Path = "/chat",
         HandshakeTimeout = TimeSpan.FromMilliseconds(100) // very short
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), serverOptions);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      // Client connects but sends nothing, exceeding handshake timeout
      await Task.Delay(300);

      // Verify that server has closed the connection (subsequent write or read should indicate close)
      var buffer = new byte[10];
      var readBytes = await stream.ReadAsync(buffer);
      await Assert.That(readBytes).IsEqualTo(0); // connection closed by server

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsListener_BindUnbindBindUnbind_SuccessiveCallsWork()
   {
      var options = new WsTransportOptions();
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

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
   public async Task WsListener_UnbindWithActiveClient_CleanlyDisconnectsClient()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new WsNetworkClient(options);
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
   public async Task WsHandshake_SendErrorResponse_HasCorrectSeparator()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat"
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      // Send bad request to trigger SendErrorResponseAsync
      var request = "INVALID_REQUEST\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(request));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      
      // The response must contain "\r\n\r\n" separating headers and body
      await Assert.That(responseText).Contains("\r\n\r\nOnly GET requests are allowed.");

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsClientServer_MultipleClients_DataExchangedWithoutLeakage()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client1 = new WsNetworkClient(options);
      var client2 = new WsNetworkClient(options);

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

   [Test]
   public async Task WsServer_ReceivesPingControlFrame_RepliesWithPongControlFrame()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var handshakeRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(handshakeRequest));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      var acceptSessionTask = listener.AcceptSessionAsync().AsTask();

      // Send Ping frame: Fin = true, Opcode = Ping (9), Masked, Length = 4 ("ping")
      var maskKey = new byte[] { 0x12, 0x34, 0x56, 0x78 };
      var rawPayload = "ping"u8.ToArray();
      var maskedPayload = new byte[rawPayload.Length];
      for (var i = 0; i < rawPayload.Length; i++)
      {
         maskedPayload[i] = (byte)(rawPayload[i] ^ maskKey[i % 4]);
      }

      var pingFrame = new byte[2 + 4 + rawPayload.Length];
      pingFrame[0] = 0x89; // Fin + Ping opcode
      pingFrame[1] = (byte)(0x80 | rawPayload.Length); // Masked + len 4
      maskKey.CopyTo(pingFrame, 2);
      maskedPayload.CopyTo(pingFrame, 6);

      await stream.WriteAsync(pingFrame);
      await stream.FlushAsync();

      // Read Pong response frame from server
      var pongBuffer = new byte[100];
      var pongRead = await stream.ReadAsync(pongBuffer);
      await Assert.That(pongRead).IsGreaterThanOrEqualTo(2 + rawPayload.Length);

      var pongHeader = pongBuffer[0];
      var pongOpcode = pongHeader & 0x0F;
      var pongLen = pongBuffer[1] & 0x7F;

      await Assert.That(pongOpcode).IsEqualTo((int)Enums.WebSocketOpcode.Pong);
      await Assert.That(pongLen).IsEqualTo(rawPayload.Length);

      var pongPayload = pongBuffer.AsSpan(2, pongLen).ToArray();
      await Assert.That(pongPayload).IsEquivalentTo(rawPayload);

      var serverSession = (await acceptSessionTask).Success!;
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsServer_ReceivesCloseControlFrame_RepliesWithCloseControlFrameAndCloses()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var actualPort = ((IPEndPoint)listener.LocalAddress).Port;
      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, actualPort);
      await using var stream = tcpClient.GetStream();

      var handshakeRequest = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(handshakeRequest));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      var acceptSessionTask = listener.AcceptSessionAsync().AsTask();

      // Send Close frame: Fin = true, Opcode = Close (8), Masked, Length = 2 (status code 1000 = 0x03E8)
      var maskKey = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
      var rawPayload = new byte[] { 0x03, 0xE8 }; // Status 1000
      var maskedPayload = new byte[2];
      maskedPayload[0] = (byte)(rawPayload[0] ^ maskKey[0]);
      maskedPayload[1] = (byte)(rawPayload[1] ^ maskKey[1]);

      var closeFrame = new byte[2 + 4 + 2];
      closeFrame[0] = 0x88; // Fin + Close opcode
      closeFrame[1] = 0x82; // Masked + len 2
      maskKey.CopyTo(closeFrame, 2);
      maskedPayload.CopyTo(closeFrame, 6);

      await stream.WriteAsync(closeFrame);
      await stream.FlushAsync();

      // Read Close response frame from server
      var closeBuffer = new byte[100];
      var closeRead = await stream.ReadAsync(closeBuffer);
      await Assert.That(closeRead).IsGreaterThanOrEqualTo(4);

      var closeHeader = closeBuffer[0];
      var closeOpcode = closeHeader & 0x0F;
      var closeLen = closeBuffer[1] & 0x7F;

      await Assert.That(closeOpcode).IsEqualTo((int)Enums.WebSocketOpcode.Close);
      await Assert.That(closeLen).IsEqualTo(2);

      var responseStatusPayload = closeBuffer.AsSpan(2, 2).ToArray();
      await Assert.That(responseStatusPayload).IsEquivalentTo(rawPayload);

      // Verify connection is now closed by server
      var subsequentRead = await stream.ReadAsync(closeBuffer);
      await Assert.That(subsequentRead).IsEqualTo(0);

      var serverSession = (await acceptSessionTask).Success!;
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsFrameParser_SingleSegmentPayload_TransmitsAndReceivesCorrectly()
   {
      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStream = (await clientSession.AcceptStreamAsync()).Success!;
      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      // Single segment data transmission
      var singleSegmentData = "SingleSegmentPayloadTest"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(singleSegmentData);
      await clientStream.Transport.Output.FlushAsync();

      var serverReadResult = await serverStream.Transport.Input.ReadAsync();
      await Assert.That(serverReadResult.Buffer.IsSingleSegment).IsTrue();
      var receivedBytes = serverReadResult.Buffer.FirstSpan.ToArray();
      serverStream.Transport.Input.AdvanceTo(serverReadResult.Buffer.End);

      await Assert.That(receivedBytes).IsEquivalentTo(singleSegmentData);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsTransport_OnMessage_FiresPerFrameWithIsolatedPayloadAndOpcode()
   {
      var receivedMessages = new System.Collections.Concurrent.ConcurrentBag<string>();
      var options = new WsTransportOptions
      {
         Path = "/chat",
         OnMessage = (session, payload, opcode) =>
         {
            var text = System.Text.Encoding.UTF8.GetString(payload.ToArray());
            receivedMessages.Add(text);
            _ = session.SendFrameAsync(payload, opcode);
         }
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var clientOptions = new WsTransportOptions { Path = "/chat" };
      var client = new WsNetworkClient(clientOptions);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var clientStream = (await clientSession.AcceptStreamAsync()).Success!;

      var clientWsStream = (WsNetworkStream)clientStream;

      // Send 3 separate messages from client
      var msgs = new[] { "Message 1", "Message 2", "Message 3" };
      foreach (var msg in msgs)
      {
         await clientWsStream.SendFrameAsync(System.Text.Encoding.UTF8.GetBytes(msg), Enums.WebSocketOpcode.Text);
      }

      // Allow OnMessage handler to process
      await Task.Delay(200);

      await Assert.That(receivedMessages.Count).IsEqualTo(3);
      await Assert.That(receivedMessages).Contains("Message 1");
      await Assert.That(receivedMessages).Contains("Message 2");
      await Assert.That(receivedMessages).Contains("Message 3");

      await clientSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsTransport_OnMessage_MultiFrameEcho_PreservesBoundaries()
   {
      var options = new WsTransportOptions
      {
         Path = "/chat",
         OnMessage = (session, payload, opcode) =>
         {
            _ = session.SendFrameAsync(payload, opcode);
         }
      };

      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalAddress).Port);
      await using var stream = tcpClient.GetStream();

      // Send HTTP Upgrade
      var request = "GET /chat HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(request));
      await stream.FlushAsync();

      var buffer = new byte[1024];
      var read = await stream.ReadAsync(buffer);
      var responseText = System.Text.Encoding.ASCII.GetString(buffer, 0, read);
      await Assert.That(responseText).Contains("101 Switching Protocols");

      // Send 3 WebSocket text frames back-to-back down the TCP socket
      var maskKey = new byte[] { 0x11, 0x22, 0x33, 0x44 };
      var sentMsgs = new[] { "msg-1-aaa", "msg-2-bbb", "msg-3-ccc" };

      var msgsBuffer = new List<byte>();
      foreach (var msgText in sentMsgs)
      {
         var rawPayload = System.Text.Encoding.UTF8.GetBytes(msgText);
         var frame = new byte[2 + 4 + rawPayload.Length];
         frame[0] = 0x81; // FIN + Text
         frame[1] = (byte)(0x80 | rawPayload.Length);
         maskKey.CopyTo(frame, 2);
         for (int i = 0; i < rawPayload.Length; i++)
         {
            frame[6 + i] = (byte)(rawPayload[i] ^ maskKey[i % 4]);
         }
         msgsBuffer.AddRange(frame);
      }

      await stream.WriteAsync(msgsBuffer.ToArray());
      await stream.FlushAsync();

      // Server should echo back 3 individual WS frames
      for (int i = 0; i < 3; i++)
      {
         var respHeader = new byte[2];
         var readHeader = await stream.ReadAsync(respHeader);
         await Assert.That(readHeader).IsEqualTo(2);
         await Assert.That(respHeader[0]).IsEqualTo((byte)0x81); // Text frame FIN

         int len = respHeader[1] & 0x7F;
         var respPayload = new byte[len];
         var readPayload = await stream.ReadAsync(respPayload);
         await Assert.That(readPayload).IsEqualTo(len);

         var echoedText = System.Text.Encoding.UTF8.GetString(respPayload);
         await Assert.That(echoedText).IsEqualTo(sentMsgs[i]);
      }

      await listener.UnbindAsync();
   }

   [Test]
   public async Task WsClientServer_WithMeterListener_TracksConnectionsStreamsAndBytes()
   {
      long recordedConnectionsOpened = 0;
      long recordedConnectionsClosed = 0;
      long recordedConnectionsActiveDelta = 0;
      long recordedStreamsActiveDelta = 0;
      long recordedBytesSent = 0;
      long recordedBytesReceived = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == TransportMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };
      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         if (instrument.Name == "beskar.transport.connections.opened")
         {
            Interlocked.Add(ref recordedConnectionsOpened, measurement);
         }
         else if (instrument.Name == "beskar.transport.connections.closed")
         {
            Interlocked.Add(ref recordedConnectionsClosed, measurement);
         }
         else if (instrument.Name == "beskar.transport.connections.active")
         {
            Interlocked.Add(ref recordedConnectionsActiveDelta, measurement);
         }
         else if (instrument.Name == "beskar.transport.streams.active")
         {
            Interlocked.Add(ref recordedStreamsActiveDelta, measurement);
         }
         else if (instrument.Name == "beskar.transport.bytes.sent")
         {
            Interlocked.Add(ref recordedBytesSent, measurement);
         }
         else if (instrument.Name == "beskar.transport.bytes.received")
         {
            Interlocked.Add(ref recordedBytesReceived, measurement);
         }
      });
      meterListener.Start();

      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var initialOpened = Volatile.Read(ref recordedConnectionsOpened);
      var initialClosed = Volatile.Read(ref recordedConnectionsClosed);
      var initialActive = Volatile.Read(ref recordedConnectionsActiveDelta);
      var initialStreamsActive = Volatile.Read(ref recordedStreamsActiveDelta);

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var openedDelta = Volatile.Read(ref recordedConnectionsOpened) - initialOpened;
      await Assert.That(openedDelta).IsGreaterThanOrEqualTo(1);

      var activeDuringConnection = Volatile.Read(ref recordedConnectionsActiveDelta) - initialActive;
      await Assert.That(activeDuringConnection).IsGreaterThanOrEqualTo(1);

      var clientStream = (await clientSession.AcceptStreamAsync()).Success!;
      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      var streamsActiveDelta = Volatile.Read(ref recordedStreamsActiveDelta) - initialStreamsActive;
      await Assert.That(streamsActiveDelta).IsGreaterThanOrEqualTo(2);

      var payload = "WS Telemetry Payload"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var readResult = await serverStream.Transport.Input.ReadAsync();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(recordedBytesSent).IsGreaterThanOrEqualTo(payload.Length);
      await Assert.That(recordedBytesReceived).IsGreaterThanOrEqualTo(payload.Length);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();

      var closedDelta = Volatile.Read(ref recordedConnectionsClosed) - initialClosed;
      await Assert.That(closedDelta).IsGreaterThanOrEqualTo(1);
   }
}

