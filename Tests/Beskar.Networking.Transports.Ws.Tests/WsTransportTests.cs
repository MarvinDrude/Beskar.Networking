using System.Buffers;
using System.Net;
using System.Net.Sockets;

namespace Beskar.Networking.Transports.Ws.Tests;

public class WsTransportTests
{
   private static int GetFreePort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task ComputeAcceptKey_WithStandardRfcKey_ReturnsExpectedBase64Hash()
   {
      var clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
      var expectedAcceptKey = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

      var acceptKey = WsHandshake.ComputeAcceptKey(clientKey);

      await Assert.That(acceptKey).IsEqualTo(expectedAcceptKey);
   }

   [Test]
   public async Task WsClientServer_LoopbackConnection_HandshakeAndDataExchangedSuccessfully()
   {
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new WsTransportOptions
      {
         Path = "/chat",
         Subprotocol = "chat-proto"
      };

      var listener = new WsNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
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
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new WsTransportOptions
      {
         Path = "/chat",
         MaxHeaderSize = 120 // Keep it very small
      };

      var listener = new WsNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, port);
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
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new WsTransportOptions
      {
         Path = "/chat",
         MaxFrameSize = 100 // Keep it very small
      };

      var listener = new WsNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
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
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      using var tcpListener = new TcpListener(endPoint);
      tcpListener.Start();

      var clientOptions = new WsTransportOptions
      {
         Path = "/mock"
      };
      var client = new WsNetworkClient(clientOptions);

      var connectTask = client.ConnectAsync(endPoint).AsTask();
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
      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var options = new WsTransportOptions { Path = "/chat" };
      var listener = new WsNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, port);
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
}
