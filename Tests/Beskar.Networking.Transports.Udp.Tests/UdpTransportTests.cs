using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Transports.Udp;

namespace Beskar.Networking.Transports.Udp.Tests;

public class UdpTransportTests
{
   [Test]
   public async Task UdpClientServer_StandardConnection_DataExchangedSuccessfully()
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      // Assert initially unbound
      await Assert.That(listener.IsBound).IsFalse();

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();
      await Assert.That(listener.IsBound).IsTrue();
      await Assert.That(listener.Stats.Binds).IsEqualTo(1);

      // Connect client
      var client = new UdpNetworkClient(options);
      await Assert.That(client.IsConnected).IsFalse();
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.Stats.ConnectionsEstablished).IsEqualTo(1);

      // Open client stream and send payload to trigger server session creation
      var clientSession = connectResult.Success!;
      var clientStreamResult = await clientSession.OpenStreamAsync();
      await Assert.That(clientStreamResult.Failed).IsFalse();
      var clientStream = clientStreamResult.Success!;

      var payload = "Hello from UDP Client!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();
      var serverSession = acceptResult.Success!;

      await Assert.That(listener.Stats.SessionsAccepted).IsEqualTo(1);

      var serverStreamResult = await serverSession.AcceptStreamAsync();
      await Assert.That(serverStreamResult.Failed).IsFalse();
      var serverStream = serverStreamResult.Success!;

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server writes to client
      var serverPayload = "Hello from UDP Server!"u8.ToArray();
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
   public async Task UdpClientServer_StatsTrackedCorrectly()
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new UdpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var clientStreamResult = await clientSession.OpenStreamAsync();
      var clientStream = clientStreamResult.Success!;

      // Verify initial stats are 0
      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(clientStream.Stats.BytesReceived).IsEqualTo(0);

      var payload = "Hi"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var acceptResult = await listener.AcceptSessionAsync();
      var serverSession = acceptResult.Success!;
      var serverStreamResult = await serverSession.AcceptStreamAsync();
      var serverStream = serverStreamResult.Success!;

      var readResult = await serverStream.Transport.Input.ReadAsync();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      // Verify stats
      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(serverStream.Stats.BytesReceived).IsEqualTo(payload.Length);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task UdpListener_IdleTimeout_DisposesSessions()
   {
      var options = new UdpTransportOptions
      {
         ClientSessionIdleTimeout = TimeSpan.FromMilliseconds(500)
      };

      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new UdpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      var clientSession = connectResult.Success!;
      var clientStream = (await clientSession.OpenStreamAsync()).Success!;

      // Send packet to establish session on server
      await clientStream.Transport.Output.WriteAsync("Ping"u8.ToArray());
      await clientStream.Transport.Output.FlushAsync();

      var acceptResult = await listener.AcceptSessionAsync();
      var serverSession = acceptResult.Success!;

      var sessionClosedToken = serverSession.SessionClosedToken;
      await Assert.That(sessionClosedToken.IsCancellationRequested).IsFalse();

      // Wait for idle cleanup loop to run and detect inactivity (timeout is 500ms, cleanup runs every 250ms)
      await Task.Delay(1000);

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsTrue();

      await clientSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task UdpClient_DisconnectAsync_ClosesSessionAndCancelsSessionClosedToken()
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new UdpNetworkClient(options);
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
   public async Task UdpListener_DynamicPortBinding_ResolvesActualLocalAddress()
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

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
   public async Task UdpClientSessionProperties_VerifyExposedCorrectly()
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new UdpNetworkClient(options);

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

      // Open stream
      var clientStreamResult = await clientSession.OpenStreamAsync();
      await Assert.That(clientStreamResult.Failed).IsFalse();
      var clientStream = clientStreamResult.Success!;

      // Send data first to trigger stream creation in Udp stream connection on server side
      var payload = "Hi"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();
      var serverSession = acceptResult.Success!;

      await Assert.That(clientSession.ActiveStreams).Count().IsEqualTo(1);
      await Assert.That(clientSession.ActiveStreams).Contains(clientStream);

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
   public async Task UdpListener_MultipleClients_IdleTimeoutAffectsOnlyIdleClient()
   {
      var options = new UdpTransportOptions
      {
         ClientSessionIdleTimeout = TimeSpan.FromMilliseconds(500)
      };

      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var clientA = new UdpNetworkClient(options);
      var clientB = new UdpNetworkClient(options);

      var connA = await clientA.ConnectAsync(listener.LocalAddress);
      var connB = await clientB.ConnectAsync(listener.LocalAddress);

      var sessionA = connA.Success!;
      var sessionB = connB.Success!;

      var streamA = (await sessionA.OpenStreamAsync()).Success!;
      var streamB = (await sessionB.OpenStreamAsync()).Success!;

      // Send to establish both sessions
      await streamA.Transport.Output.WriteAsync("A1"u8.ToArray());
      await streamA.Transport.Output.FlushAsync();

      await streamB.Transport.Output.WriteAsync("B1"u8.ToArray());
      await streamB.Transport.Output.FlushAsync();

      var serverSessionA = await listener.AcceptSessionAsync();
      var serverSessionB = await listener.AcceptSessionAsync();

      var sSessionA = serverSessionA.Success!;
      var sSessionB = serverSessionB.Success!;

      // Keep Client A active, Client B idle
      for (int i = 0; i < 5; i++)
      {
         await Task.Delay(200);
         await streamA.Transport.Output.WriteAsync("KeepAlive"u8.ToArray());
         await streamA.Transport.Output.FlushAsync();
      }

      // At this point, Client B should have timed out (> 1000ms idle)
      // Client A should still be active because we sent messages every 200ms
      await Assert.That(sSessionB.SessionClosedToken.IsCancellationRequested).IsTrue();
      await Assert.That(sSessionA.SessionClosedToken.IsCancellationRequested).IsFalse();

      await clientA.DisconnectAsync();
      await clientB.DisconnectAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task UdpListener_ResetIdleTimerOnActivity()
   {
      var options = new UdpTransportOptions
      {
         ClientSessionIdleTimeout = TimeSpan.FromMilliseconds(600)
      };

      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new UdpNetworkClient(options);
      var conn = await client.ConnectAsync(listener.LocalAddress);
      var session = conn.Success!;
      var stream = (await session.OpenStreamAsync()).Success!;

      await stream.Transport.Output.WriteAsync("Init"u8.ToArray());
      await stream.Transport.Output.FlushAsync();

      var sSession = (await listener.AcceptSessionAsync()).Success!;

      // Periodically send packets to prevent idle timeout
      for (var i = 0; i < 4; i++)
      {
         await Task.Delay(300);
         await stream.Transport.Output.WriteAsync("Activity"u8.ToArray());
         await stream.Transport.Output.FlushAsync();
      }

      // Session should still be active
      await Assert.That(sSession.SessionClosedToken.IsCancellationRequested).IsFalse();

      // Stop activity and wait longer than timeout
      await Task.Delay(1000);
      await Assert.That(sSession.SessionClosedToken.IsCancellationRequested).IsTrue();

      await client.DisconnectAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task UdpClient_InvalidEndPoint_FailsGracefully()
   {
      var options = new UdpTransportOptions();
      var client = new UdpNetworkClient(options);

      // IPAddress.Any is not a valid remote endpoint to connect to
      var connectResult = await client.ConnectAsync(new IPEndPoint(IPAddress.Any, 0));

      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();
   }
}
