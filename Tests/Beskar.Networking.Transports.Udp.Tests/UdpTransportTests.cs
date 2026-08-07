using System.Buffers;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Transports.Udp;
using Beskar.Networking.Transports.Udp.Extensions;

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

      // Resolve non-deterministic UDP packet arrival order by matching remote ports
      var clientAPort = ((IPEndPoint)sessionA.LocalAddress).Port;
      INetworkSession activeServerSession;
      INetworkSession idleServerSession;

      if (((IPEndPoint)sSessionA.RemoteAddress).Port == clientAPort)
      {
         activeServerSession = sSessionA;
         idleServerSession = sSessionB;
      }
      else
      {
         activeServerSession = sSessionB;
         idleServerSession = sSessionA;
      }

      // Keep Client A active, Client B idle
      for (var i = 0; i < 5; i++)
      {
         await Task.Delay(200);
         await streamA.Transport.Output.WriteAsync("KeepAlive"u8.ToArray());
         await streamA.Transport.Output.FlushAsync();
      }

      // Wait for client B's session to be cancelled by the cleanup task
      var timedOutB = false;
      for (var i = 0; i < 30; i++)
      {
         if (idleServerSession.SessionClosedToken.IsCancellationRequested)
         {
            timedOutB = true;
            break;
         }
         await Task.Delay(100);
      }
      await Assert.That(timedOutB).IsTrue();
      await Assert.That(activeServerSession.SessionClosedToken.IsCancellationRequested).IsFalse();

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

   [Test]
   public async Task UdpClientServer_MultipleClients_DataExchangedWithoutLeakage()
   {
      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client1 = new UdpNetworkClient(options);
      var client2 = new UdpNetworkClient(options);

      var connectResult1 = await client1.ConnectAsync(listener.LocalAddress);
      var connectResult2 = await client2.ConnectAsync(listener.LocalAddress);

      await Assert.That(connectResult1.Failed).IsFalse();
      await Assert.That(connectResult2.Failed).IsFalse();

      var clientSession1 = connectResult1.Success!;
      var clientSession2 = connectResult2.Success!;

      var clientStream1 = (await clientSession1.OpenStreamAsync()).Success!;
      var clientStream2 = (await clientSession2.OpenStreamAsync()).Success!;

      var identity1 = "Client1-Identity"u8.ToArray();
      await clientStream1.Transport.Output.WriteAsync(identity1);
      await clientStream1.Transport.Output.FlushAsync();

      var identity2 = "Client2-Identity"u8.ToArray();
      await clientStream2.Transport.Output.WriteAsync(identity2);
      await clientStream2.Transport.Output.FlushAsync();

      var acceptResult1 = await listener.AcceptSessionAsync();
      var acceptResult2 = await listener.AcceptSessionAsync();

      await Assert.That(acceptResult1.Failed).IsFalse();
      await Assert.That(acceptResult2.Failed).IsFalse();

      var serverSession1 = acceptResult1.Success!;
      var serverSession2 = acceptResult2.Success!;

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
   public async Task UdpClientServer_WithMeterListener_TracksConnectionsStreamsAndBytes()
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

      var options = new UdpTransportOptions();
      var listener = new UdpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var initialOpened = Volatile.Read(ref recordedConnectionsOpened);
      var initialClosed = Volatile.Read(ref recordedConnectionsClosed);
      var initialActive = Volatile.Read(ref recordedConnectionsActiveDelta);
      var initialStreamsActive = Volatile.Read(ref recordedStreamsActiveDelta);

      var client = new UdpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var clientStream = (await clientSession.OpenStreamAsync()).Success!;

      var payload = "UDP Telemetry Payload"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();
      var serverSession = acceptResult.Success!;

      var openedDelta = Volatile.Read(ref recordedConnectionsOpened) - initialOpened;
      await Assert.That(openedDelta).IsGreaterThanOrEqualTo(1);

      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

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

   [Test]
   public async Task UdpExtensions_RegisterCorrectly()
   {
      // Test ServerBuilder extension
      var builder = new MockServerBuilder();
      builder.UseUdp(12345);

      await Assert.That(builder.Listener).IsNotNull();
      await Assert.That(builder.Listener!.Transport).IsEqualTo(TransportKind.Udp);

      // Test ClientFactory extension
      var client = MockClientFactory.UseUdp<MockClientFactory, INetworkClient>();
      await Assert.That(client).IsNotNull();
      await Assert.That(client.Transport).IsEqualTo(TransportKind.Udp);
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
