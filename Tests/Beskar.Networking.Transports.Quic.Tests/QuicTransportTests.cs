using System.Buffers;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.Quic.Tests;

public class QuicTransportTests
{

   [Test]
   public async Task QuicClientServer_BidirectionalStream_DataExchangedSuccessfully()
   {
      if (!QuicConnection.IsSupported)
         // Skip the test if QUIC is not supported on the host platform
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
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
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

   [Test]
   public async Task QuicClientServer_VerifyTimeoutOptionsConfigured_Succeeds()
   {
      if (!QuicConnection.IsSupported || !QuicListener.IsSupported)
      {
         return;
      }

      var options = new QuicTransportOptions
      {
         IdleTimeout = TimeSpan.FromSeconds(5),
         HandshakeTimeout = TimeSpan.FromSeconds(3)
      };

      var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new QuicNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task QuicListener_BindUnbindBindUnbind_SuccessiveCallsWork()
   {
      if (!QuicConnection.IsSupported || !QuicListener.IsSupported)
      {
         return;
      }

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
   public async Task QuicListener_UnbindWithActiveClient_CleanlyDisconnectsClient()
   {
      if (!QuicConnection.IsSupported || !QuicListener.IsSupported)
      {
         return;
      }

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
      await listener.BindAsync();

      var client = new QuicNetworkClient(options);
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
   public async Task QuicClientServer_MultipleClients_DataExchangedWithoutLeakage()
   {
      if (!QuicConnection.IsSupported || !QuicListener.IsSupported)
      {
         return;
      }

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

      var client1 = new QuicNetworkClient(options);
      var client2 = new QuicNetworkClient(options);

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
   public async Task QuicClientServer_WithMeterListener_TracksConnectionsStreamsAndBytes()
   {
      if (!QuicConnection.IsSupported || !QuicListener.IsSupported)
      {
         return;
      }

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
      await listener.BindAsync();

      var initialOpened = Volatile.Read(ref recordedConnectionsOpened);
      var initialClosed = Volatile.Read(ref recordedConnectionsClosed);
      var initialActive = Volatile.Read(ref recordedConnectionsActiveDelta);
      var initialStreamsActive = Volatile.Read(ref recordedStreamsActiveDelta);

      var client = new QuicNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var openedDelta = Volatile.Read(ref recordedConnectionsOpened) - initialOpened;
      await Assert.That(openedDelta).IsGreaterThanOrEqualTo(2);

      var activeDuringConnection = Volatile.Read(ref recordedConnectionsActiveDelta) - initialActive;
      await Assert.That(activeDuringConnection).IsGreaterThanOrEqualTo(2);

      var clientStream = (await clientSession.OpenStreamAsync()).Success!;
      var payload = "QUIC Telemetry Payload"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      var streamsActiveDelta = Volatile.Read(ref recordedStreamsActiveDelta) - initialStreamsActive;
      await Assert.That(streamsActiveDelta).IsGreaterThanOrEqualTo(2);

      var readResult = await serverStream.Transport.Input.ReadAsync();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(recordedBytesSent).IsGreaterThanOrEqualTo(payload.Length);
      await Assert.That(recordedBytesReceived).IsGreaterThanOrEqualTo(payload.Length);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();

      var closedDelta = Volatile.Read(ref recordedConnectionsClosed) - initialClosed;
      await Assert.That(closedDelta).IsGreaterThanOrEqualTo(2);
   }
}
