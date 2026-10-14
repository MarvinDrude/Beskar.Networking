using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Transports.Quic.Tests;

[NotInParallel]
public class AuditQuicBugConfirmationTests
{
   [Test]
   [Timeout(5000)]
   public async Task QuicNetworkSession_ShouldCancelSessionClosedToken_WhenRemoteServerDisposesSession(CancellationToken ct)
   {
      if (!QuicConnection.IsSupported)
         return;

      var options = new QuicTransportOptions
      {
         SslClientOptions = new SslClientAuthenticationOptions
         {
            ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
            RemoteCertificateValidationCallback = (_, _, _, _) => true
         }
      };

      await using var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindRes = await listener.BindAsync(ct);
      await Assert.That(bindRes.IsSuccess).IsTrue();

      await using var client = new QuicNetworkClient(options);
      var connectRes = await client.ConnectAsync(listener.LocalAddress, ct);
      await Assert.That(connectRes.IsSuccess).IsTrue();

      var acceptRes = await listener.AcceptSessionAsync(ct);
      await Assert.That(acceptRes.IsSuccess).IsTrue();

      await using var clientSession = connectRes.Success!;
      var serverSession = acceptRes.Success!;

      // Open a stream and exchange data so StreamConnection is active on the client
      var clientStreamRes = await clientSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
      await Assert.That(clientStreamRes.IsSuccess).IsTrue();
      await using var clientStream = clientStreamRes.Success!;

      await clientStream.Transport.Output.WriteAsync("ping"u8.ToArray(), ct);
      await clientStream.Transport.Output.FlushAsync(ct);

      var serverStreamRes = await serverSession.AcceptStreamAsync(ct);
      await Assert.That(serverStreamRes.IsSuccess).IsTrue();
      await using var serverStream = serverStreamRes.Success!;

      var readRes = await serverStream.Transport.Input.ReadAsync(ct);
      serverStream.Transport.Input.AdvanceTo(readRes.Buffer.End);

      var sessionClosedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      using var reg = clientSession.SessionClosedToken.Register(() => sessionClosedTcs.TrySetResult());

      // Remote server closes and disposes its QUIC session
      await serverSession.DisposeAsync();

      var completed = await Task.WhenAny(sessionClosedTcs.Task, Task.Delay(1500, ct)) == sessionClosedTcs.Task;

      // Expected: clientSession.SessionClosedToken must be canceled when the remote peer closes the QUIC connection.
      await Assert.That(completed).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task QuicNetworkSession_ClosingSingleStream_ShouldNotCloseEntireMultiplexedSession(CancellationToken ct)
   {
      if (!QuicConnection.IsSupported)
         return;

      var options = new QuicTransportOptions
      {
         SslClientOptions = new SslClientAuthenticationOptions
         {
            ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
            RemoteCertificateValidationCallback = (_, _, _, _) => true
         }
      };

      await using var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      var bindRes = await listener.BindAsync(ct);
      await Assert.That(bindRes.IsSuccess).IsTrue();

      await using var client = new QuicNetworkClient(options);
      var connectRes = await client.ConnectAsync(listener.LocalAddress, ct);
      await Assert.That(connectRes.IsSuccess).IsTrue();

      var acceptRes = await listener.AcceptSessionAsync(ct);
      await Assert.That(acceptRes.IsSuccess).IsTrue();

      await using var clientSession = connectRes.Success!;
      await using var serverSession = acceptRes.Success!;

      // Open Stream 1 and Stream 2 on the same multiplexed QUIC session
      var clientStream1Res = await clientSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
      await Assert.That(clientStream1Res.IsSuccess).IsTrue();
      var clientStream1 = clientStream1Res.Success!;

      await clientStream1.Transport.Output.WriteAsync("stream1"u8.ToArray(), ct);
      await clientStream1.Transport.Output.FlushAsync(ct);

      var serverStream1Res = await serverSession.AcceptStreamAsync(ct);
      await Assert.That(serverStream1Res.IsSuccess).IsTrue();
      var serverStream1 = serverStream1Res.Success!;

      var read1 = await serverStream1.Transport.Input.ReadAsync(ct);
      serverStream1.Transport.Input.AdvanceTo(read1.Buffer.End);

      var clientStream2Res = await clientSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
      await Assert.That(clientStream2Res.IsSuccess).IsTrue();
      await using var clientStream2 = clientStream2Res.Success!;

      await clientStream2.Transport.Output.WriteAsync("stream2"u8.ToArray(), ct);
      await clientStream2.Transport.Output.FlushAsync(ct);

      var serverStream2Res = await serverSession.AcceptStreamAsync(ct);
      await Assert.That(serverStream2Res.IsSuccess).IsTrue();
      await using var serverStream2 = serverStream2Res.Success!;

      var read2 = await serverStream2.Transport.Input.ReadAsync(ct);
      serverStream2.Transport.Input.AdvanceTo(read2.Buffer.End);

      // Close ONLY Stream 1 on the server side (sending graceful FIN on Stream 1)
      await serverStream1.DisposeAsync();

      // Wait for clientStream1's StreamConnection to observe EOF (read == 0)
      var eofRead = await clientStream1.Transport.Input.ReadAsync(ct);
      clientStream1.Transport.Input.AdvanceTo(eofRead.Buffer.End);
      await clientStream1.DisposeAsync();

      // Expected: Closing Stream 1 must NOT cancel the entire QuicNetworkSession or disconnect the client,
      // and opening a 3rd stream on the same session must still succeed.
      await Assert.That(clientSession.SessionClosedToken.IsCancellationRequested).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      var clientStream3Res = await clientSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
      await Assert.That(clientStream3Res.IsSuccess).IsTrue();
      await clientStream3Res.Success!.DisposeAsync();
   }
}

