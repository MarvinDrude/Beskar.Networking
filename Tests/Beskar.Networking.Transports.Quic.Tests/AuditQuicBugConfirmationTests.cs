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
}
