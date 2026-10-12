using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;

namespace Beskar.Networking.Transports.Tcp.Tests;

[NotInParallel]
public class TcpLargePayloadTests
{
   private static async Task<byte[]> ReadExactAsync(PipeReader reader, int expectedLength, CancellationToken ct = default)
   {
      var received = new byte[expectedLength];
      var totalRead = 0;
      while (totalRead < expectedLength)
      {
         var result = await reader.ReadAsync(ct);
         var buffer = result.Buffer;
         var remaining = expectedLength - totalRead;
         var toCopy = (int)Math.Min(remaining, buffer.Length);
         buffer.Slice(0, toCopy).CopyTo(received.AsSpan(totalRead));
         totalRead += toCopy;
         reader.AdvanceTo(buffer.GetPosition(toCopy));
         if (result.IsCompleted && totalRead < expectedLength)
         {
            throw new EndOfStreamException($"Expected {expectedLength} bytes, but only received {totalRead} before stream completed.");
         }
      }
      return received;
   }

   [Test]
   public async Task Tcp_SendLargePayloadGreaterThan4Kb_UsingGetSpan_ExchangesDataSuccessfully()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStream = (await clientSession.OpenStreamAsync()).Success!;
      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      // 1. Client writes 8KB (> 4KB) payload using GetSpan
      const int clientPayloadSize = 8192;
      var clientPayload = new byte[clientPayloadSize];
      Random.Shared.NextBytes(clientPayload);

      var clientSpan = clientStream.Transport.Output.GetSpan(clientPayloadSize);
      await Assert.That(clientSpan.Length).IsGreaterThanOrEqualTo(clientPayloadSize);
      clientPayload.CopyTo(clientSpan);
      clientStream.Transport.Output.Advance(clientPayloadSize);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads exact 8KB
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var serverReceived = await ReadExactAsync(serverStream.Transport.Input, clientPayloadSize, cts.Token);
      await Assert.That(serverReceived).IsEquivalentTo(clientPayload);

      // 2. Server writes 16KB (> 4KB) payload using GetSpan back to client
      const int serverPayloadSize = 16384;
      var serverPayload = new byte[serverPayloadSize];
      Random.Shared.NextBytes(serverPayload);

      var serverSpan = serverStream.Transport.Output.GetSpan(serverPayloadSize);
      await Assert.That(serverSpan.Length).IsGreaterThanOrEqualTo(serverPayloadSize);
      serverPayload.CopyTo(serverSpan);
      serverStream.Transport.Output.Advance(serverPayloadSize);
      await serverStream.Transport.Output.FlushAsync();

      // Client reads exact 16KB
      var clientReceived = await ReadExactAsync(clientStream.Transport.Input, serverPayloadSize, cts.Token);
      await Assert.That(clientReceived).IsEquivalentTo(serverPayload);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task Tcp_MultipleGetSpansBeforeFlush_GreaterThan4Kb_ExchangesDataSuccessfully()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      var acceptResult = await listener.AcceptSessionAsync();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStream = (await clientSession.OpenStreamAsync()).Success!;
      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      // First chunk: 2KB
      const int chunk1Size = 2048;
      var chunk1 = new byte[chunk1Size];
      Random.Shared.NextBytes(chunk1);
      var span1 = clientStream.Transport.Output.GetSpan(chunk1Size);
      chunk1.CopyTo(span1);
      clientStream.Transport.Output.Advance(chunk1Size);

      // Second chunk: 8KB (> 4KB), forcing extra block allocation before flushing
      const int chunk2Size = 8192;
      var chunk2 = new byte[chunk2Size];
      Random.Shared.NextBytes(chunk2);
      var span2 = clientStream.Transport.Output.GetSpan(chunk2Size);
      await Assert.That(span2.Length).IsGreaterThanOrEqualTo(chunk2Size);
      chunk2.CopyTo(span2);
      clientStream.Transport.Output.Advance(chunk2Size);

      // Single flush for both chunks
      await clientStream.Transport.Output.FlushAsync();

      var expectedCombined = new byte[chunk1Size + chunk2Size];
      chunk1.CopyTo(expectedCombined.AsSpan(0));
      chunk2.CopyTo(expectedCombined.AsSpan(chunk1Size));

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var receivedCombined = await ReadExactAsync(serverStream.Transport.Input, expectedCombined.Length, cts.Token);
      await Assert.That(receivedCombined).IsEquivalentTo(expectedCombined);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task Tcp_Send64KbPayload_UsingGetSpan_ExchangesDataSuccessfully()
   {
      var options = new TcpTransportOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      var acceptResult = await listener.AcceptSessionAsync();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStream = (await clientSession.OpenStreamAsync()).Success!;
      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      const int payloadSize = 65536;
      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      var span = clientStream.Transport.Output.GetSpan(payloadSize);
      await Assert.That(span.Length).IsGreaterThanOrEqualTo(payloadSize);
      payload.CopyTo(span);
      clientStream.Transport.Output.Advance(payloadSize);
      await clientStream.Transport.Output.FlushAsync();

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var received = await ReadExactAsync(serverStream.Transport.Input, payloadSize, cts.Token);
      await Assert.That(received).IsEquivalentTo(payload);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task Tcp_SslStream_SendLargePayloadGreaterThan4Kb_UsingGetSpan_ExchangesDataSuccessfully()
   {
      using var certificate = CertificateUtility.GenerateSelfSignedCertificate();
      var options = new TcpTransportOptions
      {
         UseSsl = true,
         SslServerOptions = new SslServerAuthenticationOptions
         {
            ServerCertificate = certificate,
            ClientCertificateRequired = false
         },
         SslClientOptions = new SslClientAuthenticationOptions
         {
            TargetHost = "localhost",
            RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
         }
      };

      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), options);
      await listener.BindAsync();

      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStream = (await clientSession.OpenStreamAsync()).Success!;
      var serverStream = (await serverSession.AcceptStreamAsync()).Success!;

      const int payloadSize = 8192;
      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      var span = clientStream.Transport.Output.GetSpan(payloadSize);
      await Assert.That(span.Length).IsGreaterThanOrEqualTo(payloadSize);
      payload.CopyTo(span);
      clientStream.Transport.Output.Advance(payloadSize);
      await clientStream.Transport.Output.FlushAsync();

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var received = await ReadExactAsync(serverStream.Transport.Input, payloadSize, cts.Token);
      await Assert.That(received).IsEquivalentTo(payload);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }
}
