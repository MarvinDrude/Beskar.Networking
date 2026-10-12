using System.Buffers;
using System.IO.Pipelines;
using Beskar.Networking.Transports.NamedPipes;

namespace Beskar.Networking.Transports.NamedPipes.Tests;

[NotInParallel]
public class NamedPipeLargePayloadTests
{
   private static string GetTempPipeName() => $"beskar-test-pipe-{Guid.NewGuid():N}";

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
   public async Task NamedPipe_SendLargePayloadGreaterThan4Kb_UsingGetSpan_ExchangesDataSuccessfully()
   {
      var pipeName = GetTempPipeName();
      var endPoint = new NamedPipeEndPoint(pipeName);
      var options = new NamedPipeTransportOptions();
      var listener = new NamedPipeNetworkListener(endPoint, options);
      await listener.BindAsync();

      var client = new NamedPipeNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
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
      var clientSpanLen = clientSpan.Length;
      clientPayload.CopyTo(clientSpan);
      clientStream.Transport.Output.Advance(clientPayloadSize);
      await Assert.That(clientSpanLen).IsGreaterThanOrEqualTo(clientPayloadSize);
      await clientStream.Transport.Output.FlushAsync();

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var serverReceived = await ReadExactAsync(serverStream.Transport.Input, clientPayloadSize, cts.Token);
      await Assert.That(serverReceived).IsEquivalentTo(clientPayload);

      // 2. Server writes 16KB (> 4KB) payload using GetSpan back to client
      const int serverPayloadSize = 16384;
      var serverPayload = new byte[serverPayloadSize];
      Random.Shared.NextBytes(serverPayload);

      var serverSpan = serverStream.Transport.Output.GetSpan(serverPayloadSize);
      var serverSpanLen = serverSpan.Length;
      serverPayload.CopyTo(serverSpan);
      serverStream.Transport.Output.Advance(serverPayloadSize);
      await Assert.That(serverSpanLen).IsGreaterThanOrEqualTo(serverPayloadSize);
      await serverStream.Transport.Output.FlushAsync();

      var clientReceived = await ReadExactAsync(clientStream.Transport.Input, serverPayloadSize, cts.Token);
      await Assert.That(clientReceived).IsEquivalentTo(serverPayload);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }
}
