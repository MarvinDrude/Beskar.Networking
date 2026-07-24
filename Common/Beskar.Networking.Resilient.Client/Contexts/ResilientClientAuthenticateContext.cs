using System.Buffers;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Resilient.Client.Contexts;

/// <summary>
/// Context passed when the server issues an authentication challenge during connection handshake.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientAuthenticateContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient client receiving the challenge.
   /// </summary>
   public required ResilientClient<TFrame> Client { get; init; }

   /// <summary>
   /// The authentication challenge payload received from the server.
   /// </summary>
   public required AuthenticatePacketPayload ChallengePayload { get; init; }

   /// <summary>
   /// Cancellation token for the authentication process.
   /// </summary>
   public CancellationToken CancellationToken { get; init; }

   /// <summary>
   /// Sends an authentication response payload back to the server.
   /// </summary>
   public async ValueTask SendAuthenticateResponseAsync(AuthenticatePacketPayload responsePayload, CancellationToken ct = default)
   {
      using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, ct);
      var token = combinedCts.Token;

      var len = responsePayload.GetEncodedLength();
      using var writer = new PooledBufferWriter(len);
      if (responsePayload.TryWrite(writer.GetSpan(len), out var bytesWritten))
      {
         writer.Advance(bytesWritten);
      }

      var frame = TFrame.CreateFrame(ResilientFrameKind.Authenticate, new ReadOnlySequence<byte>(writer.WrittenMemory));
      await Client.SendAsync(frame, token);
   }
}
