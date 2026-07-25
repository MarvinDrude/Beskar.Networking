using System.Buffers;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;
using Beskar.Networking.Resilient.Server.Models;

namespace Beskar.Networking.Resilient.Server.Contexts;

/// <summary>
/// Context passed during client connect intercept (OnConnect event).
/// Allows authenticating, inspecting connect details, or denying the connection.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientConnectContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The client performing the connection handshake.
   /// </summary>
   public required ResilientServerClient<TFrame> Client { get; init; }

   /// <summary>
   /// The connect payload supplied by the client.
   /// </summary>
   public required ConnectPacketPayload ConnectPayload { get; init; }

   /// <summary>
   /// Cancellation token for the connection process.
   /// </summary>
   public CancellationToken CancellationToken { get; init; }

   /// <summary>
   /// Gets or sets whether the connection is denied.
   /// </summary>
   public bool IsDenied { get; set; }

   /// <summary>
   /// Denies and rejects the connection request.
   /// </summary>
   public void Deny()
   {
      IsDenied = true;
   }

   /// <summary>
   /// Sends an authentication challenge packet to the client.
   /// </summary>
   public async ValueTask SendAuthenticateAsync(AuthenticatePacketPayload payload, CancellationToken ct = default)
   {
      var len = payload.GetEncodedLength();
      using var writer = new PooledBufferWriter(len);
      if (payload.TryWrite(writer.GetSpan(len), out var bytesWritten))
      {
         writer.Advance(bytesWritten);
      }

      var frame = TFrame.CreateFrame(ResilientFrameKind.Authenticate, new ReadOnlySequence<byte>(writer.WrittenMemory));
      using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, ct);
      await Client.SendAsync(frame, combinedCts.Token);
   }

   /// <summary>
   /// Receives an authentication response packet from the client backing payload channel.
   /// </summary>
   public async ValueTask<AuthenticatePacketPayload?> ReceiveAuthenticateAsync(CancellationToken ct = default)
   {
      using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken, ct);
      var token = combinedCts.Token;

      var reader = Client.ControlPayloadChannel.Reader;
      while (await reader.WaitToReadAsync(token))
      {
         while (reader.TryRead(out var payload))
         {
            if (payload is AuthenticatePacketPayload authPayload)
            {
               return authPayload;
            }
         }
      }

      return null;
   }
}
