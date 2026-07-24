using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;

namespace Beskar.Networking.Resilient.Client;

/// <summary>
/// Configuration options for <see cref="ResilientClient{TFrame}"/>.
/// </summary>
public sealed class ResilientClientOptions
{
   /// <summary>
   /// Gets or sets the connect payload sent to the server during connection handshake.
   /// </summary>
   public ConnectPacketPayload ConnectPayload { get; set; } = new();

   /// <summary>
   /// Gets or sets the keep-alive options.
   /// </summary>
   public ResilientClientKeepAliveOptions KeepAlive { get; set; } = new();

   /// <summary>
   /// Gets or sets the reconnection options.
   /// </summary>
   public ResilientClientReconnectionOptions Reconnecting { get; set; } = new();

   /// <summary>
   /// Gets or sets whether the FrameReceived event should be fired for all incoming packets
   /// or only for application message packets (ResilientFrameKind.Message).
   /// Default is false (only message packets).
   /// </summary>
   public bool FrameReceivedAllPackets { get; set; } = false;

   /// <summary>
   /// Gets or sets an optional serializer interface for high-performance encoding/decoding of generic SendPayloadAsync payloads.
   /// </summary>
   public IResilientSerializer? Serializer { get; set; }
}
