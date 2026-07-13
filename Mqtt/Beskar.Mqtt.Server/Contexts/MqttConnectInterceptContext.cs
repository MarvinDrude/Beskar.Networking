using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Server.Internal;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttConnectInterceptContext(MqttServerClient client)
{
   public MqttServerClient Client { get; } = client;

   /// <summary>
   /// The CONNECT packet / options received from the client.
   /// </summary>
   public required ConnectOptions ConnectOptions { get; init; }

   /// <summary>
   /// The Network session associated with the client.
   /// </summary>
   public required INetworkSession NetworkSession { get; init; }

   /// <summary>
   /// Gets or sets the assigned client identifier as UTF-8 bytes.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> AssignedClientIdentifierUtf8Bytes { get; set; }

   /// <summary>
   /// Sets the assigned client identifier as string.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public void SetAssignedClientIdentifier(ReadOnlySpan<char> assignedClientIdentifier)
   {
      var length = Encoding.UTF8.GetByteCount(assignedClientIdentifier);
      var bytes = new byte[length];

      Encoding.UTF8.GetBytes(assignedClientIdentifier, bytes);
      AssignedClientIdentifierUtf8Bytes = bytes;
   }

   /// <summary>
   /// The Reason code to send to the client.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ConnectReasonCode ReasonCode { get; set; } = ConnectReasonCode.Success;

   /// <summary>
   /// The server reference to send back.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public string ServerReference { get; set; } = string.Empty;

   /// <summary>
   /// The user properties to send to the client.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public UserPropertyListBuilder ResponseUserProperties { get; } = new(128);

   /// <summary>
   /// The response authentication data to send to the client.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> ResponseAuthenticationData { get; set; }

   /// <summary>
   /// The reason string to send to the client.
   /// </summary>
   public string ReasonString { get; set; } = string.Empty;

   /// <summary>
   /// Cancellation token to use when processing the context.
   /// </summary>
   public required CancellationToken CancellationToken { get; init; }

   /// <summary>
   /// Wait for the next control packet from the client. For example AUTH
   /// </summary>
   public async Task<IHeapMqttOptions?> ReceiveControlPacketAsync(CancellationToken ct = default)
   {
      return await Client.ReceiveControlPacketAsync("UNKNOWN", ct);
   }
}
