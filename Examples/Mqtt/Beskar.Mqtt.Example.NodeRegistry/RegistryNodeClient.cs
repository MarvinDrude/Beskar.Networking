using System.Buffers;
using System.Net;
using System.Text;
using System.Text.Json;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Example.NodeRegistry;

/// <summary>
/// Represents a cluster node that announces itself as a retained MQTT message and
/// configures an empty retained Last Will and Testament (LWT) so the broker
/// automatically removes the node from the registry if it crashes.
/// </summary>
public sealed class RegistryNodeClient : IAsyncDisposable
{
   private readonly IMqttClient _client;
   private readonly IPEndPoint _brokerEndPoint;
   private readonly string _nodeTopic;
   private NodeRegistration _registration;
   private bool _disposed;

   public string NodeId => _registration.NodeId;

   public RegistryNodeClient(
      IPEndPoint brokerEndPoint,
      string nodeId,
      string role,
      string endpoint,
      string version,
      params string[] capabilities)
   {
      _brokerEndPoint = brokerEndPoint ?? throw new ArgumentNullException(nameof(brokerEndPoint));
      _client = MqttClientFactory.CreateTcp();
      _nodeTopic = NodeRegistryTopics.ForNode(nodeId);

      var now = DateTimeOffset.UtcNow;
      _registration = new NodeRegistration(
         NodeId: nodeId,
         Role: role,
         Endpoint: endpoint,
         Version: version,
         Capabilities: capabilities,
         StartedAtUtc: now,
         LastUpdatedAtUtc: now);
   }

   /// <summary>
   /// Connects to the broker with a retained empty-payload Last Will on the node's
   /// registry topic, then publishes the node's retained JSON registration.
   /// </summary>
   public async Task AnnounceAsync(CancellationToken ct = default)
   {
      var connectOptions = new ConnectOptions
      {
         EndPoint = _brokerEndPoint,
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(NodeId),
         CleanSession = true,
         KeepAlivePeriod = 5,
         // Last Will & Testament: publishing an empty retained message tells the MQTT broker
         // to delete the retained node record AND notify all live subscribers of removal.
         HasWill = true,
         WillTopicUtf8Bytes = Encoding.UTF8.GetBytes(_nodeTopic),
         WillPayload = ReadOnlySequence<byte>.Empty,
         WillQualityOfService = QualityOfServiceType.AtLeastOnce,
         WillRetain = true
      };

      var connectResult = await _client.ConnectAsync(connectOptions, ct);
      if (connectResult.Failed)
      {
         throw new InvalidOperationException($"Node '{NodeId}' failed to connect: {connectResult.Error.Detail}");
      }

      await PublishAnnouncementAsync(_registration, ct);
   }

   /// <summary>
   /// Updates the node's retained announcement in place (for example load or capability changes).
   /// </summary>
   public async Task UpdateStatusAsync(int activeLoadPercent, CancellationToken ct = default)
   {
      _registration = _registration with
      {
         ActiveLoadPercent = activeLoadPercent,
         LastUpdatedAtUtc = DateTimeOffset.UtcNow
      };

      await PublishAnnouncementAsync(_registration, ct);
   }

   /// <summary>
   /// Gracefully clears the retained registration and sends an MQTT DISCONNECT packet
   /// so the broker discards the Last Will.
   /// </summary>
   public async Task DeregisterAndDisconnectAsync(CancellationToken ct = default)
   {
      if (_disposed) return;

      if (_client.IsConnected)
      {
         // Clear retained entry explicitly before sending DISCONNECT
         var clearOptions = PublishOptions.Create()
            .WithTopic(_nodeTopic)
            .WithPayload(Array.Empty<byte>())
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithRetain(true)
            .Build();

         var clearResult = await _client.PublishAsync(clearOptions, ct);
         if (clearResult.Failed)
         {
            throw new InvalidOperationException($"Node '{NodeId}' failed to clear retained registration: {clearResult.Error.Detail}");
         }

         await _client.DisconnectAsync(new DisconnectOptions(), ct);
      }

      await _client.DisposeAsync();
      _disposed = true;
   }

   /// <summary>
   /// Simulates an abrupt process crash by disposing the underlying transport without
   /// sending a DISCONNECT packet, causing the broker to trigger the Last Will.
   /// </summary>
   public async Task SimulateCrashAsync()
   {
      if (_disposed) return;
      await _client.DisposeAsync();
      _disposed = true;
   }

   public ValueTask DisposeAsync()
   {
      if (_disposed) return ValueTask.CompletedTask;
      _disposed = true;
      return _client.DisposeAsync();
   }

   private async Task PublishAnnouncementAsync(NodeRegistration registration, CancellationToken ct)
   {
      var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(registration, NodeRegistration.JsonOptions);

      var publishOptions = PublishOptions.Create()
         .WithTopic(_nodeTopic)
         .WithPayload(jsonBytes)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build();

      var publishResult = await _client.PublishAsync(publishOptions, ct);
      if (publishResult.Failed)
      {
         throw new InvalidOperationException($"Node '{NodeId}' failed to announce: {publishResult.Error.Detail}");
      }
   }
}
