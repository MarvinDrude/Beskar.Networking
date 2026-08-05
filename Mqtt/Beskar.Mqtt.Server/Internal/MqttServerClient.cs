using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Extensions;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// Represents a currently connected client to the MQTT server.
/// Do not confuse this with the Session associated with this client.
/// </summary>
public sealed class MqttServerClient
{
   /// <summary>
   /// Gets a value indicating whether the client is currently connected to the server.
   /// </summary>
   [MemberNotNullWhen(true,
      nameof(Listener), nameof(_listener),
      nameof(Session), nameof(_session),
      nameof(Stream), nameof(_stream),
      nameof(_serverOptions), nameof(_controlPacketChannel))]
   public bool IsConnected => _session is not null && !_isDisconnecting;

   /// <summary>
   /// Gets the network listener that accepted the client connection.
   /// </summary>
   public INetworkListener Listener => _listener ?? throw new InvalidOperationException("Listener has not been initialized.");
   
   /// <summary>
   /// Gets the underlying network session for the client connection.
   /// </summary>
   public INetworkSession Session => _session ?? throw new InvalidOperationException("Session has not been initialized.");
   
   /// <summary>
   /// Gets the main network stream associated with the client.
   /// </summary>
   public INetworkStream Stream => _stream ?? throw new InvalidOperationException("Stream has not been initialized.");

   /// <summary>
   /// Gets the client identifier represented as a UTF-8 encoded byte array.
   /// </summary>
   public ReadOnlyMemory<byte> ClientIdUtf8Bytes => ConnectOptions?.ClientIdUtf8Bytes ?? ReadOnlyMemory<byte>.Empty;

   /// <summary>
   /// Gets a token that is canceled when the client connection is disconnected.
   /// </summary>
   public CancellationToken CancellationToken => _cancellationTokenSource?.Token ?? CancellationToken.None;
   
   /// <summary>
   /// Gets the MQTT protocol version used by the client.
   /// </summary>
   public MqttProtocolVersion ProtocolVersion { get; internal set; } = MqttProtocolVersion.Unknown;

   /// <summary>
   /// Gets the client disconnect options, populated if the client sent a DISCONNECT packet.
   /// </summary>
   public DisconnectOptions? DisconnectOptions { get; internal set; }

   /// <summary>
   /// Gets the server session state associated with this client.
   /// </summary>
   public MqttSession? MqttSession { get; internal set; }

   internal ConnectOptions? ConnectOptions { get; set; }

   private INetworkListener? _listener;
   private INetworkSession? _session;
   private INetworkStream? _stream;

   private MqttServerOptions? _serverOptions;

   private CancellationTokenSource? _cancellationTokenSource;
   private readonly Dictionary<ushort, byte[]> _topicAliases = new(16);
   private readonly Lock _topicAliasesLock = new();

   private Channel<IHeapMqttOptions>? _controlPacketChannel;
   private Channel<PublishPacket>? _outgoingPublishChannel;
   private Task? _outgoingPublishTask;

   private bool _isDisconnecting;

   /// <summary>
   /// Initializes the client instance with the specified network stream context and server options.
   /// </summary>
   /// <param name="context">The stream context for the accepted client connection.</param>
   /// <param name="serverOptions">The MQTT server options.</param>
   public void Initialize(
      NetworkServerStreamContext context,
      MqttServerOptions serverOptions)
   {
      _listener = context.Connection.Listener;
      _session = context.Connection.Session;
      _stream = context.Stream;

      _serverOptions = serverOptions;

      _cancellationTokenSource = new CancellationTokenSource();
      _controlPacketChannel = Channel.CreateUnbounded<IHeapMqttOptions>(new UnboundedChannelOptions()
      {
         SingleWriter = false,
         SingleReader = false,
      });

      var limit = serverOptions.MaxPendingMessagesPerConnection > 0
          ? serverOptions.MaxPendingMessagesPerConnection
          : 1024;

      _outgoingPublishChannel = Channel.CreateBounded<PublishPacket>(new BoundedChannelOptions(limit)
      {
         SingleWriter = false,
         SingleReader = true,
         FullMode = serverOptions.PendingMessageOverflowBehavior is MessageOverflowBehavior.DropNewest
            ? BoundedChannelFullMode.DropNewest
            : BoundedChannelFullMode.DropOldest
      });

      _outgoingPublishTask = Task.Run(ProcessOutgoingPublishesAsync, CancellationToken);
   }

   internal async Task DisconnectAsync(DisconnectOptions? options = null)
   {
      if (_isDisconnecting || !IsConnected) return;
      _isDisconnecting = true;

      if (options is not null)
      {
         DisconnectOptions = options;
      }

      if (ProtocolVersion is MqttProtocolVersion.V50
          && options is not null)
      {
         using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
         try
         {
            await _stream.Send(options, ProtocolVersion, ct: timeoutCts.Token);
         }
         catch (Exception ex)
         {
            TraceLogger.LogServerWarning("MqttServerClient: Failed to send disconnect packet: {0}", ex.Message);
         }
      }

      if (_cancellationTokenSource is not null)
      {
         await _cancellationTokenSource.CancelAsync();
      }
   }

   internal bool TryGetTopicAlias(ushort alias, [NotNullWhen(true)] out byte[]? topic)
   {
      lock (_topicAliasesLock)
      {
         return _topicAliases.TryGetValue(alias, out topic);
      }
   }

   internal void SetTopicAlias(ushort alias, byte[] topic)
   {
      lock (_topicAliasesLock)
      {
         _topicAliases[alias] = topic;
      }
   }

   internal void SetConnectOptions(ConnectOptions options)
      => ConnectOptions = options;

   internal async ValueTask<IHeapMqttOptions?> ReceiveControlPacketAsync(string hintName, CancellationToken ct = default)
   {
      if (!IsConnected)
         return null;

      try
      {
         if (_controlPacketChannel.Reader.TryRead(out var packet))
         {
            return packet;
         }

         using var timeoutToken = new CancellationTokenSource(_serverOptions.DefaultTimeout);
         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutToken.Token);

         return await _controlPacketChannel.Reader.ReadAsync(combinedCts.Token);
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogServerWarning("Timeout at control packet received for '{0}'.", hintName);
      }
      catch (Exception)
      {
         TraceLogger.LogServerWarning("Unexpected error at control packet received for '{0}'.", hintName);
      }

      TraceLogger.LogServerWarning("No control packet received for '{0}' but requested.", hintName);
      return null;
   }

   internal bool PushControlPacket(IHeapMqttOptions packet)
   {
      return _controlPacketChannel?.Writer.TryWrite(packet) ?? false;
   }

   internal void QueueOutgoingPublish(in PublishPacket packet)
   {
      _outgoingPublishChannel?.Writer.TryWrite(packet);
   }

   private async Task ProcessOutgoingPublishesAsync()
   {
      var channel = _outgoingPublishChannel;
      if (channel is null) return;

      var token = CancellationToken;
      try
      {
         var reader = channel.Reader;
         while (await reader.WaitToReadAsync(token))
         {
            while (reader.TryRead(out var packet))
            {
               if (_stream is null) break;
               await _stream.Send(in packet, ProtocolVersion, token);
            }
         }
      }
      catch (OperationCanceledException)
      {
         // Normal shutdown
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServerClient: Error in outgoing publish worker: {0}", ex.Message);
      }
   }
}
