using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Server.Extensions;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// Represents a currently connected client to the MQTT server.
/// Do not confuse this with the Session associated with this client.
/// </summary>
public sealed class MqttServerClient : IPooledObject
{
   [MemberNotNullWhen(true,
      nameof(Listener), nameof(_listener),
      nameof(Session), nameof(_session),
      nameof(Stream), nameof(_stream),
      nameof(_serverOptions), nameof(_controlPacketChannel))]
   public bool IsConnected => _session is not null && !_isDisconnecting;

   public INetworkListener Listener => _listener ?? throw new InvalidOperationException("Listener has not been initialized.");
   public INetworkSession Session => _session ?? throw new InvalidOperationException("Session has not been initialized.");
   public INetworkStream Stream => _stream ?? throw new InvalidOperationException("Stream has not been initialized.");

   public ReadOnlyMemory<byte> ClientIdUtf8Bytes => _connectOptions?.ClientIdUtf8Bytes ?? ReadOnlyMemory<byte>.Empty;

   public CancellationToken CancellationToken => _cancellationTokenSource?.Token ?? CancellationToken.None;
   public MqttProtocolVersion ProtocolVersion { get; internal set; } = MqttProtocolVersion.Unknown;

   public DisconnectOptions? DisconnectOptions { get; internal set; }

   public MqttSession? MqttSession { get; internal set; }

   private INetworkListener? _listener;
   private INetworkSession? _session;
   private INetworkStream? _stream;

   private ConnectOptions? _connectOptions;
   private MqttServerOptions? _serverOptions;

   private CancellationTokenSource? _cancellationTokenSource;
   private readonly Dictionary<ushort, byte[]> _topicAliases = new(16);

   private Channel<IHeapMqttOptions>? _controlPacketChannel;
   private bool _isDisconnecting;

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
   }

   internal async Task DisconnectAsync(DisconnectOptions? options = null)
   {
      if (_isDisconnecting || !IsConnected) return;
      _isDisconnecting = true;

      DisconnectOptions = options;

      if (ProtocolVersion is MqttProtocolVersion.V50
          && options is not null)
      {
         await _stream.Send(options, ProtocolVersion, ct: CancellationToken);
      }

      if (_cancellationTokenSource is not null)
      {
         await _cancellationTokenSource.CancelAsync();
      }
   }

   internal bool TryGetTopicAlias(ushort alias, [NotNullWhen(true)] out byte[]? topic)
   {
      return _topicAliases.TryGetValue(alias, out topic);
   }

   internal void SetTopicAlias(ushort alias, byte[] topic)
   {
      _topicAliases[alias] = topic;
   }

   internal void SetConnectOptions(ConnectOptions options)
      => _connectOptions = options;

   internal ValueTask<IHeapMqttOptions?> ReceiveControlPacketAsync(string hintName, CancellationToken ct = default)
   {
      if (!IsConnected)
         return ValueTask.FromResult<IHeapMqttOptions?>(null);

      try
      {
         if (_controlPacketChannel.Reader.TryRead(out var packet))
         {
            return ValueTask.FromResult<IHeapMqttOptions?>(packet);
         }

         using var timeoutToken = new CancellationTokenSource(_serverOptions.DefaultTimeout);
         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutToken.Token);

         return Awaited(combinedCts.Token);
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
      return ValueTask.FromResult<IHeapMqttOptions?>(null);

      async ValueTask<IHeapMqttOptions?> Awaited(CancellationToken innerCt)
      {
         try
         {
            return await _controlPacketChannel.Reader.ReadAsync(innerCt);
         }
         catch (OperationCanceledException)
         {
            TraceLogger.LogServerWarning("Timeout at control packet received for '{0}'.", hintName);
         }
         catch (Exception)
         {
            TraceLogger.LogServerWarning("Unexpected error at control packet received for '{0}'.", hintName);
         }

         return null;
      }
   }

   internal bool PushControlPacket(IHeapMqttOptions packet)
   {
      return _controlPacketChannel?.Writer.TryWrite(packet) ?? false;
   }

   public bool TryResetState()
   {
      _listener = null;
      _session = null;
      _stream = null;

      DisconnectOptions = null;
      MqttSession = null;
      _connectOptions = null;
      _isDisconnecting = false;

      _cancellationTokenSource?.Cancel();
      _cancellationTokenSource?.Dispose();
      _cancellationTokenSource = null;

      _topicAliases.Clear();
      ProtocolVersion = MqttProtocolVersion.Unknown;

      if (_controlPacketChannel is not null)
      {
         _controlPacketChannel.Writer.TryComplete();
         _controlPacketChannel = null;
      }

      return true;
   }
}
