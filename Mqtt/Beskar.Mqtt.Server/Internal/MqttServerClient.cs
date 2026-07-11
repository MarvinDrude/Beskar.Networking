using System.Diagnostics.CodeAnalysis;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Networking.Abstractions.Models;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// Represents a currently connected client to the MQTT server.
/// Do not confuse this with the Session associated with this client.
/// </summary>
public sealed class MqttServerClient : IPooledObject
{
   [MemberNotNullWhen(true,
      nameof(_connectOptions),
      nameof(Listener), nameof(_listener),
      nameof(Session), nameof(_session),
      nameof(Stream), nameof(_stream),
      nameof(_serverOptions))]
   public bool IsConnected => _connectOptions is not null;

   public INetworkListener Listener => _listener ?? throw new InvalidOperationException("Listener has not been initialized.");
   public INetworkSession Session => _session ?? throw new InvalidOperationException("Session has not been initialized.");
   public INetworkStream Stream => _stream ?? throw new InvalidOperationException("Stream has not been initialized.");

   private INetworkListener? _listener;
   private INetworkSession? _session;
   private INetworkStream? _stream;

   private ConnectOptions? _connectOptions;
   private MqttServerOptions? _serverOptions;

   private CancellationTokenSource? _cancellationTokenSource;
   private readonly Dictionary<ushort, string> _topicAliases = [with(16)];

   public void Initialize(
      NetworkServerStreamContext context,
      ConnectOptions connectOptions,
      MqttServerOptions serverOptions)
   {
      _listener = context.Connection.Listener;
      _session = context.Connection.Session;
      _stream = context.Stream;

      _connectOptions = connectOptions;
      _serverOptions = serverOptions;

      _cancellationTokenSource = new CancellationTokenSource();
   }

   public bool TryResetState()
   {
      _listener = null;
      _session = null;
      _stream = null;

      _connectOptions = null;

      _cancellationTokenSource?.Cancel();
      _cancellationTokenSource?.Dispose();
      _cancellationTokenSource = null;

      _topicAliases.Clear();

      return true;
   }
}
