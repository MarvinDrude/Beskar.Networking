using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession
{
   [MemberNotNullWhen(true,
      nameof(_serverClient))]
   public bool IsConnected { get; internal set; }

   private MqttServerClient? _serverClient;
   private volatile bool _disposed;

   private readonly MqttServer _server;

   public MqttServerClient? Client
   {
      get => _serverClient;
      internal set => _serverClient = value;
   }

   internal MqttSession(
      MqttServer server,
      MqttServerClient serverClient)
   {
      _server = server;
      _serverClient = serverClient;

      IsConnected = true;
   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      _server.SubscriptionRouter.UnsubscribeAll(this);
      _serverClient = null;
   }
}
