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

   internal MqttSession(
      MqttServerClient serverClient)
   {
      _serverClient = serverClient;
      IsConnected = true;
   }

   public async Task DisconnectAsync(CancellationToken ct = default)
   {

   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      // nothing to do yet
   }
}
