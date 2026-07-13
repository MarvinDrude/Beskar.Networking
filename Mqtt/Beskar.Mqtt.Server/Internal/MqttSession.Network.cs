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

   public MqttServerClient? Client
   {
      get => _serverClient;
      internal set => _serverClient = value;
   }

   public byte[] ClientIdUtf8Bytes { get; }

   internal MqttSession(
      MqttServer server,
      MqttServerClient? serverClient)
   {
      Server = server;
      _serverClient = serverClient;

      ClientIdUtf8Bytes = serverClient is not null
         ? serverClient.ClientIdUtf8Bytes.ToArray()
         : [];

      IsConnected = serverClient is not null;
   }

   public ValueTask DisposeAsync()
   {
      try
      {
         if (_disposed) return ValueTask.CompletedTask;
         _disposed = true;

         Server.SubscriptionRouter.UnsubscribeAll(this);

         lock (_incomingQos2Packets)
         {
            _incomingQos2Packets.Clear();
         }
         lock (_offlineQueue)
         {
            _offlineQueue.Clear();
         }

         _serverClient = null;

         return ValueTask.CompletedTask;
      }
      catch (Exception exception)
      {
         return ValueTask.FromException(exception);
      }
   }
}
