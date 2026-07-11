using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession
{
   [MemberNotNullWhen(true,
      nameof(_listener),
      nameof(_session),
      nameof(_controlStream))]
   public bool IsConnected { get; internal set; }

   private INetworkListener? _listener;
   private INetworkSession? _session;
   private INetworkStream? _controlStream;

   private volatile bool _disposed;

   internal MqttSession(
      INetworkListener listener,
      INetworkSession session,
      INetworkStream controlStream)
   {
      _listener = listener;
      _session = session;
      _controlStream = controlStream;
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
