using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Comparers;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttSessionRegistry : IAsyncDisposable
{
   private readonly Dictionary<byte[], MqttSession> _sessions = new(ByteArrayEqualityComparer.Instance);

   public MqttSession? GetSession(ReadOnlySpan<byte> clientId)
   {
      if (clientId.IsEmpty) return null;

      var alternateLookup = _sessions.GetAlternateLookup<ReadOnlySpan<byte>>();
      if (!alternateLookup.TryGetValue(clientId, out var session)) return null;

      if (session)

      return null;
   }

   public async Task ClearAsync()
   {
      await DisposeAsync();
   }

   public async ValueTask DisposeAsync()
   {
      foreach (var (_, value) in _sessions)
      {
         await value.DisposeAsync();
      }

      _sessions.Clear();
   }
}
