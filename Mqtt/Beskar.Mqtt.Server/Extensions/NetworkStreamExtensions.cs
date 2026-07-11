using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server.Extensions;

internal static class NetworkStreamExtensions
{
   extension(INetworkStream stream)
   {
      internal Task Send<TPacket>(in TPacket packet, CancellationToken ct = default)
         where TPacket : IRawMqttPacket
      {
         throw new NotImplementedException();
      }

      internal Task Send<TOptions>(TOptions options, CancellationToken ct = default)
         where TOptions : class, IHeapMqttOptions
      {
         throw new NotImplementedException();
      }
   }
}
