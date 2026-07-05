using System.Threading;
using System.Threading.Tasks;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Common.Interfaces;

public interface IMqttPacketSender
{
   Task SendAsync<TPacket>(in TPacket packet, CancellationToken ct = default)
      where TPacket : struct, IRawMqttPacket;
}
