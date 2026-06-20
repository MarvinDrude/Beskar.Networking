using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
public ref struct ConnectPacket
{
   public bool IsCleanSession;
   public ushort KeepAliveInterval;

   public ReadOnlySequence<byte> ClientIdUtf8Bytes;
}
