using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct ConnectPacket
{
   public bool IsCleanSession;
   public ushort KeepAliveInterval;

   public ReadOnlySequence<byte> ClientIdUtf8Bytes;

   public bool HasWill;
   public QualityOfServiceType WillQualityOfService;
   public bool WillRetain;

   public ReadOnlySequence<byte> WillTopicUtf8Bytes;
   public ReadOnlySequence<byte> WillMessageBytes;

   public ReadOnlySequence<byte> UsernameUtf8Bytes;
   public ReadOnlySequence<byte> PasswordBytes;

   public override string ToString()
   {
      return "CONNECT";
   }

   internal string DebuggerDisplay => ToString();
}
