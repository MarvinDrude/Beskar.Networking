using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Models;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly ref struct TopicFilter(
   ReadOnlySequence<byte> topicUtf8Bytes,
   QualityOfServiceType qualityOfService)
{
   public readonly ReadOnlySequence<byte> TopicUtf8Bytes = topicUtf8Bytes;
   public readonly QualityOfServiceType QualityOfService = qualityOfService;

   public override string ToString()
   {
      return $"TopicFilter (QoS={QualityOfService})";
   }

   internal string DebuggerDisplay => ToString();
}
